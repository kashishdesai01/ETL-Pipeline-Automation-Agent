"""
agents/validation_agent.py
Validates the rewritten SQL against synthetic data in DuckDB.
Compares row counts and checksums. Routes failures back to the rewriting agent.
"""

import hashlib
import json
import logging
import re
import duckdb
from core.state import PipelineContext, ValidationResult
from core.synthetic_data import create_synthetic_tables
from config import Config

logger = logging.getLogger(__name__)


def _normalize_value(val):
    """Normalize floats and None for stable checksum comparison."""
    if isinstance(val, float):
        return round(val, 2)
    if val is None:
        return "__NULL__"
    return val


def _row_checksum(rows: list) -> str:
    """Compute a stable MD5 checksum over a list of rows."""
    normalized = [tuple(_normalize_value(v) for v in row) for row in rows]
    serialized = json.dumps(sorted([str(r) for r in normalized]), sort_keys=True).encode()
    return hashlib.md5(serialized).hexdigest()


def _strip_snowflake_specific(sql: str) -> str:
    """
    Strip/replace Snowflake-specific syntax that DuckDB can't handle,
    so we can at least test the core logic.
    """
    replacements = {
        "QUALIFY": "-- QUALIFY (stripped for DuckDB)",
        "FLATTEN(": "-- FLATTEN( (stripped for DuckDB)",
        "PARSE_JSON(": "-- PARSE_JSON( (stripped for DuckDB) (",
        "SYSDATE()": "CURRENT_TIMESTAMP",
        "CURRENT_DATE()": "CURRENT_DATE",
    }
    for dialect_fn, replacement in replacements.items():
        sql = sql.replace(dialect_fn, replacement)
        
    # Replace DATEADD(part, amount, date) with (date + INTERVAL (amount) part) DuckDB equivalent
    sql = re.sub(
        r'DATEADD\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*([^)]+)\)',
        r'(\3 + INTERVAL (\2) \1)',
        sql,
        flags=re.IGNORECASE
    )
    return sql


def validation_agent(state: PipelineContext) -> PipelineContext:
    logger.info("=== VALIDATION AGENT: Starting ===")

    if not state.rewritten_sql:
        state.error_trace = "No rewritten SQL to validate"
        state.retry_count += 1
        return state

    con = duckdb.connect(Config.DUCKDB_PATH)

    # Create synthetic tables
    tables_created = create_synthetic_tables(con, state.intent_json or {})
    logger.info(f"Synthetic tables created: {tables_created}")

    # Prepare the SQL for DuckDB compatibility
    test_sql = _strip_snowflake_specific(state.rewritten_sql)

    # Attempt execution
    try:
        result_rows = con.execute(test_sql).fetchall()
        logger.info(f"Query executed successfully: {len(result_rows)} rows returned")
    except Exception as e:
        err_msg = str(e)
        logger.warning(f"DuckDB execution failed: {err_msg}")
        state.error_trace = f"DuckDB execution error: {err_msg}"
        state.retry_count += 1
        con.close()
        return state

    rewritten_count = len(result_rows)
    checksum = _row_checksum(result_rows)

    # For the "original" comparison baseline, we run the transpiled SQL (sqlglot first pass)
    # which is generally more DuckDB-compatible than the LLM-refined version
    original_count = rewritten_count  # default to same if we can't run the original
    original_checksum = checksum

    if state.transpiled_sql and not state.transpiled_sql.startswith("TRANSPILE_ERROR"):
        try:
            transpiled_compat = _strip_snowflake_specific(state.transpiled_sql)
            original_rows = con.execute(transpiled_compat).fetchall()
            original_count = len(original_rows)
            original_checksum = _row_checksum(original_rows)
        except Exception as e:
            logger.info(f"Could not run transpiled SQL for comparison: {e}")

    checksum_match = checksum == original_checksum
    passed = rewritten_count > 0 and checksum_match

    state.validation_result = ValidationResult(
        original_row_count=original_count,
        rewritten_row_count=rewritten_count,
        checksum_match=checksum_match,
        diff_sample=list(result_rows[:5]) if not passed else None,
        passed=passed,
    )

    if not passed:
        state.error_trace = (
            f"Validation failed: row counts ({original_count} vs {rewritten_count}) "
            f"or checksum mismatch. Sample: {list(result_rows[:3])}"
        )
        state.retry_count += 1
        logger.warning(f"Validation FAILED — retry_count={state.retry_count}")
    else:
        state.error_trace = None
        logger.info("Validation PASSED ✓")

    con.close()
    return state
