"""
core/tools.py
sqlglot-based SQL parsing and transpilation utilities.
All LLM agents use these tools as deterministic preprocessing steps.
"""

import logging
import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import ParseError

logger = logging.getLogger(__name__)


def parse_sql_ast(sql: str, dialect: str = "oracle") -> dict:
    """
    Parse legacy SQL and extract structural metadata via the sqlglot AST.
    Falls back gracefully if the parse fails (e.g., PL/SQL procedural blocks).
    """
    try:
        parsed = sqlglot.parse_one(sql, read=dialect, error_level=sqlglot.ErrorLevel.WARN)
    except ParseError as e:
        logger.warning(f"sqlglot parse failed for dialect={dialect}: {e}")
        return {
            "parse_error": str(e),
            "fallback": True,
            "tables": [],
            "columns": [],
            "aggregations": [],
            "joins": [],
            "filters": [],
        }

    tables = list({t.name for t in parsed.find_all(exp.Table) if t.name})
    columns = list({c.name for c in parsed.find_all(exp.Column) if c.name})
    aggregations = [str(a) for a in parsed.find_all(exp.AggFunc)]
    joins = [str(j.kind) for j in parsed.find_all(exp.Join)]
    filters = [str(w) for w in parsed.find_all(exp.Where)]
    ctes = [c.alias for c in parsed.find_all(exp.CTE)]
    subqueries = len(list(parsed.find_all(exp.Subquery)))

    return {
        "tables": tables,
        "columns": columns,
        "aggregations": aggregations[:10],  # cap for prompt size
        "joins": joins,
        "filters": [f[:200] for f in filters],  # truncate long filters
        "ctes": ctes,
        "subquery_count": subqueries,
        "fallback": False,
    }


def transpile_sql(sql: str, source: str, target: str) -> str:
    """
    Mechanically translate SQL from one dialect to another via sqlglot.
    This is the deterministic first pass before LLM refinement.
    Returns an error string prefixed with TRANSPILE_ERROR if it fails.
    """
    try:
        results = sqlglot.transpile(
            sql,
            read=source,
            write=target,
            pretty=True,
            error_level=sqlglot.ErrorLevel.WARN,
        )
        return results[0] if results else ""
    except ParseError as e:
        logger.warning(f"Transpile failed ({source}→{target}): {e}")
        return f"TRANSPILE_ERROR: {str(e)}\n\nOriginal SQL:\n{sql}"


def validate_sql_syntax(sql: str, dialect: str = "snowflake") -> tuple[bool, str]:
    """
    Check if SQL is syntactically valid for the given dialect using sqlglot.
    Returns (is_valid, error_message).
    Use this to gatekeep LLM output before sending to DuckDB.
    """
    # Strip TRANSPILE_ERROR prefix if present
    if sql.startswith("TRANSPILE_ERROR"):
        return False, sql

    try:
        sqlglot.parse_one(sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE)
        return True, ""
    except ParseError as e:
        return False, str(e)
