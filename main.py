"""
main.py — CLI entry point for a single migration run.
Usage: python main.py legacy_samples/oracle_001.sql [--target snowflake]
"""

import argparse
import json
import logging

from core.graph import build_graph
from core.state import PipelineContext
from config import Config, TargetDialect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_migration(sql_path: str, target_dialect: str = "snowflake") -> dict:
    """Run the full migration pipeline on a legacy SQL file."""
    with open(sql_path, "r") as f:
        legacy_code = f.read()

    Config.TARGET_DIALECT = TargetDialect(target_dialect)

    initial_state = PipelineContext(
        raw_legacy_code=legacy_code,
        source_dialect=Config.SOURCE_DIALECT,
        target_dialect=Config.TARGET_DIALECT.value,
    )

    logger.info(f"Starting migration: {sql_path} → {target_dialect}")
    graph = build_graph()
    raw_final = graph.invoke(initial_state)
    final = PipelineContext(**raw_final) if isinstance(raw_final, dict) else raw_final

    report = {
        "source_file": sql_path,
        "target_dialect": target_dialect,
        "intent": final.intent_json,
        "migration_plan": [s.model_dump() for s in final.migration_plan],
        "rewritten_sql": final.rewritten_sql,
        "validation": final.validation_result.model_dump() if final.validation_result else None,
        "retry_count": final.retry_count,
        "success": (
            final.validation_result.passed
            if final.validation_result
            else final.rewritten_sql is not None
        ),
    }

    print("\n" + "=" * 60)
    print("MIGRATION COMPLETE")
    print("=" * 60)
    if report["success"]:
        print("✅ Status: SUCCESS")
    else:
        print("⚠️  Status: PARTIAL (validation failed or max retries hit)")
        if final.error_trace:
            print(f"   Error: {final.error_trace}")

    print(f"\n📋 Intent: {final.intent_json.get('summary', 'N/A') if final.intent_json else 'N/A'}")
    print(f"🔄 Retries: {final.retry_count}")
    if final.validation_result:
        v = final.validation_result
        print(f"📊 Validation: {v.original_row_count} → {v.rewritten_row_count} rows | checksum={'✓' if v.checksum_match else '✗'}")

    print("\n--- REWRITTEN SQL ---")
    print(final.rewritten_sql or "No output")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Migration Agent")
    parser.add_argument("sql_file", help="Path to legacy SQL file")
    parser.add_argument(
        "--target",
        default="snowflake",
        choices=["snowflake", "bigquery", "databricks", "redshift"],
        help="Target SQL dialect",
    )
    parser.add_argument("--output", help="Optional path to save JSON report")
    args = parser.parse_args()

    report = run_migration(args.sql_file, args.target)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Report saved to {args.output}")
