"""
evals/run_evals.py
Batch evaluation runner — runs all golden set SQL files through the full pipeline
and computes a pass rate. Saves results to evals/scores.json.

Usage: python evals/run_evals.py
"""

import os
import json
import logging
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.graph import build_graph
from core.state import PipelineContext
from config import Config

logging.basicConfig(level=logging.WARNING)  # Suppress info logs during batch run

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden_set")
SCORES_PATH = os.path.join(os.path.dirname(__file__), "scores.json")


def run_evals(target_dialect: str = "snowflake"):
    """Run all files in golden_set/ and compute pass rate."""
    if not os.path.exists(GOLDEN_DIR):
        print(f"❌ Golden set directory not found: {GOLDEN_DIR}")
        print("   Copy your legacy SQL files to evals/golden_set/")
        return

    files = sorted([f for f in os.listdir(GOLDEN_DIR) if f.endswith((".sql", ".py"))])
    if not files:
        print("❌ No .sql files found in evals/golden_set/")
        return

    print(f"\n🔍 Running eval suite: {len(files)} files → target={target_dialect}")
    print("─" * 60)

    results = []
    graph = build_graph()

    for fname in files:
        path = os.path.join(GOLDEN_DIR, fname)
        with open(path) as f:
            code = f.read()

        state = PipelineContext(
            raw_legacy_code=code,
            source_dialect=Config.SOURCE_DIALECT,
            target_dialect=target_dialect,
        )

        try:
            final = graph.invoke(state)
            passed = bool(
                final.validation_result.passed
                if final.validation_result
                else final.rewritten_sql
            )
            retries = final.retry_count
            error = final.error_trace
        except Exception as e:
            passed = False
            retries = -1
            error = str(e)

        icon = "✅" if passed else "❌"
        print(f"{icon} {fname:<40} retries={retries}")
        if error and not passed:
            print(f"   ⚠ {error[:100]}")

        results.append({
            "file": fname,
            "passed": passed,
            "retries": retries,
            "error": error if not passed else None,
        })

    total = len(results)
    passed_count = sum(1 for r in results if r["passed"])
    pass_rate = (passed_count / total * 100) if total > 0 else 0

    print("─" * 60)
    print(f"\n📊 Pass Rate: {pass_rate:.1f}% ({passed_count}/{total} passed)")
    print(f"💾 Report saved to: {SCORES_PATH}")

    with open(SCORES_PATH, "w") as f:
        json.dump(
            {"pass_rate": round(pass_rate, 1), "total": total, "passed": passed_count, "results": results},
            f,
            indent=2,
        )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="snowflake",
                        choices=["snowflake", "bigquery", "databricks", "redshift"])
    args = parser.parse_args()
    run_evals(args.target)
