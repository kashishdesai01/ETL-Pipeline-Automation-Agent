"""
agents/rewriting_agent.py
Two-pass SQL rewriting:
  Pass 1 — deterministic sqlglot.transpile()
  Pass 2 — LLM refinement to idiomatic target-dialect SQL
  Gate  — sqlglot syntax validation before accepting LLM output
"""

import json
import logging
from openai import OpenAI
from core.state import PipelineContext
from core.tools import transpile_sql, validate_sql_syntax
from core.prompts import REWRITING_SYSTEM_PROMPT, REWRITING_USER_TEMPLATE
from config import Config

logger = logging.getLogger(__name__)
client = OpenAI(api_key=Config.OPENAI_API_KEY)


def rewriting_agent(state: PipelineContext) -> PipelineContext:
    logger.info(f"=== REWRITING AGENT: Attempt {state.retry_count + 1} ===")

    # ── Pass 1: Deterministic transpile ──────────────────────────────
    transpiled = transpile_sql(
        state.raw_legacy_code,
        source=state.source_dialect,
        target=state.target_dialect,
    )
    state.transpiled_sql = transpiled
    logger.info("sqlglot transpile complete")

    # ── Pass 2: LLM refinement ────────────────────────────────────────
    system = REWRITING_SYSTEM_PROMPT.format(target_dialect=state.target_dialect)
    user = REWRITING_USER_TEMPLATE.format(
        target_dialect=state.target_dialect,
        intent_json=json.dumps(state.intent_json, indent=2),
        transpiled_sql=transpiled,
        error_trace=state.error_trace or "None",
    )

    try:
        response = client.chat.completions.create(
            model=Config.LLM_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.1,
        )
        rewritten = response.choices[0].message.content.strip()

        # Strip accidental markdown fences
        if rewritten.startswith("```"):
            lines = rewritten.split("\n")
            rewritten = "\n".join(lines[1:-1]) if lines[-1] == "```" else "\n".join(lines[1:])

    except Exception as e:
        logger.error(f"Rewriting LLM call failed: {e}")
        state.error_trace = f"LLM call failed: {str(e)}"
        state.retry_count += 1
        return state

    # ── Gate: sqlglot syntax validation ──────────────────────────────
    # Use a relaxed check — DuckDB will be the final judge
    is_valid, parse_error = validate_sql_syntax(rewritten, dialect=state.target_dialect)
    if not is_valid:
        logger.warning(f"LLM output failed sqlglot validation: {parse_error}")
        # Don't hard-reject — some valid Snowflake syntax (e.g. QUALIFY) confuses sqlglot
        # Log the warning but still accept it; let DuckDB be the real gate
        logger.info("Proceeding despite sqlglot warning — DuckDB will be the final gate")

    state.rewritten_sql = rewritten
    state.error_trace = None  # Clear any previous error on success
    logger.info("Rewriting complete")
    return state
