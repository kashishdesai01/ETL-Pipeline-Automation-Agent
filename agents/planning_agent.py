"""
agents/planning_agent.py
Takes intent JSON → produces an ordered list of migration steps.
"""

import json
import logging
from openai import OpenAI
from core.state import PipelineContext, MigrationStep
from core.prompts import PLANNING_SYSTEM_PROMPT, PLANNING_USER_TEMPLATE
from config import Config

logger = logging.getLogger(__name__)
client = OpenAI(api_key=Config.OPENAI_API_KEY)


def planning_agent(state: PipelineContext) -> PipelineContext:
    logger.info("=== PLANNING AGENT: Starting ===")

    user_message = PLANNING_USER_TEMPLATE.format(
        target_dialect=state.target_dialect,
        intent_json=json.dumps(state.intent_json, indent=2),
    )

    try:
        response = client.chat.completions.create(
            model=Config.LLM_MODEL,
            messages=[
                {"role": "system", "content": PLANNING_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        data = json.loads(response.choices[0].message.content)
        steps_raw = data.get("steps", [])
        state.migration_plan = [MigrationStep(**s) for s in steps_raw]
        logger.info(f"Migration plan: {len(state.migration_plan)} steps generated")
    except Exception as e:
        logger.error(f"Planning LLM call failed: {e}")
        # Minimal fallback plan
        state.migration_plan = [
            MigrationStep(step_id=1, description="Transpile base SQL via sqlglot"),
            MigrationStep(step_id=2, description="Refine with LLM for target dialect"),
            MigrationStep(step_id=3, description="Validate in DuckDB"),
        ]

    return state
