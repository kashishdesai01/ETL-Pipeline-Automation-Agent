"""
agents/parsing_agent.py
Reads legacy SQL → produces structured intent JSON via sqlglot AST + LLM.
"""

import json
import logging
from openai import OpenAI
from core.state import PipelineContext
from core.tools import parse_sql_ast
from core.prompts import PARSING_SYSTEM_PROMPT, PARSING_USER_TEMPLATE
from config import Config

logger = logging.getLogger(__name__)
client = OpenAI(api_key=Config.OPENAI_API_KEY)


def parsing_agent(state: PipelineContext) -> PipelineContext:
    logger.info("=== PARSING AGENT: Starting ===")

    # Step 1: Deterministic AST analysis
    ast_metadata = parse_sql_ast(state.raw_legacy_code, dialect=state.source_dialect)
    logger.info(f"AST extracted: tables={ast_metadata.get('tables')}, fallback={ast_metadata.get('fallback')}")

    # Step 2: LLM intent extraction
    # Format context files
    context_str = "None"
    if state.context_files:
        context_str = "\n".join([f"--- File: {k} ---\n{v}\n" for k, v in state.context_files.items()])

    user_message = PARSING_USER_TEMPLATE.format(
        source_dialect=state.source_dialect,
        raw_sql=state.raw_legacy_code,
        ast_metadata=json.dumps(ast_metadata, indent=2),
        project_context=context_str,
    )

    try:
        response = client.chat.completions.create(
            model=Config.LLM_MODEL,
            messages=[
                {"role": "system", "content": PARSING_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        response_content = response.choices[0].message.content
        if response_content is None:
            raise ValueError("No content in response")
        intent_json = json.loads(response_content)
        logger.info(f"Intent extracted: summary='{intent_json.get('summary', '')[:80]}...'")
    except Exception as e:
        logger.error(f"Parsing LLM call failed: {e}")
        # Graceful fallback
        intent_json = {
            "summary": "Failed to extract intent — see error",
            "input_tables": ast_metadata.get("tables", []),
            "output_tables": [],
            "transformations": [],
            "filters": ast_metadata.get("filters", []),
            "aggregations": ast_metadata.get("aggregations", []),
            "dependencies": [],
            "complexity": "high",
            "dialect_constructs": [],
            "migration_notes": f"LLM parse failed: {str(e)}",
        }

    state.intent_json = intent_json
    return state
