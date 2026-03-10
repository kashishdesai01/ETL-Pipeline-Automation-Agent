"""
core/graph.py
LangGraph StateGraph: wires all 4 agents together with conditional retry routing.

Flow:
  parse → plan → rewrite → validate
                    ↑           │
                    └─ (retry) ←┘ (if failed and retry_count < max)
"""

import logging
from langgraph.graph import StateGraph, END
from langgraph.graph.state import CompiledStateGraph
from core.state import PipelineContext
from agents.parsing_agent import parsing_agent
from agents.planning_agent import planning_agent
from agents.rewriting_agent import rewriting_agent
from agents.validation_agent import validation_agent
from config import Config

logger = logging.getLogger(__name__)


def _route_after_rewrite(state: PipelineContext) -> str:
    """After rewrite: if there's an error, stay in rewrite (retry). Otherwise validate."""
    if state.error_trace and state.retry_count < Config.MAX_REWRITE_RETRIES:
        logger.info(f"Routing: rewrite failed, retrying (attempt {state.retry_count})")
        return "rewrite"
    return "validate"


def _route_after_validate(state: PipelineContext) -> str:
    """After validate: if passed or out of retries, end. Otherwise retry rewrite."""
    val = state.validation_result
    if val and val.passed:
        logger.info("Routing: validation passed → END")
        return END
    if state.retry_count >= Config.MAX_REWRITE_RETRIES:
        logger.warning(f"Routing: max retries ({Config.MAX_REWRITE_RETRIES}) reached → END")
        return END
    logger.info(f"Routing: validation failed, retrying rewrite (retry_count={state.retry_count})")
    return "rewrite"


def build_graph() -> CompiledStateGraph:
    graph = StateGraph(PipelineContext)

    # Register nodes
    graph.add_node("parse", parsing_agent)
    graph.add_node("plan", planning_agent)
    graph.add_node("rewrite", rewriting_agent)
    graph.add_node("validate", validation_agent)

    # Linear edges
    graph.set_entry_point("parse")
    graph.add_edge("parse", "plan")
    graph.add_edge("plan", "rewrite")

    # Conditional routing after rewrite
    graph.add_conditional_edges(
        "rewrite",
        _route_after_rewrite,
        {"rewrite": "rewrite", "validate": "validate"},
    )

    # Conditional routing after validate
    graph.add_conditional_edges(
        "validate",
        _route_after_validate,
        {"rewrite": "rewrite", END: END},
    )

    return graph.compile()
