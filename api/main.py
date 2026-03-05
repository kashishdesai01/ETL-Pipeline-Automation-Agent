"""
api/main.py
FastAPI REST server — receives a legacy SQL file upload, runs the LangGraph pipeline,
and returns the full migration result as JSON.
"""

import logging
from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from core.graph import build_graph
from core.state import PipelineContext
from config import Config, TargetDialect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ETL Migration Agent API",
    description="LangGraph-powered ETL pipeline migration: Oracle/MySQL → Snowflake (and more)",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class MigrationResponse(BaseModel):
    intent: Optional[dict]
    migration_plan: list
    legacy_sql: str
    transpiled_sql: Optional[str]
    rewritten_sql: Optional[str]
    validation: Optional[dict]
    retry_count: int
    success: bool
    error: Optional[str]


@app.post("/migrate", response_model=MigrationResponse)
async def migrate(
    file: UploadFile,
    target_dialect: str = Form(default="snowflake"),
    source_dialect: str = Form(default="oracle"),
):
    """
    Run the full ETL migration pipeline on an uploaded legacy SQL file.
    Returns parsed intent, migration plan, rewritten SQL, and validation results.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    legacy_code = (await file.read()).decode("utf-8")

    if not legacy_code.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    # Validate target dialect
    try:
        target_enum = TargetDialect(target_dialect)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid target dialect '{target_dialect}'. Must be one of: {[d.value for d in TargetDialect]}",
        )

    Config.TARGET_DIALECT = target_enum

    initial_state = PipelineContext(
        raw_legacy_code=legacy_code,
        source_dialect=source_dialect,
        target_dialect=target_enum.value,
    )

    logger.info(f"Migration request: {file.filename} | {source_dialect} → {target_dialect}")

    try:
        graph = build_graph()
        final: PipelineContext = graph.invoke(initial_state)
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {str(e)}")

    success = bool(
        final.validation_result.passed
        if final.validation_result
        else final.rewritten_sql
    )

    return MigrationResponse(
        intent=final.intent_json,
        migration_plan=[s.model_dump() for s in final.migration_plan],
        legacy_sql=legacy_code,
        transpiled_sql=final.transpiled_sql,
        rewritten_sql=final.rewritten_sql,
        validation=final.validation_result.model_dump() if final.validation_result else None,
        retry_count=final.retry_count,
        success=success,
        error=final.error_trace if not success else None,
    )


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0"}


@app.get("/dialects")
def get_dialects():
    """Return available source and target dialects."""
    return {
        "source_dialects": ["oracle", "mysql", "postgres", "bigquery", "spark"],
        "target_dialects": [d.value for d in TargetDialect],
    }
