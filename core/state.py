from pydantic import BaseModel, Field
from typing import Optional, List


class MigrationStep(BaseModel):
    step_id: int
    description: str
    status: str = "pending"  # pending | done | failed


class ValidationResult(BaseModel):
    original_row_count: int
    rewritten_row_count: int
    checksum_match: bool
    diff_sample: Optional[list] = None
    passed: bool


class PipelineContext(BaseModel):
    """Shared state passed between all LangGraph agent nodes."""

    raw_legacy_code: str
    source_dialect: str = "oracle"
    target_dialect: str = "snowflake"
    context_files: dict[str, str] = Field(default_factory=dict)

    # Parsing Agent outputs
    intent_json: Optional[dict] = None

    # Planning Agent outputs
    migration_plan: List[MigrationStep] = Field(default_factory=list)

    # Rewriting Agent outputs
    transpiled_sql: Optional[str] = None
    rewritten_sql: Optional[str] = None

    # Validation Agent outputs
    validation_result: Optional[ValidationResult] = None

    # Retry / error tracking
    retry_count: int = 0
    error_trace: Optional[str] = None

    # Final output
    final_report: Optional[dict] = None
