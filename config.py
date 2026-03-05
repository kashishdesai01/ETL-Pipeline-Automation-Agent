import os
from enum import Enum
from dotenv import load_dotenv

load_dotenv()


class TargetDialect(str, Enum):
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    DATABRICKS = "databricks"
    REDSHIFT = "redshift"


class Config:
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    SOURCE_DIALECT: str = os.getenv("SOURCE_DIALECT", "oracle")
    TARGET_DIALECT: TargetDialect = TargetDialect(
        os.getenv("TARGET_DIALECT", "snowflake")
    )
    MAX_REWRITE_RETRIES: int = int(os.getenv("MAX_REWRITE_RETRIES", "3"))
    DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", ":memory:")
