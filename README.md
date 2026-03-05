# ETL Pipeline Automation Agent

An agentic AI system that reads legacy ETL pipelines (Oracle SQL, MySQL) and rewrites them into modern, optimized SQL for Snowflake (and other targets). Built with LangGraph, GPT-4o, sqlglot, and DuckDB.

## Architecture

```
Legacy SQL → Parsing Agent → Planning Agent → Rewriting Agent → Validation Agent
               (sqlglot + LLM)  (LLM step plan) (sqlglot + LLM)  (DuckDB + checksum)
                                                       ↑                   │
                                                       └─── retry loop ────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | LangGraph (StateGraph) |
| LLM | OpenAI GPT-4o |
| SQL Parsing | sqlglot |
| Local Validation | DuckDB |
| Backend API | FastAPI |
| Frontend | React + Vite (TypeScript) |

## Setup

### 1. Python Environment

```bash
# Install uv (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Copy and set your API key
cp .env.example .env
# Edit .env and add your OPENAI_API_KEY
```

### 2. Run a Single Migration (CLI)

```bash
uv run python main.py legacy_samples/oracle_001.sql --target snowflake
```

### 3. Run the Full Stack (FastAPI + React)

**Terminal 1 — Backend:**
```bash
uv run uvicorn api.main:app --reload --port 8000
```

**Terminal 2 — Frontend:**
```bash
cd frontend && npm run dev
# Open http://localhost:5173
```

### 4. Run Tests

```bash
uv run pytest tests/ -v
```

### 5. Run Eval Suite

```bash
# Copy SQL files to evals/golden_set/
cp legacy_samples/*.sql evals/golden_set/
uv run python evals/run_evals.py --target snowflake
```

## Project Structure

```
├── agents/               # 4 LangGraph agent nodes
│   ├── parsing_agent.py  # sqlglot AST + LLM → intent JSON
│   ├── planning_agent.py # LLM → ordered migration steps
│   ├── rewriting_agent.py# sqlglot transpile + LLM refine
│   └── validation_agent.py# DuckDB execution + checksum
├── core/
│   ├── graph.py          # LangGraph StateGraph (retry routing)
│   ├── state.py          # PipelineContext Pydantic model
│   ├── prompts.py        # All prompt templates
│   ├── tools.py          # sqlglot wrappers
│   └── synthetic_data.py # DuckDB test data generator
├── api/main.py           # FastAPI REST server
├── frontend/             # React + Vite UI
├── legacy_samples/       # Example legacy SQL inputs
├── evals/                # Evaluation framework
└── main.py               # CLI entry point
```

## Supported Dialects

| Source | Target |
|--------|--------|
| Oracle | Snowflake |
| MySQL  | BigQuery |
| PostgreSQL | Databricks |
| BigQuery | Redshift |

## Key Features

- **Two-pass rewriting**: deterministic `sqlglot.transpile()` + LLM refinement
- **Self-correcting loop**: up to 3 retries with DuckDB error fed back to LLM
- **Local validation**: row count + MD5 checksum comparison in DuckDB
- **Structured outputs**: all LLM calls use `response_format={"type": "json_object"}`
- **Configurable targets**: switch dialect with a single env var or API param
