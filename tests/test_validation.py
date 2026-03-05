"""
tests/test_validation.py
Unit tests for: synthetic data generation, DuckDB execution, checksum logic.
Run with: pytest tests/ -v
"""

import duckdb
import pytest
from core.synthetic_data import create_synthetic_tables
from agents.validation_agent import _row_checksum, _normalize_value, _strip_snowflake_specific

MOCK_INTENT = {
    "input_tables": ["orders", "customers"],
    "summary": "Test intent",
}


def test_create_synthetic_tables_creates_tables():
    con = duckdb.connect(":memory:")
    tables = create_synthetic_tables(con, MOCK_INTENT)
    assert "orders" in tables
    assert "customers" in tables
    # Tables should have rows
    row_count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert row_count > 0
    con.close()


def test_create_synthetic_tables_empty_intent():
    con = duckdb.connect(":memory:")
    tables = create_synthetic_tables(con, {})
    # Should create a fallback 'data' table
    assert len(tables) > 0
    con.close()


def test_checksum_is_stable():
    rows = [(1, "a", 2.5), (2, "b", 3.0)]
    c1 = _row_checksum(rows)
    c2 = _row_checksum(rows)
    assert c1 == c2


def test_checksum_differs_for_different_rows():
    rows_a = [(1, "a"), (2, "b")]
    rows_b = [(1, "a"), (2, "c")]
    assert _row_checksum(rows_a) != _row_checksum(rows_b)


def test_normalize_float_rounding():
    assert _normalize_value(1.123456) == 1.12
    assert _normalize_value(None) == "__NULL__"
    assert _normalize_value("hello") == "hello"


def test_strip_snowflake_keeps_basic_sql():
    sql = "SELECT id, region FROM orders WHERE amount > 100"
    result = _strip_snowflake_specific(sql)
    assert "SELECT" in result


def test_strip_snowflake_replaces_sysdate():
    sql = "SELECT SYSDATE() AS run_at FROM orders"
    result = _strip_snowflake_specific(sql)
    assert "CURRENT_TIMESTAMP" in result
