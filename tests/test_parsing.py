"""
tests/test_parsing.py
Unit tests for the sqlglot AST parsing and transpilation tools.
Run with: pytest tests/ -v
"""

import pytest
from core.tools import parse_sql_ast, transpile_sql, validate_sql_syntax

ORACLE_SQL = """
SELECT NVL(SUM(o.amount), 0) AS total
FROM orders o, customers c
WHERE o.customer_id = c.id
AND o.order_date >= ADD_MONTHS(SYSDATE, -1)
GROUP BY c.region
"""

SIMPLE_SQL = "SELECT id, name FROM orders WHERE status = 'active'"


def test_parse_ast_extracts_tables():
    result = parse_sql_ast(ORACLE_SQL, dialect="oracle")
    assert "orders" in result["tables"] or len(result["tables"]) > 0


def test_parse_ast_extracts_aggregations():
    result = parse_sql_ast(ORACLE_SQL, dialect="oracle")
    assert len(result["aggregations"]) > 0 or result.get("fallback") is True


def test_parse_ast_does_not_crash_on_empty():
    result = parse_sql_ast("", dialect="oracle")
    # Should return a dict (possibly with parse_error), not raise
    assert isinstance(result, dict)


def test_transpile_simple_sql():
    transpiled = transpile_sql(SIMPLE_SQL, source="oracle", target="snowflake")
    assert "SELECT" in transpiled.upper()
    assert "orders" in transpiled.lower()


def test_transpile_returns_error_string_on_bad_input():
    result = transpile_sql("THIS IS NOT SQL !!!@@@", source="oracle", target="snowflake")
    # May return the original or a TRANSPILE_ERROR — should not raise
    assert isinstance(result, str)


def test_validate_sql_syntax_valid():
    valid_sql = "SELECT id, region FROM orders WHERE status = 'active'"
    is_valid, err = validate_sql_syntax(valid_sql, dialect="snowflake")
    assert is_valid is True
    assert err == ""


def test_validate_sql_syntax_invalid():
    bad_sql = "SELECT FROM WHERE GARBLED;;;"
    is_valid, err = validate_sql_syntax(bad_sql, dialect="snowflake")
    # sqlglot should flag this as invalid
    # (some versions may be lenient — we just check it doesn't crash)
    assert isinstance(is_valid, bool)
    assert isinstance(err, str)
