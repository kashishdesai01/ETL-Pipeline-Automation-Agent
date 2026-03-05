"""
core/synthetic_data.py
Generates synthetic DuckDB tables from the intent JSON produced by the Parsing Agent.
This lets us run both the legacy (emulated) and rewritten SQL locally.
"""

import logging
import duckdb

logger = logging.getLogger(__name__)

# Default column templates by common table name patterns
_TABLE_TEMPLATES = {
    "orders": """
        id INTEGER,
        customer_id INTEGER,
        product_id INTEGER,
        region VARCHAR,
        status VARCHAR,
        amount DECIMAL(10,2),
        quantity INTEGER,
        order_date DATE,
        created_at TIMESTAMP
    """,
    "customers": """
        id INTEGER,
        name VARCHAR,
        email VARCHAR,
        region VARCHAR,
        country VARCHAR,
        segment VARCHAR,
        created_at TIMESTAMP
    """,
    "products": """
        id INTEGER,
        name VARCHAR,
        category VARCHAR,
        price DECIMAL(10,2),
        sku VARCHAR
    """,
    "regions": """
        id INTEGER,
        name VARCHAR,
        country VARCHAR,
        zone VARCHAR
    """,
    "line_items": """
        id INTEGER,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price DECIMAL(10,2)
    """,
}

_GENERIC_SCHEMA = """
    id INTEGER,
    name VARCHAR,
    category VARCHAR,
    region VARCHAR,
    amount DECIMAL(10,2),
    quantity INTEGER,
    status VARCHAR,
    created_at TIMESTAMP
"""

_SEED_ROWS = 500


def _get_schema(table_name: str) -> str:
    """Pick a schema template matching the table name, or use generic fallback."""
    lower = table_name.lower()
    for key, schema in _TABLE_TEMPLATES.items():
        if key in lower:
            return schema
    return _GENERIC_SCHEMA


def create_synthetic_tables(
    con: duckdb.DuckDBPyConnection, intent_json: dict
) -> list[str]:
    """
    Create and seed DuckDB tables based on intent JSON input_tables.
    Returns the list of table names created.
    """
    tables_created = []
    input_tables = intent_json.get("input_tables", [])

    if not input_tables:
        logger.warning("No input_tables found in intent_json — creating a default 'data' table")
        input_tables = ["data"]

    for table in input_tables:
        schema = _get_schema(table)
        lower_table = table.lower()
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} ({schema})")

            # Use schema-specific inserts so column counts always match
            if "orders" in lower_table:
                con.execute(f"""
                    INSERT INTO {table}(id, customer_id, product_id, region, status, amount, quantity, order_date, created_at)
                    SELECT
                        row_number() OVER ()                                                         AS id,
                        (random()*200)::INT + 1                                                      AS customer_id,
                        (random()*50)::INT + 1                                                       AS product_id,
                        ['North','South','East','West'][(random()*3)::INT + 1]                       AS region,
                        ['active','inactive','pending'][(random()*2)::INT + 1]                       AS status,
                        (random() * 10000)::DECIMAL(10,2)                                            AS amount,
                        (random() * 100)::INT + 1                                                    AS quantity,
                        (NOW() - INTERVAL ((random()*365)::INT || ' days'))::DATE                    AS order_date,
                        NOW() - INTERVAL ((random()*365)::INT || ' days')                            AS created_at
                    FROM range({_SEED_ROWS})
                """)
            elif "customers" in lower_table:
                con.execute(f"""
                    INSERT INTO {table}(id, name, email, region, country, segment, created_at)
                    SELECT
                        row_number() OVER ()                                                         AS id,
                        'Customer_' || (random()*1000)::INT                                          AS name,
                        'user' || (random()*1000)::INT || '@example.com'                             AS email,
                        ['North','South','East','West'][(random()*3)::INT + 1]                       AS region,
                        ['US','UK','DE','FR'][(random()*3)::INT + 1]                                 AS country,
                        ['SMB','Enterprise','Consumer'][(random()*2)::INT + 1]                       AS segment,
                        NOW() - INTERVAL ((random()*365)::INT || ' days')                            AS created_at
                    FROM range({_SEED_ROWS})
                """)
            elif "line_items" in lower_table:
                con.execute(f"""
                    INSERT INTO {table}(id, order_id, product_id, quantity, unit_price)
                    SELECT
                        row_number() OVER ()      AS id,
                        (random()*500)::INT + 1   AS order_id,
                        (random()*50)::INT + 1    AS product_id,
                        (random()*10)::INT + 1    AS quantity,
                        (random()*500)::DECIMAL(10,2) AS unit_price
                    FROM range({_SEED_ROWS})
                """)
            elif "products" in lower_table:
                con.execute(f"""
                    INSERT INTO {table}(id, name, category, price, sku)
                    SELECT
                        row_number() OVER ()                                                         AS id,
                        'Product_' || (random()*100)::INT                                            AS name,
                        ['Electronics','Clothing','Food','Tools'][(random()*3)::INT + 1]             AS category,
                        (random()*500)::DECIMAL(10,2)                                                AS price,
                        'SKU-' || (random()*10000)::INT                                              AS sku
                    FROM range({_SEED_ROWS})
                """)
            elif "regions" in lower_table:
                con.execute(f"""
                    INSERT INTO {table}(id, name, country, zone)
                    SELECT
                        row_number() OVER ()                                                         AS id,
                        ['North','South','East','West'][(random()*3)::INT + 1]                       AS name,
                        ['US','UK','DE','FR'][(random()*3)::INT + 1]                                 AS country,
                        ['Zone-A','Zone-B','Zone-C'][(random()*2)::INT + 1]                          AS zone
                    FROM range({_SEED_ROWS})
                """)
            else:
                # Generic fallback — uses the _GENERIC_SCHEMA columns explicitly
                con.execute(f"""
                    INSERT INTO {table}(id, name, category, region, amount, quantity, status, created_at)
                    SELECT
                        row_number() OVER ()                                                         AS id,
                        'Item_' || (random()*1000)::INT                                              AS name,
                        ['Electronics','Clothing','Food','Tools'][(random()*3)::INT + 1]             AS category,
                        ['North','South','East','West'][(random()*3)::INT + 1]                       AS region,
                        (random()*10000)::DECIMAL(10,2)                                              AS amount,
                        (random()*100)::INT + 1                                                      AS quantity,
                        ['active','inactive','pending'][(random()*2)::INT + 1]                       AS status,
                        NOW() - INTERVAL ((random()*365)::INT || ' days')                            AS created_at
                    FROM range({_SEED_ROWS})
                """)

            tables_created.append(table)
            logger.info(f"Created and seeded table: {table} ({_SEED_ROWS} rows)")
        except Exception as e:
            logger.error(f"Failed to create table {table}: {e}")

    return tables_created
