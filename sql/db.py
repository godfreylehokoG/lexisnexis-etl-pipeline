"""
Database connection and SQL execution helper.
Uses psycopg v3 for all database operations.
"""

from pathlib import Path

import psycopg


def get_connection(db_config: dict) -> psycopg.Connection:
    """Create and return a psycopg v3 connection."""

    conn = psycopg.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        autocommit=False,
    )
    return conn


def execute_sql_file(conn: psycopg.Connection, filepath: str) -> None:
    """Read and execute an entire SQL file."""

    sql_path = Path(filepath)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")

    sql = sql_path.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(sql)

    conncommit()