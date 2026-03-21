"""
Load layer.
Uses psycopg v3 client-side COPY for bulk loading into PostgreSQL.
"""

import io

import psycopg
from psycopg import sql
import pandas as pd


def copy_dataframe(conn: psycopg.Connection, df: pd.DataFrame, table_name: str, columns: list[str]) -> int:
    """
    Load a DataFrame into PostgreSQL using client-side COPY.

    Uses psycopg v3's copy_from with CSV format for best performance.
    Returns the number of rows loaded.
    """

    if df.empty:
        return 0

    # Build CSV buffer from DataFrame
    buffer = io.StringIO()
    df[columns].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Build COPY statement
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    copy_query = sql.SQL("COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')").format(
        table=sql.Identifier(table_name),
        cols=cols,
    )

    with conn.cursor() as cur:
        with cur.copy(copy_query) as copy:
            while data := buffer.read(8192):
                copy.write(data)

    conn.commit()

    return len(df)


def load_customers(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    """Load customers DataFrame into PostgreSQL."""

    columns = ["customer_id", "email", "full_name", "signup_date", "country_code", "is_active"]
    return copy_dataframe(conn, df, "customers", columns)


def load_orders(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    """Load orders DataFrame into PostgreSQL."""

    columns = ["order_id", "customer_id", "order_ts", "status", "total_amount", "currency"]
    return copy_dataframe(conn, df, "orders", columns)


def load_order_items(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    """Load order items DataFrame into PostgreSQL."""

    columns = ["order_id", "line_no", "sku", "quantity", "unit_price", "category"]
    return copy_dataframe(conn, df, "order_items", columns)