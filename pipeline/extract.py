"""
Extraction layer.
Reads raw CSV and JSONL files into pandas DataFrames.
"""

import pandas as pd


def extract_customers(filepath: str) -> pd.DataFrame:
    """Read customers CSV into a DataFrame."""

    df = pd.read_csv(
        filepath,
        dtype={
            "customer_id": "Int64",
            "email": str,
            "full_name": str,
            "country_code": str,
            "is_active": str,
        },
        parse_dates=["signup_date"],
    )

    return df


def extract_orders(filepath: str) -> pd.DataFrame:
    """Read orders JSONL into a DataFrame (one JSON object per line)."""

    df = pd.read_json(filepath, lines=True)

    # Ensure correct dtypes
    df["order_id"] = df["order_id"].astype("Int64")
    df["customer_id"] = df["customer_id"].astype("Int64")
    df["total_amount"] = df["total_amount"].astype(float)
    df["status"] = df["status"].astype(str)
    df["currency"] = df["currency"].astype(str)

    return df


def extract_order_items(filepath: str) -> pd.DataFrame:
    """Read order items CSV into a DataFrame."""

    df = pd.read_csv(
        filepath,
        dtype={
            "order_id": "Int64",
            "line_no": "Int64",
            "sku": str,
            "quantity": "Int64",
            "unit_price": float,
            "category": str,
        },
    )

    return df