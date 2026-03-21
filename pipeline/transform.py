"""
Transformation and validation layer.
Cleans, validates, and splits data into clean + rejected DataFrames.
Each function returns: (clean_df, rejected_df)
"""

import re

import pandas as pd


VALID_STATUSES = {"placed", "shipped", "cancelled", "refunded"}
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def transform_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and validate customers.

    Rules:
    - Normalize emails to lowercase
    - Reject invalid emails (no @ sign)
    - Deduplicate by email, keeping earliest signup_date
    """

    rejected_rows = []

    # --- Step 1: Normalize email to lowercase ---
    df["email"] = df["email"].str.strip().str.lower()

    # --- Step 2: Validate email format ---
    invalid_email_mask = ~df["email"].apply(lambda x: bool(EMAIL_REGEX.match(str(x))))
    invalid_emails = df[invalid_email_mask].copy()
    invalid_emails["_rejection_reason"] = "invalid_email:no_at_sign"
    rejected_rows.append(invalid_emails)

    df = df[~invalid_email_mask].copy()

    # --- Step 3: Deduplicate emails (keep earliest signup) ---
    df = df.sort_values("signup_date")
    duplicated_mask = df.duplicated(subset=["email"], keep="first")
    duplicates = df[duplicated_mask].copy()
    duplicates["_rejection_reason"] = "duplicate_email:kept_earlier_signup"
    rejected_rows.append(duplicates)

    df = df[~duplicated_mask].copy()

    # --- Step 4: Normalize types ---
    df["is_active"] = df["is_active"].str.lower().map({"true": True, "false": False})
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date

    # Replace empty country_code with None
    df["country_code"] = df["country_code"].where(df["country_code"].notna(), None)

    # --- Combine rejected ---
    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    return df, rejected


def transform_orders(
    df: pd.DataFrame,
    valid_customer_ids: set[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and validate orders.

    Rules:
    - Standardize order_ts to UTC
    - Reject orders with invalid status
    - Reject orders referencing unknown customers
    """

    rejected_rows = []

    # --- Step 1: Reject unknown customer_ids ---
    orphan_mask = ~df["customer_id"].isin(valid_customer_ids)
    orphans = df[orphan_mask].copy()
    orphans["_rejection_reason"] = orphans["customer_id"].apply(
        lambda x: f"unknown_customer:{x}"
    )
    rejected_rows.append(orphans)

    df = df[~orphan_mask].copy()

    # --- Step 2: Reject invalid statuses ---
    invalid_status_mask = ~df["status"].isin(VALID_STATUSES)
    invalid_statuses = df[invalid_status_mask].copy()
    invalid_statuses["_rejection_reason"] = invalid_statuses["status"].apply(
        lambda x: f"invalid_status:{x}"
    )
    rejected_rows.append(invalid_statuses)

    df = df[~invalid_status_mask].copy()

    # --- Step 3: Parse and standardize timestamps to UTC ---
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True, format="mixed")

    # --- Combine rejected ---
    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    return df, rejected


def transform_order_items(
    df: pd.DataFrame,
    valid_order_ids: set[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and validate order items.

    Rules:
    - Reject items belonging to invalid/quarantined orders
    - Reject items with non-positive quantity
    - Reject items with non-positive unit_price
    """

    rejected_rows = []

    # --- Step 1: Reject orphaned order items ---
    orphan_mask = ~df["order_id"].isin(valid_order_ids)
    orphans = df[orphan_mask].copy()
    orphans["_rejection_reason"] = orphans["order_id"].apply(
        lambda x: f"orphaned_order:{x}"
    )
    rejected_rows.append(orphans)

    df = df[~orphan_mask].copy()

    # --- Step 2: Reject non-positive quantities ---
    bad_qty_mask = df["quantity"] <= 0
    bad_qty = df[bad_qty_mask].copy()
    bad_qty["_rejection_reason"] = bad_qty["quantity"].apply(
        lambda x: f"non_positive_quantity:{x}"
    )
    rejected_rows.append(bad_qty)

    df = df[~bad_qty_mask].copy()

    # --- Step 3: Reject non-positive unit prices ---
    bad_price_mask = df["unit_price"] <= 0
    bad_prices = df[bad_price_mask].copy()
    bad_prices["_rejection_reason"] = bad_prices["unit_price"].apply(
        lambda x: f"non_positive_unit_price:{x}"
    )
    rejected_rows.append(bad_prices)

    df = df[~bad_price_mask].copy()

    # --- Combine rejected ---
    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    return df, rejected