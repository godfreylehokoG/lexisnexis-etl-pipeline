"""
Unit tests for the transformation layer.
Run with: python -m pytest tests/ -v
"""

import pandas as pd
import pytest

from pipeline.transform import (
    transform_customers,
    transform_orders,
    transform_order_items,
)


# ============================================================
# CUSTOMER TESTS
# ============================================================

class TestTransformCustomers:

    def test_email_normalization(self):
        """Mixed case emails should be lowercased."""
        df = pd.DataFrame([{
            "customer_id": 1,
            "email": "JOHN.Smith@Example.com",
            "full_name": "John Smith",
            "signup_date": "2024-01-10",
            "country_code": "ZA",
            "is_active": "true",
        }])

        clean, rejected = transform_customers(df)

        assert len(clean) == 1
        assert clean.iloc[0]["email"] == "john.smith@example.com"

    def test_invalid_email_rejected(self):
        """Emails without @ sign should be quarantined."""
        df = pd.DataFrame([{
            "customer_id": 1,
            "email": "bademail",
            "full_name": "Bad Email",
            "signup_date": "2024-01-10",
            "country_code": "ZA",
            "is_active": "true",
        }])

        clean, rejected = transform_customers(df)

        assert len(clean) == 0
        assert len(rejected) == 1
        assert "invalid_email" in rejected.iloc[0]["_rejection_reason"]

    def test_duplicate_email_keeps_earliest(self):
        """When emails collide after normalization, keep earliest signup."""
        df = pd.DataFrame([
            {
                "customer_id": 4,
                "email": "dup@EXAMPLE.com",
                "full_name": "Dupe A",
                "signup_date": "2024-01-01",
                "country_code": "ZA",
                "is_active": "true",
            },
            {
                "customer_id": 5,
                "email": "dup@example.com",
                "full_name": "Dupe B",
                "signup_date": "2024-02-01",
                "country_code": "ZA",
                "is_active": "true",
            },
        ])

        clean, rejected = transform_customers(df)

        assert len(clean) == 1
        assert len(rejected) == 1
        assert clean.iloc[0]["customer_id"] == 4
        assert rejected.iloc[0]["customer_id"] == 5

    def test_missing_country_code_allowed(self):
        """Customers with no country_code should still be loaded."""
        df = pd.DataFrame([{
            "customer_id": 3,
            "email": "alex@example.com",
            "full_name": "Alex Null",
            "signup_date": "2024-01-20",
            "country_code": None,
            "is_active": "true",
        }])

        clean, rejected = transform_customers(df)

        assert len(clean) == 1
        assert len(rejected) == 0


# ============================================================
# ORDER TESTS
# ============================================================

class TestTransformOrders:

    def test_valid_order_passes(self):
        """Orders with valid customer and status should pass."""
        df = pd.DataFrame([{
            "order_id": 1001,
            "customer_id": 1,
            "order_ts": "2024-03-01T08:12:00+02:00",
            "status": "placed",
            "total_amount": 250.50,
            "currency": "ZAR",
        }])

        clean, rejected = transform_orders(df, valid_customer_ids={1})

        assert len(clean) == 1
        assert len(rejected) == 0

    def test_unknown_customer_rejected(self):
        """Orders referencing non-existent customers should be quarantined."""
        df = pd.DataFrame([{
            "order_id": 1003,
            "customer_id": 999,
            "order_ts": "2024-03-02T10:00:00+02:00",
            "status": "placed",
            "total_amount": 75.00,
            "currency": "ZAR",
        }])

        clean, rejected = transform_orders(df, valid_customer_ids={1, 2, 3})

        assert len(clean) == 0
        assert len(rejected) == 1
        assert "unknown_customer" in rejected.iloc[0]["_rejection_reason"]

    def test_invalid_status_rejected(self):
        """Orders with status not in allowed set should be quarantined."""
        df = pd.DataFrame([{
            "order_id": 1004,
            "customer_id": 3,
            "order_ts": "2024-03-03T11:30:00Z",
            "status": "processing",
            "total_amount": 60.00,
            "currency": "ZAR",
        }])

        clean, rejected = transform_orders(df, valid_customer_ids={3})

        assert len(clean) == 0
        assert len(rejected) == 1
        assert "invalid_status" in rejected.iloc[0]["_rejection_reason"]

    def test_timestamps_standardized_to_utc(self):
        """All timestamps should be converted to UTC."""
        df = pd.DataFrame([{
            "order_id": 1001,
            "customer_id": 1,
            "order_ts": "2024-03-01T08:12:00+02:00",
            "status": "placed",
            "total_amount": 250.50,
            "currency": "ZAR",
        }])

        clean, rejected = transform_orders(df, valid_customer_ids={1})

        ts = clean.iloc[0]["order_ts"]
        assert str(ts.tzinfo) == "UTC"
        assert ts.hour == 6  # 08:12 +02:00 = 06:12 UTC


# ============================================================
# ORDER ITEMS TESTS
# ============================================================

class TestTransformOrderItems:

    def test_valid_item_passes(self):
        """Items with valid order, positive qty and price should pass."""
        df = pd.DataFrame([{
            "order_id": 1001,
            "line_no": 1,
            "sku": "A-001",
            "quantity": 1,
            "unit_price": 250.50,
            "category": "Electronics",
        }])

        clean, rejected = transform_order_items(df, valid_order_ids={1001})

        assert len(clean) == 1
        assert len(rejected) == 0

    def test_orphaned_order_rejected(self):
        """Items belonging to quarantined orders should be quarantined."""
        df = pd.DataFrame([{
            "order_id": 1003,
            "line_no": 1,
            "sku": "C-100",
            "quantity": 1,
            "unit_price": 75.00,
            "category": "Toys",
        }])

        clean, rejected = transform_order_items(df, valid_order_ids={1001, 1002})

        assert len(clean) == 0
        assert len(rejected) == 1
        assert "orphaned_order" in rejected.iloc[0]["_rejection_reason"]

    def test_zero_quantity_rejected(self):
        """Items with quantity <= 0 should be quarantined."""
        df = pd.DataFrame([{
            "order_id": 1004,
            "line_no": 2,
            "sku": "D-333",
            "quantity": 0,
            "unit_price": 45.00,
            "category": "Books",
        }])

        clean, rejected = transform_order_items(df, valid_order_ids={1004})

        assert len(clean) == 0
        assert len(rejected) == 1
        assert "non_positive_quantity" in rejected.iloc[0]["_rejection_reason"]

    def test_zero_price_rejected(self):
        """Items with unit_price <= 0 should be quarantined."""
        df = pd.DataFrame([{
            "order_id": 1005,
            "line_no": 1,
            "sku": "E-777",
            "quantity": 1,
            "unit_price": 0.00,
            "category": "Electronics",
        }])

        clean, rejected = transform_order_items(df, valid_order_ids={1005})

        assert len(clean) == 0
        assert len(rejected) == 1
        assert "non_positive_unit_price" in rejected.iloc[0]["_rejection_reason"]

    def test_partial_order_keeps_valid_items(self):
        """When one item is invalid, other items in same order survive."""
        df = pd.DataFrame([
            {
                "order_id": 1008,
                "line_no": 1,
                "sku": "H-654",
                "quantity": 2,
                "unit_price": 110.00,
                "category": "Sports",
            },
            {
                "order_id": 1008,
                "line_no": 2,
                "sku": "H-655",
                "quantity": 1,
                "unit_price": 0.00,
                "category": "Sports",
            },
        ])

        clean, rejected = transform_order_items(df, valid_order_ids={1008})

        assert len(clean) == 1
        assert len(rejected) == 1
        assert clean.iloc[0]["sku"] == "H-654"
        assert rejected.iloc[0]["sku"] == "H-655"