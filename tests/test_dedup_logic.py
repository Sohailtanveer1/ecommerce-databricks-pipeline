"""
Unit tests for the Silver-layer dedup logic (clean_and_dedup).

Run with: pytest tests/test_dedup_logic.py
Requires: pyspark, pytest installed locally, or run inside a Databricks
Repos-connected cluster.
"""

from datetime import datetime, timedelta

# `spark` fixture and src/ import paths are provided by tests/conftest.py.
from silver_transform_orders import clean_and_dedup

COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "amount",
    "order_status",
    "updated_at",
]


def test_dedup_keeps_latest_version(spark):
    """When the same order_id appears twice with different updated_at,
    the row with the LATER updated_at should survive."""
    now = datetime.now()
    earlier = now - timedelta(hours=2)

    data = [
        ("ORD1", "CUST1", "PROD1", "2026-08-01", 100.0, "pending", earlier),
        ("ORD1", "CUST1", "PROD1", "2026-08-01", 100.0, "shipped", now),  # newer, should survive
    ]
    columns = COLUMNS
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert len(result) == 1
    assert result[0]["order_status"] == "shipped"


def test_dedup_renames_nothing_when_customer_id_present(spark):
    """clean_and_dedup should preserve customer_id as-is (Bronze already uses
    the canonical column name — no rename happens in Silver)."""
    data = [
        ("ORD9", "CUST9", "PROD9", "2026-08-01", 20.0, "pending", datetime.now()),
    ]
    columns = COLUMNS
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert result[0]["customer_id"] == "CUST9"


def test_dedup_drops_null_order_id(spark):
    data = [
        (None, "CUST1", "PROD1", "2026-08-01", 100.0, "pending", datetime.now()),
        ("ORD2", "CUST2", "PROD2", "2026-08-01", 50.0, "pending", datetime.now()),
    ]
    columns = COLUMNS
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert len(result) == 1
    assert result[0]["order_id"] == "ORD2"


def test_no_duplicates_passes_through_unchanged(spark):
    data = [
        ("ORD3", "CUST3", "PROD3", "2026-08-01", 75.0, "pending", datetime.now()),
    ]
    columns = COLUMNS
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert len(result) == 1
