"""
Unit tests for the Silver-layer dedup logic (clean_and_dedup).

Run with: pytest tests/test_dedup_logic.py
Requires: pyspark, pytest installed locally, or run inside a Databricks
Repos-connected cluster.
"""

import pytest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src", "silver"))
from silver_transform_orders import clean_and_dedup


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("dedup-tests")
        .getOrCreate()
    )


def test_dedup_keeps_latest_version(spark):
    """When the same order_id appears twice with different updated_at,
    the row with the LATER updated_at should survive."""
    now = datetime.now()
    earlier = now - timedelta(hours=2)

    data = [
        ("ORD1", "CUST1", "PROD1", "2026-08-01", 100.0, "pending", earlier),
        ("ORD1", "CUST1", "PROD1", "2026-08-01", 100.0, "shipped", now),  # newer, should survive
    ]
    columns = ["order_id", "cust_id", "product_id", "order_date", "amount", "order_status", "updated_at"]
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert len(result) == 1
    assert result[0]["order_status"] == "shipped"


def test_dedup_drops_null_order_id(spark):
    data = [
        (None, "CUST1", "PROD1", "2026-08-01", 100.0, "pending", datetime.now()),
        ("ORD2", "CUST2", "PROD2", "2026-08-01", 50.0, "pending", datetime.now()),
    ]
    columns = ["order_id", "cust_id", "product_id", "order_date", "amount", "order_status", "updated_at"]
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert len(result) == 1
    assert result[0]["order_id"] == "ORD2"


def test_no_duplicates_passes_through_unchanged(spark):
    data = [
        ("ORD3", "CUST3", "PROD3", "2026-08-01", 75.0, "pending", datetime.now()),
    ]
    columns = ["order_id", "cust_id", "product_id", "order_date", "amount", "order_status", "updated_at"]
    df = spark.createDataFrame(data, columns)

    result = clean_and_dedup(df).collect()

    assert len(result) == 1
