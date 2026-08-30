"""
Tests for the standardization layer (framework.standardize). snake_case and
infer_type are pure Python; standardize() needs the `spark` fixture (conftest).
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from framework.standardize import infer_type, snake_case, standardize  # noqa: E402


def test_snake_case():
    assert snake_case("CustomerID") == "customer_id"
    assert snake_case("OrderAmount") == "order_amount"
    assert snake_case("order-date") == "order_date"
    assert snake_case("last change.at") == "last_change_at"
    assert snake_case("already_snake") == "already_snake"


def test_infer_type_by_suffix_and_prefix():
    assert infer_type("customer_id") == "string"
    assert infer_type("order_amount") == "decimal(18,2)"
    assert infer_type("fx_rate") == "decimal(18,6)"
    assert infer_type("order_date") == "date"
    assert infer_type("updated_at") == "timestamp"
    assert infer_type("is_current") == "boolean"
    assert infer_type("order_qty") == "int"
    assert infer_type("some_free_text") is None


def test_standardize_renames_snakecases_and_types(spark):
    df = spark.createDataFrame(
        [("C1", "  50.00 ", "2026-08-01", "  hello  ")],
        ["CustID", "OrderAmount", "order_date", "note"],
    )
    out = standardize(df, rename={"CustID": "customer_id"})
    types = dict(out.dtypes)

    assert "customer_id" in out.columns and "order_amount" in out.columns
    assert types["order_amount"] == "decimal(18,2)"  # by *_amount convention
    assert types["order_date"] == "date"  # by *_date convention
    row = out.collect()[0]
    assert row["note"] == "hello"  # trimmed
    assert str(row["order_amount"]) == "50.00"  # string cast to decimal


def test_metadata_columns_are_left_alone(spark):
    df = spark.createDataFrame([("C1", ["e"])], ["CustID", "_dq_errors"])
    out = standardize(df, rename={"CustID": "customer_id"})
    assert "_dq_errors" in out.columns  # exempt from rename/typing


def test_explicit_cast_overrides_convention(spark):
    df = spark.createDataFrame([("5",)], ["order_qty"])  # convention would be int
    out = standardize(df, cast={"order_qty": "bigint"})
    assert dict(out.dtypes)["order_qty"] == "bigint"
