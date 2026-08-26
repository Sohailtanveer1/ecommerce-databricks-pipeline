"""
Unit tests for the config-driven data-quality validation framework
(common.data_quality). `spark` fixture + import paths come from conftest.py.
"""

import pytest

from common.data_quality import (
    DataQualityError,
    DataQualityValidator,
    OnFailure,
    from_config,
)

COLS = ["order_id", "customer_id", "amount", "order_status", "updated_at"]


def _df(spark, rows):
    from datetime import datetime

    data = [(o, c, a, s, datetime.now()) for (o, c, a, s) in rows]
    return spark.createDataFrame(data, COLS)


def test_fail_mode_raises_on_violation(spark):
    df = _df(spark, [("O1", None, 10.0, "paid")])  # null customer_id
    v = DataQualityValidator("orders", OnFailure.FAIL).not_null("customer_id")
    with pytest.raises(DataQualityError):
        v.validate(df)


def test_quarantine_splits_valid_and_bad(spark):
    df = _df(
        spark,
        [
            ("O1", "C1", 10.0, "paid"),  # good
            ("O2", None, 10.0, "paid"),  # bad: null customer_id
            ("O3", "C3", -5.0, "paid"),  # bad: negative amount
        ],
    )
    v = (
        DataQualityValidator("orders", OnFailure.QUARANTINE)
        .not_null("customer_id")
        .non_negative("amount")
    )
    outcome = v.validate(df)

    assert outcome.total_rows == 3
    assert outcome.valid.count() == 1
    assert outcome.quarantine_rows == 2
    assert not outcome.passed  # violations were recorded


def test_allowed_values(spark):
    df = _df(spark, [("O1", "C1", 10.0, "teleported")])  # invalid status
    v = DataQualityValidator("orders", OnFailure.QUARANTINE).allowed_values(
        "order_status", ["pending", "paid", "shipped"]
    )
    outcome = v.validate(df)
    assert outcome.valid.count() == 0
    assert outcome.quarantine_rows == 1


def test_unique_detects_duplicate_keys(spark):
    df = _df(spark, [("O1", "C1", 10.0, "paid"), ("O1", "C2", 20.0, "paid")])
    v = DataQualityValidator("orders", OnFailure.WARN).unique("order_id")
    outcome = v.validate(df)
    uniq = [r for r in outcome.results if r.check.startswith("unique")][0]
    assert not uniq.passed
    assert uniq.violations == 1


def test_min_row_count(spark):
    df = _df(spark, [("O1", "C1", 10.0, "paid")])
    v = DataQualityValidator("orders", OnFailure.WARN).min_row_count(5)
    outcome = v.validate(df)
    rc = [r for r in outcome.results if r.check.startswith("min_row_count")][0]
    assert not rc.passed


def test_valid_batch_passes_clean(spark):
    df = _df(spark, [("O1", "C1", 10.0, "paid"), ("O2", "C2", 20.0, "shipped")])
    v = (
        DataQualityValidator("orders", OnFailure.FAIL)
        .not_null("order_id", "customer_id")
        .non_negative("amount")
        .allowed_values("order_status", ["paid", "shipped"])
        .unique("order_id")
    )
    outcome = v.validate(df)  # should not raise
    assert outcome.passed
    assert outcome.valid.count() == 2


def test_from_config_builds_rules(spark):
    cfg = {
        "on_failure": "quarantine",
        "not_null": ["order_id", "customer_id"],
        "ranges": {"amount": {"min": 0}},
        "allowed_values": {"order_status": ["paid", "shipped"]},
        "unique": ["order_id"],
    }
    df = _df(
        spark,
        [
            ("O1", "C1", 10.0, "paid"),
            ("O2", "C2", -1.0, "paid"),  # bad amount
        ],
    )
    outcome = from_config("orders", cfg).validate(df)
    assert outcome.valid.count() == 1
    assert outcome.quarantine_rows == 1
