"""
Unit tests for the quarantine classifier (framework.quarantine.classify) —
the metadata-driven split of good vs bad rows. `spark` fixture + import paths
come from conftest.py (which adds src/ and src/framework via the layer loop).
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from framework.quarantine import classify  # noqa: E402

COLS = ["order_id", "customer_id", "amount", "order_status", "_rescued_data"]
OBJ = {
    "object_id": "sqlserver_erp.orders",
    "dq": {
        "not_null": ["order_id", "customer_id"],
        "ranges": {"amount": {"min": 0}},
    },
}


def _df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_rescued_rows_are_quarantined(spark):
    df = _df(
        spark,
        [
            ("O1", "C1", 10.0, "paid", None),  # clean
            ("O2", "C2", 20.0, "paid", "{bad csv}"),  # file-malformed
        ],
    )
    valid, frames, _ = classify(df, OBJ, "run1")
    assert valid.count() == 1
    assert sum(f.count() for f in frames) == 1
    reasons = {r["_dq_reason"] for f in frames for r in f.select("_dq_reason").collect()}
    assert "rescued" in reasons


def test_dq_failures_are_quarantined(spark):
    df = _df(
        spark,
        [
            ("O1", "C1", 10.0, "paid", None),  # clean
            ("O2", None, 20.0, "paid", None),  # null customer_id
            ("O3", "C3", -5.0, "paid", None),  # negative amount
        ],
    )
    valid, frames, results = classify(df, OBJ, "run2")
    assert valid.count() == 1
    assert sum(f.count() for f in frames) == 2
    assert any(not r.passed for r in results)


def test_rescued_and_dq_both_captured(spark):
    df = _df(
        spark,
        [
            ("O1", "C1", 10.0, "paid", None),  # clean
            ("O2", "C2", 20.0, "paid", "{bad}"),  # rescued
            ("O3", None, 5.0, "paid", None),  # dq fail
        ],
    )
    valid, frames, _ = classify(df, OBJ, "run3")
    assert valid.count() == 1
    assert sum(f.count() for f in frames) == 2


def test_clean_batch_has_no_quarantine(spark):
    df = _df(spark, [("O1", "C1", 10.0, "paid", None), ("O2", "C2", 20.0, "paid", None)])
    valid, frames, _ = classify(df, OBJ, "run4")
    assert valid.count() == 2
    assert sum(f.count() for f in frames) == 0


def test_stamp_columns_present(spark):
    df = _df(spark, [("O2", "C2", 20.0, "paid", "{bad}")])
    _, frames, _ = classify(df, OBJ, "run5")
    cols = frames[0].columns
    for c in ("_dq_errors", "_dq_reason", "_dq_dataset", "_dq_run_id", "_dq_ts", "_dq_resolved"):
        assert c in cols
