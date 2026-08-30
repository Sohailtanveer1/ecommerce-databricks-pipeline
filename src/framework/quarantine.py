"""
Quarantine (dead-letter) mechanism — mitigate bad records without failing the
batch or silently dropping them.

Two classes of "bad" are captured, per object, into
`<catalog>.quarantine.<system>__<object>`:
  1. **File-level malformed** — rows Auto Loader could not parse cleanly, captured
     in the `_rescued_data` column (extra/typed-wrong fields). Reason = 'rescued'.
  2. **Rule failures** — rows that fail the object's data-quality rules
     (not-null, ranges, allowed-values, regex, ...). Reason = 'dq'.

Good rows continue to Silver; bad rows land in quarantine tagged with WHAT failed
(`_dq_errors`), WHEN, and the run id — so they're queryable, alertable, and
**remediable**: fix the source/file (or the rule) and call `reprocess()` to
re-validate and promote the now-valid rows to Silver, marking them resolved.

Counts flow to control.dq_results + control.pipeline_runs.rows_quarantined, and a
high quarantine ratio raises an alert.
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.data_quality import from_config  # noqa: E402
from framework import control  # noqa: E402
from framework.alerting import raise_alert  # noqa: E402

# Fraction of a batch quarantined above which we escalate to an alert.
QUARANTINE_ALERT_RATIO = float(os.environ.get("QUARANTINE_ALERT_RATIO", "0.10"))

_META = [
    "_dq_errors",
    "_dq_reason",
    "_dq_dataset",
    "_dq_run_id",
    "_dq_ts",
    "_dq_resolved",
    "_dq_resolved_ts",
]


def quarantine_table(obj: dict) -> str:
    return obj["target_quarantine"]


def _stamp(df: DataFrame, *, object_id: str, run_id: str, reason: str) -> DataFrame:
    """Add the standard quarantine metadata columns (idempotent)."""
    if "_dq_errors" not in df.columns:
        df = df.withColumn("_dq_errors", F.array().cast("array<string>"))
    return (
        df.withColumn("_dq_reason", F.lit(reason))
        .withColumn("_dq_dataset", F.lit(object_id))
        .withColumn("_dq_run_id", F.lit(run_id))
        .withColumn("_dq_ts", F.current_timestamp())
        .withColumn("_dq_resolved", F.lit(False))
        .withColumn("_dq_resolved_ts", F.lit(None).cast("timestamp"))
    )


def _write_quarantine(df: DataFrame, table: str) -> int:
    n = df.count()
    if n:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    return n


def _log_dq(spark, run_id, object_id, results, catalog):
    if not results:
        return
    rows = [
        (run_id, object_id, r.check, bool(r.passed), int(r.violations), r.detail) for r in results
    ]
    (
        spark.createDataFrame(
            rows, ["run_id", "object_id", "check_name", "passed", "violations", "detail"]
        )
        .withColumn("checked_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.control.dq_results")
    )


def classify(df: DataFrame, obj: dict, run_id: str):
    """Pure split (no writes/logging, so it's unit-testable): returns
    (valid_df, [quarantine_frames], dq_results). Captures file-malformed rows
    (_rescued_data) and DQ-rule failures separately."""
    object_id = obj["object_id"]
    valid = df
    frames: list[DataFrame] = []
    results: list = []

    # 1) file-level malformed rows (Auto Loader rescued data)
    if "_rescued_data" in df.columns:
        bad = df.filter(F.col("_rescued_data").isNotNull())
        valid = valid.filter(F.col("_rescued_data").isNull())
        frames.append(
            _stamp(
                bad.withColumn("_dq_errors", F.array(F.lit("rescued_data"))),
                object_id=object_id,
                run_id=run_id,
                reason="rescued",
            )
        )

    # 2) rule failures (config-driven DQ, quarantine mode)
    rules = obj.get("dq") or {}
    if rules:
        outcome = from_config(object_id, {**rules, "on_failure": "quarantine"}).validate(valid)
        results = outcome.results
        valid = outcome.valid
        if outcome.quarantined is not None and outcome.quarantine_rows:
            frames.append(
                _stamp(outcome.quarantined, object_id=object_id, run_id=run_id, reason="dq")
            )
    return valid, frames, results


def apply(
    spark: SparkSession, df: DataFrame, obj: dict, run_id: str, catalog: str = "ecommerce_dev"
) -> tuple[DataFrame, int]:
    """Split df into (valid -> return, bad -> quarantine table). Logs + alerts.
    `obj` is a control/registry object dict carrying `dq` rules + target_quarantine."""
    object_id = obj["object_id"]
    qtable = quarantine_table(obj)
    total = df.count()

    valid, frames, results = classify(df, obj, run_id)
    _log_dq(spark, run_id, object_id, results, catalog)
    quarantined = sum(_write_quarantine(f, qtable) for f in frames)

    # record + escalate
    control.log_run(
        spark,
        run_id=run_id,
        pipeline="quarantine",
        object_id=object_id,
        layer="silver",
        status="SUCCEEDED",
        rows_read=total,
        rows_written=(total - quarantined),
        rows_quarantined=quarantined,
        catalog=catalog,
    )
    if total and quarantined / total >= QUARANTINE_ALERT_RATIO:
        raise_alert(
            spark,
            severity="WARN",
            source=object_id,
            title=f"High quarantine rate for {object_id}",
            body=f"{quarantined}/{total} rows quarantined -> {qtable}",
            catalog=catalog,
        )
    print(f"[quarantine] {object_id}: {quarantined}/{total} quarantined -> {qtable}")
    return valid, quarantined


def reprocess(spark: SparkSession, obj: dict, run_id: str, catalog: str = "ecommerce_dev") -> int:
    """Remediation loop: re-validate unresolved quarantined rows against the
    CURRENT rules; promote the now-valid ones to Silver and mark them resolved.
    Run after a bad file is re-dropped/corrected or a rule is fixed."""
    from delta.tables import DeltaTable

    qtable = quarantine_table(obj)
    if not spark.catalog.tableExists(qtable):
        print(f"[reprocess] {obj['object_id']}: no quarantine table")
        return 0

    pending = spark.read.table(qtable).filter(~F.col("_dq_resolved"))
    if pending.isEmpty():
        print(f"[reprocess] {obj['object_id']}: nothing pending")
        return 0

    clean = pending.drop(*[c for c in _META if c in pending.columns])
    rules = obj.get("dq") or {}
    outcome = (
        from_config(obj["object_id"], {**rules, "on_failure": "quarantine"}).validate(clean)
        if rules
        else None
    )
    promotable = outcome.valid if outcome else clean

    n = promotable.count()
    if n:
        promotable.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            obj["target_silver"]
        )
        # mark the promoted rows resolved (by run + not-yet-resolved)
        DeltaTable.forName(spark, qtable).update(
            condition="NOT _dq_resolved",
            set={"_dq_resolved": "true", "_dq_resolved_ts": "current_timestamp()"},
        )
    print(f"[reprocess] {obj['object_id']}: promoted {n} remediated row(s) to Silver")
    control.log_run(
        spark,
        run_id=run_id,
        pipeline="reprocess_quarantine",
        object_id=obj["object_id"],
        layer="silver",
        status="SUCCEEDED",
        rows_written=n,
        catalog=catalog,
    )
    return n
