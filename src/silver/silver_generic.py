"""
Bronze → Silver, metadata-driven, with quarantine. For every registered object:
  read Bronze -> dedup (latest per PK) -> quarantine bad rows (file-malformed +
  DQ failures) -> MERGE the clean rows into Silver.

This is where "mitigate bad records from a file or any source" happens: nothing
is dropped or fails the batch — good rows flow to Silver, bad rows go to the
object's quarantine table for review + remediation (framework.quarantine).
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402
from framework import control, quarantine  # noqa: E402
from framework.alerting import raise_alert  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")


def _dedup(df: DataFrame, pks: list[str]) -> DataFrame:
    """Latest-wins per PK using ingestion_timestamp (deterministic, not arbitrary)."""
    if not pks or "ingestion_timestamp" not in df.columns:
        return df
    w = Window.partitionBy(*pks).orderBy(F.col("ingestion_timestamp").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _merge_silver(spark: SparkSession, df: DataFrame, table: str, pks: list[str]):
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)
    if pks:
        cond = " AND ".join([f"t.{k} = s.{k}" for k in pks])
        (
            DeltaTable.forName(spark, table)
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


def transform_object(spark: SparkSession, obj: dict):
    bronze, silver = obj["target_bronze"], obj["target_silver"]
    pks = obj.get("primary_keys") or []
    run_id = control.new_run_id()
    if not spark.catalog.tableExists(bronze):
        print(f"[silver] {obj['object_id']}: no bronze yet, skip")
        return

    src = _dedup(spark.read.table(bronze), pks).withColumn(
        "processed_timestamp", F.current_timestamp()
    )
    valid, _ = quarantine.apply(spark, src, obj, run_id, CATALOG)  # bad rows -> quarantine
    # never let quarantine metadata leak into Silver
    valid = valid.drop(*[c for c in ("_rescued_data",) if c in valid.columns])
    _merge_silver(spark, valid, silver, pks)
    print(f"[silver] {obj['object_id']} -> {silver}")


def run(spark: SparkSession):
    for obj in [o.__dict__ for o in control.load_sources()]:
        if not obj.get("enabled", True):
            continue
        try:
            transform_object(spark, obj)
        except Exception as exc:  # noqa: BLE001 - isolate per object
            raise_alert(
                spark,
                severity="CRITICAL",
                source=obj["object_id"],
                title=f"Silver transform failed: {obj['object_id']}",
                body=str(exc),
                catalog=CATALOG,
            )
            control.log_run(
                spark,
                run_id=control.new_run_id(),
                pipeline="silver_generic",
                object_id=obj["object_id"],
                layer="silver",
                status="FAILED",
                error=str(exc),
                catalog=CATALOG,
            )
            print(f"[silver] FAILED {obj['object_id']}: {exc}")


if __name__ == "__main__":
    run(get_spark("silver-generic"))
