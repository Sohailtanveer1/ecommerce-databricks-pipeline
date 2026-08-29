"""
CDC → Bronze (pattern: cdc). Source #1 (Postgres via Debezium).

Debezium writes change events (JSON envelopes) to ADLS
`landing/cdc/<source_system>/<table>/` — one file stream per table. Each envelope
has {op: c|r|u|d, before, after, ts_ms, source}. Auto Loader tails the landing
folder; for each micro-batch we apply changes to the Bronze Delta table with a
MERGE keyed on the primary key, so Bronze reflects the current row state
(inserts/updates upsert, deletes tombstone via __deleted flag).

Metadata-driven: iterates every enabled object of pattern 'cdc' from
control.source_objects. Idempotent + exactly-once via the Auto Loader checkpoint.
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402
from framework import control  # noqa: E402
from framework.alerting import raise_alert  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")
LANDING_CDC = os.environ.get(
    "LANDING_CDC", f"abfss://landing@{os.environ.get('ADLS','REPLACE_ME')}.dfs.core.windows.net/cdc"
)
CHECKPOINT_BASE = os.environ.get(
    "CHECKPOINT_BASE",
    f"abfss://checkpoints@{os.environ.get('ADLS','REPLACE_ME')}.dfs.core.windows.net/cdc",
)


def _apply_changes(batch_df: DataFrame, bronze_table: str, pks: list[str], spark: SparkSession):
    """MERGE one micro-batch of Debezium envelopes into Bronze (latest-per-key)."""
    # Keep the latest event per PK in this batch (ts_ms desc), flatten `after`.
    from pyspark.sql import Window

    w = Window.partitionBy(*[F.col(f"after.{k}").alias(k) for k in pks]).orderBy(
        F.col("ts_ms").desc()
    )
    latest = (
        batch_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .withColumn("__op", F.col("op"))
        .withColumn("__deleted", F.col("op") == F.lit("d"))
        .withColumn("__source_ts", (F.col("ts_ms") / 1000).cast("timestamp"))
    )
    # For deletes Debezium puts the key in `before`; coalesce before/after.
    payload = F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    flat = (
        latest.select(
            payload.alias("p"),
            "__op",
            "__deleted",
            "__source_ts",
        )
        .select("p.*", "__op", "__deleted", "__source_ts")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    if not spark.catalog.tableExists(bronze_table):
        flat.limit(0).write.format("delta").saveAsTable(bronze_table)

    cond = " AND ".join([f"t.{k} = s.{k}" for k in pks])
    (
        DeltaTable.forName(spark, bronze_table)
        .alias("t")
        .merge(flat.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def ingest_object(spark: SparkSession, obj: dict):
    object_id = obj["object_id"]
    table = obj["source_table"]
    pks = obj["primary_keys"]
    bronze = obj["target_bronze"]
    src_path = f"{LANDING_CDC}/{obj['source_system']}/{table}/"
    ckpt = f"{CHECKPOINT_BASE}/{object_id}"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{ckpt}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(src_path)
    )
    q = (
        stream.writeStream.foreachBatch(lambda df, _bid: _apply_changes(df, bronze, pks, spark))
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    print(f"[cdc] {object_id} -> {bronze}")


def run(spark: SparkSession):
    objects = control.get_objects(spark, "cdc", CATALOG)
    print(f"[cdc] {len(objects)} object(s) to process")
    for obj in objects:
        try:
            ingest_object(spark, obj)
        except Exception as exc:  # noqa: BLE001 - isolate per-object failure
            # One bad table must not fail the whole batch (pipeline-level retry
            # will reprocess only this object from the run log).
            raise_alert(
                spark,
                severity="CRITICAL",
                source=obj["object_id"],
                title=f"CDC ingest failed: {obj['object_id']}",
                body=str(exc),
                catalog=CATALOG,
            )
            control.log_run(
                spark,
                run_id=control.new_run_id(),
                pipeline="cdc_to_bronze",
                object_id=obj["object_id"],
                layer="bronze",
                status="FAILED",
                error=str(exc),
                catalog=CATALOG,
            )
            print(f"[cdc] FAILED {obj['object_id']}: {exc}")


if __name__ == "__main__":
    run(get_spark("cdc-to-bronze"))
