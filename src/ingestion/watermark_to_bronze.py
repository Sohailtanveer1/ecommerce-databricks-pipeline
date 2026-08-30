"""
Watermark landing → Bronze (pattern: watermark). Source #3 (SQL Server via SHIR).

ADF's Copy activity (over the Self-Hosted IR) has already pulled the incremental
slice `WHERE <watermark_col> > <last_watermark>` into
`landing/sqlserver_erp/<table>/` as Parquet. This job appends new landed files to
Bronze (Auto Loader tracks which files are new) and advances control.watermarks
to the max watermark value seen. Iterates every enabled 'watermark' object.
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402
from framework import control  # noqa: E402
from framework.alerting import raise_alert  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")
ADLS = os.environ.get("ADLS", "REPLACE_ME")
LANDING = f"abfss://landing@{ADLS}.dfs.core.windows.net"
CKPT = f"abfss://checkpoints@{ADLS}.dfs.core.windows.net/watermark"


def _resolve_column(df: DataFrame, wm_col: str | None) -> str | None:
    """Match the configured watermark column to the DataFrame's actual column,
    ignoring case (source/landed casing often differs). Returns the real column
    name or None if absent."""
    if not wm_col:
        return None
    by_lower = {c.lower(): c for c in df.columns}
    return by_lower.get(wm_col.lower())


def ingest_object(spark: SparkSession, obj: dict):
    object_id, table, bronze = obj["object_id"], obj["source_table"], obj["target_bronze"]
    wm_col = obj["watermark_column"]
    src = f"{LANDING}/{obj['source_system']}/{table}/"
    ckpt = f"{CKPT}/{object_id}"
    run_id = control.new_run_id()
    control.log_run(
        spark,
        run_id=run_id,
        pipeline="watermark_to_bronze",
        object_id=object_id,
        layer="bronze",
        status="STARTED",
        catalog=CATALOG,
    )

    def _batch(df: DataFrame, _bid):
        if df.isEmpty():
            return
        enriched = df.withColumn("source_file_path", F.col("_metadata.file_path")).withColumn(
            "ingestion_timestamp", F.current_timestamp()
        )
        enriched.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            bronze
        )
        # Resolve the per-table watermark column case-insensitively (sources spell
        # it lastupdatedate / last_updated_at / updated_ts ...). If it's genuinely
        # absent, alert rather than silently not advancing (that would re-pull forever).
        actual = _resolve_column(df, wm_col)
        if actual:
            mx = df.agg(F.max(actual)).collect()[0][0]
            if mx is not None:
                control.set_watermark(
                    spark, object_id, str(mx), run_id, watermark_column=wm_col, catalog=CATALOG
                )
        elif wm_col:
            raise_alert(
                spark,
                severity="WARN",
                source=object_id,
                title=f"Watermark column '{wm_col}' not found for {object_id}",
                body=f"available columns: {df.columns}",
                catalog=CATALOG,
            )

    q = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{ckpt}/_schema")
        # Permissive: type-mismatched values -> _rescued_data (not a failed load);
        # evolve schema instead of crashing. A wholly corrupt parquet file is an
        # infra issue (ADF wrote it) -> per-object try/except alerts, not silent loss.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(src)
        .writeStream.foreachBatch(_batch)
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    control.log_run(
        spark,
        run_id=run_id,
        pipeline="watermark_to_bronze",
        object_id=object_id,
        layer="bronze",
        status="SUCCEEDED",
        catalog=CATALOG,
    )
    print(f"[watermark] {object_id} -> {bronze}")


def run(spark: SparkSession):
    for obj in control.get_objects(spark, "watermark", CATALOG):
        try:
            ingest_object(spark, obj)
        except Exception as exc:  # noqa: BLE001
            raise_alert(
                spark,
                severity="CRITICAL",
                source=obj["object_id"],
                title=f"Watermark ingest failed: {obj['object_id']}",
                body=str(exc),
                catalog=CATALOG,
            )
            control.log_run(
                spark,
                run_id=control.new_run_id(),
                pipeline="watermark_to_bronze",
                object_id=obj["object_id"],
                layer="bronze",
                status="FAILED",
                error=str(exc),
                catalog=CATALOG,
            )
            print(f"[watermark] FAILED {obj['object_id']}: {exc}")


if __name__ == "__main__":
    run(get_spark("watermark-to-bronze"))
