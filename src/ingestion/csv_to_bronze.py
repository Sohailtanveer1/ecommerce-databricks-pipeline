"""
CSV → Bronze (pattern: csv). Source #4 (scheduled files from another team).

Multiple CSV objects, each landing many files over time in its own ADLS folder.
One Auto Loader stream per object (checkpoint = exactly-once file discovery),
with rescued-data capture so a malformed column never drops a row. Iterates
every enabled 'csv' object from control.source_objects.

Production reality handled here: files can arrive late, partial, duplicated, or
with drifting schema — Auto Loader + rescuedData + schema evolution absorb these;
the run log records counts for the freshness/row-count alerts.
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
CKPT = f"abfss://checkpoints@{ADLS}.dfs.core.windows.net/csv"


def ingest_object(spark: SparkSession, obj: dict):
    object_id, bronze = obj["object_id"], obj["target_bronze"]
    path = f"abfss://landing@{ADLS}.dfs.core.windows.net/{obj['options']['path']}"
    ckpt = f"{CKPT}/{object_id}"
    run_id = control.new_run_id()

    def _batch(df: DataFrame, _bid):
        n = df.count()
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(bronze)
        control.log_run(
            spark,
            run_id=run_id,
            pipeline="csv_to_bronze",
            object_id=object_id,
            layer="bronze",
            status="SUCCEEDED",
            rows_written=n,
            catalog=CATALOG,
        )

    q = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{ckpt}/_schema")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(path)
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .writeStream.foreachBatch(_batch)
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    print(f"[csv] {object_id} -> {bronze}")


def run(spark: SparkSession):
    for obj in control.get_objects(spark, "csv", CATALOG):
        try:
            ingest_object(spark, obj)
        except Exception as exc:  # noqa: BLE001
            raise_alert(
                spark,
                severity="WARN",
                source=obj["object_id"],
                title=f"CSV ingest failed: {obj['object_id']}",
                body=str(exc),
                catalog=CATALOG,
            )
            print(f"[csv] FAILED {obj['object_id']}: {exc}")


if __name__ == "__main__":
    run(get_spark("csv-to-bronze"))
