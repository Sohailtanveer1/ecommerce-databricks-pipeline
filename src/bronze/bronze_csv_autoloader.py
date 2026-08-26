"""
Source #1 — CSV files landing in ADLS Gen2, ingested with Auto Loader.

Auto Loader (cloudFiles) incrementally discovers new CSVs via a checkpoint, so
re-running only picks up files not yet processed — no manual "which files did I
already load?" bookkeeping. `trigger(availableNow=True)` makes it a scheduled
batch sweep (cheap) rather than an always-on stream.

Schema is inferred + persisted at `schemaLocation`, with rescued-data capture so
a malformed/extra column never silently drops a row.
"""

from __future__ import annotations

import os
import sys

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402

# Per-environment; set via job params. REPLACE_ME is the ADLS account name.
LANDING_PATH = os.environ.get(
    "CSV_LANDING", "abfss://landing@REPLACE_ME.dfs.core.windows.net/marketing/"
)
SCHEMA_LOCATION = os.environ.get(
    "CSV_SCHEMA_LOC", "abfss://checkpoints@REPLACE_ME.dfs.core.windows.net/schema/marketing"
)
CHECKPOINT = os.environ.get(
    "CSV_CHECKPOINT", "abfss://checkpoints@REPLACE_ME.dfs.core.windows.net/bronze_marketing"
)
BRONZE_TABLE = os.environ.get("CSV_BRONZE_TABLE", "ecommerce_dev.bronze.marketing_ad_spend")


def run(spark: SparkSession):
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Rows with unparseable/extra fields are captured, not dropped.
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_PATH)
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()
    print(f"CSV Auto Loader run complete -> {BRONZE_TABLE}")


if __name__ == "__main__":
    run(get_spark("bronze-csv-autoloader"))
