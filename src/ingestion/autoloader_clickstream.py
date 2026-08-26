"""
Auto Loader ingestion for clickstream events (page views, cart adds, checkouts).

Why Auto Loader here specifically: clickstream events arrive continuously
and unpredictably throughout the day, unlike the scheduled DB extract.
Auto Loader's checkpoint-based incremental file detection (via cloud
storage event notifications) avoids expensive repeated directory listings
and guarantees exactly-once file processing.
"""

import os
import sys

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402

from common.spark_session import get_spark  # noqa: E402

RAW_EVENTS_PATH = "abfss://raw@storageaccount.dfs.core.windows.net/clickstream/"
# For AWS: "s3://raw-bucket/clickstream/"

SCHEMA_LOCATION = "/mnt/schema/clickstream"
CHECKPOINT_LOCATION = "/mnt/checkpoints/bronze_clickstream"
BRONZE_TABLE = "bronze.clickstream_events"


def run_autoloader_stream(spark: SparkSession):
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        # Event Grid (Azure) / S3 event notifications instead of directory listing.
        .option("cloudFiles.useNotifications", "true")
        # Cap files per micro-batch so a large backlog is drained in bounded,
        # right-sized batches instead of one giant skewed one.
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        # Evolve the schema in place rather than failing when new event fields appear.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(RAW_EVENTS_PATH)
    )

    df_with_metadata = df.selectExpr(
        "*",
        "_metadata.file_path as source_file_path",
        "current_timestamp() as ingestion_timestamp",
    )

    query = (
        df_with_metadata.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("append")
        .trigger(availableNow=True)  # process what's available, then stop (scheduled sweep)
        .table(BRONZE_TABLE)
    )

    query.awaitTermination()
    print(f"Auto Loader run complete, wrote to {BRONZE_TABLE}")


if __name__ == "__main__":
    run_autoloader_stream(get_spark("autoloader-clickstream"))
