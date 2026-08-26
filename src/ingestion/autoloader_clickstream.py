"""
Auto Loader ingestion for clickstream events (page views, cart adds, checkouts).

Why Auto Loader here specifically: clickstream events arrive continuously
and unpredictably throughout the day, unlike the scheduled DB extract.
Auto Loader's checkpoint-based incremental file detection (via cloud
storage event notifications) avoids expensive repeated directory listings
and guarantees exactly-once file processing.
"""

from pyspark.sql import SparkSession

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
        .option("cloudFiles.useNotifications", "true")  # Event Grid (Azure) / S3 notifications (AWS)
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
    spark = SparkSession.builder.getOrCreate()
    run_autoloader_stream(spark)
