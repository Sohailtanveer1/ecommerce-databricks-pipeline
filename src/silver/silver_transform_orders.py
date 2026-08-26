"""
Bronze -> Silver transformation for orders.

Reads Bronze incrementally via Delta structured streaming (foreachBatch),
so each run only processes new records since the last checkpoint rather
than reprocessing the full Bronze table.

Deduplication uses ROW_NUMBER() ordered by a recency timestamp -- NOT
dropDuplicates(), which keeps an arbitrary (non-deterministic) row when
duplicates exist. This matters when the same order_id can appear more
than once in an incremental batch (e.g. an update landing alongside the
original insert).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, current_timestamp
from delta.tables import DeltaTable

BRONZE_TABLE = "bronze.orders"
SILVER_TABLE = "silver.orders"
CHECKPOINT_LOCATION = "/mnt/checkpoints/silver_orders"


def clean_and_dedup(bronze_batch_df: DataFrame) -> DataFrame:
    """Applies dedup (latest-wins), null checks, standardization."""
    window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

    return (
        bronze_batch_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .filter(col("order_id").isNotNull())
        .filter(col("order_date").isNotNull())
        .withColumn("order_date", col("order_date").cast("date"))
        .withColumn("processed_timestamp", current_timestamp())
    )


def run_data_quality_checks(df: DataFrame):
    """Fails loudly rather than silently loading bad data downstream."""
    null_customer_ids = df.filter(col("customer_id").isNull()).count()
    if null_customer_ids > 0:
        raise ValueError(
            f"Data quality check failed: {null_customer_ids} rows with null customer_id"
        )

    negative_amounts = df.filter(col("amount") < 0).count()
    if negative_amounts > 0:
        raise ValueError(
            f"Data quality check failed: {negative_amounts} rows with negative amount"
        )


def process_batch(micro_batch_df: DataFrame, batch_id: int, spark: SparkSession):
    silver_batch = clean_and_dedup(micro_batch_df)
    run_data_quality_checks(silver_batch)

    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    (
        silver_table.alias("target")
        .merge(silver_batch.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Batch {batch_id}: merged {silver_batch.count()} rows into {SILVER_TABLE}")


def run(spark: SparkSession):
    bronze_stream = spark.readStream.table(BRONZE_TABLE)

    query = (
        bronze_stream.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(df, batch_id, spark)
        )
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
