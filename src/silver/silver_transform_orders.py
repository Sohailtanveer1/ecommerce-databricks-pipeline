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

Validation is delegated to the config-driven DataQualityValidator
(common.data_quality). Bad rows are quarantined to a side table (per config)
instead of silently dropped or hard-failing the whole batch.
"""

import os
import sys

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql.functions import col, current_timestamp, row_number  # noqa: E402

from common.config import dq_rules  # noqa: E402
from common.data_quality import from_config, write_quarantine  # noqa: E402
from common.spark_session import get_spark  # noqa: E402

BRONZE_TABLE = "bronze.orders"
SILVER_TABLE = "silver.orders"
CHECKPOINT_LOCATION = "/mnt/checkpoints/silver_orders"


def clean_and_dedup(bronze_batch_df: DataFrame) -> DataFrame:
    """Latest-wins dedup + standardization. Null-key rows that can't be merged
    are dropped here; richer validation happens in the DQ step."""
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


def validate(df: DataFrame) -> DataFrame:
    """Run config-driven data-quality checks; quarantine violating rows and
    return only the rows safe to publish to Silver."""
    rules = dq_rules("orders")
    if not rules:
        return df
    outcome = from_config("orders", rules).validate(df)
    if rules.get("quarantine_table"):
        write_quarantine(outcome, rules["quarantine_table"])
    return outcome.valid


def process_batch(micro_batch_df: DataFrame, batch_id: int, spark: SparkSession):
    silver_batch = validate(clean_and_dedup(micro_batch_df))

    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    (
        silver_table.alias("target")
        .merge(silver_batch.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    # No extra .count() here: MERGE emits its own rowcount metrics, and the DQ
    # step already reported validated/quarantined totals for this batch.
    print(f"Batch {batch_id}: merged validated rows into {SILVER_TABLE}")


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
    run(get_spark("silver-transform-orders"))
