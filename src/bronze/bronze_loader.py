"""
Bronze loader: reads raw landed files (from batch extract or Auto Loader
output) and writes them into Bronze Delta tables, append-only.

Bronze is intentionally minimal -- no dedup, no validation, no business
logic. It's the untouched historical record we can always replay from
if a downstream bug is found. Metadata columns (source_file_path,
ingestion_timestamp) are added for traceability only.
"""

import os
import sys

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import current_timestamp, input_file_name  # noqa: E402

from common.spark_session import get_spark  # noqa: E402

RAW_ORDERS_PATH = "abfss://raw@storageaccount.dfs.core.windows.net/erp/orders/"
BRONZE_ORDERS_TABLE = "bronze.orders"


def load_orders_to_bronze(spark: SparkSession, run_date: str):
    raw_path = f"{RAW_ORDERS_PATH}{run_date}/"

    # Cache so the read happens once and feeds both the write and the rowcount
    # (optimizeWrite/autoCompact from the tuned session keep Bronze files sized).
    df = (
        spark.read.json(raw_path)
        .withColumn("source_file_path", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
        .cache()
    )

    df.write.format("delta").mode("append").saveAsTable(BRONZE_ORDERS_TABLE)
    row_count = df.count()  # served from cache, not a re-read of the raw files
    df.unpersist()

    print(f"Loaded {row_count} rows from {raw_path} into {BRONZE_ORDERS_TABLE}")


if __name__ == "__main__":
    import datetime

    spark = get_spark("bronze-loader")
    run_date = datetime.datetime.now().strftime("%Y-%m-%d")
    load_orders_to_bronze(spark, run_date)
