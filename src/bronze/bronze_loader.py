"""
Bronze loader: reads raw landed files (from batch extract or Auto Loader
output) and writes them into Bronze Delta tables, append-only.

Bronze is intentionally minimal -- no dedup, no validation, no business
logic. It's the untouched historical record we can always replay from
if a downstream bug is found. Metadata columns (source_file_path,
ingestion_timestamp) are added for traceability only.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

RAW_ORDERS_PATH = "abfss://raw@storageaccount.dfs.core.windows.net/erp/orders/"
BRONZE_ORDERS_TABLE = "bronze.orders"


def load_orders_to_bronze(spark: SparkSession, run_date: str):
    raw_path = f"{RAW_ORDERS_PATH}{run_date}/"

    df = (
        spark.read.json(raw_path)
        .withColumn("source_file_path", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    row_count = df.count()
    df.write.format("delta").mode("append").saveAsTable(BRONZE_ORDERS_TABLE)

    print(f"Loaded {row_count} rows from {raw_path} into {BRONZE_ORDERS_TABLE}")


if __name__ == "__main__":
    import datetime

    spark = SparkSession.builder.getOrCreate()
    run_date = datetime.datetime.now().strftime("%Y-%m-%d")
    load_orders_to_bronze(spark, run_date)
