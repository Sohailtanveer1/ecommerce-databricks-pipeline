"""
Batch extract: pulls incremental rows from the transactional database
(orders, customers) based on an `updated_at` watermark, and lands them
as raw files in cloud storage for Bronze ingestion.

Why batch over CDC: for a small e-commerce company's data volume, a
scheduled incremental pull is simpler and cheaper to operate than
standing up CDC infrastructure (Debezium/Fivetran), unless near-real-time
order visibility becomes a genuine business requirement.
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json

RAW_LANDING_PATH = "abfss://raw@storageaccount.dfs.core.windows.net/erp/orders/"
# For AWS: "s3://raw-bucket/erp/orders/"

WATERMARK_TABLE = "control.ingestion_watermarks"


def get_last_watermark(spark: SparkSession, source_name: str) -> str:
    """Reads the last successfully processed watermark for a given source."""
    result = (
        spark.read.table(WATERMARK_TABLE)
        .filter(f"source_name = '{source_name}'")
        .collect()
    )
    if not result:
        return "1970-01-01 00:00:00"  # first-ever run
    return result[0]["last_watermark"]


def save_watermark(spark: SparkSession, source_name: str, new_watermark: str):
    """Persists the new high-watermark after a successful run."""
    from delta.tables import DeltaTable

    watermark_df = spark.createDataFrame(
        [(source_name, new_watermark, datetime.now())],
        ["source_name", "last_watermark", "updated_at"],
    )
    watermark_table = DeltaTable.forName(spark, WATERMARK_TABLE)
    (
        watermark_table.alias("target")
        .merge(watermark_df.alias("source"), "target.source_name = source.source_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def extract_orders(spark: SparkSession, db_url: str, db_props: dict):
    last_watermark = get_last_watermark(spark, "orders")

    query = f"""
        (SELECT order_id, customer_id, product_id, order_date, amount,
                order_status, updated_at
         FROM orders
         WHERE updated_at > '{last_watermark}') t
    """

    new_orders_df = spark.read.jdbc(url=db_url, table=query, properties=db_props)

    row_count = new_orders_df.count()
    if row_count == 0:
        print(f"No new orders since watermark {last_watermark}")
        return

    run_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{RAW_LANDING_PATH}{run_date}/"
    new_orders_df.write.mode("append").json(output_path)

    new_watermark = (
        new_orders_df.selectExpr("max(updated_at) as max_updated_at")
        .collect()[0]["max_updated_at"]
    )
    save_watermark(spark, "orders", str(new_watermark))

    print(f"Extracted {row_count} new/updated orders. New watermark: {new_watermark}")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    db_url = dbutils.secrets.get(scope="kv-scope", key="orders-db-jdbc-url")
    db_props = {
        "user": dbutils.secrets.get(scope="kv-scope", key="orders-db-user"),
        "password": dbutils.secrets.get(scope="kv-scope", key="orders-db-password"),
        "driver": "org.postgresql.Driver",
    }

    extract_orders(spark, db_url, db_props)
