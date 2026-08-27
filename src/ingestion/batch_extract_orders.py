"""
Batch extract: pulls incremental rows from the transactional database
(orders, customers) based on an `updated_at` watermark, and lands them
as raw files in cloud storage for Bronze ingestion.

Why batch over CDC: for a small e-commerce company's data volume, a
scheduled incremental pull is simpler and cheaper to operate than
standing up CDC infrastructure (Debezium/Fivetran), unless near-real-time
order visibility becomes a genuine business requirement.
"""

import os
import sys
from datetime import datetime

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import col  # noqa: E402

from common.spark_session import get_spark  # noqa: E402

RAW_LANDING_PATH = "abfss://raw@storageaccount.dfs.core.windows.net/erp/orders/"
# For AWS: "s3://raw-bucket/erp/orders/"

WATERMARK_TABLE = "control.ingestion_watermarks"


def _validate_watermark(watermark: str) -> str:
    """Ensures a watermark is a real timestamp before it is interpolated into
    the JDBC pushdown query. Guards against a malformed/poisoned control-table
    value turning into SQL injection against the source database."""
    # Accept the epoch sentinel and any parseable timestamp; reject anything else.
    if watermark == "1970-01-01 00:00:00":
        return watermark
    try:
        datetime.strptime(watermark, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Refusing to use non-timestamp watermark: {watermark!r}") from exc
    return watermark


def get_last_watermark(spark: SparkSession, source_name: str) -> str:
    """Reads the last successfully processed watermark for a given source."""
    # Parameterized column comparison (not an f-string predicate) so source_name
    # can never be interpreted as SQL.
    result = spark.read.table(WATERMARK_TABLE).filter(col("source_name") == source_name).collect()
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
    last_watermark = _validate_watermark(get_last_watermark(spark, "orders"))

    query = f"""
        (SELECT order_id, customer_id, product_id, order_date, amount,
                currency, order_status, updated_at
         FROM orders
         WHERE updated_at > '{last_watermark}') t
    """

    # Cache once so the single JDBC pull feeds the count, the max-watermark, and
    # the write -- instead of re-executing the source query three times.
    new_orders_df = spark.read.jdbc(url=db_url, table=query, properties=db_props).cache()

    # count + max watermark in a single aggregation pass.
    stats = new_orders_df.selectExpr(
        "count(*) as cnt", "max(updated_at) as max_updated_at"
    ).collect()[0]
    row_count = stats["cnt"]
    if row_count == 0:
        print(f"No new orders since watermark {last_watermark}")
        new_orders_df.unpersist()
        return

    run_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{RAW_LANDING_PATH}{run_date}/"
    new_orders_df.write.mode("append").json(output_path)

    new_watermark = stats["max_updated_at"]
    save_watermark(spark, "orders", str(new_watermark))
    new_orders_df.unpersist()

    print(f"Extracted {row_count} new/updated orders. New watermark: {new_watermark}")


if __name__ == "__main__":
    spark = get_spark("batch-extract-orders")

    db_url = dbutils.secrets.get(scope="kv-scope", key="orders-db-jdbc-url")
    db_props = {
        "user": dbutils.secrets.get(scope="kv-scope", key="orders-db-user"),
        "password": dbutils.secrets.get(scope="kv-scope", key="orders-db-password"),
        "driver": "org.postgresql.Driver",
    }

    extract_orders(spark, db_url, db_props)
