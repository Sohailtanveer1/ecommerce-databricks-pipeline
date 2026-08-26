"""
Silver -> Gold: fact_orders merge.

Straightforward upsert on order_id. Point-in-time correctness against
dim_product's SCD2 versions is handled at query time (see
sql/materialized_views.sql), not baked in here -- this keeps the fact
table simple and avoids re-writing fact rows when a dimension changes.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

SILVER_ORDERS_TABLE = "silver.orders"
GOLD_FACT_ORDERS_TABLE = "gold.fact_orders"


def run(spark: SparkSession):
    silver_orders = spark.read.table(SILVER_ORDERS_TABLE)
    fact_orders = DeltaTable.forName(spark, GOLD_FACT_ORDERS_TABLE)

    (
        fact_orders.alias("target")
        .merge(silver_orders.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"fact_orders merge complete: {silver_orders.count()} rows processed")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
