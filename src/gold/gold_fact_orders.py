"""
Silver -> Gold: fact_orders merge.

Straightforward upsert on order_id. Point-in-time correctness against
dim_product's SCD2 versions is handled at query time (see
sql/materialized_views.sql), not baked in here -- this keeps the fact
table simple and avoids re-writing fact rows when a dimension changes.
"""

import os
import sys

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from common.spark_session import get_spark  # noqa: E402

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
    # MERGE reports its own inserted/updated metrics in the Delta history; no
    # extra full-table .count() action needed here.
    print(f"fact_orders merge complete into {GOLD_FACT_ORDERS_TABLE}")


if __name__ == "__main__":
    run(get_spark("gold-fact-orders"))
