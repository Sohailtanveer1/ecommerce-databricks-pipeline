"""
Silver -> Gold: SCD Type 2 merge for dim_product.

Why SCD Type 2, not Type 1: if a product's category is reclassified,
overwriting the dimension in place (Type 1) would silently rewrite
historical reporting -- "revenue by category, June" would change
retroactively if the product moves categories in September. SCD Type 2
preserves a version per change, with an effective date range, so
fact_orders can join to the dimension version that was actually active
on the order date.

Two-step pattern:
  1. Close out the currently-active row if a tracked attribute changed.
  2. Insert a fresh "current" row for new products and just-closed ones.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit
from delta.tables import DeltaTable

SILVER_PRODUCTS_TABLE = "silver.products"
GOLD_DIM_PRODUCT_TABLE = "gold.dim_product"

# Attributes that trigger a new SCD2 version when changed
TRACKED_ATTRIBUTES = ["category", "product_name"]


def build_change_condition() -> str:
    return " OR ".join([f"target.{attr} != source.{attr}" for attr in TRACKED_ATTRIBUTES])


def run(spark: SparkSession):
    silver_products = spark.read.table(SILVER_PRODUCTS_TABLE)
    dim_product = DeltaTable.forName(spark, GOLD_DIM_PRODUCT_TABLE)

    change_condition = build_change_condition()

    # Step 1: close out changed current records
    (
        dim_product.alias("target")
        .merge(
            silver_products.alias("source"),
            "target.product_id = source.product_id AND target.is_current = true",
        )
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                "is_current": "false",
                "effective_end_date": "current_date()",
            },
        )
        .execute()
    )

    # Step 2: insert fresh current-version rows for new products + just-closed ones
    current_gold = spark.read.table(GOLD_DIM_PRODUCT_TABLE).filter(col("is_current") == True)

    new_versions = (
        silver_products.alias("s")
        .join(current_gold.alias("d"), "product_id", "left_anti")
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )

    row_count = new_versions.count()
    if row_count > 0:
        new_versions.write.format("delta").mode("append").saveAsTable(GOLD_DIM_PRODUCT_TABLE)

    print(f"dim_product SCD2 merge complete: {row_count} new/versioned rows inserted")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
