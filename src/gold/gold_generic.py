"""
Gold layer, metadata-driven (config/gold_model.yaml):

* **Dimensions** — SCD Type 2 with a generated **surrogate key** (`<entity>_sk`)
  and the source **natural key** (`<entity>_id`). A change in any tracked
  attribute closes the current version (`is_current=false`, `effective_end_date`)
  and opens a new one. Deterministic surrogate = xxhash64(dim, natural_key,
  effective_start_date).
* **Facts** — resolve each dimension to the surrogate key that was **active at the
  event date** (point-in-time join on `effective_start_date..effective_end_date`),
  then MERGE measures + degenerate dims at the declared grain.

All output goes through the standardization layer, so Gold obeys the naming/type
conventions (docs/NAMING_CONVENTIONS.md). Add a dim/fact in yaml, not code.
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402
from framework import control  # noqa: E402
from framework.alerting import raise_alert  # noqa: E402
from framework.standardize import standardize  # noqa: E402

_MODEL = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "config",
    "gold_model.yaml",
)


def load_model(path: str | None = None) -> dict:
    with open(path or _MODEL) as fh:
        return yaml.safe_load(fh)


def _entity(dim_name: str) -> str:
    return dim_name[4:] if dim_name.startswith("dim_") else dim_name


def build_dimension(spark: SparkSession, d: dict, catalog: str):
    name, nk, tracked, attrs = d["name"], d["natural_key"], d["scd2"], d["attributes"]
    entity = _entity(name)
    sk = f"{entity}_sk"
    dim_table = f"{catalog}.gold.{name}"
    src = standardize(spark.read.table(d["source"]).select(nk, *attrs))

    def as_versions(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("effective_start_date", F.current_date())
            .withColumn("effective_end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn(sk, F.xxhash64(F.lit(name), F.col(nk), F.col("effective_start_date")))
            .select(sk, nk, *attrs, "effective_start_date", "effective_end_date", "is_current")
        )

    if not spark.catalog.tableExists(dim_table):
        as_versions(src.limit(0)).write.format("delta").saveAsTable(dim_table)

    # 1) close current versions whose tracked attributes changed (null-safe compare)
    change_cond = " OR ".join(f"NOT (t.{a} <=> s.{a})" for a in tracked)
    (
        DeltaTable.forName(spark, dim_table)
        .alias("t")
        .merge(src.alias("s"), f"t.{nk} = s.{nk} AND t.is_current = true")
        .whenMatchedUpdate(
            condition=change_cond,
            set={"is_current": "false", "effective_end_date": "current_date()"},
        )
        .execute()
    )

    # 2) insert fresh versions for new naturals + just-closed ones (left-anti current)
    current_keys = spark.read.table(dim_table).filter(F.col("is_current")).select(nk)
    new = as_versions(src.join(F.broadcast(current_keys), nk, "left_anti"))
    if not new.isEmpty():
        new.write.format("delta").mode("append").saveAsTable(dim_table)
    print(f"[gold] dimension {name} built ({dim_table})")


def build_fact(spark: SparkSession, f: dict, catalog: str):
    name = f["name"]
    fact_table = f"{catalog}.gold.{name}"
    event_date = f["event_date"]
    df = spark.read.table(f["source"])

    fk_sks = []
    for dref in f.get("dimensions", []):
        dim, nk = dref["dim"], dref["natural_key"]
        sk = f"{_entity(dim)}_sk"
        dd = spark.read.table(f"{catalog}.gold.{dim}").select(
            F.col(nk).alias("_k"),
            F.col(sk),
            F.col("effective_start_date").alias("_es"),
            F.col("effective_end_date").alias("_ee"),
        )
        # point-in-time: the dim version active on the event date
        df = df.join(
            dd,
            (F.col(nk) == F.col("_k"))
            & (F.col(event_date) >= F.col("_es"))
            & (F.col(event_date) <= F.coalesce(F.col("_ee"), F.current_date())),
            "left",
        ).drop("_k", "_es", "_ee")
        fk_sks.append(sk)

    keep = (
        list(f["grain"])
        + [d["natural_key"] for d in f.get("dimensions", [])]
        + fk_sks
        + list(f["measures"])
        + list(f.get("degenerate", []))
    )
    seen: set[str] = set()
    cols = [c for c in keep if not (c in seen or seen.add(c))]
    fact = standardize(df.select(*cols)).withColumn("processed_timestamp", F.current_timestamp())

    if not spark.catalog.tableExists(fact_table):
        fact.limit(0).write.format("delta").saveAsTable(fact_table)
    cond = " AND ".join(f"t.{g} = s.{g}" for g in f["grain"])
    (
        DeltaTable.forName(spark, fact_table)
        .alias("t")
        .merge(fact.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[gold] fact {name} built ({fact_table})")


def run(spark: SparkSession):
    model = load_model()
    catalog = model.get("catalog", "ecommerce_dev")
    for d in model.get("dimensions", []):
        try:
            build_dimension(spark, d, catalog)
        except Exception as exc:  # noqa: BLE001
            raise_alert(
                spark,
                severity="CRITICAL",
                source=d["name"],
                title=f"Gold dimension failed: {d['name']}",
                body=str(exc),
                catalog=catalog,
            )
            control.log_run(
                spark,
                run_id=control.new_run_id(),
                pipeline="gold_generic",
                object_id=d["name"],
                layer="gold",
                status="FAILED",
                error=str(exc),
                catalog=catalog,
            )
            raise
    for fct in model.get("facts", []):
        try:
            build_fact(spark, fct, catalog)
        except Exception as exc:  # noqa: BLE001
            raise_alert(
                spark,
                severity="CRITICAL",
                source=fct["name"],
                title=f"Gold fact failed: {fct['name']}",
                body=str(exc),
                catalog=catalog,
            )
            control.log_run(
                spark,
                run_id=control.new_run_id(),
                pipeline="gold_generic",
                object_id=fct["name"],
                layer="gold",
                status="FAILED",
                error=str(exc),
                catalog=catalog,
            )
            raise


if __name__ == "__main__":
    run(get_spark("gold-generic"))
