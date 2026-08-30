"""
REST API → Bronze (pattern: rest). Source #2 (partner APIs).

Generic, multi-endpoint puller driven by control.source_objects. Handles:
  * a fixed User-Agent (some CDNs 403 without one — learned the hard way),
  * simple limit/skip pagination,
  * incremental-by-date endpoints (templated {run_date}),
  * activity-level retry on transient HTTP errors.
Lands the raw JSON to `landing/rest/<object>/<date>/` and appends typed rows to
Bronze with ingest metadata.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402
from framework import control  # noqa: E402
from framework.retry import with_retry  # noqa: E402
from framework.runner import run_objects  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")
ADLS = os.environ.get("ADLS", "REPLACE_ME")
LANDING = f"abfss://landing@{ADLS}.dfs.core.windows.net/rest"
UA = {"User-Agent": "ecommerce-pipeline/2.0"}


@with_retry(max_attempts=4, base_delay=2.0)
def _get(url: str) -> dict | list:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310
        return json.loads(r.read().decode("utf-8"))


def _fetch(obj: dict, run_date: str) -> list[dict]:
    opts = obj["options"]
    endpoint = opts["endpoint"].replace("{run_date}", run_date)
    params = json.loads(opts["params"]) if opts.get("params", "").startswith("{") else {}
    pagination = json.loads(opts["pagination"]) if "pagination" in opts else None

    rows: list[dict] = []
    if pagination and pagination.get("type") == "limit_skip":
        size = int(pagination["page_size"])
        skip = 0
        while True:
            q = {**params, pagination["limit_param"]: size, pagination["skip_param"]: skip}
            data = _get(f"{endpoint}?{urllib.parse.urlencode(q)}")
            page = data.get("products", data) if isinstance(data, dict) else data
            if not page:
                break
            rows.extend(page)
            skip += size
            if len(page) < size:
                break
    else:
        url = f"{endpoint}?{urllib.parse.urlencode(params)}" if params else endpoint
        data = _get(url)
        rows = data if isinstance(data, list) else data.get("rates_rows", [data])
    return rows


def ingest_object(spark: SparkSession, obj: dict, run_date: str):
    object_id, bronze = obj["object_id"], obj["target_bronze"]
    rows = _fetch(obj, run_date)
    if not rows:
        print(f"[rest] {object_id}: no rows")
        return
    # land raw json (immutable) then write typed bronze
    raw = f"{LANDING}/{obj['object_name']}/{run_date}/data.json"
    spark.createDataFrame([(json.dumps(rows),)], ["raw"]).coalesce(1).write.mode("overwrite").text(
        raw
    )
    (
        spark.createDataFrame([json.dumps(r) for r in rows], "string")
        .select(F.from_json("value", "map<string,string>").alias("m"))
        .select("m.*")
        .withColumn("_run_date", F.lit(run_date))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze)
    )
    print(f"[rest] {object_id}: {len(rows)} rows -> {bronze}")


def run(spark: SparkSession, run_date: str | None = None):
    run_date = run_date or date.today().isoformat()
    # Endpoints pulled in PARALLEL (framework.runner); per-object isolation + resume
    # unchanged (raw JSON overwritten per date; Silver dedups on PK).
    run_objects(
        spark,
        control.get_objects(spark, "rest", CATALOG),
        lambda s, o: ingest_object(s, o, run_date),
        pipeline="rest_to_bronze",
        catalog=CATALOG,
    )


if __name__ == "__main__":
    rd = sys.argv[1] if len(sys.argv) > 1 else None
    run(get_spark("rest-to-bronze"), rd)
