"""
Source #3 — REST API ingestion: daily FX rates (base USD) from Frankfurter
(https://www.frankfurter.app, keyless/free). Different pattern from the CSV and
JDBC sources: HTTP pull, JSON schema-on-read, incremental-by-date.

Why FX here: orders arrive in USD/EUR/GBP (see the Postgres seed). Gold
normalizes revenue to a single reporting currency by joining to these rates —
a realistic reason for a third source, not a bolt-on.

Flow (medallion-consistent):
  1. Fetch rates for the run date.
  2. Land the RAW json in ADLS `landing/fx/<date>/` (immutable, replayable).
  3. Append typed rows to `bronze.fx_rates` (append-only, with ingest metadata).

Secrets: Frankfurter needs none. If you swap to a keyed API, read the key from
the Key Vault-backed secret scope: dbutils.secrets.get("kv-dev", "fx-api-key").
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.spark_session import get_spark  # noqa: E402

# Set these from a job parameter / widget per environment, or a config lookup.
LANDING_BASE = os.environ.get(
    "FX_LANDING_BASE", "abfss://landing@REPLACE_ME.dfs.core.windows.net/fx"
)
BRONZE_TABLE = os.environ.get("FX_BRONZE_TABLE", "ecommerce_dev.bronze.fx_rates")
BASE_CURRENCY = "USD"
SYMBOLS = ["EUR", "GBP", "INR", "BRL"]
API_URL = "https://api.frankfurter.app/{run_date}"


def fetch_rates(run_date: str) -> dict:
    """Pull rates for a date. Isolated + pure so it is unit-testable with a stub."""
    import urllib.request

    url = f"{API_URL.format(run_date=run_date)}?from={BASE_CURRENCY}&to={','.join(SYMBOLS)}"
    # The API's CDN returns 403 to requests without a User-Agent, so set one
    # explicitly (urllib/Databricks send none by default).
    req = urllib.request.Request(url, headers={"User-Agent": "ecommerce-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - fixed trusted host
        payload = json.loads(resp.read().decode("utf-8"))
    if "rates" not in payload:
        raise ValueError(f"Unexpected FX API response for {run_date}: {payload}")
    return payload


def to_rows(payload: dict) -> list[dict]:
    """Flatten {base, date, rates:{CUR:rate}} into one row per currency, plus the
    identity row base->base = 1.0 so Gold joins never miss USD orders."""
    as_of = payload["date"]
    base = payload.get("base", BASE_CURRENCY)
    rows = [{"as_of_date": as_of, "base_currency": base, "quote_currency": base, "rate": 1.0}]
    for cur, rate in payload["rates"].items():
        rows.append(
            {"as_of_date": as_of, "base_currency": base, "quote_currency": cur, "rate": float(rate)}
        )
    return rows


def run(spark: SparkSession, run_date: str | None = None):
    run_date = run_date or date.today().isoformat()
    payload = fetch_rates(run_date)
    rows = to_rows(payload)

    # 1) land raw json (immutable source of truth for replay/lineage)
    raw_path = f"{LANDING_BASE}/{payload['date']}/rates.json"
    spark.createDataFrame([(json.dumps(payload),)], ["raw"]).coalesce(1).write.mode(
        "overwrite"
    ).text(raw_path)

    # 2) typed append into bronze with ingestion metadata
    df = (
        spark.createDataFrame(rows)
        .withColumn("as_of_date", F.to_date("as_of_date"))
        .withColumn("source_api", F.lit("frankfurter"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    print(f"FX ingest complete for {payload['date']}: {len(rows)} rows -> {BRONZE_TABLE}")


if __name__ == "__main__":
    run_date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y-%m-%d")
    run(get_spark("rest-api-fx"), run_date_arg)
