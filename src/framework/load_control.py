"""
Entry job: sync config/sources.yaml into the Delta control tables. Run after the
control DDL and whenever sources.yaml changes (it's a MERGE, so it's idempotent).
This is what makes the registry version-controlled AND queryable by ADF.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.spark_session import get_spark  # noqa: E402
from framework import control  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")

if __name__ == "__main__":
    spark = get_spark("load-control")
    n = control.sync_registry(spark, CATALOG)
    print(f"[load_control] registry synced: {n} objects")
