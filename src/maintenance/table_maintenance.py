"""
Scheduled table-maintenance job: OPTIMIZE (+ liquid clustering / ZORDER),
ANALYZE (statistics), and VACUUM, driven entirely by the ``optimization``
section of pipeline_config.yaml.

Run weekly (see the maintenance job in resources/) — decoupled from the daily
ETL so compaction never blocks ingestion, and matched to the volume actually
written at this scale.
"""

import os
import sys

# Make src/ importable when run as a Databricks spark_python_task.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import optimization  # noqa: E402
from common.optimize import (  # noqa: E402
    analyze_table,
    apply_table_properties,
    optimize_table,
    vacuum_table,
)
from common.spark_session import get_spark  # noqa: E402


def run(spark):
    cfg = optimization()
    tables = cfg.get("tables", {})
    default_props = cfg.get("table_properties")
    vacuum_hours = int(cfg.get("vacuum_retention_hours", 168))

    for table, tcfg in tables.items():
        tcfg = tcfg or {}
        props = {**(default_props or {}), **(tcfg.get("properties") or {})}
        apply_table_properties(spark, table, props)
        optimize_table(
            spark,
            table,
            zorder_cols=tcfg.get("zorder"),
            cluster_cols=tcfg.get("cluster_by"),
        )
        analyze_table(spark, table, columns=tcfg.get("analyze_columns"))
        if tcfg.get("vacuum", True):
            retain = tcfg.get("vacuum_retention_hours", vacuum_hours)
            vacuum_table(spark, table, retention_hours=retain)

    print(f"[maintenance] completed for {len(tables)} table(s)")


if __name__ == "__main__":
    run(get_spark("table-maintenance"))
