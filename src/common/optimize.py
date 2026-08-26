"""
Delta Lake optimization helpers.

Two layers of optimization:

1. **Write-time table properties** (:func:`apply_table_properties`) — set once
   per table so every write self-optimizes: optimized writes, auto-compaction,
   right-sized files, deletion vectors (merge/delete rewrite fewer files), and
   change-data-feed where downstream streams need it.

2. **Scheduled maintenance** (:func:`optimize_table`, :func:`vacuum_table`,
   :func:`analyze_table`) — periodic compaction + data-skipping layout via
   OPTIMIZE, statistics for the cost-based optimizer via ANALYZE, and storage
   reclamation via VACUUM.

Data-skipping layout uses **liquid clustering** (``CLUSTER BY``) when available
— the current Databricks best practice that replaces rigid partitioning + a
separate ZORDER, avoiding small-file skew as data grows — and falls back to
``OPTIMIZE ... ZORDER BY`` on runtimes without it.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# Sensible defaults; override per table via apply_table_properties(props=...).
DEFAULT_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.enableDeletionVectors": "true",
    # Only the leading columns need data-skipping stats; cheaper to collect.
    "delta.dataSkippingNumIndexedCols": "8",
}


def apply_table_properties(spark: SparkSession, table: str, props: dict | None = None):
    """Idempotently set Delta table properties (ALTER TABLE ... SET TBLPROPERTIES)."""
    merged = {**DEFAULT_TABLE_PROPERTIES, **(props or {})}
    kv = ", ".join(f"'{k}' = '{v}'" for k, v in merged.items())
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({kv})")
    print(f"[optimize] table properties applied to {table}")


def cluster_by(spark: SparkSession, table: str, columns: list[str]) -> bool:
    """Enable liquid clustering on a table. Returns True on success, False if the
    runtime doesn't support it (caller can fall back to ZORDER)."""
    try:
        spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(columns)})")
        print(f"[optimize] liquid clustering set on {table} by {columns}")
        return True
    except Exception as exc:  # noqa: BLE001 - runtime capability probe
        print(f"[optimize] CLUSTER BY not available for {table} ({exc}); will ZORDER instead")
        return False


def optimize_table(
    spark: SparkSession,
    table: str,
    zorder_cols: list[str] | None = None,
    cluster_cols: list[str] | None = None,
):
    """Compact and lay out a table for data skipping.

    Prefers liquid clustering (plain ``OPTIMIZE`` once ``CLUSTER BY`` is set);
    otherwise uses ``OPTIMIZE ... ZORDER BY``.
    """
    clustered = False
    if cluster_cols:
        clustered = cluster_by(spark, table, cluster_cols)

    if clustered or not zorder_cols:
        spark.sql(f"OPTIMIZE {table}")
    else:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({', '.join(zorder_cols)})")
    print(f"[optimize] OPTIMIZE complete for {table}")


def analyze_table(spark: SparkSession, table: str, columns: list[str] | None = None):
    """Refresh table/column statistics for the cost-based optimizer."""
    target = f"FOR COLUMNS {', '.join(columns)}" if columns else "FOR ALL COLUMNS"
    spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS {target}")
    print(f"[optimize] statistics computed for {table}")


def vacuum_table(spark: SparkSession, table: str, retention_hours: int = 168):
    """Reclaim storage from files no longer referenced, past the retention window."""
    spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
    print(f"[optimize] VACUUM complete for {table} (retain {retention_hours}h)")
