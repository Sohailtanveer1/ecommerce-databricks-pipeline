"""
Central Spark session builder with performance defaults baked in.

On a Databricks cluster most of these are already on by default, but setting
them explicitly (a) documents intent, (b) makes local/CI runs behave like the
cluster, and (c) guarantees they survive a runtime/default change. Photon is a
cluster-level setting (see the job cluster definition), not a session conf.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# Applied to every session unless overridden. Keys are Spark confs.
PERFORMANCE_CONF = {
    # Adaptive Query Execution: runtime re-planning, skew join handling, and
    # coalescing of tiny shuffle partitions -> fewer, right-sized files.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Delta write-side optimizations: compact small files as they are written
    # and pack them toward a sensible target size.
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    # Auto-broadcast small dimension tables (<= 32MB) to avoid shuffles on joins.
    "spark.sql.autoBroadcastJoinThreshold": str(32 * 1024 * 1024),
    # Prune partitions/files dynamically from the other side of a join.
    "spark.databricks.optimizer.dynamicPartitionPruning": "true",
    # Keep shuffle partitions modest for a small-data workload; AQE coalesces
    # further at runtime. Avoids the 200-tiny-file default blow-up.
    "spark.sql.shuffle.partitions": "64",
    # FAIR scheduling so the parallel per-object runner's concurrent Spark jobs
    # share the cluster fairly instead of queueing FIFO (framework.runner).
    "spark.scheduler.mode": "FAIR",
}


def get_spark(app_name: str = "ecommerce-pipeline", extra_conf: dict | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in {**PERFORMANCE_CONF, **(extra_conf or {})}.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()
