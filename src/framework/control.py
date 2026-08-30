"""
Control-plane access: load the object registry from config/sources.yaml into
the Delta control tables, and read/update watermarks + run log at runtime.

This is what makes the pipeline metadata-driven: jobs ask the control tables
"what objects of pattern X should I process, and from what watermark?" instead
of hardcoding tables.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_SOURCES_YAML = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "config",
    "sources.yaml",
)


@dataclass
class SourceObject:
    object_id: str
    source_system: str
    pattern: str
    connection: str | None
    object_name: str
    source_schema: str | None
    source_table: str | None
    watermark_column: str | None
    primary_keys: list[str]
    load_type: str | None
    target_bronze: str
    target_silver: str
    target_quarantine: str
    options: dict = field(default_factory=dict)
    dq: dict = field(default_factory=dict)
    standardize: dict = field(default_factory=dict)
    enabled: bool = True


def _flatten(cfg: dict) -> list[SourceObject]:
    """Turn the nested sources.yaml into flat SourceObject rows."""
    catalog = cfg.get("defaults", {}).get("catalog", "ecommerce_dev")
    default_enabled = cfg.get("defaults", {}).get("enabled", True)
    out: list[SourceObject] = []
    for src in cfg.get("sources", []):
        system = src["source_system"]
        pattern = src["pattern"]
        conn = src.get("connection")
        for obj in src["objects"]:
            name = obj.get("name") or obj.get("table")
            options = {
                k: str(v)
                for k, v in obj.items()
                if k
                not in (
                    "name",
                    "table",
                    "schema",
                    "watermark_column",
                    "primary_keys",
                    "load_type",
                    "dq",
                    "standardize",
                )
            }
            out.append(
                SourceObject(
                    object_id=f"{system}.{name}",
                    source_system=system,
                    pattern=pattern,
                    connection=conn,
                    object_name=name,
                    source_schema=obj.get("schema"),
                    source_table=obj.get("table"),
                    watermark_column=obj.get("watermark_column"),
                    primary_keys=obj.get("primary_keys", []),
                    load_type=obj.get("load_type", "incremental"),
                    target_bronze=f"{catalog}.bronze.{system}__{name}",
                    target_silver=f"{catalog}.silver.{system}__{name}",
                    target_quarantine=f"{catalog}.quarantine.{system}__{name}",
                    options=options,
                    dq=obj.get("dq", {}),
                    standardize=obj.get("standardize", {}),
                    enabled=default_enabled,
                )
            )
    return out


def load_sources(path: str | None = None) -> list[SourceObject]:
    with open(path or _SOURCES_YAML) as fh:
        return _flatten(yaml.safe_load(fh))


def sync_registry(spark: SparkSession, catalog: str = "ecommerce_dev", path: str | None = None):
    """Upsert config/sources.yaml into control.source_objects (idempotent MERGE)."""
    from delta.tables import DeltaTable

    objs = load_sources(path)
    rows = [
        (
            o.object_id,
            o.source_system,
            o.pattern,
            o.connection,
            o.object_name,
            o.source_schema,
            o.source_table,
            o.watermark_column,
            o.primary_keys,
            o.load_type,
            o.target_bronze,
            o.options,
            o.enabled,
        )
        for o in objs
    ]
    cols = [
        "object_id",
        "source_system",
        "pattern",
        "connection",
        "object_name",
        "source_schema",
        "source_table",
        "watermark_column",
        "primary_keys",
        "load_type",
        "target_bronze",
        "options",
        "enabled",
    ]
    df = spark.createDataFrame(rows, cols).withColumn("updated_at", F.current_timestamp())

    tgt = DeltaTable.forName(spark, f"{catalog}.control.source_objects")
    (
        tgt.alias("t")
        .merge(df.alias("s"), "t.object_id = s.object_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[control] synced {len(rows)} objects into {catalog}.control.source_objects")
    return len(rows)


def get_objects(spark: SparkSession, pattern: str, catalog: str = "ecommerce_dev") -> list[dict]:
    """Return enabled objects for a pattern — the list ADF/jobs iterate over."""
    df = (
        spark.read.table(f"{catalog}.control.source_objects")
        .filter((F.col("pattern") == pattern) & (F.col("enabled")))
        .orderBy("object_id")
    )
    return [r.asDict() for r in df.collect()]


def get_watermark(spark: SparkSession, object_id: str, catalog: str = "ecommerce_dev") -> str:
    df = (
        spark.read.table(f"{catalog}.control.watermarks")
        .filter(F.col("object_id") == object_id)
        .select("last_watermark_value")
        .collect()
    )
    return df[0][0] if df and df[0][0] is not None else "1900-01-01 00:00:00"


def set_watermark(
    spark: SparkSession,
    object_id: str,
    value: str,
    run_id: str,
    status: str = "SUCCEEDED",
    watermark_column: str | None = None,
    catalog: str = "ecommerce_dev",
):
    from delta.tables import DeltaTable

    df = (
        spark.createDataFrame(
            [(object_id, watermark_column, str(value), run_id, status)],
            ["object_id", "watermark_column", "last_watermark_value", "last_run_id", "last_status"],
        )
        .withColumn("last_run_ts", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    tgt = DeltaTable.forName(spark, f"{catalog}.control.watermarks")
    (
        tgt.alias("t")
        .merge(df.alias("s"), "t.object_id = s.object_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def log_run(
    spark: SparkSession,
    *,
    run_id: str,
    pipeline: str,
    object_id: str | None,
    layer: str,
    status: str,
    attempt: int = 1,
    rows_read: int | None = None,
    rows_written: int | None = None,
    rows_quarantined: int | None = None,
    error: str | None = None,
    duration_sec: float | None = None,
    parent_run_id: str | None = None,
    catalog: str = "ecommerce_dev",
):
    """Append a run-log row. Call at start (STARTED) and end (SUCCEEDED/FAILED)."""
    row = [
        (
            run_id,
            parent_run_id,
            pipeline,
            object_id,
            layer,
            status,
            attempt,
            rows_read,
            rows_written,
            rows_quarantined,
            error,
            duration_sec,
        )
    ]
    cols = [
        "run_id",
        "parent_run_id",
        "pipeline",
        "object_id",
        "layer",
        "status",
        "attempt",
        "rows_read",
        "rows_written",
        "rows_quarantined",
        "error_message",
        "duration_sec",
    ]
    (
        spark.createDataFrame(row, cols)
        .withColumn("started_at", F.current_timestamp())
        .withColumn("ended_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.control.pipeline_runs")
    )


def new_run_id() -> str:
    return uuid.uuid4().hex
