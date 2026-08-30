"""
Parallel per-object runner.

Runs each object's ingestion concurrently over the SHARED cluster via a driver-
side thread pool. Spark supports concurrent job submission from multiple threads;
with the FAIR scheduler (set in common.spark_session) they share the cluster
fairly, so N objects finish in ~max(object_time) instead of sum(object_time).

**Error handling and resume behaviour are unchanged** (the explicit requirement):
  * per-object isolation — each object runs in its own try/except; one failure is
    logged FAILED + alerted and does NOT stop the others (no thread kills the pool);
  * idempotency/exactly-once — each object keeps its OWN Auto Loader checkpoint,
    watermark, and MERGE target, so parallelism changes nothing about correctness;
  * resume — failures are recorded in control.pipeline_runs, so pipeline-level
    retry still reprocesses only the objects marked FAILED.

Parallelism is a pure throughput optimization layered on top of the existing
sequential semantics.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession

from framework import control
from framework.alerting import raise_alert

DEFAULT_MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "4"))


def run_objects(
    spark: SparkSession,
    objects: list[dict],
    ingest_fn: Callable[[SparkSession, dict], None],
    *,
    pipeline: str,
    catalog: str = "ecommerce_dev",
    layer: str = "bronze",
    max_workers: int | None = None,
) -> dict[str, str]:
    """Run ingest_fn(spark, obj) for each object in parallel. Returns
    {object_id: SUCCEEDED|FAILED}. Never raises for a single-object failure."""
    max_workers = max_workers or DEFAULT_MAX_PARALLEL
    if not objects:
        print(f"[{pipeline}] no objects to process")
        return {}
    # Group this pipeline's concurrent Spark jobs into one FAIR scheduler pool.
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pipeline)

    def _one(obj: dict) -> tuple[str, str, str | None]:
        oid = obj["object_id"]
        try:
            ingest_fn(spark, obj)
            return oid, "SUCCEEDED", None
        except Exception as exc:  # noqa: BLE001 - deliberate per-object isolation boundary
            raise_alert(
                spark,
                severity="CRITICAL",
                source=oid,
                title=f"{pipeline} failed: {oid}",
                body=str(exc),
                catalog=catalog,
            )
            control.log_run(
                spark,
                run_id=control.new_run_id(),
                pipeline=pipeline,
                object_id=oid,
                layer=layer,
                status="FAILED",
                error=str(exc),
                catalog=catalog,
            )
            return oid, "FAILED", str(exc)

    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=pipeline) as pool:
        for fut in as_completed([pool.submit(_one, o) for o in objects]):
            oid, status, err = fut.result()
            results[oid] = status
            print(f"[{pipeline}] {oid}: {status}" + (f" -- {err}" if err else ""))

    ok = sum(1 for s in results.values() if s == "SUCCEEDED")
    print(
        f"[{pipeline}] {ok}/{len(results)} objects succeeded (parallel, max_workers={max_workers})"
    )
    return results
