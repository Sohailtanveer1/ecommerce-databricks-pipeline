"""
Medallion orchestrator: Bronze (already landed by the ingestion jobs) -> Silver
-> Gold -> Gold views. Runs after all four ingestion pipelines succeed.

Each step logs to control.pipeline_runs and validates via the DQ framework
(common.data_quality) with quarantine. TODO for tomorrow: generalize the Silver
and Gold transforms to be metadata-driven per object (like ingestion) — for now
this wires the existing per-entity transforms and is the place to add new ones.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.spark_session import get_spark  # noqa: E402
from framework import control  # noqa: E402
from framework.alerting import raise_alert  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")

# (module, run) pairs for each transform step, in order. Extend as layers grow.
STEPS = [
    ("silver.silver_generic", "bronze -> silver (metadata-driven, quarantine bad rows)"),
    ("gold.gold_generic", "gold (metadata-driven SCD2 dims + facts, point-in-time keys)"),
]


def run(spark):
    run_id = control.new_run_id()
    for module_path, label in STEPS:
        import importlib

        try:
            mod = importlib.import_module(module_path)
            mod.run(spark)
            control.log_run(
                spark,
                run_id=run_id,
                pipeline="run_medallion",
                object_id=module_path,
                layer="silver_gold",
                status="SUCCEEDED",
                catalog=CATALOG,
            )
            print(f"[medallion] ok: {label}")
        except Exception as exc:  # noqa: BLE001
            raise_alert(
                spark,
                severity="CRITICAL",
                source=module_path,
                title=f"Medallion step failed: {label}",
                body=str(exc),
                catalog=CATALOG,
            )
            control.log_run(
                spark,
                run_id=run_id,
                pipeline="run_medallion",
                object_id=module_path,
                layer="silver_gold",
                status="FAILED",
                error=str(exc),
                catalog=CATALOG,
            )
            raise


if __name__ == "__main__":
    # Make the layer packages importable by dotted path.
    _src = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for layer in ("silver", "gold"):
        sys.path.append(os.path.join(_src, layer))
    run(get_spark("run-medallion"))
