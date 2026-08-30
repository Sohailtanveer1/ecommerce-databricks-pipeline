"""
Remediation entry job: re-validate quarantined rows and promote the now-valid
ones to Silver. Run on-demand after fixing a source/file or adjusting DQ rules
(e.g. the other team re-drops a corrected CSV, or a bad allowed-values list is
widened). Optionally scope to one object via argv.

  python reprocess_quarantine.py                    # all objects
  python reprocess_quarantine.py partner_files.supplier_costs
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.spark_session import get_spark  # noqa: E402
from framework import control, quarantine  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")


def run(spark, only: str | None = None):
    total = 0
    for o in control.load_sources():
        obj = o.__dict__
        if only and obj["object_id"] != only:
            continue
        total += quarantine.reprocess(spark, obj, control.new_run_id(), CATALOG)
    print(f"[reprocess] promoted {total} remediated row(s) total")


if __name__ == "__main__":
    run(get_spark("reprocess-quarantine"), sys.argv[1] if len(sys.argv) > 1 else None)
