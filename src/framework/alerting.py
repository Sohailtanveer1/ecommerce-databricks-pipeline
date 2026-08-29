"""
Alerting via a decoupled outbox (control.alerts).

Jobs never call Slack/email/Log Analytics inline (that would couple ingestion to
a flaky HTTP call and lose alerts on failure). Instead they append to
control.alerts; a separate dispatch job (or ADF web activity) reads unsent rows
and delivers them, marking them sent. This makes alerting retryable and auditable.

Delivery targets (configured per env):
  * Azure Monitor / Log Analytics (the metrics + alert-rules backbone — see
    infra/terraform/foundation/monitoring.tf)
  * A webhook (Teams/Slack) for CRITICAL
  * ADF's own on-failure email (pipeline-level, configured in the pipeline JSON)
"""

from __future__ import annotations

import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SEVERITIES = ("INFO", "WARN", "CRITICAL")


def raise_alert(
    spark: SparkSession,
    *,
    severity: str,
    source: str,
    title: str,
    body: str = "",
    catalog: str = "ecommerce_dev",
):
    """Append an alert to the outbox. Non-fatal — never let alerting break a job."""
    assert severity in SEVERITIES, severity
    try:
        row = [(uuid.uuid4().hex, severity, source, title, body, False)]
        cols = ["alert_id", "severity", "source", "title", "body", "sent"]
        (
            spark.createDataFrame(row, cols)
            .withColumn("created_at", F.current_timestamp())
            .withColumn("sent_at", F.lit(None).cast("timestamp"))
            .write.format("delta")
            .mode("append")
            .saveAsTable(f"{catalog}.control.alerts")
        )
        print(f"[alert:{severity}] {source}: {title}")
    except Exception as exc:  # noqa: BLE001
        # Alerting must not mask the real error.
        print(f"[alert] failed to record alert ({exc}); original signal: {title}")


def dispatch_pending(
    spark: SparkSession, webhook_url: str | None = None, catalog: str = "ecommerce_dev"
) -> int:
    """Deliver unsent alerts and mark them sent. Run as a small scheduled job.
    Here we POST CRITICAL/WARN to a webhook if configured; INFO is left for
    Log Analytics dashboards."""
    import json
    import urllib.request

    from delta.tables import DeltaTable

    pending = spark.read.table(f"{catalog}.control.alerts").filter(~F.col("sent")).collect()
    sent_ids = []
    for a in pending:
        delivered = True
        if webhook_url and a["severity"] in ("WARN", "CRITICAL"):
            try:
                payload = json.dumps(
                    {"text": f"[{a['severity']}] {a['title']}\n{a['body']}"}
                ).encode()
                req = urllib.request.Request(
                    webhook_url, data=payload, headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(req, timeout=15)
            except Exception as exc:  # noqa: BLE001
                print(f"[alert] webhook delivery failed for {a['alert_id']}: {exc}")
                delivered = False
        if delivered:
            sent_ids.append(a["alert_id"])

    if sent_ids:
        tgt = DeltaTable.forName(spark, f"{catalog}.control.alerts")
        id_list = ",".join(f"'{i}'" for i in sent_ids)
        tgt.update(
            condition=f"alert_id IN ({id_list})",
            set={"sent": "true", "sent_at": "current_timestamp()"},
        )
    print(f"[alert] dispatched {len(sent_ids)}/{len(pending)} pending alerts")
    return len(sent_ids)
