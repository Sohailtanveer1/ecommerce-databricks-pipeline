"""Entry job: deliver pending alerts from the control.alerts outbox. Runs at the
end of the master pipeline (on Succeeded OR Failed) so failures always notify.
Webhook URL comes from the Key Vault-backed secret scope."""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.spark_session import get_spark  # noqa: E402
from framework.alerting import dispatch_pending  # noqa: E402

CATALOG = os.environ.get("CATALOG", "ecommerce_dev")

if __name__ == "__main__":
    spark = get_spark("dispatch-alerts")
    # In Databricks: webhook = dbutils.secrets.get("kv-dev", "alerts-webhook-url")
    webhook = os.environ.get("ALERTS_WEBHOOK_URL")
    dispatch_pending(spark, webhook_url=webhook, catalog=CATALOG)
