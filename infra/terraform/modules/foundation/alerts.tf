# Monitoring & alerting: an action group (who gets notified) + alert rules on
# ADF pipeline failures. Data-quality/freshness alerts flow through the
# control.alerts outbox (dispatched from Databricks) into the same action group
# via its webhook. (var.alert_email is declared in variables.tf)

resource "azurerm_monitor_action_group" "main" {
  name                = "ag-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  short_name          = "ecomalert"

  email_receiver {
    name          = "data-team"
    email_address = var.alert_email
  }
  # Add a webhook_receiver here to fan CRITICAL alerts into Teams/Slack.

  tags = local.common_tags
}

# Fire when any ADF pipeline run fails.
resource "azurerm_monitor_metric_alert" "adf_pipeline_failed" {
  name                = "alert-adf-pipeline-failed"
  resource_group_name = azurerm_resource_group.rg.name
  scopes              = [azurerm_data_factory.adf.id]
  description         = "An ADF pipeline run failed."
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.main.id
  }

  tags = local.common_tags
}
