# Log Analytics + diagnostic settings so ADF runs, Key Vault access, and
# storage operations are observable/auditable (an interview must-have: "how do
# you monitor and audit this pipeline?").
resource "azurerm_log_analytics_workspace" "law" {
  name                = "law-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days # tunable; changing it is update-in-place
  tags                = local.common_tags
}

resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                       = "diag-adf"
  target_resource_id         = azurerm_data_factory.adf.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.law.id

  enabled_log {
    category_group = "allLogs"
  }
  metric {
    category = "AllMetrics"
  }
}

resource "azurerm_monitor_diagnostic_setting" "kv" {
  name                       = "diag-kv"
  target_resource_id         = azurerm_key_vault.kv.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.law.id

  enabled_log {
    category = "AuditEvent"
  }
  metric {
    category = "AllMetrics"
  }
}
