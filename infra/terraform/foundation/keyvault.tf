# Key Vault — single source of truth for secrets. ADF and Databricks reference
# secrets here; no credential is ever committed or put in a linked-service body.
resource "azurerm_key_vault" "kv" {
  name                      = "kv-${local.base}-${local.suffix}"
  resource_group_name       = azurerm_resource_group.rg.name
  location                  = azurerm_resource_group.rg.location
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  sku_name                  = "standard"
  purge_protection_enabled  = false # trial-friendly; enable in prod
  enable_rbac_authorization = true  # RBAC over legacy access policies (best practice)

  tags = local.common_tags
}

# You: manage secrets.
resource "azurerm_role_assignment" "kv_admin_me" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = var.my_object_id
}

# ADF managed identity: read secrets referenced by linked services.
resource "azurerm_role_assignment" "kv_reader_adf" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_key_vault_secret" "pg_user" {
  name         = "postgres-username"
  value        = var.postgres_username
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_admin_me]
  tags         = local.common_tags
}

resource "azurerm_key_vault_secret" "pg_password" {
  name         = "postgres-password"
  value        = var.postgres_password
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_admin_me]
  tags         = local.common_tags
}

resource "azurerm_key_vault_secret" "pg_jdbc_url" {
  name         = "postgres-jdbc-url"
  value        = var.postgres_jdbc_url
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_admin_me]
  tags         = local.common_tags
}

resource "azurerm_key_vault_secret" "fx_api_key" {
  name         = "fx-api-key"
  value        = var.fx_api_key != "" ? var.fx_api_key : "none"
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_admin_me]
  tags         = local.common_tags
}
