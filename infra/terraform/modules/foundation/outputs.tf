output "resource_group" {
  value = azurerm_resource_group.rg.name
}

output "adls_account_name" {
  value = azurerm_storage_account.adls.name
}

output "adls_dfs_endpoint" {
  value = "abfss://<container>@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.dbw.id
}

output "databricks_access_connector_id" {
  value = azurerm_databricks_access_connector.uc.id
}

output "data_factory_name" {
  value = azurerm_data_factory.adf.name
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}

output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "adls_containers" {
  value = local.containers
}

# Run `terraform output -raw shir_primary_key` to register the Self-Hosted IR
# on your Docker host during SHIR install.
output "shir_primary_key" {
  value     = azurerm_data_factory_integration_runtime_self_hosted.shir.primary_authorization_key
  sensitive = true
}
