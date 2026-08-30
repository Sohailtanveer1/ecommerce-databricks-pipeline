output "resource_group" { value = module.foundation.resource_group }
output "adls_account_name" { value = module.foundation.adls_account_name }
output "adls_containers" { value = module.foundation.adls_containers }
output "databricks_workspace_url" { value = module.foundation.databricks_workspace_url }
output "databricks_workspace_id" { value = module.foundation.databricks_workspace_id }
output "databricks_access_connector_id" { value = module.foundation.databricks_access_connector_id }
output "key_vault_id" { value = module.foundation.key_vault_id }
output "key_vault_uri" { value = module.foundation.key_vault_uri }
output "data_factory_name" { value = module.foundation.data_factory_name }
output "shir_primary_key" {
  value     = module.foundation.shir_primary_key
  sensitive = true
}
