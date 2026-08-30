provider "azurerm" {
  features {}
}
# Authenticates to this env's workspace (from the foundation state) via az login / OIDC.
provider "databricks" {
  host                        = data.terraform_remote_state.foundation.outputs.databricks_workspace_url
  azure_workspace_resource_id = data.terraform_remote_state.foundation.outputs.databricks_workspace_id
}
