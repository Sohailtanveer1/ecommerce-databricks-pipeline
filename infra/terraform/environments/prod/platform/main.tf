# prod platform — reads this env's foundation outputs and builds Unity Catalog.
data "terraform_remote_state" "foundation" {
  backend = "local"
  config  = { path = "../foundation/terraform.tfstate" }
}
module "platform" {
  source                         = "../../../modules/platform"
  environment                    = "prod"
  catalog_name                   = "ecommerce_prod"
  adls_account_name              = data.terraform_remote_state.foundation.outputs.adls_account_name
  adls_containers                = data.terraform_remote_state.foundation.outputs.adls_containers
  databricks_access_connector_id = data.terraform_remote_state.foundation.outputs.databricks_access_connector_id
  key_vault_id                   = data.terraform_remote_state.foundation.outputs.key_vault_id
  key_vault_uri                  = data.terraform_remote_state.foundation.outputs.key_vault_uri
  group_data_engineers           = var.group_data_engineers
  group_analysts                 = var.group_analysts
  group_pii_readers              = var.group_pii_readers
}
