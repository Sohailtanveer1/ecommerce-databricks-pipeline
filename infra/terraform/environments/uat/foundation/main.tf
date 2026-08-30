# uat foundation — calls the reusable module with uat-specific sizing.
module "foundation" {
  source                   = "../../../modules/foundation"
  project                  = var.project
  environment              = "uat"
  location                 = var.location
  my_object_id             = var.my_object_id
  postgres_jdbc_url        = var.postgres_jdbc_url
  postgres_username        = var.postgres_username
  postgres_password        = var.postgres_password
  fx_api_key               = var.fx_api_key
  alert_email              = var.alert_email
  storage_replication_type = "LRS"
  databricks_sku           = "premium"
  log_retention_days       = 60
}
