# dev foundation — calls the reusable module with dev-specific sizing.
module "foundation" {
  source                   = "../../../modules/foundation"
  project                  = var.project
  environment              = "dev"
  location                 = var.location
  my_object_id             = var.my_object_id
  postgres_jdbc_url        = var.postgres_jdbc_url
  postgres_username        = var.postgres_username
  postgres_password        = var.postgres_password
  fx_api_key               = var.fx_api_key
  alert_email              = var.alert_email
  storage_replication_type = "LRS"
  databricks_sku           = "premium"
  # Exposed as tunables in dev for the change-management exercise (docs/TERRAFORM_CHANGE_EXERCISE.md).
  log_retention_days = var.log_retention_days
  extra_containers   = var.extra_containers
}
