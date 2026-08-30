variable "environment" {
  type    = string
  default = "dev"
}

variable "catalog_name" {
  description = "Unity Catalog catalog for this environment."
  type        = string
  default     = "ecommerce_dev"
}

# AAD groups for governance grants. Create these in Entra ID (or reuse existing)
# and add them to the Databricks account. Defaults are placeholders.
variable "group_data_engineers" {
  type    = string
  default = "data-engineers"
}

variable "group_analysts" {
  type    = string
  default = "data-analysts"
}

variable "group_pii_readers" {
  type    = string
  default = "pii-authorized"
}

# --- Foundation outputs, passed in by the environment root ---
variable "adls_account_name" {
  type = string
}

variable "adls_containers" {
  type = list(string)
}

variable "databricks_access_connector_id" {
  type = string
}

variable "key_vault_id" {
  type = string
}

variable "key_vault_uri" {
  type = string
}
