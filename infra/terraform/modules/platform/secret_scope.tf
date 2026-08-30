# Key Vault-backed secret scope: Databricks reads pipeline secrets (Postgres,
# API keys) straight from Key Vault, so there is ONE secret store shared by ADF
# and Databricks. Secrets are never duplicated into Databricks-managed scopes.
resource "databricks_secret_scope" "kv" {
  name = "kv-${var.environment}"

  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }
}
