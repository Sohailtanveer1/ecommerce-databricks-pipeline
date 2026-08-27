variable "project" {
  description = "Short project slug used in resource names (lowercase, no spaces)."
  type        = string
  default     = "ecomlake"
}

variable "environment" {
  description = "Environment name (dev/prod). Drives naming and tags."
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region. Pick one your trial has quota in."
  type        = string
  default     = "eastus"
}

variable "my_object_id" {
  description = <<-EOT
    Your Azure AD object id (run: az ad signed-in-user show --query id -o tsv).
    Used to grant you admin on Key Vault so you can read/write secrets.
  EOT
  type        = string
}

variable "postgres_jdbc_url" {
  description = "JDBC URL of the Docker Postgres as seen from the Self-Hosted IR host. The SHIR runs on the Docker host, so it reaches the published port at localhost:5432."
  type        = string
  default     = "jdbc:postgresql://localhost:5432/ecommerce"
}

variable "postgres_username" {
  description = "Postgres username (stored in Key Vault, not in state files where avoidable)."
  type        = string
  default     = "ecom_reader"
  sensitive   = true
}

variable "postgres_password" {
  description = "Postgres password (stored in Key Vault)."
  type        = string
  sensitive   = true
}

variable "fx_api_key" {
  description = "API key/token for the REST source (blank if the chosen API is keyless)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "tags" {
  description = "Extra tags merged onto every resource."
  type        = map(string)
  default     = {}
}

# --- Tunables used by the change-management exercise (docs/TERRAFORM_CHANGE_EXERCISE.md) ---

variable "log_retention_days" {
  description = "Log Analytics retention. Changing this is an UPDATE-IN-PLACE — the demo change."
  type        = number
  default     = 30
}

variable "extra_containers" {
  description = "Additional ADLS containers to create beyond the medallion set. Adding one is an ADD (new resource), not a replacement — the second demo change."
  type        = list(string)
  default     = []
}
