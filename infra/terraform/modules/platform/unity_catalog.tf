# ============================================================
# Unity Catalog: storage credential -> external locations -> catalog/schemas.
# Assumes a UC metastore exists in the region and is assigned to the workspace
# (Azure auto-provisions one for new workspaces; if not, create + assign it in
# the account console first — see infra/terraform/README.md).
# ============================================================

locals {
  abfss = {
    for c in var.adls_containers :
    c => "abfss://${c}@${var.adls_account_name}.dfs.core.windows.net/"
  }
}

# The managed identity (access connector) UC uses to reach ADLS — no keys/SAS.
resource "databricks_storage_credential" "adls" {
  name = "sc_${var.environment}_adls"
  azure_managed_identity {
    access_connector_id = var.databricks_access_connector_id
  }
  comment = "Managed-identity credential for the ${var.environment} lakehouse."
}

# One external location per medallion container.
resource "databricks_external_location" "loc" {
  for_each        = local.abfss
  name            = "el_${var.environment}_${each.key}"
  url             = each.value
  credential_name = databricks_storage_credential.adls.name
  comment         = "External location for ${each.key}."
}

resource "databricks_catalog" "this" {
  name          = var.catalog_name
  comment       = "E-commerce lakehouse (${var.environment})."
  storage_root  = local.abfss["bronze"] # managed-table root; external paths still used per layer
  force_destroy = true                  # dev: allow destroy even with tables present
  depends_on    = [databricks_external_location.loc]
}

resource "databricks_schema" "layers" {
  for_each      = toset(["bronze", "silver", "gold", "governance", "control", "quarantine"])
  catalog_name  = databricks_catalog.this.name
  name          = each.value
  comment       = "${each.value} layer."
  force_destroy = true
}
