# ADLS Gen2 (hierarchical namespace) — the lakehouse storage for all layers.
resource "azurerm_storage_account" "adls" {
  name                     = "st${local.base}${local.suffix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # cheapest; interview note: use ZRS/GRS in prod
  account_kind             = "StorageV2"
  is_hns_enabled           = true # <- makes it ADLS Gen2 (Data Lake)

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true # Databricks UC uses the access connector; ADF uses MI. Keys stay off the data path.

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    versioning_enabled = true
  }

  tags = local.common_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "containers" {
  for_each           = toset(local.containers)
  name               = each.value
  storage_account_id = azurerm_storage_account.adls.id
}

# Managed identity Databricks Unity Catalog uses to reach ADLS (no keys/SAS).
resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbw-ac-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
  tags = local.common_tags
}

# UC access connector -> data-plane RBAC on the lake.
resource "azurerm_role_assignment" "uc_blob_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# ADF managed identity -> data-plane RBAC on the lake (so Copy activities write landing/).
resource "azurerm_role_assignment" "adf_blob_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}
