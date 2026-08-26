# ============================================================
# Azure Data Factory: the BATCH ORCHESTRATOR.
# ADF copies from sources into ADLS `landing`, then triggers Databricks jobs
# for Bronze -> Silver -> Gold. Security: system-assigned managed identity +
# Key Vault references. No secret ever lives in a linked-service body.
# ============================================================
resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.base}-${local.suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# Self-Hosted Integration Runtime: the bridge to your LOCAL Docker Postgres.
# A cloud IR cannot reach localhost/host.docker.internal; the SHIR runs on YOUR
# machine and makes the outbound connection. Install it on the Docker host and
# register it with the key from `terraform output shir_primary_key`.
resource "azurerm_data_factory_integration_runtime_self_hosted" "shir" {
  name            = "shir-local-docker"
  data_factory_id = azurerm_data_factory.adf.id
}

# ---- Linked services -------------------------------------------------------

# Key Vault, so other linked services can pull secrets by reference.
resource "azurerm_data_factory_linked_service_key_vault" "kv" {
  name            = "ls_keyvault"
  data_factory_id = azurerm_data_factory.adf.id
  key_vault_id    = azurerm_key_vault.kv.id
}

# ADLS Gen2 via the factory's managed identity (no account key).
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = "https://${azurerm_storage_account.adls.name}.dfs.core.windows.net"
  use_managed_identity = true
}

# Azure Databricks via managed identity; ADF spins a job cluster per run.
resource "azurerm_data_factory_linked_service_azure_databricks" "dbx" {
  name                       = "ls_databricks"
  data_factory_id            = azurerm_data_factory.adf.id
  adb_domain                 = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
  msi_work_space_resource_id = azurerm_databricks_workspace.dbw.id

  new_cluster_config {
    node_type             = "Standard_DS3_v2"
    cluster_version       = "14.3.x-scala2.12"
    min_number_of_workers = 1
    max_number_of_workers = 2
    driver_node_type      = "Standard_DS3_v2"

    custom_tags = local.common_tags
  }
}

# Postgres over the SHIR, password pulled from Key Vault at runtime.
# Custom service is used so we can reference a Key Vault secret AND connect via
# the self-hosted IR — the typed resource supports neither together.
resource "azurerm_data_factory_linked_custom_service" "postgres" {
  name            = "ls_postgres_local"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "PostgreSqlV2"

  type_properties_json = <<-JSON
    {
      "server": "localhost",
      "port": 5432,
      "database": "ecommerce",
      "sslMode": 1,
      "authenticationType": "Basic",
      "username": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "ls_keyvault", "type": "LinkedServiceReference" },
        "secretName": "postgres-username"
      },
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "ls_keyvault", "type": "LinkedServiceReference" },
        "secretName": "postgres-password"
      },
      "connectVia": {
        "referenceName": "${azurerm_data_factory_integration_runtime_self_hosted.shir.name}",
        "type": "IntegrationRuntimeReference"
      }
    }
  JSON

  depends_on = [azurerm_data_factory_linked_service_key_vault.kv]
}
