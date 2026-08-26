# Databricks workspace (Premium tier is required for Unity Catalog + table ACLs).
# Creating the workspace is free; you only pay DBUs when a cluster runs.
resource "azurerm_databricks_workspace" "dbw" {
  name                = "dbw-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  tags = local.common_tags
}
