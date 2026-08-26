# Reads the foundation layer's outputs (workspace, storage, access connector,
# key vault) from its local state. Keeping platform (Unity Catalog / workspace
# objects) in a SEPARATE state from the Azure foundation limits blast radius —
# an interview-defensible IaC practice.
data "terraform_remote_state" "foundation" {
  backend = "local"
  config = {
    path = "../foundation/terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# Authenticates to the workspace with your Azure login (az login / OIDC in CI).
# No PAT needed.
provider "databricks" {
  host                        = data.terraform_remote_state.foundation.outputs.databricks_workspace_url
  azure_workspace_resource_id = data.terraform_remote_state.foundation.outputs.databricks_workspace_id
}
