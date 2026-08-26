provider "azurerm" {
  features {
    key_vault {
      # Trial-friendly: purge on destroy so re-runs don't hit soft-delete name clashes.
      purge_soft_delete_on_destroy = true
    }
  }
  # subscription_id / tenant_id are taken from `az login` (Azure CLI auth).
}

provider "azuread" {}

provider "random" {}
