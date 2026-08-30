terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.116" }
    azuread = { source = "hashicorp/azuread", version = "~> 2.53" }
    random  = { source = "hashicorp/random", version = "~> 3.6" }
  }
  # Per-env state. Local by default (this folder). For shared/CI use azurerm:
  # backend "azurerm" { resource_group_name = "rg-tfstate", storage_account_name = "sttfstate<uniq>", container_name = "tfstate", key = "uat/foundation.tfstate" }
}
