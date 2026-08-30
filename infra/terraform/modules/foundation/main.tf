data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  # Globally-unique-friendly base. Storage/KeyVault/ADF names have tight rules,
  # so we derive compact names from project+env+suffix.
  base   = "${var.project}${var.environment}"
  suffix = random_string.suffix.result

  common_tags = merge({
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    owner       = "data-engineering"
  }, var.tags)

  # ADLS Gen2 containers (filesystems) that back the medallion + control planes.
  # `extra_containers` lets the change-exercise add one via tfvars (an ADD, not a
  # replacement) — see docs/TERRAFORM_CHANGE_EXERCISE.md.
  containers = distinct(concat(
    ["landing", "bronze", "silver", "gold", "quarantine", "checkpoints"],
    var.extra_containers,
  ))
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.base}"
  location = var.location
  tags     = local.common_tags
}
