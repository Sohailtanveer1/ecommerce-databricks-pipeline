# ============================================================
# Governance grants — least privilege by layer.
#   * Engineers: full on the catalog (build/operate the pipeline).
#   * Analysts:  read-only on GOLD only; Bronze/Silver are denied by default
#     (no grant == no access in Unity Catalog).
# Column masking + row filters are applied in SQL (see sql/governance/).
# ============================================================

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name

  grant {
    principal  = var.group_data_engineers
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "MODIFY", "SELECT"]
  }
  grant {
    principal  = var.group_analysts
    privileges = ["USE_CATALOG"]
  }
}

# Analysts can read GOLD only.
resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["gold"].name}"

  grant {
    principal  = var.group_analysts
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = var.group_data_engineers
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}

# Engineers operate Bronze/Silver; analysts get nothing here (default deny).
resource "databricks_grants" "silver" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["silver"].name}"
  grant {
    principal  = var.group_data_engineers
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}

resource "databricks_grants" "bronze" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["bronze"].name}"
  grant {
    principal  = var.group_data_engineers
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}
