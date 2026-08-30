output "catalog" {
  value = databricks_catalog.this.name
}

output "schemas" {
  value = [for s in databricks_schema.layers : s.name]
}

output "secret_scope" {
  value = databricks_secret_scope.kv.name
}

output "external_locations" {
  value = { for k, v in databricks_external_location.loc : k => v.url }
}
