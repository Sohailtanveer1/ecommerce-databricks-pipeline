# Cost-control cluster policy: caps autoscale, forces spot/terminate-on-idle,
# pins the runtime. Jobs (via the Databricks Asset Bundle) reference this so no
# one can accidentally launch an expensive always-on cluster on the trial.
resource "databricks_cluster_policy" "cost_capped" {
  name = "cost-capped-${var.environment}"

  definition = jsonencode({
    "spark_version" : {
      "type" : "fixed",
      "value" : "14.3.x-scala2.12"
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["Standard_DS3_v2", "Standard_DS4_v2"],
      "defaultValue" : "Standard_DS3_v2"
    },
    "autoscale.max_workers" : {
      "type" : "range",
      "maxValue" : 4,
      "defaultValue" : 2
    },
    "autotermination_minutes" : {
      "type" : "fixed",
      "value" : 20
    },
    "azure_attributes.availability" : {
      "type" : "fixed",
      "value" : "SPOT_WITH_FALLBACK_AZURE"
    }
  })
}

output "cluster_policy_id" {
  value = databricks_cluster_policy.cost_capped.id
}
