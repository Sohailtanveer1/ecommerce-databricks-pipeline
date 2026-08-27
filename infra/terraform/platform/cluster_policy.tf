# Cost-control cluster policy for a FREE TRIAL: single-node, smallest nodes,
# spot, short auto-terminate. Prevents accidentally launching multi-node/always-on
# clusters that blow the trial's tiny vCPU quota. (Prefer serverless SQL/jobs on
# the trial; this only bounds the case where a real Spark cluster is needed.)
resource "databricks_cluster_policy" "cost_capped" {
  name = "cost-capped-${var.environment}"

  definition = jsonencode({
    "spark_version" : {
      "type" : "fixed",
      "value" : "14.3.x-scala2.12"
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["Standard_DS3_v2", "Standard_D4ds_v5", "Standard_DS4_v2"],
      "defaultValue" : "Standard_DS3_v2"
    },
    # Cap at a single worker so autoscale can't balloon on the trial.
    "autoscale.max_workers" : {
      "type" : "range",
      "maxValue" : 1,
      "defaultValue" : 1
    },
    "autotermination_minutes" : {
      "type" : "fixed",
      "value" : 10
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
