output "cloud" { value = var.cloud }

output "storage_uris" {
  description = "Medallion layer URIs, whichever cloud is active"
  value       = local.is_gcp ? module.gcp[0].storage_uris : module.azure[0].storage_uris
}

output "streaming_endpoint" {
  value     = local.is_gcp ? module.gcp[0].pubsub_topics : module.azure[0].eventhub_namespace
  sensitive = false
}

output "databricks_host" {
  value = local.is_gcp ? module.gcp[0].databricks_host : module.azure[0].databricks_host
}

output "warehouse" {
  value = local.is_gcp ? module.gcp[0].bigquery_dataset : module.azure[0].synapse_workspace
}
