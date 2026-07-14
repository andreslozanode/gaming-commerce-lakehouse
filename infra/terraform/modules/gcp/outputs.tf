output "storage_uris" {
  value = { for layer, bucket in google_storage_bucket.layer : layer => "gs://${bucket.name}" }
}
output "pubsub_topics"    { value = [for t in google_pubsub_topic.events : t.name] }
output "bigquery_dataset" { value = google_bigquery_dataset.marts.dataset_id }
output "databricks_host"  { value = "https://accounts.gcp.databricks.com" }
output "pipeline_sa"      { value = google_service_account.pipeline.email }
output "wif_provider"     { value = google_iam_workload_identity_pool_provider.github.name }
