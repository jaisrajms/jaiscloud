# Outputs the composition driver asserts against. Everything is a real
# attribute of the created resource, so a name the provider cannot compute is a
# plan-time failure rather than a silent empty assertion.

output "input_bucket" {
  value = google_storage_bucket.input.name
}

output "output_bucket" {
  value = google_storage_bucket.output.name
}

output "staging_bucket" {
  value = google_storage_bucket.staging.name
}

output "job_code_object" {
  value = google_storage_bucket_object.job_code.name
}

output "job_input_object" {
  value = google_storage_bucket_object.job_input.name
}

output "bq_dataset" {
  value = google_bigquery_dataset.datalake.dataset_id
}

output "bq_table" {
  value = google_bigquery_table.events.table_id
}

output "bq_routine" {
  value = google_bigquery_routine.normalize_region.routine_id
}

output "bq_row_access_policy" {
  value = google_bigquery_row_access_policy.regional_only.policy_id
}

output "gke_cluster" {
  value = google_container_cluster.datalake.name
}

output "dataproc_cluster" {
  value = google_dataproc_cluster.datalake.name
}

output "controller_pool" {
  value = local.controller_pool
}

output "spark_pool" {
  value = local.spark_pool
}

output "dataproc_namespace" {
  value = local.namespace
}

# google_dataproc_job.id is projects/P/regions/R/jobs/<server-generated id>.
output "dataproc_job" {
  value = google_dataproc_job.wordcount.id
}

output "metastore_service" {
  value = google_dataproc_metastore_service.datalake.service_id
}

output "metastore_federation" {
  value = google_dataproc_metastore_federation.datalake.federation_id
}

output "job_service_account" {
  value = google_service_account.job.email
}
