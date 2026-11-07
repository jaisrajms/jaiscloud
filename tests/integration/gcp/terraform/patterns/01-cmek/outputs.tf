# Outputs consumed by run.sh's REST assertions: the KMS resource names the data
# resources were configured with and the resources that reference them.

output "data_key" {
  value = local.data_key
}

output "primary_key_id" {
  value = google_kms_crypto_key.primary.id
}

output "rotated_key_id" {
  value = google_kms_crypto_key.rotated.id
}

output "global_key_id" {
  value = google_kms_crypto_key.global.id
}

output "bucket_name" {
  value = google_storage_bucket.cmek.name
}

output "dataset_id" {
  value = google_bigquery_dataset.cmek.dataset_id
}

output "secret_id" {
  value = google_secret_manager_secret.cmek.id
}

output "secret_version" {
  value = google_secret_manager_secret_version.cmek.version
}
