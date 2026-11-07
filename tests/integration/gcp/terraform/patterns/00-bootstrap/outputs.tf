# Outputs consumed by run.sh's REST assertions: the values Terraform computed
# for the created project and the resources that reference it.

output "project_id" {
  value = google_project.bootstrap.project_id
}

output "project_number" {
  value = google_project.bootstrap.number
}

output "billing_account" {
  value = google_project.bootstrap.billing_account
}

output "enabled_services" {
  value = [for s in google_project_service.apis : s.service]
}

output "service_account_email" {
  value = google_service_account.stack.email
}

output "bucket_name" {
  value = google_storage_bucket.stack.name
}

output "bucket_self_link" {
  value = google_storage_bucket.stack.self_link
}

output "topic_id" {
  value = google_pubsub_topic.stack.id
}

output "subscription_id" {
  value = google_pubsub_subscription.stack.id
}

output "secret_id" {
  value = google_secret_manager_secret.stack.id
}

output "secret_version" {
  value = google_secret_manager_secret_version.stack.version
}

output "key_ring_id" {
  value = google_kms_key_ring.stack.id
}

output "crypto_key_id" {
  value = google_kms_crypto_key.stack.id
}
