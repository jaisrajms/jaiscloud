# Outputs the multi-environment driver asserts against. The names are the ones
# the driver rebuilds for the REST read-backs; `env` and the version count are
# what proves each workspace applied its own instance.

output "env" {
  value = local.env
}

output "bucket_input" {
  value = google_storage_bucket.lake["input"].name
}

output "bucket_output" {
  value = google_storage_bucket.lake["output"].name
}

output "topic" {
  value = google_pubsub_topic.events.id
}

output "audit_topic" {
  value = google_pubsub_topic.audit.id
}

output "secret" {
  value = google_secret_manager_secret.config.id
}

output "secret_version_count" {
  value = tostring(length(google_secret_manager_secret_version.config))
}
