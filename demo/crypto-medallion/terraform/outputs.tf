# Outputs consumed by scripts/democtl.py to build the runtime env and the
# in-cluster manifests.

output "project" {
  value = var.project
}

output "region" {
  value = var.region
}

output "buckets" {
  value = var.buckets
}

output "service_account_email" {
  value = google_service_account.runner.email
}

output "feed_secret_id" {
  value = google_secret_manager_secret.feed.secret_id
}

output "kms_key" {
  value = local.key_resource
}

output "events_topic" {
  value = google_pubsub_topic.events.id
}

output "events_subscription" {
  value = google_pubsub_subscription.events.id
}
