# Outputs of composition pattern 02. run.sh reads these back from Terraform
# state and asserts the API agrees; they are also the handles it uses to build
# the REST URLs.

output "topic_id" {
  description = "Fully qualified Pub/Sub topic name every producer writes to."
  value       = google_pubsub_topic.events.id
}

output "subscription_id" {
  description = "Push subscription name (the arm that delivers to the sink)."
  value       = google_pubsub_subscription.push.id
}

output "job_id" {
  description = "Cloud Scheduler job resource name (projects/.../locations/.../jobs/...)."
  value       = google_cloud_scheduler_job.tick.id
}

output "scheduler_payload" {
  description = "Base64 payload the scheduler publishes; the push message echoes it."
  value       = local.scheduler_payload
}

output "bucket_name" {
  description = "Bucket the Eventarc trigger and the storage-triggered function observe."
  value       = google_storage_bucket.events.name
}

output "source_object" {
  description = "Object holding the functions' deployment archive."
  value       = google_storage_bucket_object.source.name
}

output "trigger_id" {
  description = "Eventarc trigger resource name (projects/.../locations/.../triggers/...)."
  value       = google_eventarc_trigger.gcs.id
}

output "service_account_email" {
  description = "Service account the Eventarc trigger runs as."
  value       = google_service_account.eventarc.email
}

output "pubsub_function" {
  description = "Function id triggered by a message on the topic."
  value       = google_cloudfunctions_function.pubsub.name
}

output "storage_function" {
  description = "Function id triggered by an object finalize in the bucket."
  value       = google_cloudfunctions_function.storage.name
}
