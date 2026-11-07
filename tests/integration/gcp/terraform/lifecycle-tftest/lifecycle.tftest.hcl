# Provider-visible assertions for the lifecycle gate (TF5). Run by
# lifecycle-cases/tftest.sh with `terraform test` / `tofu test`.
#
# The run applies the three representative resources, so `apply` exits non-zero
# if any create or read fails; the assertions then check the computed values a
# Terraform configuration can actually observe. Fields the provider hides
# (`metageneration`, `locationType`, `primary.state`, the policy etag) are
# asserted by the shell cases through the REST API.

run "provider_visible_attributes" {
  command = apply

  assert {
    condition     = google_storage_bucket.b.url == "gs://lc-tftest-bucket"
    error_message = "bucket url was ${google_storage_bucket.b.url}"
  }

  assert {
    condition     = endswith(google_storage_bucket.b.self_link, "/storage/v1/b/lc-tftest-bucket")
    error_message = "bucket self_link was ${google_storage_bucket.b.self_link}"
  }

  assert {
    condition     = google_storage_bucket.b.location == "US"
    error_message = "bucket location was ${google_storage_bucket.b.location}"
  }

  assert {
    condition     = google_storage_bucket.b.time_created != "" && google_storage_bucket.b.versioning[0].enabled
    error_message = "bucket time_created/versioning were ${google_storage_bucket.b.time_created} / ${google_storage_bucket.b.versioning[0].enabled}"
  }

  assert {
    condition     = google_pubsub_topic.t.id == "projects/${var.project}/topics/lc-tftest-topic"
    error_message = "topic id was ${google_pubsub_topic.t.id}"
  }

  assert {
    condition     = google_pubsub_topic.t.message_retention_duration == "86600s"
    error_message = "topic message_retention_duration was ${google_pubsub_topic.t.message_retention_duration}"
  }

  assert {
    condition     = google_pubsub_subscription.s.id == "projects/${var.project}/subscriptions/lc-tftest-sub"
    error_message = "subscription id was ${google_pubsub_subscription.s.id}"
  }

  assert {
    condition     = google_pubsub_subscription.s.topic == google_pubsub_topic.t.id
    error_message = "subscription topic was ${google_pubsub_subscription.s.topic}"
  }

  assert {
    condition     = google_pubsub_subscription.s.ack_deadline_seconds == 20
    error_message = "subscription ack_deadline_seconds was ${google_pubsub_subscription.s.ack_deadline_seconds}"
  }

  assert {
    condition     = google_kms_key_ring.r.id == "projects/${var.project}/locations/global/keyRings/lc-tftest-ring"
    error_message = "key ring id was ${google_kms_key_ring.r.id}"
  }

  assert {
    condition     = google_kms_crypto_key.k.id == "projects/${var.project}/locations/global/keyRings/lc-tftest-ring/cryptoKeys/lc-tftest-key"
    error_message = "crypto key id was ${google_kms_crypto_key.k.id}"
  }

  assert {
    condition     = google_kms_crypto_key.k.purpose == "ENCRYPT_DECRYPT" && google_kms_crypto_key.k.rotation_period == "7776000s"
    error_message = "crypto key purpose/rotation was ${google_kms_crypto_key.k.purpose} / ${google_kms_crypto_key.k.rotation_period}"
  }

  assert {
    condition     = google_kms_crypto_key.k.primary[0].state == "ENABLED"
    error_message = "crypto key primary state was ${google_kms_crypto_key.k.primary[0].state}"
  }

  assert {
    condition     = google_kms_crypto_key.k.version_template[0].algorithm == "GOOGLE_SYMMETRIC_ENCRYPTION" && google_kms_crypto_key.k.version_template[0].protection_level == "SOFTWARE"
    error_message = "crypto key version_template was ${google_kms_crypto_key.k.version_template[0].algorithm} / ${google_kms_crypto_key.k.version_template[0].protection_level}"
  }
}
