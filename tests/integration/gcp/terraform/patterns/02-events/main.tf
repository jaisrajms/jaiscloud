# Composition pattern 02 — event-driven chain (TF4-2).
#
# The canonical event stack is "a scheduler or a bucket write produces an event,
# Pub/Sub carries it, a function or an HTTP sink consumes it". Every link is
# served and tested at the service level, but no Terraform composition proves the
# *dependency graph* and the delivery path together:
#
#   Cloud Scheduler job --(pubsubTarget)--> Pub/Sub topic --(push)--> HTTP sink
#                                                       \--(eventTrigger)--> Function
#   GCS object write --(Eventarc)--> httpEndpoint sink
#                    \--(eventTrigger)--> Function
#
# What it proves that the fixture and the module cases do not:
#
#   - the provider can create these resources against one project with
#     cross-resource references (`topic.id`, `bucket.name`, `service_account.email`)
#     and the whole graph re-plans clean;
#   - a delivery genuinely arrives at the far end. The driver starts a local HTTP
#     echo sink (`patterns/helpers/echo-sink`) and asserts the Pub/Sub push body
#     and the Eventarc CloudEvents headers it received, rather than trusting that
#     "the resource exists";
#   - the Cloud Scheduler pubsubTarget and each function's event trigger actually
#     fan out: one message reaches both the push subscription and the function,
#     and one object write reaches both the standalone Eventarc trigger and the
#     storage-triggered function;
#   - destroy leaves no orphan topic, subscription, job, trigger or function.
#
# Time is deterministic: the scheduler job is ENABLED with a far-future cron
# (nothing fires on its own during the run), and the driver fires it with
# projects.locations.jobs.run (Cloud Scheduler's documented "force a run now"
# call), which real GCP only allows on an ENABLED job. A live cron would make the
# run wait for a wall-clock minute; the due-time engine itself is covered by the
# emulator's time-control suite and the scheduler module case.

variable "name_suffix" {
  type        = string
  description = "Run-unique suffix so a repeated run against one emulator cannot collide on a resource name."
  default     = ""
}

variable "sink_url" {
  type        = string
  description = "Base URL of the local HTTP echo sink the push subscription and the Eventarc httpEndpoint trigger deliver to."
}

locals {
  suffix = var.name_suffix

  topic        = "jc-tf-events-topic${local.suffix}"
  subscription = "jc-tf-events-push${local.suffix}"
  job          = "jc-tf-events-job${local.suffix}"
  bucket       = "jc-tf-events-${var.project}${local.suffix}"
  source_key   = "function-source.zip"
  trigger      = "jc-tf-events-gcs${local.suffix}"
  pubsub_fn    = "jc-tf-events-pubsub${local.suffix}"
  storage_fn   = "jc-tf-events-storage${local.suffix}"

  # The payload the scheduler publishes. base64 is the API's wire encoding: the
  # provider passes the string through, so the message the sink receives carries
  # exactly this value under message.data.
  scheduler_payload = base64encode("{\"event\":\"jaiscloud-terraform-scheduler\"}")
}

# ── Pub/Sub: the topic every producer writes to ──────────────────────────────
resource "google_pubsub_topic" "events" {
  project = var.project
  name    = local.topic

  labels = {
    env = "compat-test"
  }
}

# The push arm of the chain. The endpoint is a plain HTTP URL (the local sink):
# real GCP allows an http push endpoint, and delivering to the emulator's own
# API would not prove that the message left the Pub/Sub engine.
resource "google_pubsub_subscription" "push" {
  project = var.project
  name    = local.subscription
  topic   = google_pubsub_topic.events.id

  ack_deadline_seconds = 10

  push_config {
    push_endpoint = "${var.sink_url}/pubsub"
  }

  labels = {
    env = "compat-test"
  }
}

# ── Cloud Scheduler: the producer ────────────────────────────────────────────
# The job is ENABLED with a far-future cron (1 January) and the driver fires it
# with projects.locations.jobs.run, Cloud Scheduler's documented "force it now"
# call: a near cron would make the run wait for a wall-clock minute, and driving
# the due-time engine is covered by the emulator's time-control suite. So this
# pattern proves the producer -> topic -> consumer graph deterministically.
#
# The job must be ENABLED. Real Cloud Scheduler answers 400 FAILED_PRECONDITION
# ("Job.state must be ENABLED for RunJob.") for jobs.run on a paused job, and the
# emulator now does the same (TF2-12-2). The provider's create issues jobs:resume
# after storing an unpaused job, which real GCP tolerates and the emulator now
# does too (TF2-12), so `paused = false` is creatable.
#
# retry_config is explicit even though it carries the API defaults: the provider
# drops an all-zero block from the create body, the read fills the documented
# defaults back in (TF3-4-2), and Terraform would then plan their removal on
# every re-plan. Sending the same values keeps the re-plan clean.
resource "google_cloud_scheduler_job" "tick" {
  project   = var.project
  region    = var.region
  name      = local.job
  schedule  = "0 0 1 1 *"
  time_zone = "UTC"
  paused    = false

  pubsub_target {
    topic_name = google_pubsub_topic.events.id
    data       = local.scheduler_payload
    attributes = {
      source = "cloud-scheduler"
    }
  }

  # The API's documented default retry config (retryCount 0,
  # minBackoffDuration 5s, maxBackoffDuration 1h, maxDoublings 5,
  # maxRetryDuration 0 = unlimited).
  retry_config {
    retry_count = 0
  }
}

# ── Cloud Storage: the event source ──────────────────────────────────────────
resource "google_storage_bucket" "events" {
  project       = var.project
  name          = local.bucket
  location      = "US"
  force_destroy = true

  labels = {
    env = "compat-test"
  }
}

# The function's deployment archive. The provider resolves source_archive_bucket
# + source_archive_object into the v1 API's sourceArchiveUrl, and the emulator
# fetches the object when the function is created, so the object must exist and
# hold a real archive. The archive is committed next to this module (a
# two-handler index.js) and the pattern's run.sh copies it into the driver's work
# dir, so a bare `terraform plan` here and the assembled root module both resolve
# it; the mock executor runs no code, so the handlers are never invoked — this
# pattern proves the control plane and the event delivery, not a runtime.
resource "google_storage_bucket_object" "source" {
  bucket       = google_storage_bucket.events.name
  name         = local.source_key
  source       = "${path.module}/function-source.zip"
  content_type = "application/zip"
}

# ── Eventarc: a GCS trigger with an HTTP endpoint destination ────────────────
resource "google_service_account" "eventarc" {
  project      = var.project
  account_id   = "jc-tf-events${local.suffix}"
  display_name = "jaiscloud terraform events trigger"
}

resource "google_eventarc_trigger" "gcs" {
  project  = var.project
  location = var.region
  name     = local.trigger

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.events.name
  }

  destination {
    http_endpoint {
      uri = "${var.sink_url}/eventarc"
    }
  }

  service_account = google_service_account.eventarc.email

  labels = {
    env = "compat-test"
  }
}

# ── Cloud Functions: two event triggers ──────────────────────────────────────
# A v1 function is the surface the pinned consumer module drives, and its
# `event_trigger` block is the shortest path to a provisioned Eventarc trigger
# (the emulator materializes one backing trigger per Pub/Sub- or
# storage-triggered function).
resource "google_cloudfunctions_function" "pubsub" {
  project = var.project
  region  = var.region
  name    = local.pubsub_fn

  runtime               = "nodejs20"
  entry_point           = "handlePubsub"
  source_archive_bucket = google_storage_bucket.events.name
  source_archive_object = google_storage_bucket_object.source.name
  available_memory_mb   = 256
  timeout               = 60

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.events.id
  }

  labels = {
    env = "compat-test"
  }
}

resource "google_cloudfunctions_function" "storage" {
  project = var.project
  region  = var.region
  name    = local.storage_fn

  runtime               = "nodejs20"
  entry_point           = "handleStorage"
  source_archive_bucket = google_storage_bucket.events.name
  source_archive_object = google_storage_bucket_object.source.name
  available_memory_mb   = 256
  timeout               = 60

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.events.name
  }

  labels = {
    env = "compat-test"
  }
}
