# Differential corpus case — foundation.
#
# The smallest stack that still touches the resource shapes a first Terraform
# project uses: a bucket (labels, versioning, lifecycle, CORS) with one object,
# a Pub/Sub topic + subscription, a Secret Manager secret + version, a service
# account with a project IAM grant, and a KMS key ring + key.
#
# Everything here is cheap and self-cleaning on real GCP (the tool destroys the
# stack even when the run fails); it is the case a real-GCP recording always
# covers. Names carry the tool's run suffix so a recording is repeatable.

variable "name_suffix" {
  type        = string
  description = "Per-run suffix (the differential tool templates it into the corpus variables)."
}

# ── Cloud Storage ────────────────────────────────────────────────────────────
resource "google_storage_bucket" "diff" {
  project                     = var.project
  name                        = "jc-tf-diff-${var.name_suffix}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  labels = {
    env        = "differential"
    managed_by = "terraform"
  }

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  cors {
    origin          = ["https://example.com"]
    method          = ["GET", "HEAD"]
    response_header = ["Content-Type"]
    max_age_seconds = 3600
  }
}

resource "google_storage_bucket_object" "diff" {
  bucket       = google_storage_bucket.diff.name
  name         = "hello.txt"
  content      = "hello from the terraform differential corpus\n"
  content_type = "text/plain"
}

# ── Pub/Sub ──────────────────────────────────────────────────────────────────
resource "google_pubsub_topic" "diff" {
  project = var.project
  name    = "jc-tf-diff-${var.name_suffix}"

  labels = {
    env = "differential"
  }
}

resource "google_pubsub_subscription" "diff" {
  project = var.project
  name    = "jc-tf-diff-sub-${var.name_suffix}"
  topic   = google_pubsub_topic.diff.id

  ack_deadline_seconds       = 20
  message_retention_duration = "600s"

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "120s"
  }
}

# ── Secret Manager ───────────────────────────────────────────────────────────
resource "google_secret_manager_secret" "diff" {
  project   = var.project
  secret_id = "jc-tf-diff-${var.name_suffix}"

  labels = {
    env = "differential"
  }

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "diff" {
  secret      = google_secret_manager_secret.diff.id
  secret_data = "differential-secret-value"
}

# ── IAM service account ──────────────────────────────────────────────────────
resource "google_service_account" "diff" {
  project      = var.project
  account_id   = "jc-tf-diff-${var.name_suffix}"
  display_name = "jaiscloud terraform differential"
}

resource "google_project_iam_member" "diff" {
  project = var.project
  role    = "roles/viewer"
  member  = "serviceAccount:${google_service_account.diff.email}"
}

# ── Cloud KMS ────────────────────────────────────────────────────────────────
resource "google_kms_key_ring" "diff" {
  project  = var.project
  name     = "jc-tf-diff-ring-${var.name_suffix}"
  location = var.region
}

resource "google_kms_crypto_key" "diff" {
  name     = "jc-tf-diff-key-${var.name_suffix}"
  key_ring = google_kms_key_ring.diff.id
  purpose  = "ENCRYPT_DECRYPT"
}
