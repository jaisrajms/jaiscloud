# Crypto-medallion demo — Terraform infra subset (design §5.1).
#
# Creates: the enabled APIs, the demo service account, the feed secret, the
# medallion buckets (with CMEK on the curated/gold bucket), a KMS key ring/key,
# and the Pub/Sub topic/subscription used for GCS notifications, Dataproc
# lifecycle events and Monitoring incident notifications.
#
# The data-platform resources (Managed Kafka, Dataproc, Cloud Run, Eventarc,
# Cloud Scheduler, BigQuery, Firestore) are created by scripts/democtl.py with
# gcloud/REST — the provider cannot reach them (design §5.1).

locals {
  # CMEK key id has the form projects/{p}/locations/{l}/keyRings/{r}/cryptoKeys/{k}.
  key_resource = "projects/${var.project}/locations/${var.region}/keyRings/crypto-medallion/cryptoKeys/demo-cmek"
}

# ── Service Usage ─────────────────────────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset(var.enabled_services)

  project            = var.project
  service            = each.value
  disable_on_destroy = false
}

# ── IAM service account ───────────────────────────────────────────────────────
# The Cloud Scheduler job's OIDC token and the in-cluster workloads use this SA.
resource "google_service_account" "runner" {
  account_id   = "demo-runner"
  display_name = "crypto medallion demo runner"
  project      = var.project

  depends_on = [google_project_service.apis]
}

# ── Secret Manager — the feed endpoint/credentials the bridge reads ───────────
resource "google_secret_manager_secret" "feed" {
  secret_id = "crypto-feed-url"
  project   = var.project

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "feed" {
  secret      = google_secret_manager_secret.feed.id
  secret_data = "wss://ws-feed.exchange.coinbase.com"

  depends_on = [google_secret_manager_secret.feed]
}

resource "google_secret_manager_secret_iam_member" "feed_accessor" {
  project   = var.project
  secret_id = google_secret_manager_secret.feed.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.runner.email}"
}

# ── Cloud KMS — CMEK for the curated (gold) bucket ───────────────────────────
resource "google_kms_key_ring" "demo" {
  name     = "crypto-medallion"
  location = var.region
  project  = var.project

  depends_on = [google_project_service.apis]
}

resource "google_kms_crypto_key" "demo" {
  name     = "demo-cmek"
  key_ring = google_kms_key_ring.demo.id
  purpose  = "ENCRYPT_DECRYPT"
}

# ── Cloud Storage — the medallion + serving buckets ──────────────────────────
resource "google_storage_bucket" "bronze" {
  name          = var.buckets.bronze
  location      = "US"
  project       = var.project
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "silver" {
  name          = var.buckets.silver
  location      = "US"
  project       = var.project
  force_destroy = true

  depends_on = [google_project_service.apis]
}

# Gold/curated carries CMEK (design §5.1).
resource "google_storage_bucket" "gold" {
  name          = var.buckets.gold
  location      = "US"
  project       = var.project
  force_destroy = true

  encryption {
    default_kms_key_name = local.key_resource
  }

  depends_on = [google_kms_crypto_key.demo]
}

resource "google_storage_bucket" "leaderboard" {
  name          = var.buckets.leaderboard
  location      = "US"
  project       = var.project
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "rollup" {
  name          = var.buckets.rollup
  location      = "US"
  project       = var.project
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "capture" {
  name          = var.buckets.capture
  location      = "US"
  project       = var.project
  force_destroy = true

  depends_on = [google_project_service.apis]
}

# Driver scripts, staged jars and streaming checkpoints.
resource "google_storage_bucket" "jobs" {
  name          = var.buckets.jobs
  location      = "US"
  project       = var.project
  force_destroy = true

  depends_on = [google_project_service.apis]
}

# ── Pub/Sub — GCS notifications / Dataproc lifecycle / Monitoring incidents ──
resource "google_pubsub_topic" "events" {
  name    = "crypto-medallion-events"
  project = var.project

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "events" {
  name                 = "crypto-medallion-events-sub"
  topic                = google_pubsub_topic.events.id
  project              = var.project
  ack_deadline_seconds = 20
}
