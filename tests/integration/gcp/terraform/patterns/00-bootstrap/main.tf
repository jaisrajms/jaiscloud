# Composition pattern 00 — project bootstrap (TF4-1).
#
# The most common GCP Terraform stack is "create a project, link billing,
# enable the APIs, grant a service account a role, then put the resources in
# it". This pattern is that stack minus the org/folder hierarchy (the emulator
# is project-scoped, wire-contract §8): a project created by Terraform, its
# billing association, four enabled APIs, a service account, a project IAM
# grant, a GCS bucket, a Pub/Sub topic + subscription, a Secret Manager
# secret + version and a Cloud KMS key ring + key — all in one apply.
#
# What it proves that the fixture does not: every resource resolves its
# reference against the project Terraform just created (`google_project.x`),
# the provider's post-create reads of that new project succeed, the second plan
# is clean across the whole dependency graph, and destroy tears the graph down
# before the project (the resources name the project, so the graph orders it
# last).
#
# The project id is passed in by run.sh and is unique per run: the emulator,
# like real GCP, keeps a deleted project in DELETE_REQUESTED state and rejects
# a duplicate create, so a fixed id would make a second run against the same
# emulator fail.

variable "bootstrap_project" {
  type        = string
  description = "Project id Terraform creates and deploys the stack into (unique per run; see run.sh)."
}

variable "billing_account" {
  type        = string
  description = "Bare Cloud Billing account id the project is associated with (the provider prepends billingAccounts/)."
  default     = "0X0X0X-0X0X0X-0X0X0X"
}

locals {
  # The APIs a stack of this shape enables. Four resources in one apply is the
  # composition's "batch": the provider's google_project_service creates one at
  # a time, while the API's own services:batchEnable is covered by the Service
  # Usage REST/gRPC tests.
  apis = [
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudkms.googleapis.com",
  ]
}

# ── Cloud Resource Manager: the project ──────────────────────────────────────
# deletion_policy = "DELETE" is required: the provider's default
# ("PREVENT") only forgets the resource, leaving the project behind. With
# DELETE the provider calls projects.delete, and the emulator — like real GCP —
# moves the project to DELETE_REQUESTED rather than removing the row.
#
# auto_create_network = false exercises the provider's pre-requisite probe: it
# first asks Service Usage about the dummy project `00000000000`, which the
# emulator rejects with real GCP's 403 ("Project '00000000000' not found or
# permission denied.") since TF4-1-1. It then enables compute.googleapis.com and
# reads the project's default network, which the emulator does not synthesize,
# so the provider's documented 404 branch logs "no need to delete it" and
# continues. Actually creating/deleting a default network remains outside this
# composition's scope (TF4-1-2).
resource "google_project" "bootstrap" {
  project_id          = var.bootstrap_project
  name                = "jaiscloud tf bootstrap"
  deletion_policy     = "DELETE"
  billing_account     = var.billing_account
  auto_create_network = false

  labels = {
    env = "compat-test"
  }
}

# ── Service Usage: enable the APIs the stack uses ────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset(local.apis)

  project = google_project.bootstrap.project_id
  service = each.value

  disable_on_destroy = true
}

# ── IAM: the service account and its project grant ───────────────────────────
resource "google_service_account" "stack" {
  project      = google_project.bootstrap.project_id
  account_id   = "jc-tf-boot-sa"
  display_name = "jaiscloud terraform bootstrap service account"
}

resource "google_project_iam_member" "publisher" {
  project = google_project.bootstrap.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.stack.email}"
}

# ── Cloud Storage: a bucket in the new project ───────────────────────────────
resource "google_storage_bucket" "stack" {
  project       = google_project.bootstrap.project_id
  name          = "${var.bootstrap_project}-storage"
  location      = "US"
  force_destroy = true

  labels = {
    env = "compat-test"
  }

  depends_on = [google_project_service.apis]
}

# ── Pub/Sub: a topic and a subscription on it ─────────────────────────────────
resource "google_pubsub_topic" "stack" {
  project = google_project.bootstrap.project_id
  name    = "jc-tf-boot-topic"

  labels = {
    env = "compat-test"
  }

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "stack" {
  project = google_project.bootstrap.project_id
  name    = "jc-tf-boot-sub"
  topic   = google_pubsub_topic.stack.id

  ack_deadline_seconds = 20
}

# ── Secret Manager: a secret and its first version ───────────────────────────
resource "google_secret_manager_secret" "stack" {
  project   = google_project.bootstrap.project_id
  secret_id = "jc-tf-boot-secret"

  replication {
    auto {}
  }

  labels = {
    env = "compat-test"
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "stack" {
  secret      = google_secret_manager_secret.stack.id
  secret_data = "jaiscloud-tf-bootstrap-secret"
}

# ── Cloud KMS: a key ring and an encryption key ──────────────────────────────
resource "google_kms_key_ring" "stack" {
  project  = google_project.bootstrap.project_id
  name     = "jc-tf-boot-ring"
  location = var.region

  depends_on = [google_project_service.apis]
}

resource "google_kms_crypto_key" "stack" {
  name     = "jc-tf-boot-key"
  key_ring = google_kms_key_ring.stack.id
  purpose  = "ENCRYPT_DECRYPT"
}
