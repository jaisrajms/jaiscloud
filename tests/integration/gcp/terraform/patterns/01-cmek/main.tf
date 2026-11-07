# Composition pattern 01 — CMEK everywhere (TF4-1).
#
# The second canonical stack: one Cloud KMS key protects every data-at-rest
# surface the emulator models — a GCS bucket's default encryption, a BigQuery
# dataset's default encryption and a Secret Manager secret's replication
# policy. Secret Manager's automatic replication only accepts a key in the
# `global` location, so the stack carries a regional ring for storage/BigQuery
# and a global ring for the secret, exactly as the API requires.
#
# It also proves the key reference is *changeable*: run.sh applies the stack
# with the primary key, then re-applies with a second regional key selected
# (`active_key = "rotated"`) for the bucket and dataset, asserts both responses
# echo the new key, and requires the re-plan to be clean. The secret's
# replication key is created once: the provider's `replication` block is
# ForceNew, so rotating it through Terraform would replace the secret; the
# emulator's PATCH path for that field is covered by the service/REST tests.
#
# Names carry a per-run suffix (passed by run.sh) because the provider never
# calls the documented key-ring delete (TF5-4), so a key ring survives a
# destroy and a fixed name would collide on the next run against the same
# emulator.

variable "cmek_suffix" {
  type        = string
  description = "Per-run suffix for this pattern's resource names (see run.sh)."
}

variable "active_key" {
  type        = string
  description = "Which regional key the bucket and dataset use: primary or rotated."
  default     = "primary"

  validation {
    condition     = contains(["primary", "rotated"], var.active_key)
    error_message = "active_key must be primary or rotated."
  }
}

# ── Cloud KMS: the regional rings the data resources use ────────────────────
resource "google_kms_key_ring" "primary" {
  project  = var.project
  name     = "jc-tf-cmek-ring-${var.cmek_suffix}"
  location = var.region
}

resource "google_kms_crypto_key" "primary" {
  name     = "jc-tf-cmek-key-${var.cmek_suffix}"
  key_ring = google_kms_key_ring.primary.id
  purpose  = "ENCRYPT_DECRYPT"
}

resource "google_kms_key_ring" "rotated" {
  project  = var.project
  name     = "jc-tf-cmek-ring2-${var.cmek_suffix}"
  location = var.region
}

resource "google_kms_crypto_key" "rotated" {
  name     = "jc-tf-cmek-key2-${var.cmek_suffix}"
  key_ring = google_kms_key_ring.rotated.id
  purpose  = "ENCRYPT_DECRYPT"
}

locals {
  # The key the storage and BigQuery resources reference; run.sh flips
  # active_key to prove the reference changes in place and re-plans clean.
  data_key = var.active_key == "rotated" ? google_kms_crypto_key.rotated.id : google_kms_crypto_key.primary.id
}

# ── Cloud KMS: the global ring Secret Manager's automatic replication needs ──
resource "google_kms_key_ring" "global" {
  project  = var.project
  name     = "jc-tf-cmek-gring-${var.cmek_suffix}"
  location = "global"
}

resource "google_kms_crypto_key" "global" {
  name     = "jc-tf-cmek-gkey-${var.cmek_suffix}"
  key_ring = google_kms_key_ring.global.id
  purpose  = "ENCRYPT_DECRYPT"
}

# ── Cloud Storage: a bucket with a default CMEK ─────────────────────────────
resource "google_storage_bucket" "cmek" {
  project       = var.project
  name          = "jc-tf-cmek-bucket-${var.cmek_suffix}"
  location      = var.region
  force_destroy = true

  encryption {
    default_kms_key_name = local.data_key
  }

  labels = {
    env = "compat-test"
  }
}

# ── BigQuery: a dataset with default encryption ─────────────────────────────
resource "google_bigquery_dataset" "cmek" {
  project    = var.project
  dataset_id = "jc_tf_cmek_ds_${replace(var.cmek_suffix, "-", "_")}"
  location   = var.region

  default_encryption_configuration {
    kms_key_name = local.data_key
  }

  labels = {
    env = "compat-test"
  }
}

# ── Secret Manager: automatic replication protected by the global key ───────
resource "google_secret_manager_secret" "cmek" {
  project   = var.project
  secret_id = "jc-tf-cmek-secret-${var.cmek_suffix}"

  replication {
    auto {
      customer_managed_encryption {
        kms_key_name = google_kms_crypto_key.global.id
      }
    }
  }

  labels = {
    env = "compat-test"
  }
}

resource "google_secret_manager_secret_version" "cmek" {
  secret      = google_secret_manager_secret.cmek.id
  secret_data = "jaiscloud-tf-cmek-secret"
}
