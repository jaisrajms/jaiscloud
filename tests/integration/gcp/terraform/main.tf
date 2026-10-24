# Resources exercised by the jaiscloud-gcp Terraform / OpenTofu compat suite.
#
# Scope: only services jaiscloud-gcp implements. Adding a resource whose service
# is unimplemented (currently Cloud Run v2) makes `apply` fail and aborts the
# whole run — keep this file in sync with the emulator's supported surface. See
# README.md.

# ── Cloud Storage ─────────────────────────────────────────────────────────────
resource "google_storage_bucket" "compat" {
  name          = "jaiscloud-tf-bucket"
  location      = "US"
  force_destroy = true

  labels = {
    env = "compat-test"
  }
}

resource "google_storage_bucket_object" "readme" {
  bucket       = google_storage_bucket.compat.name
  name         = "README.txt"
  content      = "jaiscloud terraform compat test"
  content_type = "text/plain"
}

# ── IAM service account ───────────────────────────────────────────────────────
resource "google_service_account" "compat" {
  account_id   = "jaiscloud-tf-sa"
  display_name = "jaiscloud terraform compat service account"
}

# ── Secret Manager ────────────────────────────────────────────────────────────
resource "google_secret_manager_secret" "compat" {
  secret_id = "jaiscloud-tf-secret"
  project   = var.project

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "compat" {
  secret      = google_secret_manager_secret.compat.id
  secret_data = "jaiscloud-tf-secret-value"
}

resource "google_secret_manager_secret" "cmek" {
  secret_id = "jaiscloud-tf-secret-cmek"
  project   = var.project

  replication {
    auto {
      customer_managed_encryption {
        # Automatic replication requires the CMEK key to reside in `global`
        # (Secret Manager CMEK guide); a regional key is rejected by the API.
        kms_key_name = google_kms_crypto_key.compat_global.id
      }
    }
  }
}

# A user-managed replication policy: the secret lives in the listed replicas and
# each replica may name its own CMEK key, which must reside in that replica's
# location. Reading it back (the provider's computed `replication`) is the DC45
# acceptance: the emulator must echo this policy, not rewrite it as automatic.
resource "google_secret_manager_secret" "user_managed" {
  secret_id = "jaiscloud-tf-secret-user-managed"
  project   = var.project

  replication {
    user_managed {
      replicas {
        location = var.region

        customer_managed_encryption {
          kms_key_name = google_kms_crypto_key.compat.id
        }
      }

      replicas {
        location = "us-east1"
      }
    }
  }
}

resource "google_secret_manager_secret_iam_member" "accessor" {
  project   = var.project
  secret_id = google_secret_manager_secret.compat.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.compat.email}"
}

# ── Cloud KMS ─────────────────────────────────────────────────────────────────
resource "google_kms_key_ring" "compat" {
  name     = "jaiscloud-tf-keyring"
  location = var.region
  project  = var.project
}

resource "google_kms_crypto_key" "compat" {
  name     = "jaiscloud-tf-key"
  key_ring = google_kms_key_ring.compat.id
  purpose  = "ENCRYPT_DECRYPT"
}

# A `global` key ring: Secret Manager's automatic replication only accepts CMEK
# keys in the `global` location, so the CMEK secret above uses this key.
resource "google_kms_key_ring" "compat_global" {
  name     = "jaiscloud-tf-keyring-global"
  location = "global"
  project  = var.project
}

resource "google_kms_crypto_key" "compat_global" {
  name     = "jaiscloud-tf-key-global"
  key_ring = google_kms_key_ring.compat_global.id
  purpose  = "ENCRYPT_DECRYPT"
}

# ── Pub/Sub ───────────────────────────────────────────────────────────────────
resource "google_pubsub_topic" "compat" {
  name = "jaiscloud-tf-topic"

  labels = {
    env = "compat-test"
  }
}

resource "google_pubsub_subscription" "compat" {
  name                 = "jaiscloud-tf-sub"
  topic                = google_pubsub_topic.compat.id
  ack_deadline_seconds = 20
}

# Two members on one topic exercise the getIamPolicy -> merge -> setIamPolicy
# read-modify-write flow; both grants must survive (fresh-etag OCC).
resource "google_pubsub_topic_iam_member" "publisher" {
  topic  = google_pubsub_topic.compat.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.compat.email}"
}

resource "google_pubsub_topic_iam_member" "viewer" {
  topic  = google_pubsub_topic.compat.name
  role   = "roles/pubsub.viewer"
  member = "user:compat-viewer@example.com"
}

resource "google_pubsub_subscription_iam_member" "subscriber" {
  subscription = google_pubsub_subscription.compat.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.compat.email}"
}

# ── Cloud Resource Manager — project-level IAM ────────────────────────────────
# Exercises the project getIamPolicy -> merge -> setIamPolicy read-modify-write
# (fresh-etag OCC) that google_project_iam_member performs.
resource "google_project_iam_member" "publisher" {
  project = var.project
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.compat.email}"
}

# ── Service Usage ─────────────────────────────────────────────────────────────
# google_project_service enables the API on create (services.enable; the
# emulator also serves services:batchEnable for multi-service batches) and reads
# back via the CRM project lookup + services list with filter=state:ENABLED.
# disable_on_destroy exercises services:disable on destroy.
resource "google_project_service" "run_api" {
  project = var.project
  service = "run.googleapis.com"

  disable_on_destroy = true
}

# ── Cloud SQL for PostgreSQL ──────────────────────────────────────────────────
resource "google_sql_database_instance" "compat" {
  name                = "jaiscloud-tf-postgres"
  project             = var.project
  region              = var.region
  database_version    = "POSTGRES_18"
  deletion_protection = false

  settings {
    tier = "db-custom-1-3840"
  }
}

resource "google_sql_database" "compat" {
  name     = "appdb"
  project  = var.project
  instance = google_sql_database_instance.compat.name
}

resource "google_sql_user" "compat" {
  name     = "app"
  project  = var.project
  instance = google_sql_database_instance.compat.name
  password = "jaiscloud-tf-password"
}

# ── Dataproc Metastore — federation + database IAM ────────────────────────────
# google_dataproc_metastore_federation exercises the separate
# DataprocMetastoreFederation surface (create/get/update/delete + LRO), and
# google_dataproc_metastore_database_iam_member exercises the IAM mixin at the
# database level (metadata-only: the emulator keys the policy by resource name).
resource "google_dataproc_metastore_federation" "compat" {
  federation_id = "jaiscloud-tf-federation"
  location      = var.region
  project       = var.project
  version       = "3.1.2"

  backend_metastores {
    name           = "projects/${var.project}/locations/${var.region}/services/jaiscloud-tf-metastore"
    metastore_type = "DATAPROC_METASTORE"
    rank           = "0"
  }
}

resource "google_dataproc_metastore_database_iam_member" "compat" {
  project     = var.project
  location    = var.region
  service_id  = "jaiscloud-tf-metastore"
  database    = "default"
  role        = "roles/metastore.admin"
  member      = "serviceAccount:${google_service_account.compat.email}"
}

# ── Outputs (consumed by run.sh spot checks) ──────────────────────────────────
output "bucket_name" {
  value = google_storage_bucket.compat.name
}

output "object_name" {
  value = google_storage_bucket_object.readme.name
}

output "service_account_email" {
  value = google_service_account.compat.email
}

output "secret_id" {
  value = google_secret_manager_secret.compat.secret_id
}

output "key_ring_name" {
  value = google_kms_key_ring.compat.name
}

output "crypto_key_name" {
  value = google_kms_crypto_key.compat.name
}

output "pubsub_topic_name" {
  value = google_pubsub_topic.compat.name
}

output "pubsub_subscription_name" {
  value = google_pubsub_subscription.compat.name
}

output "sql_instance_name" {
  value = google_sql_database_instance.compat.name
}

output "sql_database_name" {
  value = google_sql_database.compat.name
}

output "sql_user_name" {
  value = google_sql_user.compat.name
}

output "enabled_service" {
  value = google_project_service.run_api.service
}

output "metastore_federation_name" {
  value = google_dataproc_metastore_federation.compat.name
}
