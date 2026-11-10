# Resources exercised by the jaiscloud-gcp Terraform / OpenTofu compat suite.
#
# Scope: only services jaiscloud-gcp implements. Adding a resource whose service
# is unimplemented (Cloud Run Jobs/WorkerPools, Artifact Registry, ...) makes
# `apply` fail and aborts the whole run — keep this file in sync with the
# emulator's supported surface. Cloud Run v2 services are served and declared
# here (the module case is in module-cases/cloudrun.sh); see README.md.

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

# ── Cloud Monitoring: declarative uptime check ────────────────────────────────
# The emulator stores and echoes the configuration; it never runs the probe, so
# nothing here depends on egress. `uptime_display_name` is a variable so the
# suite can drive the provider's in-place update (PATCH + updateMask) path.
resource "google_monitoring_uptime_check_config" "compat" {
  project      = var.project
  display_name = var.uptime_display_name
  timeout      = "10s"
  period       = "300s"

  monitored_resource {
    type = "uptime_url"
    labels = {
      host       = "shop.example.com"
      project_id = var.project
    }
  }

  http_check {
    path         = "/healthz"
    port         = 443
    use_ssl      = true
    validate_ssl = true
  }

  selected_regions = ["USA", "EUROPE", "ASIA_PACIFIC"]

  user_labels = {
    team = "compat-test"
  }
}

# ── IAM service account ───────────────────────────────────────────────────────
resource "google_service_account" "compat" {
  account_id   = "jaiscloud-tf-sa"
  display_name = "jaiscloud terraform compat service account"
}

# display_name is optional and the provider's attribute is not computed: an
# account created without it must read back without the field, or the second
# plan reports `display_name = "<account id>" -> null` (TF2-14). The re-plan
# phase is the assertion; run.sh also checks the raw response omits the field.
resource "google_service_account" "nodesc" {
  account_id = "jaiscloud-tf-sa-nodesc"
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

# A regional secret (projects.locations.secrets): the location is the storage
# location, so there is no replication policy; CMEK is the top-level
# customerManagedEncryption block, and its key must reside in the secret's own
# location (the region-local key below).
resource "google_secret_manager_regional_secret" "regional" {
  secret_id = "jaiscloud-tf-regional-secret"
  location  = var.region
  project   = var.project

  labels = {
    env = "compat-test"
  }

  annotations = {
    owner = "terraform"
  }

  deletion_protection = false

  customer_managed_encryption {
    kms_key_name = google_kms_crypto_key.compat.id
  }
}

resource "google_secret_manager_regional_secret_version" "regional" {
  secret          = google_secret_manager_regional_secret.regional.id
  secret_data     = "jaiscloud-tf-regional-secret-value"
  deletion_policy = "DELETE"
}

resource "google_secret_manager_regional_secret_iam_member" "regional" {
  project   = var.project
  location  = google_secret_manager_regional_secret.regional.location
  secret_id = google_secret_manager_regional_secret.regional.secret_id
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

# ── Compute Engine: project common instance metadata ─────────────────────────
# google_compute_project_metadata is the writable half of the project resource:
# the provider GETs projects/{p} to read commonInstanceMetadata.fingerprint,
# POSTs projects/{p}/setCommonInstanceMetadata with items + that fingerprint,
# polls the returned DONE operation, then re-reads the project. The second plan
# above is the assertion that projects.get reports the same set (TF1-5), and
# run.sh checks the wire body.
resource "google_compute_project_metadata" "compat" {
  project = var.project

  metadata = {
    compat-key = "compat-value"
  }
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

# The emulator mints a real self-signed client certificate (PEM), its one-time
# private key, and the instance's server CA. The provider's read (sslCerts.get)
# must converge without a permadiff: instance/common_name/cert_serial_number/
# create_time/expiration_time/sha1_fingerprint all round-trip.
resource "google_sql_ssl_cert" "compat" {
  common_name = "jaiscloud-tf-client"
  project     = var.project
  instance    = google_sql_database_instance.compat.name
}

# ── Dataproc Metastore — service port + federation + database IAM ─────────────
# google_dataproc_metastore_service requests an explicit `port` (TF2-13): real
# Metastore echoes a requested port and the provider's port is Optional+Computed,
# so a Service that assigned its own port instead would permadiff on the second
# plan. The service is the backend the federation and the database IAM grant
# below name, and its read-back port/endpointUri are assertions in run.sh.
resource "google_dataproc_metastore_service" "compat" {
  service_id = "jaiscloud-tf-metastore"
  location   = var.region
  project    = var.project
  port       = 9080
  tier       = "DEVELOPER"

  hive_metastore_config {
    version = "3.1.2"
  }

  labels = {
    env = "compat-test"
  }
}

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
  project    = var.project
  location   = var.region
  service_id = "jaiscloud-tf-metastore"
  database   = "default"
  role       = "roles/metastore.admin"
  member     = "serviceAccount:${google_service_account.compat.email}"
}

# ── GKE: the Dataproc-on-GKE prerequisite ─────────────────────────────────────
# google_container_cluster is the resource DPG37 was about: its read
# (resourceContainerClusterRead) dereferences legacyAbac.enabled,
# networkConfig.network, shieldedNodes.enabled and
# defaultMaxPodsConstraint.maxPodsPerNode unconditionally, so the emulator's
# cluster read has to render them (as real GKE always does) or the provider
# SIGSEGVs. A clean apply + a no-diff second plan is the assertion.
#
# It is separate from the Dataproc resource below because the emulator routes
# GKE by host: the provider keeps only the scheme+host of a custom endpoint, so
# it cannot send the /container path prefix the emulator also accepts, and the
# bare /v1/.../clusters path belongs to Managed Kafka. run.sh therefore derives
# container.<host> (container.localhost for the default endpoint) and passes it
# as container_endpoint.
resource "google_container_cluster" "gke" {
  name                = var.gke_cluster
  location            = var.region
  initial_node_count  = 1
  deletion_protection = false

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }
}

# ── Dataproc on GKE (virtual cluster) ─────────────────────────────────────────
# google_dataproc_cluster with virtual_cluster_config is the provider path the
# emulator's NodePool reconciler has to satisfy: the create is accepted, the
# pools it names are materialised in the GKE API, and the read-back carries no
# nodePoolConfig (it is input-only in the real API).
#
# SPARK is pinned to a fully-qualified release on purpose: aliases are
# dereferenced at cluster creation (real GCP behaviour), so a config carrying
# "3.5" would show a permanent diff against the stored "3.5-dataproc-28".
resource "google_dataproc_cluster" "gke" {
  name   = "jaiscloud-tf-dataproc-gke"
  region = var.region
  labels = { env = "compat-test" }

  depends_on = [google_container_cluster.gke]

  virtual_cluster_config {
    staging_bucket = "jaiscloud-tf-dataproc-staging"

    kubernetes_cluster_config {
      kubernetes_namespace = "jaiscloud-tf-dataproc"

      gke_cluster_config {
        gke_cluster_target = "projects/${var.project}/locations/${var.region}/clusters/${var.gke_cluster}"

        node_pool_target {
          node_pool = "jaiscloud-tf-dataproc-pool"
          roles     = ["DEFAULT", "CONTROLLER"]

          node_pool_config {
            config {
              machine_type    = "n1-standard-4"
              local_ssd_count = 1
            }

            autoscaling {
              min_node_count = 1
              max_node_count = 3
            }

            locations = ["${var.region}-a"]
          }
        }

        node_pool_target {
          node_pool = "jaiscloud-tf-dataproc-spark"
          roles     = ["SPARK_DRIVER", "SPARK_EXECUTOR"]
        }
      }

      kubernetes_software_config {
        component_version = {
          SPARK = "3.5-dataproc-28"
        }
      }
    }
  }
}

# ── Dataproc on Compute Engine (config) ───────────────────────────────────────
# The plain cluster_config shape is the one that used to panic the provider: its
# read-back flattens config.software_config without a nil check, so the emulator
# must report the service defaults a real cluster always carries (image version,
# instance groups, zone) instead of omitting config.softwareConfig (DPG38). A
# clean apply + refresh with no diff is the assertion.
resource "google_dataproc_cluster" "gce" {
  name   = "jaiscloud-tf-dataproc-gce"
  region = var.region
  labels = { env = "compat-test" }

  cluster_config {
    worker_config {
      num_instances = 2
    }
  }
}

# Resource-level IAM on a Dataproc cluster. The Discovery document publishes
# getIamPolicy/setIamPolicy/testIamPermissions on projects/{p}/regions/{r}/
# clusters/{id}, and google_dataproc_cluster_iam_member drives them (with the
# computed etag read back into state, so a re-plan only converges if the get
# returns the stored policy verbatim). The member is the fixture's own service
# account; `member` is what the provider sends after the non-authoritative
# read-modify-write.
resource "google_dataproc_cluster_iam_member" "compat" {
  project = var.project
  region  = var.region
  cluster = google_dataproc_cluster.gce.name
  role    = "roles/dataproc.viewer"
  member  = "serviceAccount:${google_service_account.compat.email}"
}

# ── Cloud Billing: the aggregate data "google_project" read ──────────────────
# The data source always reads projects.getBillingInfo (to populate
# `billing_account`) alongside the Resource Manager project lookup. Before CB1
# that call was unrouted and every data "google_project" read failed with
# UnknownService, which is what blocked the terraform-google-pubsub module's
# default service-agent email. The emulator models only the project↔account
# association, so an unlinked project reads back an empty billing_account.
data "google_project" "compat" {
  project_id = var.project
}

# ── Cloud Billing: the project association write path ────────────────────────
# `billing_account` drives the provider's write path (CB2): it first checks
# billingAccounts/{account}:testIamPermissions for
# billing.resourceAssociations.create (a default-permissive echo here), then
# POSTs the project, PUTs projects/{p}/billingInfo and reads the association
# back. The Terraform value is the BARE account id; the provider prepends
# "billingAccounts/" itself, so passing the full resource name here would yield
# billingAccounts/billingAccounts/… and a failed-precondition error.
#
# deletion_policy = "DELETE" is required because the provider defaults to
# "PREVENT" (which only removes the resource from state). The project gets its
# own id so the fixture's main project stays unlinked — the data source's
# empty-account assertion above depends on that.
resource "google_project" "billing" {
  project_id      = "jaiscloud-tf-billing"
  name            = "jaiscloud tf billing"
  deletion_policy = "DELETE"
  billing_account = "0X0X0X-0X0X0X-0X0X0X"
}

# ── Cloud Run (v2 service) ────────────────────────────────────────────────────
# The v2 service/revision control plane is served over both transports; Jobs,
# WorkerPools and the v1 surface (google_cloud_run_service, domain mappings) are
# not, so this declares only the service. The consumer path is the official
# module gate (module-cases/cloudrun.sh); this raw resource exists for the field
# the module's v2 submodule does not expose — a container `readiness_probe` —
# and for the in-place env update run.sh drives. Traffic targets are
# deliberately not set: the emulator stores them and reflects them in
# `trafficStatuses`, but does not enforce the split (wire-contract §5).
resource "google_cloud_run_v2_service" "compat" {
  project             = var.project
  name                = "jaiscloud-tf-run"
  location            = var.region
  description         = "jaiscloud terraform compat cloud run service"
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = false

  labels = {
    env = "compat-test"
  }

  template {
    service_account = google_service_account.compat.email
    timeout         = "300s"

    containers {
      name  = "app"
      image = "us-docker.pkg.dev/cloudrun/container/hello"

      env {
        name  = "MODE"
        value = var.run_env_value
      }

      ports {
        container_port = 8080
      }

      # The module's v2 submodule exposes startup_probe/liveness_probe but not
      # readiness_probe: this is the raw-only field the fixture exists for.
      readiness_probe {
        period_seconds    = 10
        timeout_seconds   = 5
        failure_threshold = 2
        success_threshold = 2
        http_get {
          path = "/ready"
          port = 8080
        }
      }
    }
  }
}

# ── Outputs (consumed by run.sh spot checks) ──────────────────────────────────
output "bucket_name" {
  value = google_storage_bucket.compat.name
}

output "object_name" {
  value = google_storage_bucket_object.readme.name
}

output "uptime_check_id" {
  value = google_monitoring_uptime_check_config.compat.uptime_check_id
}

output "service_account_email" {
  value = google_service_account.compat.email
}

output "service_account_nodesc_email" {
  value = google_service_account.nodesc.email
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

output "sql_ssl_cert_fingerprint" {
  value = google_sql_ssl_cert.compat.sha1_fingerprint
}

output "enabled_service" {
  value = google_project_service.run_api.service
}

output "project_metadata_project" {
  value = google_compute_project_metadata.compat.project
}

output "metastore_service_name" {
  value = google_dataproc_metastore_service.compat.name
}

output "metastore_federation_name" {
  value = google_dataproc_metastore_federation.compat.name
}

output "dataproc_gke_cluster_name" {
  value = google_dataproc_cluster.gke.name
}

output "dataproc_gce_cluster_name" {
  value = google_dataproc_cluster.gce.name
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.compat.dataset_id
}

output "bigquery_table_id" {
  value = google_bigquery_table.compat.table_id
}

output "bigquery_routine_id" {
  value = google_bigquery_routine.compat.routine_id
}

output "bigquery_row_access_policy_id" {
  value = google_bigquery_row_access_policy.compat.policy_id
}

output "billing_account" {
  value = data.google_project.compat.billing_account
}

# The google_project resource's stored association. The provider reads
# projects/{p}/billingInfo back after the write and strips the
# "billingAccounts/" prefix, so the attribute holds the bare account id the
# configuration names (no permadiff); the wire itself carries the full resource
# name (run.sh asserts both).
output "project_billing_account" {
  value = google_project.billing.billing_account
}

output "run_service_name" {
  value = google_cloud_run_v2_service.compat.name
}

# ── BigQuery: dataset-scoped routine + table-scoped row access policy (NF31) ──
# routines.* and rowAccessPolicies.* are declared resource CRUD; both are stored
# and echoed over the existing BigQuery store (memory and --dsn). The two
# variables drive the in-place update assertions run.sh makes after the first
# apply.
resource "google_bigquery_dataset" "compat" {
  dataset_id = "jaiscloud_tf_bq"
  location   = "US"
}

resource "google_bigquery_table" "compat" {
  dataset_id = google_bigquery_dataset.compat.dataset_id
  table_id   = "jaiscloud_tf_table"

  # The provider's default (true) refuses destroy client-side, which the
  # run.sh destroy (best-effort, like every fixture cleanup) silently left the
  # table and dataset behind. The fixture is ephemeral, so opt out.
  deletion_protection = false

  schema = jsonencode([
    {
      name = "region"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "amount"
      type = "INT64"
      mode = "NULLABLE"
    },
  ])
}

resource "google_bigquery_routine" "compat" {
  dataset_id      = google_bigquery_dataset.compat.dataset_id
  routine_id      = "jaiscloud_tf_routine"
  routine_type    = "SCALAR_FUNCTION"
  language        = "SQL"
  definition_body = var.routine_body
  description     = "jaiscloud terraform compatibility routine"

  arguments {
    name          = "x"
    argument_kind = "FIXED_TYPE"
    mode          = "IN"
    data_type     = jsonencode({ typeKind = "INT64" })
  }

  return_type = jsonencode({ typeKind = "INT64" })
}

resource "google_bigquery_row_access_policy" "compat" {
  dataset_id       = google_bigquery_dataset.compat.dataset_id
  table_id         = google_bigquery_table.compat.table_id
  policy_id        = "jaiscloud_tf_policy"
  filter_predicate = var.row_access_policy_predicate
  grantees         = ["user:compat@example.com"]
}

# ── BigQuery resource IAM (TF1-1) ────────────────────────────────────────────
# The dataset plane is BigQuery's legacy `access` ACL: real BigQuery has no
# datasets.getIamPolicy, so the provider reads GET ...?accessPolicyVersion=3 and
# writes a PATCH carrying the whole access list. The table plane is the
# tables.getIamPolicy / setIamPolicy / testIamPermissions mixin served on the
# shared policy store. run.sh asserts both wire shapes and the destroy cleanup.
resource "google_bigquery_dataset_iam_member" "compat_viewer" {
  dataset_id = google_bigquery_dataset.compat.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "user:tf-dataset@example.com"
}

resource "google_bigquery_table_iam_member" "compat_viewer" {
  dataset_id = google_bigquery_dataset.compat.dataset_id
  table_id   = google_bigquery_table.compat.table_id
  role       = "roles/bigquery.dataViewer"
  member     = "user:tf-table@example.com"
}

# ── Cloud DNS zone IAM (TF1-3) ───────────────────────────────────────────────
# Cloud DNS publishes the google.iam.v1.IAMPolicy mixin on managedZones
# (dns/v1/{+resource}:getIamPolicy, :setIamPolicy, :testIamPermissions), and
# google_dns_managed_zone_iam_member drives it: the provider reads the policy,
# adds the member, writes it back with the etag, and re-reads it into state, so
# the second plan only converges if the get returns the stored policy verbatim.
# The zone itself is a plain served managedZones.create; hashicorp/google 7.x
# drops the old deletion_protection attribute for DNS zones (deletion_policy
# defaults to DELETE), so no destroy opt-out is needed.
resource "google_dns_managed_zone" "compat" {
  name     = "jaiscloud-tf-zone"
  dns_name = "jaiscloud-tf.example.com."
}

resource "google_dns_managed_zone_iam_member" "compat" {
  project      = var.project
  managed_zone = google_dns_managed_zone.compat.name
  role         = "roles/dns.admin"
  member       = "user:tf-dns@example.com"
}

output "dns_zone_name" {
  value = google_dns_managed_zone.compat.name
}
