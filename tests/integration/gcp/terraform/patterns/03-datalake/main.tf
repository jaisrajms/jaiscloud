# Composition pattern 03 — data lake + GKE/Dataproc (TF4-3).
#
# The canonical analytics stack is "objects land in a bucket, BigQuery owns the
# catalog, and a Spark job on Dataproc-on-GKE reads the data under a Metastore
# catalog". The individual services are served and tested — the fixture already
# carries a GKE cluster, a virtual cluster and a BigQuery routine, and the
# module cases drive Dataproc and BigQuery consumers — but no Terraform
# composition proves the whole graph:
#
#   input/output/staging buckets  <-- the Spark job reads/writes here
#        |                              |
#        |                    Dataproc job on the virtual cluster
#        |                              |
#        +--> BigQuery dataset/table/routine/row-access-policy
#                                       |
#   GKE cluster --(target)--> Dataproc-on-GKE virtual cluster --(catalog)-->
#                              Dataproc Metastore service --> federation
#
# What it proves that the fixture and the module cases do not:
#
#   - one apply creates the stack with cross-resource references (`bucket.name`,
#     `container_cluster.name`, `metastore_service.id`) and the whole graph
#     re-plans clean — the acceptance is a clean second plan, not the apply exit
#     code;
#   - the virtual cluster's two node-pool targets are reconciled into the GKE
#     API and readable through `container.v1` (the emulator models the GKE
#     control plane as metadata; nothing is scheduled onto those pools);
#   - the provider submits a Dataproc job against the virtual cluster and waits
#     for the terminal `DONE` state;
#   - the Metastore service the federation names exists, and the database-level
#     IAM binding is readable through `:getIamPolicy`;
#   - destroying the job and the virtual cluster alone leaves the reconciled
#     node pools readable (real Dataproc does not own the pools it creates in a
#     customer's GKE cluster), and the full destroy removes everything else.
#
# The k3d/Helm/Java half of the customer story — a real Spark driver writing the
# word count — is `demo/dpgke-lifecycle`, not this pattern: it needs a cluster
# and the k8s executor. This pattern is Terraform-only and runs against a local
# emulator (see README.md next to this file).
#
# The GKE shape is the fixture's proven prerequisite, not a new one: a regional
# cluster with Workload Identity, `initial_node_count = 1` and
# `deletion_protection = false`, targeted by a virtual cluster in the same
# region whose pools live in one of that region's zones.

variable "name_suffix" {
  type        = string
  description = "Run-unique suffix so a repeated run against one emulator cannot collide on a resource name."
  default     = ""
}

locals {
  suffix = var.name_suffix

  input_bucket   = "${var.project}-jcdl-${local.suffix}-input"
  output_bucket  = "${var.project}-jcdl-${local.suffix}-output"
  staging_bucket = "${var.project}-jcdl-${local.suffix}-staging"

  gke_cluster      = "jcdl-gke-${local.suffix}"
  dataproc_cluster = "jcdl-vc-${local.suffix}"
  namespace        = "jcdl-${local.suffix}"

  # The two pools Dataproc reconciles into the GKE cluster: a controller pool
  # with an explicit 1..3 autoscaler and a driver/executor pool that relies on
  # the API's 1/10 defaults (the fixture's proven shape).
  controller_pool = "jcdl-controller-${local.suffix}"
  spark_pool      = "jcdl-spark-${local.suffix}"

  metastore_service_id = "jcdl-ms-${local.suffix}"
  federation_id        = "jcdl-fed-${local.suffix}"

  bq_dataset = "jcdl_${replace(local.suffix, "-", "_")}"
  bq_table   = "events"
  bq_routine = "normalize_region"
  bq_policy  = "regional_only"

  # The job's code and seed data live in the input bucket, so the objects
  # Terraform uploads and the URIs the job names are the same thing.
  job_code_object  = "code/wordcount.py"
  job_input_object = "data/events.csv"

  # A regional target lets each pool pick a zone in the region; the provider
  # requires `locations` even though the API can infer it.
  node_pool_location = "${var.region}-a"

  gke_cluster_target = "projects/${var.project}/locations/${var.region}/clusters/${google_container_cluster.datalake.name}"
}

# ── Cloud Storage: the data lake ─────────────────────────────────────────────
# Input (job code + seed data), output (job results) and staging (Dataproc's
# driver logs and staged files). UBLA is the shape a job bucket uses on real
# GCP; the re-plan proves the emulator echoes it.
resource "google_storage_bucket" "input" {
  project                     = var.project
  name                        = local.input_bucket
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    env  = "compat-test"
    role = "input"
  }
}

resource "google_storage_bucket" "output" {
  project                     = var.project
  name                        = local.output_bucket
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    env  = "compat-test"
    role = "output"
  }
}

resource "google_storage_bucket" "staging" {
  project                     = var.project
  name                        = local.staging_bucket
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    env  = "compat-test"
    role = "staging"
  }
}

# The job's PySpark file and a small CSV it counts. The mock executor never
# opens them; they exist so the URIs the job names resolve to real objects (the
# Terraform graph is what this pattern measures, not a Spark runtime). This is
# a URI target, not the demo's word-count program: the real driver
# (`demo/dpgke-lifecycle/spark/wordcount.py`) runs only in that demo's k8s gate.
resource "google_storage_bucket_object" "job_code" {
  bucket  = google_storage_bucket.input.name
  name    = local.job_code_object
  content = <<-PY
    # Placeholder main file: the provider only needs a resolvable gs:// URI.
    import sys
    PY
}

resource "google_storage_bucket_object" "job_input" {
  bucket  = google_storage_bucket.input.name
  name    = local.job_input_object
  content = <<-CSV
    region,amount
    us-central1,10
    europe-west1,20
    us-central1,30
  CSV
}

# ── BigQuery: the catalog the data lands in ──────────────────────────────────
resource "google_bigquery_dataset" "datalake" {
  project       = var.project
  dataset_id    = local.bq_dataset
  friendly_name = "jaiscloud terraform data lake"
  description   = "jaiscloud terraform data lake composition (TF4-3)"
  location      = "US"

  labels = {
    env = "compat-test"
  }
}

resource "google_bigquery_table" "events" {
  project    = var.project
  dataset_id = google_bigquery_dataset.datalake.dataset_id
  table_id   = local.bq_table

  description = "events loaded from the input bucket"

  # Provider 7.x guards table deletion by default; a composition pattern must
  # be able to tear its own stack down.
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

  labels = {
    env = "compat-test"
  }
}

# A routine and a row access policy at the dataset/table level: the two
# BigQuery "resource IAM" surfaces the provider can drive without the
# dataset/table getIamPolicy/setIamPolicy pair (TF1-1, a separate session).
resource "google_bigquery_routine" "normalize_region" {
  project         = var.project
  dataset_id      = google_bigquery_dataset.datalake.dataset_id
  routine_id      = local.bq_routine
  routine_type    = "SCALAR_FUNCTION"
  language        = "SQL"
  definition_body = "LOWER(region)"
  description     = "Normalize a region label"

  arguments {
    name          = "region"
    argument_kind = "FIXED_TYPE"
    mode          = "IN"
    data_type     = jsonencode({ typeKind = "STRING" })
  }

  return_type = jsonencode({ typeKind = "STRING" })
}

resource "google_bigquery_row_access_policy" "regional_only" {
  project          = var.project
  dataset_id       = google_bigquery_dataset.datalake.dataset_id
  table_id         = google_bigquery_table.events.table_id
  policy_id        = local.bq_policy
  filter_predicate = "region = 'us-central1'"
  grantees         = ["user:datalake-reader@example.com"]
}

# ── GKE: the Dataproc-on-GKE prerequisite ────────────────────────────────────
resource "google_container_cluster" "datalake" {
  project             = var.project
  name                = local.gke_cluster
  location            = var.region
  initial_node_count  = 1
  deletion_protection = false

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }
}

# ── Dataproc: the virtual cluster and its two node-pool targets ──────────────
# SPARK is pinned to a fully-qualified release on purpose: aliases are
# dereferenced at cluster creation (real GCP behaviour), so a config carrying
# "3.5" would show a permanent diff against the stored "3.5-dataproc-28".
resource "google_dataproc_cluster" "datalake" {
  project = var.project
  name    = local.dataproc_cluster
  region  = var.region

  labels = {
    env = "compat-test"
  }

  depends_on = [google_container_cluster.datalake]

  virtual_cluster_config {
    staging_bucket = google_storage_bucket.staging.name

    kubernetes_cluster_config {
      kubernetes_namespace = local.namespace

      gke_cluster_config {
        gke_cluster_target = local.gke_cluster_target

        node_pool_target {
          node_pool = local.controller_pool
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

            locations = [local.node_pool_location]
          }
        }

        node_pool_target {
          node_pool = local.spark_pool
          roles     = ["SPARK_DRIVER", "SPARK_EXECUTOR"]
        }
      }

      kubernetes_software_config {
        component_version = {
          SPARK = "3.5-dataproc-28"
        }
      }
    }

    # The catalog the job reads through. Dataproc validates the reference at
    # cluster creation (real Dataproc does too) and injects the service's
    # thrift endpoint at job submission, so the metastore half of the graph is
    # wired to the data path rather than being a disconnected component.
    auxiliary_services_config {
      metastore_config {
        dataproc_metastore_service = google_dataproc_metastore_service.datalake.id
      }
    }
  }
}

# ── The job: provision -> run ────────────────────────────────────────────────
# wait_for_completion makes the provider poll GetJob until the terminal state,
# so the resource's first read is `DONE` and the driver can assert it. The
# emulator's mock executor advances one state per read.
resource "google_dataproc_job" "wordcount" {
  project = var.project
  region  = var.region

  placement {
    cluster_name = google_dataproc_cluster.datalake.name
  }

  pyspark_config {
    main_python_file_uri = "gs://${google_storage_bucket.input.name}/${google_storage_bucket_object.job_code.name}"

    args = [
      "gs://${google_storage_bucket.input.name}/${google_storage_bucket_object.job_input.name}",
      "gs://${google_storage_bucket.output.name}/counts",
    ]
  }

  labels = {
    env = "compat-test"
  }

  wait_for_completion = true
}

# ── Dataproc Metastore: the catalog the job reads through ────────────────────
# A real service (not a dangling reference) backs the federation, and the
# job's service account gets a database-level grant. The catalog data plane is
# metadata-only here; the control plane and the IAM mixin are what this proves.
resource "google_dataproc_metastore_service" "datalake" {
  project    = var.project
  service_id = local.metastore_service_id
  location   = var.region
  tier       = "DEVELOPER"

  hive_metastore_config {
    version = "3.1.2"
  }

  labels = {
    env = "compat-test"
  }
}

resource "google_dataproc_metastore_federation" "datalake" {
  project       = var.project
  federation_id = local.federation_id
  location      = var.region
  version       = "3.1.2"

  backend_metastores {
    name           = google_dataproc_metastore_service.datalake.id
    metastore_type = "DATAPROC_METASTORE"
    rank           = "0"
  }
}

resource "google_service_account" "job" {
  project      = var.project
  account_id   = "jcdl-job-${local.suffix}"
  display_name = "jaiscloud terraform data lake job"
}

resource "google_dataproc_metastore_database_iam_member" "job" {
  project    = var.project
  location   = var.region
  service_id = google_dataproc_metastore_service.datalake.service_id
  database   = "default"
  role       = "roles/metastore.admin"
  member     = "serviceAccount:${google_service_account.job.email}"
}
