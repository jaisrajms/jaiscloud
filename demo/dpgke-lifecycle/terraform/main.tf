# Resources for the Dataproc-on-GKE lifecycle demo.
#
# Scope: the bucket + cluster + virtual-cluster infrastructure, the demo's
# PySpark job, and nothing installed on a cluster. The job is submitted through
# the provider's google_dataproc_job (the "provision -> run" half of the story);
# the Helm/Java legs install their own submitter later.
#
# Keep this file to services the emulator implements. Adding a resource whose
# service is not served makes apply fail and aborts the whole run.

locals {
  # The buckets can live in a second project (DGD8); everything else stays in
  # var.project.
  storage_project = coalesce(var.storage_project, var.project)

  # GCS bucket names are a global namespace, so the prefix is what keeps
  # "input"/"output"/"staging" unique to this deployment.
  bucket_prefix = coalesce(var.bucket_prefix, var.project)

  buckets = {
    input   = "${local.bucket_prefix}-input"
    output  = "${local.bucket_prefix}-output"
    staging = "${local.bucket_prefix}-staging"
  }

  # The job's code lives in the input bucket rather than in the staging bucket:
  # the bucket IAM below already gives the job read access, and it keeps the
  # object that Terraform creates and the object Spark executes the same thing.
  job_code_object = "code/wordcount.py"

  # The target is written through the resource rather than var.gke_cluster_name
  # so the dependency on the GKE cluster is explicit in both directions.
  gke_cluster_target = "projects/${var.project}/locations/${var.gke_location}/clusters/${google_container_cluster.gke.name}"

  # A zonal target requires its node pools in that same zone; a regional target
  # lets each pool pick a zone inside the region. The provider insists on
  # `locations` even though the API can infer it, so derive it from gke_location
  # rather than hardcoding a zone that the regional shape would reject.
  node_pool_location = can(regex("^${var.region}-[a-z]$", var.gke_location)) ? var.gke_location : "${var.region}-a"
}

# ── Service Usage ─────────────────────────────────────────────────────────────
# The APIs the resources below belong to. disable_on_destroy = false on purpose:
# a demo teardown deletes the buckets and clusters at the same time, and
# disabling the API that authorises those deletes can race them.
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "dataproc.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
  ])

  project            = var.project
  service            = each.value
  disable_on_destroy = false
}

# ── IAM service account ───────────────────────────────────────────────────────
# The identity the job runs as, and the principal the bucket grants below name.
# On the emulator a service account is metadata only — it is stored and can be
# referenced by name, but nothing enforces what it is allowed to do.
resource "google_service_account" "job" {
  project      = var.project
  account_id   = "dpgke-job"
  display_name = "dataproc-on-gke demo job"

  depends_on = [google_project_service.apis]
}

# ── Cloud Storage ─────────────────────────────────────────────────────────────
# Three buckets: the job's input, its output, and Dataproc's staging bucket
# (driver logs, spark properties and the files a cluster stages there).
#
# They are created in local.storage_project instead of var.project so the same
# module can show cross-project staging (DGD8): a cluster in project A writing
# into a bucket owned by project B. The service enablement above covers
# var.project only; a second storage project is not enrolled here because the
# emulator does not gate calls on API enablement.
#
# uniform_bucket_level_access is what a job bucket should use on real GCP (ACLs
# off, access only through the bucket IAM grants below), so the module sets it
# and relies on the emulator echoing it back (DGD10). The provider sends it in
# the create body and in the PATCH body on update; a bucket read that dropped it
# would show up as a permanent diff in the re-plan step of run.sh.
resource "google_storage_bucket" "input" {
  name                        = local.buckets.input
  project                     = local.storage_project
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "output" {
  name                        = local.buckets.output
  project                     = local.storage_project
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true

  depends_on = [google_project_service.apis]
}

# Dataproc stages into this one, so it is named by the virtual cluster below
# rather than by the job.
resource "google_storage_bucket" "staging" {
  name                        = local.buckets.staging
  project                     = local.storage_project
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true

  depends_on = [google_project_service.apis]
}

# ── Bucket IAM ────────────────────────────────────────────────────────────────
# The real-GCP grant a job needs: read the input, read and write the output. On
# this emulator bucket IAM is metadata-only (DGD9) — the bindings are stored and
# echoed but never enforced — so these do not gate anything. They are here
# because the module is the copyable shape; DGD5 later replaces them with
# downscoped tokens handed to the job, which is what actually constrains it.
resource "google_storage_bucket_iam_member" "job_input_viewer" {
  bucket = google_storage_bucket.input.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.job.email}"
}

resource "google_storage_bucket_iam_member" "job_output_admin" {
  bucket = google_storage_bucket.output.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.job.email}"
}

# ── Objects the job uses ──────────────────────────────────────────────────────
# The PySpark file is uploaded as an object and the job below names it by URI,
# so the customer story is the real one: code in GCS, job submission points at
# it, no local file is handed to the cluster. The source is the demo's tracked
# script (../spark/wordcount.py); Terraform hashes it into state, so editing the
# job forces a re-upload.
resource "google_storage_bucket_object" "job_code" {
  name   = local.job_code_object
  bucket = google_storage_bucket.input.name
  source = "${path.module}/../spark/wordcount.py"
}

# A small seed document so the job has something to read. DGD6's k3d run uses
# this same object and compares the output; here the mock executor never opens
# it, which the gate states rather than pretends otherwise.
resource "google_storage_bucket_object" "job_input" {
  name    = var.job_input_object
  bucket  = google_storage_bucket.input.name
  content = <<-TEXT
    dataproc on gke reads this object and writes a word count
    the quick brown fox jumps over the lazy dog
    dataproc on gke is the job the demo proves
  TEXT
}

# ── GKE cluster ───────────────────────────────────────────────────────────────
# The Dataproc-on-GKE prerequisite. Two traps are worth knowing:
#
#   (a) google_container_cluster's read dereferences legacyAbac.enabled,
#       networkConfig.network/.subnetwork, shieldedNodes.enabled and
#       defaultMaxPodsConstraint.maxPodsPerNode unconditionally. Real GKE always
#       renders them and the emulator does too; if a read ever omits one, the
#       provider SIGSEGVs during refresh instead of reporting a diff.
#   (b) Workload Identity is enabled because Dataproc-on-GKE requires it, but its
#       IAM bindings are not asserted here: the roles
#       `gcloud container clusters create --setup-workload-identity` would grant
#       are a recorded no-fix on the emulator side. The workload_pool value is
#       what matters to the virtual cluster's create-time validation.
resource "google_container_cluster" "gke" {
  name                = var.gke_cluster_name
  project             = var.project
  location            = var.gke_location
  initial_node_count  = 1
  deletion_protection = false

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }

  depends_on = [google_project_service.apis]
}

# ── Dataproc on GKE (virtual cluster) ─────────────────────────────────────────
# The virtual cluster is a Dataproc *region* resource whose target is a *zonal*
# GKE cluster (the GKP1 shape): region us-central1 plus
# gkeClusterTarget projects/{p}/locations/us-central1-a/clusters/{name}. A
# regional target works too; a target outside var.region does not.
#
# nodePoolTarget names the two pools Dataproc reconciles into the GKE cluster — a
# controller pool and a driver/executor pool — and the provider expands the short
# pool ids into the full projects/.../nodePools/... names the API requires.
# nodePoolConfig is input-only on real GCP ("It will not be returned by the
# API"), the emulator's read does not echo it, and both pools are adopted on
# refresh rather than re-specified, so a re-plan is clean. `locations` is
# required by the provider even where the API could infer it, and both pools must
# name the same zone: local.node_pool_location tracks gke_location for a zonal
# cluster and falls back to the region's first zone for a regional one.
#
# Destroy order: deleting the virtual cluster leaves the reconciled node pools
# behind (real Dataproc does not own them); deleting the GKE cluster is what
# removes them. The depends_on below is what makes Terraform create GKE first and
# destroy it last — without it a destroy could remove the target out from under
# the pools it is meant to reclaim.
resource "google_dataproc_cluster" "gke" {
  name    = var.dataproc_cluster_name
  project = var.project
  region  = var.region

  virtual_cluster_config {
    staging_bucket = google_storage_bucket.staging.name

    kubernetes_cluster_config {
      kubernetes_namespace = var.namespace

      gke_cluster_config {
        gke_cluster_target = local.gke_cluster_target

        node_pool_target {
          node_pool = "${var.dataproc_cluster_name}-controller"
          roles     = ["DEFAULT", "CONTROLLER"]

          node_pool_config {
            config {
              machine_type = var.machine_type
            }

            autoscaling {
              min_node_count = var.min_nodes
              max_node_count = var.max_nodes
            }

            locations = [local.node_pool_location]
          }
        }

        node_pool_target {
          node_pool = "${var.dataproc_cluster_name}-spark"
          roles     = ["SPARK_DRIVER", "SPARK_EXECUTOR"]

          node_pool_config {
            config {
              machine_type = var.machine_type
            }

            autoscaling {
              min_node_count = var.min_nodes
              max_node_count = var.max_nodes
            }

            locations = [local.node_pool_location]
          }
        }
      }

      kubernetes_software_config {
        component_version = {
          SPARK = var.spark_release
        }
      }
    }
  }

  depends_on = [google_container_cluster.gke]
}

# ── The job: provision -> run ─────────────────────────────────────────────────
# google_dataproc_job is the provider resource that calls jobs.submit. This is
# the demo's proof that the "provision" half is not all Terraform can do: the
# job below points at the uploaded Spark file, names the virtual cluster as its
# placement, and reads/writes the module's own buckets.
#
# wait_for_completion = true is deliberate. With the default (false) the
# provider stops waiting at RUNNING, so the resource's first read would not yet
# be the terminal state; the demo wants a job it can assert as DONE. The
# provider's waiter polls GetJob, and the emulator's mock executor advances one
# state per read (PENDING -> SETUP_DONE -> RUNNING -> DONE), so this converges
# without a driver.
#
# Destroy order is encoded in the references, not in a prose promise:
#   * placement.cluster_name names google_dataproc_cluster.gke, so Terraform
#     creates the virtual cluster first and destroys the job before it;
#   * the virtual cluster's gke_cluster_target and depends_on name the GKE
#     cluster, so the GKE cluster is created first and destroyed last.
# Without those edges a destroy could delete the GKE cluster while the job or
# the virtual cluster still referenced it. run.sh proves the middle step: a
# targeted destroy of the virtual cluster leaves both reconciled node pools
# readable, and only the GKE cluster delete removes them.
resource "google_dataproc_job" "wordcount" {
  project = var.project
  region  = var.region

  placement {
    cluster_name = google_dataproc_cluster.gke.name
  }

  pyspark_config {
    main_python_file_uri = "gs://${google_storage_bucket.input.name}/${google_storage_bucket_object.job_code.name}"

    # The job's own contract: read the seeded input object, write under the
    # output bucket's prefix. The output is a prefix, not a file, because
    # Spark writes a directory of part files.
    args = [
      "gs://${google_storage_bucket.input.name}/${google_storage_bucket_object.job_input.name}",
      "gs://${google_storage_bucket.output.name}/${var.job_output_prefix}",
    ]
  }

  wait_for_completion = true
}
