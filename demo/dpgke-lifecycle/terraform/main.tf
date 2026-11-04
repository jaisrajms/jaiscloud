# Resources for the Dataproc-on-GKE lifecycle demo.
#
# Scope: the bucket + cluster + virtual-cluster infrastructure only. No job is
# submitted and nothing is installed on a cluster here; the job and the installer
# are separate legs of the demo.
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
# uniform_bucket_level_access is deliberately NOT set. The emulator's bucket read
# always reports iamConfiguration.uniformBucketLevelAccess.enabled = false (the
# field is never stored), so setting it would produce a permanent diff. Add it
# back when the emulator echoes it.
resource "google_storage_bucket" "input" {
  name          = local.buckets.input
  project       = local.storage_project
  location      = "US"
  force_destroy = true

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "output" {
  name          = local.buckets.output
  project       = local.storage_project
  location      = "US"
  force_destroy = true

  depends_on = [google_project_service.apis]
}

# Dataproc stages into this one, so it is named by the virtual cluster below
# rather than by the job.
resource "google_storage_bucket" "staging" {
  name          = local.buckets.staging
  project       = local.storage_project
  location      = "US"
  force_destroy = true

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
