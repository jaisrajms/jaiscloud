# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-dataproc (TF3-5)
#
# The pinned consumer is the CFT Dataproc blueprint (commit 56c7c7ed630f). It
# creates one served resource:
#
#   google_dataproc_cluster   -> clusters.create / .get / .patch / .delete
#
# and exposes the GCE-shaped `cluster_config` (master/worker/software/security
# blocks) and the GKE-shaped `virtual_cluster_config`. The fixture already
# declares both shapes as raw resources; this case is the *module consumer*
# gate: the module renders the cluster config and the provider converges.
#
# The generated root module adds a `google_dataproc_job` (pyspark) in the
# module's cluster, so the served job path (jobs.submit / .get / .list /
# .delete) is gated by the same case, plus a `google_dataproc_job_iam_member`
# that drives the jobs IAM mixin (TF1-2; the fixture covers the cluster one).
#
# Pruned surface, recorded rather than worked around:
#
#  * The module's `main.tf` unconditionally reads
#    `data.google_compute_default_service_account` and grants that account
#    `roles/dataproc.worker` through `google_project_iam_member`. Both are
#    excluded with `-target`: the data source calls Compute
#    `projects.get` (`GET /compute/v1/projects/{project}`), which the emulator
#    does not serve yet — a 501 recorded as TF2-1
#    (`fix/gcp-compute-projects-get`). Everything the case targets is the
#    module's cluster, the root module's job and its job IAM member; the
#    module's IAM prerequisite is not part of the measured surface. This is the
#    same "prune the input that reaches an unserved surface and record it" rule
#    the other cases follow.
#  * `virtual_cluster_config` is not driven here: the fixture already covers the
#    GKE-shaped cluster and its NodePool reconciliation, and this case is the
#    GCE-shaped consumer path. The module's GKE inputs are left null.
#
# Provider note: `dataproc_custom_endpoint` is the bare origin (the Dataproc
# client appends v1/). The case pins google = 7.46.2, the same version as the
# other 7.x cases. Every apply log must be free of 400/501 and
# `unsupported update_mask`.
#
# Lifecycle:
#
#   apply (module cluster + root-module pyspark job)
#     -> REST asserts (cluster name/labels/config counts/software version;
#        job placement/pyspark/labels)
#   plan -detailed-exitcode -> No changes
#   mutate (labels, worker count) -> apply -> REST asserts -> No changes
#   destroy -> cluster and job GET are 404
#
# The provider creates the job with a server-generated id, so the case reads
# the root module's `job_id` output instead of assuming one.

MODULE_REPO="terraform-google-dataproc"
MODULE_COMMIT="56c7c7ed630f"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-dataproc"

CAPTURE_RESOURCES="google_dataproc_cluster,google_dataproc_job"

CLUSTER="jc-tf-dataproc"
JOB_LABEL="terraform-case"
STAGING="gs://jc-tf-dataproc-staging"

# The module reads default service account through Compute projects.get, which
# is not served yet (TF2-1), and grants it a project role. Target the two
# resources the case measures, for plan, apply and destroy alike.
TARGETS="-target=module.dataproc.google_dataproc_cluster.cluster -target=google_dataproc_job.job -target=google_dataproc_job_iam_member.job"
export TF_CLI_ARGS_plan="$TARGETS"
export TF_CLI_ARGS_apply="$TARGETS"
export TF_CLI_ARGS_destroy="$TARGETS"

# write_module generates the root module for one gate phase.
#   $1 revision label (kept in the job labels, so a mutate re-creates the job)
#   $2 cluster labels (HCL map literal)
#   $3 worker count (HCL number literal)
write_module() {
  local revision="$1" labels="$2" workers="$3"

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
    }
  }
}

variable "endpoint" {
  type    = string
  default = "$ENDPOINT"
}

variable "project" {
  type    = string
  default = "$PROJECT"
}

provider "google" {
  project               = var.project
  region                = "us-central1"
  user_project_override = false

  dataproc_custom_endpoint = "\${var.endpoint}/"
}

module "dataproc" {
  source = "$MODULE_DIR"

  project_id = var.project
  name       = "$CLUSTER"
  region     = "us-central1"
  labels     = $labels

  cluster_config = {
    master_config = {
      num_instances = 1
      machine_type  = "n1-standard-4"
    }
    worker_config = {
      num_instances = $workers
      machine_type  = "n1-standard-4"
    }
    software_config = {
      image_version = "2.2-debian12"
    }
  }
}

resource "google_dataproc_job" "job" {
  project = var.project
  region  = "us-central1"

  placement {
    cluster_name = module.dataproc.cluster_name
  }

  labels = {
    env      = "$JOB_LABEL"
    revision = "$revision"
  }

  pyspark_config {
    main_python_file_uri = "$STAGING/wordcount.py"
    args                 = ["$STAGING/input", "$STAGING/output"]
  }
}

# Job resource-level IAM (TF1-2): google_dataproc_job_iam_member drives
# jobs.getIamPolicy/setIamPolicy on projects/{p}/regions/{r}/jobs/{job_id}. The
# job id is server-generated, so it is taken from the resource's computed
# reference. A non-authoritative member keeps the policy read-modify-write
# non-empty, and the read-back etag must converge (plan No changes below).
resource "google_dataproc_job_iam_member" "job" {
  project = var.project
  region  = "us-central1"
  job_id  = google_dataproc_job.job.reference[0].job_id
  role    = "roles/dataproc.viewer"
  member  = "user:tf-dataproc-iam@example.com"
}

output "job_id" {
  value = google_dataproc_job.job.id
}
EOF
}

cluster_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/regions/us-central1/clusters/$CLUSTER"
}

job_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/regions/us-central1/jobs/$1"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

post_body() {
  curl -sf -X POST -H "Content-Type: application/json" -d '{}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_post_body is check_body for the IAM custom methods.
check_post_body() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(post_body "$url")"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

check_body() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(body_of "$url")"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

job_id() {
  tf output -raw job_id 2>/dev/null | awk -F/ '{print $NF}' || true
}

case_main() {
  local id body

  write_module "v1" '{ env = "terraform-case" }' 2
  init_phase
  plan_phase "plan (module dataproc cluster + root job)"
  apply_phase "apply (module dataproc cluster + root job)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "cluster echoes its name, labels and worker count" "$(cluster_url)" \
    "\"clusterName\":\"$CLUSTER\"" \
    '"env":"terraform-case"' \
    '"numInstances":2'
  check_body "cluster echoes the requested software version" "$(cluster_url)" \
    '"imageVersion":"2.2-debian12"'
  check_body "cluster reports a master and a worker instance group" "$(cluster_url)" \
    '"masterConfig"' \
    '"workerConfig"'

  id="$(job_id)"
  if [[ -z "$id" ]]; then
    fail "the root module exposes the created job id"
  else
    pass "the root module exposes the created job id"
    check_body "job echoes its placement, labels and pyspark driver" "$(job_url "$id")" \
      "\"clusterName\":\"$CLUSTER\"" \
      '"env":"terraform-case"' \
      '"revision":"v1"' \
      "$STAGING/wordcount.py" \
      '"state":"DONE"'
    check_post_body "job IAM policy carries the granted member" \
      "$(job_url "$id"):getIamPolicy" \
      '"bindings"' '"role":"roles/dataproc.viewer"' '"user:tf-dataproc-iam@example.com"'
  fi

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate: new cluster labels and a third worker. The job reference is
  # unchanged, so only the cluster is PATCHed.
  write_module "v1" '{ env = "terraform-case-v2", tier = "gold" }' 3
  apply_phase "apply (cluster labels + worker count)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "cluster echoes the mutated labels and worker count" "$(cluster_url)" \
    '"env":"terraform-case-v2"' \
    '"tier":"gold"' \
    '"numInstances":3'

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  destroy_phase

  local status
  status="$(status_of "$(cluster_url)")"
  if [[ "$status" == "404" ]]; then
    pass "cluster GET after destroy is 404"
  else
    fail "cluster GET after destroy = HTTP $status, want 404"
  fi
  if [[ -n "$id" ]]; then
    status="$(status_of "$(job_url "$id")")"
    if [[ "$status" == "404" ]]; then
      pass "job GET after destroy is 404"
    else
      fail "job GET after destroy = HTTP $status, want 404"
    fi
  fi
}
