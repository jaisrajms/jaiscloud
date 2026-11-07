# shellcheck shell=bash
#
# Case: terraform-google-modules/terraform-google-kubernetes-engine (TF3-6)
#
# The pinned consumer is the CFT `terraform-google-kubernetes-engine` ROOT module
# at v47.0.0 — the most widely used GKE module. It renders one
# `google_container_cluster`, one `google_container_node_pool`, a node service
# account and its project IAM grants (plus client-side `random_string`).
#
# This case is deliberately **plan-only** (wave-plan TF3-6). GKE's control plane
# in the emulator is metadata-only: cluster/nodePool create/get/list/update/
# delete are served, but a real cluster's node bootstrap, kubelet and data plane
# are not, and the module's default surface reaches firewall/service-account
# resources whose apply behavior is not what this family measures. Forcing an
# apply would either fail the whole aggregate or produce a green that says
# nothing, so the gate measures what the module *would* send:
#
#   init -> validate -> plan (saved; no apply, no destroy)
#
# and asserts that the module's plan contains the cluster, the default node pool
# and the node service account. A provider panic or an unrouted request fails;
# an API diagnostic is a recorded gap (the case sets
# PLAN_ONLY_ALLOW_DIAGNOSTICS=1 and prints the plan diagnostics as the gap list).
#
# The plan also proves the two reads the module depends on are served:
#
#   data.google_container_engine_versions.{region,zone} -> Container
#     GetServerConfig (served over container.v1)
#
# `zones` is supplied, so the module does not read
# `data.google_compute_zones` (a metadata-only Compute surface the emulator
# serves, but one this case does not need); supplying it keeps the plan
# deterministic.
#
# The GKE provider route is host-based: the provider keeps only scheme+host of
# `container_custom_endpoint`, so the endpoint carries the `container.` host and
# the emulator routes `container.v1` by it (terraform-endpoints.yaml). The
# compute/iam endpoints cover the module's data sources and IAM resources; they
# are only read at plan time, not applied.
#
# `deletion_protection = false` and a concrete `kubernetes_version` are set so
# the rendered plan is stable; neither is applied.

MODULE_REPO="terraform-google-kubernetes-engine"
MODULE_COMMIT="a19597ac18ad"

# Plan-only by construction (wave-plan TF3-6). The driver reads this before
# case_main and skips apply/destroy and the FC6 capture.
PLAN_ONLY=1
# A plan diagnostic from an unmodelled data plane is recorded, not a failure;
# the plan still has to reach the resources the case asserts.
PLAN_ONLY_ALLOW_DIAGNOSTICS=1

CLUSTER="jc-tf-gke"

write_module() {
  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "= 2.38.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "= 3.7.2"
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
  project                  = var.project
  region                   = "us-central1"
  zone                     = "us-central1-a"
  user_project_override    = false
  container_custom_endpoint    = "$(endpoint_for container)"
  compute_custom_endpoint      = "$(endpoint_for compute)"
  iam_custom_endpoint          = "$(endpoint_for iam)"
  iam_beta_custom_endpoint     = "\${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "$(endpoint_for resourcemanager)"
}

module "gke" {
  source = "$MODULE_DIR"

  project_id = var.project
  name       = "$CLUSTER"
  region     = "us-central1"
  regional   = true
  zones      = ["us-central1-a", "us-central1-b", "us-central1-c"]

  network           = "jc-tf-gke-net"
  subnetwork        = "jc-tf-gke-sub"
  ip_range_pods     = "jc-tf-gke-pods"
  ip_range_services = "jc-tf-gke-services"

  kubernetes_version  = "1.33"
  release_channel     = "REGULAR"
  deletion_protection = false

  master_authorized_networks = [{
    cidr_block   = "10.0.0.0/8"
    display_name = "internal"
  }]
}
EOF
}

case_main() {
  write_module
  init_phase
  plan_only_phase "plan (module gke cluster + default node pool)"

  plan_includes "module.gke.google_container_cluster.primary" \
    "plan renders the cluster"
  plan_includes "module.gke.google_container_node_pool.pools[\"default-node-pool\"]" \
    "plan renders the default node pool"
  plan_includes "module.gke.google_service_account.cluster_service_account[0]" \
    "plan renders the node service account"

  # The engine-versions data source is the read the module cannot avoid: it is
  # served by Container GetServerConfig, and its absence would have failed the
  # plan above. Assert it so a future routing change is visible as a read, not
  # only as a generic plan failure.
  if grep -qE 'data\.google_container_engine_versions\.(region|zone): Read complete' "$WORK/plan.log"; then
    pass "plan read container engine versions from GetServerConfig"
  else
    fail "plan did not read data.google_container_engine_versions (see $WORK/plan.log)"
  fi

  # Record the plan's own diagnostics as the gap list. Warnings are expected
  # where the module or provider deprecates a field; they are printed so the
  # scorecard can carry them, and are not a failure (real GCP plans would show
  # the same provider-side deprecation).
  local gaps
  gaps="$(grep -E '^(Warning|Error):' "$WORK/plan.log" || true)"
  if [[ -n "$gaps" ]]; then
    echo "== plan diagnostics (recorded gaps)"
    printf '%s\n' "$gaps"
  else
    echo "== plan diagnostics: none"
  fi

  echo "== plan-only: no apply, no destroy (GKE's control plane is metadata-only)"
}
