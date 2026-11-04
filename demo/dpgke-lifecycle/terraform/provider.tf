# Dataproc-on-GKE lifecycle demo — Terraform module.
#
# Creates the infrastructure the rest of the demo runs on: the enabled APIs, the
# service account a job runs as, the input/output/staging buckets, a GKE cluster
# and a Dataproc-on-GKE virtual cluster that targets it. Nothing here submits a
# job or installs anything on a cluster.
#
# The provider is driven against jaiscloud-gcp through per-service custom
# endpoints, the mechanism proven by tests/integration/gcp/terraform/. Only
# services the emulator implements are declared; everything this module needs is
# in that set.
#
# Credentials: the emulator ignores auth, but the provider needs *some* token, so
# the caller exports GOOGLE_OAUTH_ACCESS_TOKEN (any non-empty value). Against real
# GCP the provider would use Application Default Credentials and nothing else in
# this module would change.

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.36"
    }
  }
}

variable "endpoint" {
  type        = string
  description = "Base URL of the running jaiscloud-gcp REST endpoint."
  default     = "http://localhost:8080"
}

variable "container_endpoint" {
  type        = string
  description = "Override for the GKE endpoint. Leave null: the container.<host> form derived below is the only one the provider can reach GKE on."
  default     = null
}

# The hashicorp/google provider STRIPS the path from a custom endpoint: it keeps
# scheme+host and re-appends its own service path, and the single path segment it
# insists on (present only to satisfy the provider's endpoint regexp) is never
# sent. A path-only endpoint such as http://localhost:8080/container/ therefore
# degenerates to http://localhost:8080/.
#
# That matters because /v1/projects/{p}/locations/{l}/clusters is genuinely
# shared with Managed Kafka on a single origin. The emulator does narrow it, so
# nothing is silently answered by the wrong service: a request carrying
# clusterId is Managed Kafka's, a create body with a GKE `cluster` object and no
# clusterId is GKE's, and otherwise it probes which service holds a cluster at
# that project+location. But that is a heuristic over one origin, and a module a
# customer copies should not depend on it.
#
# Real GKE publishes its own host, and the emulator serves GKE on a host whose
# first DNS label is "container" (the container.googleapis.com form). Deriving
# that host from var.endpoint in HCL keeps the module self-contained: the compat
# fixture does the same transform in run.sh, and the crypto-medallion demo does
# neither — which is why it cannot do GKE-shaped work through the provider at
# all.
locals {
  # regex returns the capture groups only, so [0] is the scheme and [1] the host
  # (with its port, if any).
  endpoint_parts = regex("^(https?)://([^/]+)", var.endpoint)

  derived_container_endpoint = "${local.endpoint_parts[0]}://container.${local.endpoint_parts[1]}/container/"

  container_endpoint = coalesce(var.container_endpoint, local.derived_container_endpoint)
}

provider "google" {
  project = var.project
  region  = var.region

  # Pinned false rather than inherited from the environment: when set, the
  # provider sends userProject=<project> as a quota project on billing-enabled
  # calls, and the emulator does not model requester-pays at all (DGD9).
  user_project_override = false

  # The provider appends its own "/b/{bucket}/..." to this base, so the emulator
  # needs the /storage/v1/ service root here rather than the bare origin.
  storage_custom_endpoint = "${var.endpoint}/storage/v1/"

  # IAM (the job service account, the bucket IAM members). Both roots are set:
  # google_service_account in this provider series goes through the beta IAM
  # client, and an unset beta endpoint falls back to the real iam.googleapis.com —
  # a 401 ("Request had invalid authentication credentials") that looks like a
  # credentials problem but is really a missing custom endpoint. The v1 form is
  # what the emulator serves.
  iam_custom_endpoint      = "${var.endpoint}/"
  iam_beta_custom_endpoint = "${var.endpoint}/v1/"

  # Service Usage + the Cloud Resource Manager v1 project lookup that
  # google_project_service performs on every read. The v1 root is the documented
  # form for both.
  service_usage_custom_endpoint    = "${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "${var.endpoint}/v1/"

  # Dataproc (the virtual cluster). The provider keeps only scheme+host, so no
  # path prefix is needed and none would be honoured.
  dataproc_custom_endpoint = "${var.endpoint}/"

  # GKE. See the locals block above: a container.* host, which is also all the
  # provider can express — it keeps scheme+host only.
  #
  # Do not add a clusterId query parameter to the GKE create to help the routing
  # along: GKE and Managed Kafka share
  # /v1/projects/{p}/locations/{l}/clusters, and clusterId is exactly the signal
  # that makes a request Kafka's. The provider sends none, which is correct and
  # must stay that way.
  container_custom_endpoint = local.container_endpoint
}
