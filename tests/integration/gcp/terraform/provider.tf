# JaisCloud GCP Terraform / OpenTofu compatibility fixture.
#
# Drives the hashicorp/google provider against jaiscloud-gcp through per-service
# custom endpoints. Only services jaiscloud-gcp implements are declared here;
# Cloud Run v2, Service Usage and Cloud Resource Manager project-level IAM are
# deliberately excluded (see README.md).
#
# Credentials: the emulator ignores auth, but the provider needs *some* token,
# so run.sh exports GOOGLE_OAUTH_ACCESS_TOKEN.

terraform {
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

variable "project" {
  type        = string
  description = "GCP project id the fixture deploys into."
  default     = "test-project"
}

variable "region" {
  type        = string
  description = "Default region for regional resources (Cloud SQL, KMS)."
  default     = "us-central1"
}

variable "gke_cluster" {
  type        = string
  description = "Name of the GKE cluster the fixture creates as the Dataproc-on-GKE prerequisite."
  default     = "jaiscloud-tf-gke"
}

variable "container_endpoint" {
  type        = string
  description = "Endpoint for container.googleapis.com with a container.-prefixed host (the emulator routes GKE by host; see provider.tf)."
  default     = "http://container.localhost:8080/container/"
}

provider "google" {
  project = var.project
  region  = var.region

  user_project_override = false

  storage_custom_endpoint        = "${var.endpoint}/storage/v1/"
  iam_custom_endpoint            = "${var.endpoint}/"
  iam_beta_custom_endpoint       = "${var.endpoint}/v1/"
  secret_manager_custom_endpoint = "${var.endpoint}/v1/"
  sql_custom_endpoint            = "${var.endpoint}/sql/v1beta4/"
  kms_custom_endpoint            = "${var.endpoint}/v1/"
  pubsub_custom_endpoint         = "${var.endpoint}/v1/"

  # Service Usage + the Cloud Resource Manager v1 project lookup that
  # google_project_service performs on every read, and project-level IAM for
  # google_project_iam_member.
  service_usage_custom_endpoint    = "${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "${var.endpoint}/v1/"

  # Dataproc Metastore (service + federation + database/table IAM). The
  # federation resource (google_dataproc_metastore_federation) and the
  # database IAM member go through the same v1 custom endpoint.
  dataproc_metastore_custom_endpoint = "${var.endpoint}/v1/"

  # Dataproc (clusters incl. the Dataproc-on-GKE virtual cluster). The provider
  # keeps only the scheme+host of a custom endpoint, so the plain v1 root is
  # right for dataproc: it carries no path-prefix requirement.
  dataproc_custom_endpoint = "${var.endpoint}/"

  # GKE. The emulator serves container.v1 either under a "/container/" path
  # prefix or under a host whose first DNS label is "container" (real
  # container.googleapis.com), because the bare /v1/.../clusters path collides
  # with Managed Kafka. The provider keeps only scheme+host here too, so the
  # container.* host that run.sh derives is the only way to reach GKE through
  # it.
  container_custom_endpoint = var.container_endpoint
}
