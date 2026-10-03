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
}
