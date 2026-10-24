# Crypto-medallion demo — Terraform infra subset (design §5.1).
#
# Drives the hashicorp/google provider against the running jaiscloud-gcp
# emulator through per-service custom endpoints (the mechanism proven by
# tests/integration/gcp/terraform/). The provider's endpoint story does NOT
# reach every service the demo needs, so the data-platform resources (Managed
# Kafka, Dataproc, Cloud Run, Eventarc, Scheduler, BigQuery, Firestore) are
# created by gcloud/REST instead (design §5.1, plan_docs/gcp-terraform-compat-2026-09-22.md).
#
# The project itself is created before Terraform runs: `google_project` cannot
# work here because the provider reads the project's billing account from the
# real Cloud Billing API, which the emulator does not serve (verified: apply
# fails with a billing CREDENTIALS_MISSING). scripts/democtl.py does
# `gcloud projects create` then applies this config inside it.
#
# Credentials: the emulator ignores auth, but the provider needs *some* token,
# so the caller exports GOOGLE_OAUTH_ACCESS_TOKEN.

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
  description = "GCP project id the demo deploys into."
  default     = "crypto-medallion"
}

variable "region" {
  type        = string
  description = "Default region for regional resources (KMS, Metastore)."
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
  kms_custom_endpoint            = "${var.endpoint}/v1/"
  pubsub_custom_endpoint         = "${var.endpoint}/v1/"

  # Service Usage (enable the demo's APIs) + the Cloud Resource Manager v1
  # project lookup google_project_service performs on every read.
  service_usage_custom_endpoint    = "${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "${var.endpoint}/v1/"
}
