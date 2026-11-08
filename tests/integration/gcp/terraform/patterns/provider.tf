# Shared provider block for the TF4 composition patterns.
#
# patterns/lib.sh copies this file into the throwaway work dir together with a
# pattern's own `*.tf`, so each pattern directory is a complete root module
# (main.tf + outputs.tf) once assembled. The per-service custom endpoints come
# from terraform-endpoints.yaml, the single map the fixture (`run.sh`), the
# module driver (`run-module.sh`) and the corpus tool (`tools/tfcorpus`)
# consume. The locals below are the offline defaults for a bare
# `terraform plan` in this directory (no Go, no generated tfvars): they are the
# same values the map emits, derived from var.endpoint.
#
# Credentials: the emulator ignores auth, but the provider needs *some* token,
# so the pattern driver exports GOOGLE_OAUTH_ACCESS_TOKEN.

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
  description = "GCP project the pattern's provider defaults to (the bootstrap pattern creates its own project and passes it explicitly)."
  default     = "test-project"
}

variable "region" {
  type        = string
  description = "Default region for regional resources (KMS, keys, buckets)."
  default     = "us-central1"
}

variable "endpoints" {
  type        = map(string)
  description = "Provider custom-endpoint values generated from terraform-endpoints.yaml (the driver passes them as a tfvars file)."
  default     = {}
}

locals {
  # container is routed by host, not by path: the provider keeps only
  # scheme+host of a custom endpoint, so the first DNS label must be
  # "container" (the emulator routes GKE that way, and the bare /v1/... path
  # belongs to Managed Kafka). The emitted tfvars carry the same rewrite.
  container_endpoint = "${replace(var.endpoint, "://", "://container.")}/container/"

  endpoint_defaults = {
    big_query_custom_endpoint          = "${var.endpoint}/bigquery/v2/"
    cloud_billing_custom_endpoint      = "${var.endpoint}/v1/"
    cloud_functions_custom_endpoint    = "${var.endpoint}/v1/"
    cloud_scheduler_custom_endpoint    = "${var.endpoint}/v1/"
    container_custom_endpoint          = local.container_endpoint
    dataproc_custom_endpoint           = "${var.endpoint}/"
    dataproc_metastore_custom_endpoint = "${var.endpoint}/v1/"
    eventarc_custom_endpoint           = "${var.endpoint}/v1/"
    iam_custom_endpoint                = "${var.endpoint}/"
    iam_beta_custom_endpoint           = "${var.endpoint}/v1/"
    kms_custom_endpoint                = "${var.endpoint}/v1/"
    pubsub_custom_endpoint             = "${var.endpoint}/v1/"
    resource_manager_custom_endpoint   = "${var.endpoint}/v1/"
    secret_manager_custom_endpoint     = "${var.endpoint}/v1/"
    service_usage_custom_endpoint      = "${var.endpoint}/v1/"
    storage_custom_endpoint            = "${var.endpoint}/storage/v1/"
  }
  endpoints = merge(local.endpoint_defaults, var.endpoints)
}

provider "google" {
  project = var.project
  region  = var.region

  user_project_override = false

  big_query_custom_endpoint          = local.endpoints["big_query_custom_endpoint"]
  cloud_billing_custom_endpoint      = local.endpoints["cloud_billing_custom_endpoint"]
  cloud_functions_custom_endpoint    = local.endpoints["cloud_functions_custom_endpoint"]
  cloud_scheduler_custom_endpoint    = local.endpoints["cloud_scheduler_custom_endpoint"]
  container_custom_endpoint          = local.endpoints["container_custom_endpoint"]
  dataproc_custom_endpoint           = local.endpoints["dataproc_custom_endpoint"]
  dataproc_metastore_custom_endpoint = local.endpoints["dataproc_metastore_custom_endpoint"]
  eventarc_custom_endpoint           = local.endpoints["eventarc_custom_endpoint"]
  iam_custom_endpoint                = local.endpoints["iam_custom_endpoint"]
  iam_beta_custom_endpoint           = local.endpoints["iam_beta_custom_endpoint"]
  kms_custom_endpoint                = local.endpoints["kms_custom_endpoint"]
  pubsub_custom_endpoint             = local.endpoints["pubsub_custom_endpoint"]
  resource_manager_custom_endpoint   = local.endpoints["resource_manager_custom_endpoint"]
  secret_manager_custom_endpoint     = local.endpoints["secret_manager_custom_endpoint"]
  service_usage_custom_endpoint      = local.endpoints["service_usage_custom_endpoint"]
  storage_custom_endpoint            = local.endpoints["storage_custom_endpoint"]
}
