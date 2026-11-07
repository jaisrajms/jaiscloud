# JaisCloud GCP Terraform / OpenTofu compatibility fixture.
#
# Drives the hashicorp/google provider against jaiscloud-gcp through per-service
# custom endpoints. The endpoint values come from one artifact,
# terraform-endpoints.yaml: run.sh asks tools/tfcorpus for a generated
# `endpoints` tfvars file, and the locals below are the offline defaults for a
# bare `terraform plan` in this directory. Cloud Run v2 is served but its
# consumer case arrives in wave-plan W2.2; Service Usage and Cloud Resource
# Manager project-level IAM are included (google_project_service,
# google_project_iam_member).
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

variable "uptime_display_name" {
  type        = string
  description = "Display name of the uptime check configuration; changing it drives an in-place update."
  default     = "jaiscloud-tf-uptime"
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

variable "routine_body" {
  type        = string
  description = "BigQuery routine SQL body; changing it drives an in-place routines.update."
  default     = "x * 2"
}

variable "row_access_policy_predicate" {
  type        = string
  description = "BigQuery row access policy filter predicate; changing it drives an in-place rowAccessPolicies.update."
  default     = "region = \"EU\""
}

variable "endpoints" {
  type        = map(string)
  description = "Provider custom-endpoint values generated from terraform-endpoints.yaml (run.sh passes them as a tfvars file)."
  default     = {}
}

locals {
  # Offline defaults for a bare `terraform plan`: the same values
  # terraform-endpoints.yaml generates, derived from var.endpoint.
  endpoint_defaults = {
    big_query_custom_endpoint          = "${var.endpoint}/bigquery/v2/"
    cloud_billing_custom_endpoint      = "${var.endpoint}/v1/"
    dns_custom_endpoint                = "${var.endpoint}/dns/v1/"
    sql_custom_endpoint                = "${var.endpoint}/sql/v1beta4/"
    compute_custom_endpoint            = "${var.endpoint}/compute/v1/"
    container_custom_endpoint          = var.container_endpoint
    dataproc_custom_endpoint           = "${var.endpoint}/"
    eventarc_custom_endpoint           = "${var.endpoint}/v1/"
    firestore_custom_endpoint          = "${var.endpoint}/v1/"
    cloud_functions_custom_endpoint    = "${var.endpoint}/v1/"
    cloudfunctions2_custom_endpoint    = "${var.endpoint}/v2/"
    iam_custom_endpoint                = "${var.endpoint}/"
    iam_beta_custom_endpoint           = "${var.endpoint}/v1/"
    kms_custom_endpoint                = "${var.endpoint}/v1/"
    logging_custom_endpoint            = "${var.endpoint}/v2/"
    managed_kafka_custom_endpoint      = "${var.endpoint}/v1/"
    redis_custom_endpoint              = "${var.endpoint}/v1/"
    dataproc_metastore_custom_endpoint = "${var.endpoint}/v1/"
    monitoring_custom_endpoint         = "${var.endpoint}/"
    pubsub_custom_endpoint             = "${var.endpoint}/v1/"
    resource_manager_custom_endpoint   = "${var.endpoint}/v1/"
    cloud_run_v2_custom_endpoint       = "${var.endpoint}/v2/"
    cloud_scheduler_custom_endpoint    = "${var.endpoint}/v1/"
    secret_manager_custom_endpoint     = "${var.endpoint}/v1/"
    service_usage_custom_endpoint      = "${var.endpoint}/v1/"
    storage_custom_endpoint            = "${var.endpoint}/storage/v1/"
    cloud_tasks_custom_endpoint        = "${var.endpoint}/v2/"
    workflows_custom_endpoint          = "${var.endpoint}/v1/"
  }
  endpoints = merge(local.endpoint_defaults, var.endpoints)
}

provider "google" {
  project = var.project
  region  = var.region

  user_project_override = false

  big_query_custom_endpoint     = local.endpoints["big_query_custom_endpoint"]
  cloud_billing_custom_endpoint = local.endpoints["cloud_billing_custom_endpoint"]
  dns_custom_endpoint           = local.endpoints["dns_custom_endpoint"]
  sql_custom_endpoint           = local.endpoints["sql_custom_endpoint"]
  compute_custom_endpoint       = local.endpoints["compute_custom_endpoint"]
  # GKE. The provider keeps only the scheme+host of a custom endpoint, so the
  # container.* host that run.sh derives is the only way to reach GKE through
  # it; the emulator serves container.v1 under that host token or the
  # /container/ path prefix, because the bare /v1/.../clusters path collides
  # with Managed Kafka.
  container_custom_endpoint = local.endpoints["container_custom_endpoint"]
  # Dataproc. RemoveBasePathVersion strips the version, so the origin is right
  # for the generated client (it appends v1/ itself).
  dataproc_custom_endpoint  = local.endpoints["dataproc_custom_endpoint"]
  eventarc_custom_endpoint  = local.endpoints["eventarc_custom_endpoint"]
  firestore_custom_endpoint = local.endpoints["firestore_custom_endpoint"]
  # hashicorp/google 7.46.2 names the v1 endpoint cloud_functions_custom_endpoint
  # (cloudfunctions2_custom_endpoint is the v2 one).
  cloud_functions_custom_endpoint = local.endpoints["cloud_functions_custom_endpoint"]
  cloudfunctions2_custom_endpoint = local.endpoints["cloudfunctions2_custom_endpoint"]
  iam_custom_endpoint             = local.endpoints["iam_custom_endpoint"]
  iam_beta_custom_endpoint        = local.endpoints["iam_beta_custom_endpoint"]
  kms_custom_endpoint             = local.endpoints["kms_custom_endpoint"]
  logging_custom_endpoint         = local.endpoints["logging_custom_endpoint"]
  managed_kafka_custom_endpoint   = local.endpoints["managed_kafka_custom_endpoint"]
  redis_custom_endpoint           = local.endpoints["redis_custom_endpoint"]
  # Dataproc Metastore (service + federation + database/table IAM).
  dataproc_metastore_custom_endpoint = local.endpoints["dataproc_metastore_custom_endpoint"]
  # Cloud Monitoring (declarative uptime check configurations). The provider
  # appends its own /v3/ path root, so the endpoint is the bare origin.
  monitoring_custom_endpoint = local.endpoints["monitoring_custom_endpoint"]
  pubsub_custom_endpoint     = local.endpoints["pubsub_custom_endpoint"]
  # Service Usage + the Cloud Resource Manager v1 project lookup that
  # google_project_service performs on every read, and project-level IAM for
  # google_project_iam_member.
  resource_manager_custom_endpoint = local.endpoints["resource_manager_custom_endpoint"]
  cloud_run_v2_custom_endpoint     = local.endpoints["cloud_run_v2_custom_endpoint"]
  cloud_scheduler_custom_endpoint  = local.endpoints["cloud_scheduler_custom_endpoint"]
  secret_manager_custom_endpoint   = local.endpoints["secret_manager_custom_endpoint"]
  service_usage_custom_endpoint    = local.endpoints["service_usage_custom_endpoint"]
  storage_custom_endpoint          = local.endpoints["storage_custom_endpoint"]
  cloud_tasks_custom_endpoint      = local.endpoints["cloud_tasks_custom_endpoint"]
  workflows_custom_endpoint        = local.endpoints["workflows_custom_endpoint"]
}
