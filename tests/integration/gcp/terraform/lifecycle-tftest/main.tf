# Root module for the Terraform-native part of the lifecycle gate (TF5).
#
# `terraform test` (lifecycle.tftest.hcl) asserts the attributes the provider
# exposes after read-back: computed ids, self links and nested objects. The
# shell cases assert the server-side fields the provider hides (metageneration,
# locationType, primary/destroy state, policy etags) through the emulator's REST
# API. Both exist deliberately: HCL conditions are the strongest check of what a
# Terraform user sees, REST is the only way to see the wire the provider
# normalises away.
#
# `add_terraform_attribution_label = false` keeps the computed label maps empty
# so the assertions do not depend on the provider's attribution label.

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
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
  description = "GCP project id."
  default     = "test-project"
}

provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false
  add_terraform_attribution_label = false

  storage_custom_endpoint = "${var.endpoint}/storage/v1/"
  pubsub_custom_endpoint  = "${var.endpoint}/v1/"
  kms_custom_endpoint     = "${var.endpoint}/v1/"
}

resource "google_storage_bucket" "b" {
  name     = "lc-tftest-bucket"
  location = "US"

  versioning {
    enabled = true
  }
}

resource "google_pubsub_topic" "t" {
  name                       = "lc-tftest-topic"
  message_retention_duration = "86600s"
}

resource "google_pubsub_subscription" "s" {
  name                 = "lc-tftest-sub"
  topic                = google_pubsub_topic.t.name
  ack_deadline_seconds = 20

  expiration_policy {
    ttl = ""
  }
}

resource "google_kms_key_ring" "r" {
  name     = "lc-tftest-ring"
  location = "global"
}

resource "google_kms_crypto_key" "k" {
  name     = "lc-tftest-key"
  key_ring = google_kms_key_ring.r.id
  purpose  = "ENCRYPT_DECRYPT"

  rotation_period = "7776000s"

  version_template {
    algorithm        = "GOOGLE_SYMMETRIC_ENCRYPTION"
    protection_level = "SOFTWARE"
  }
}
