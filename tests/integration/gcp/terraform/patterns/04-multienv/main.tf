# Composition pattern 04 — multi-environment (TF4-4).
#
# The same small stack (a bucket pair, a topic, a secret with two versions) is
# applied twice, once per Terraform workspace: `staging` and `prod` are two
# states over one configuration, which is how a team provisions the same root
# module per environment. Every name carries `terraform.workspace`, so the two
# environments are distinct resources of the same shape, and destroying one
# workspace's state cannot reach the other's.
#
# What it proves that the fixture, the module cases and the other patterns do
# not:
#
#   - `for_each` (the bucket pair) and `count` (the secret versions) build the
#     per-environment resources from one configuration instead of repeated
#     resource blocks;
#   - two workspaces coexist: both environments are readable at once, with
#     different names and different `env` labels;
#   - an environment also spans two projects through a second provider
#     configuration: the stack's own resources use the default provider
#     (`var.project`) and the audit topic uses the `shared` alias
#     (`<project>-shared`, the cross-environment data plane). The alias resolves
#     the project; the server's read-back of the audit topic proves the resource
#     landed there, and the alias is the standard Terraform idiom for "one
#     resource in another project";
#   - a `terraform destroy` in `staging` leaves every `prod` resource readable
#     and `prod`'s plan clean, and vice versa.

variable "name_suffix" {
  type        = string
  description = "Run-unique suffix so two runs against one emulator cannot collide on a bucket, topic or secret name."
  default     = ""
}

locals {
  # The workspace is the environment: the driver applies this module once in
  # `staging` and once in `prod` (patterns/lib.sh's workspace_select).
  env    = terraform.workspace
  suffix = var.name_suffix

  # One `for_each` instance per role: the input and output bucket of the
  # environment's lake.
  bucket_roles = toset(["input", "output"])
}

# ── The second provider configuration: the shared (cross-environment) project ─
# An aliased provider inherits nothing from the default one, so it needs its own
# endpoint. Only Pub/Sub is set because the shared resource below is a topic;
# the default provider in patterns/provider.tf carries the full endpoint map.
provider "google" {
  alias  = "shared"
  region = var.region

  project               = "${var.project}-shared"
  user_project_override = false

  pubsub_custom_endpoint = local.endpoints["pubsub_custom_endpoint"]
}

# ── The environment's own stack ──────────────────────────────────────────────
# No explicit `project` on these resources: they take it from the default
# provider, which is how the alias's routing below stays observable.
resource "google_storage_bucket" "lake" {
  for_each = local.bucket_roles

  name          = "jc-tf-multienv-${local.env}-${each.key}${local.suffix}"
  location      = "US"
  force_destroy = true

  labels = {
    env  = local.env
    role = each.key
  }
}

resource "google_pubsub_topic" "events" {
  name = "jc-tf-multienv-${local.env}-events${local.suffix}"

  labels = {
    env = local.env
  }
}

resource "google_secret_manager_secret" "config" {
  secret_id = "jc-tf-multienv-${local.env}-config${local.suffix}"

  replication {
    auto {}
  }

  labels = {
    env = local.env
  }
}

# `count` rather than `for_each`: the versions are an ordered revision log, not
# a map. Two versions prove the secret is a real version container, not a single
# blob; the driver asserts both exist through versions.list.
resource "google_secret_manager_secret_version" "config" {
  count = 2

  secret      = google_secret_manager_secret.config.id
  secret_data = "env=${local.env} revision=${count.index + 1}"
}

# ── The shared plane, routed through the `shared` alias ─────────────────────
# No explicit `project`: this must land in `${var.project}-shared` because of
# the provider, and the driver asserts the resource name says so.
resource "google_pubsub_topic" "audit" {
  provider = google.shared

  name = "jc-tf-multienv-${local.env}-audit${local.suffix}"

  labels = {
    env   = local.env
    scope = "shared"
  }
}
