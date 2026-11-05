# shellcheck shell=bash
#
# Case: terraform-google-pubsub
#
# The pinned consumer is terraform-google-modules/terraform-google-pubsub. Its
# `main.tf` reads `data "google_project"` on every plan to build the default
# Pub/Sub service agent e-mail from `data.google_project.project.number`. That
# data source also exposes `billing_account`, so the plan performs
# `GET /v1/projects/{p}/billingInfo` — the call that used to 404 with
# `UnknownService` and blocked the module (plan CB1/CB3).
#
# The provider block carries exactly the custom endpoints the module touches
# (one trailing slash is mandatory for the provider's endpoint regexp):
#   pubsub          -> the topic/subscription the module creates
#   resource_manager-> data "google_project"
#   cloud_billing   -> the data source's billing_account read (the CB1 gap)
#   iam             -> the module's service-agent IAM member
#   service_usage   -> the provider's project-service lookups

MODULE_REPO="terraform-google-pubsub"
MODULE_COMMIT="6cf1860"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-pubsub"

TOPIC="jc-module-topic"
SUB="jc-module-sub"

case_main() {
  cat >"$WORK/main.tf" <<EOF
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.2, < 9"
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
  project = var.project
  region  = "us-central1"

  user_project_override = false

  pubsub_custom_endpoint            = "\${var.endpoint}/v1/"
  resource_manager_custom_endpoint  = "\${var.endpoint}/v1/"
  cloud_billing_custom_endpoint     = "\${var.endpoint}/v1/"
  iam_custom_endpoint               = "\${var.endpoint}/"
  service_usage_custom_endpoint     = "\${var.endpoint}/v1/"
}

module "pubsub" {
  source = "$MODULE_DIR"

  project_id = var.project
  topic      = "$TOPIC"

  pull_subscriptions = [
    {
      name = "$SUB"
    },
  ]
}

output "topic" {
  value = module.pubsub.topic
}

output "subscription_names" {
  value = module.pubsub.subscription_names
}
EOF

  init_phase
  plan_phase "plan (data.google_project.number read, topic + subscription + IAM member planned)"

  if [[ "$FAIL" -gt 0 ]]; then
    summary
    exit 1
  fi

  apply_phase

  # Spot checks through the emulator's own REST API: `apply` exiting 0 is not
  # enough, the module's resources must be readable, and the billingInfo
  # singleton the data source read must answer the no-account default (the CB1
  # gap).
  echo "== spot checks"
  check "module created the topic" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" "\"name\":\"projects/$PROJECT/topics/$TOPIC\""
  check "module created the pull subscription" \
    "$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB" \
    "\"name\":\"projects/$PROJECT/subscriptions/$SUB\"" \
    "\"topic\":\"projects/$PROJECT/topics/$TOPIC\""
  # data "google_project" reads projects.getBillingInfo for its billing_account
  # attribute; the emulator models no account catalog, so the live answer is the
  # four-field default. Before CB1 this path was an UnknownService 404, which is
  # what blocked the module.
  check "data.google_project billingInfo read answers the no-account default" \
    "$ENDPOINT/v1/projects/$PROJECT/billingInfo" \
    "\"name\":\"projects/$PROJECT/billingInfo\"" \
    '"billingAccountName":""' '"billingEnabled":false'

  # A second plan must report no changes: the module's IAM member, topic and
  # subscription must read back exactly what the provider stored on apply (no
  # permadiff on the emulator's synthesized project number either).
  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  destroy_phase
}
