# shellcheck shell=bash
#
# Case: terraform-google-pubsub topic field updates (PTF2)
#
# The pinned consumer is the same terraform-google-pubsub module the `pubsub`
# case uses. This case drives its *topic* resource through the whole mutation
# lifecycle the provider's update_mask exercises:
#
#   create (labels + kmsKeyName + retention + storage policy) -> read back
#   -> plan -detailed-exitcode = 0
#   -> change labels/retention/kms/storage policy (+ schemaSettings on a raw
#      topic, because the module's schema input needs a schemas API the emulator
#      does not serve) -> apply -> no 400 -> plan = 0
#   -> remove message_storage_policy -> apply -> plan = 0 (the clear contract)
#
# Before PTF2 the re-applies failed with
# `400 unsupported update_mask path: messageStoragePolicy` / `schemaSettings`
# and the plan after create showed "2 to change".

MODULE_REPO="terraform-google-pubsub"
MODULE_COMMIT="6cf1860"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-pubsub"

TOPIC="jc-topic-update-fields"
RAW_TOPIC="jc-topic-schema"
SCHEMA="projects/$PROJECT/schemas/jc-schema"
KEY_ONE="projects/$PROJECT/locations/global/keyRings/jc/cryptoKeys/key-one"
KEY_TWO="projects/$PROJECT/locations/global/keyRings/jc/cryptoKeys/key-two"

case_main() {
  cat >"$WORK/main.tf" <<EOF
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      # Pinned: the recorded pre-fix baseline and this gate both ran against
      # google 7.46.2 (the existing pubsub case resolves the same version from
      # ">= 6.2, < 9").
      version = "= 7.46.2"
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

# base -> changed -> cleared; each phase is a distinct apply followed by a
# no-changes plan. TF_VAR_mutation selects it without rewriting the config.
variable "mutation" {
  type    = string
  default = "base"
}

locals {
  changed = var.mutation == "changed"
  cleared = var.mutation == "cleared"

  labels = local.changed ? { stage = "changed", team = "data" } : { stage = "base", team = "data" }
  retention = local.changed ? "1800s" : "600s"
  kms_key = local.changed ? "$KEY_TWO" : "$KEY_ONE"
  storage_policy = (
    local.cleared ? {} :
    local.changed ? { allowed_persistence_regions = ["europe-west1"] } :
    { allowed_persistence_regions = ["us-central1"] }
  )
  schema_encoding = local.changed ? "BINARY" : "JSON"
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

  project_id             = var.project
  topic                  = "$TOPIC"
  create_subscriptions   = false
  grant_token_creator    = false
  topic_labels           = local.labels
  topic_kms_key_name     = local.kms_key
  topic_message_retention_duration = local.retention
  message_storage_policy = local.storage_policy
}

# schemaSettings is only wire-exercised here: the emulator has no schemas API to
# create the referenced schema, so the module's schema input (which needs a
# google_pubsub_schema) cannot run. The provider stores/serves the setting
# verbatim.
resource "google_pubsub_topic" "schema" {
  project = var.project
  name    = "$RAW_TOPIC"

  schema_settings {
    schema   = "$SCHEMA"
    encoding = local.schema_encoding
  }
}
EOF

  init_phase

  # ── phase 1: create ──────────────────────────────────────────────────────
  export TF_VAR_mutation=base
  plan_phase "plan (module topic + raw schema topic)"
  if [[ "$FAIL" -gt 0 ]]; then
    summary
    exit 1
  fi
  apply_phase "apply create"
  assert_no_api_errors
  echo "== spot checks (create read-back)"
  check "create served labels/kms/retention/storage policy" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" \
    '"stage":"base"' "\"kmsKeyName\":\"$KEY_ONE\"" '"messageRetentionDuration":"600s"' \
    '"allowedPersistenceRegions":["us-central1"]'
  check "create served schema settings" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$RAW_TOPIC" \
    "\"schema\":\"$SCHEMA\"" '"encoding":"JSON"'
  plan_no_changes "plan after create reports no changes"

  # ── phase 2: change every mutable field ──────────────────────────────────
  export TF_VAR_mutation=changed
  apply_phase "apply changed labels/retention/kms/storage policy/schema settings"
  assert_no_api_errors
  echo "== spot checks (changed read-back)"
  check "changed fields served" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" \
    '"stage":"changed"' "\"kmsKeyName\":\"$KEY_TWO\"" '"messageRetentionDuration":"1800s"' \
    '"allowedPersistenceRegions":["europe-west1"]'
  check "changed schema encoding served" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$RAW_TOPIC" '"encoding":"BINARY"'
  plan_no_changes "plan after change reports no changes"

  # ── phase 3: remove message_storage_policy ───────────────────────────────
  # google_pubsub_topic.message_storage_policy is Optional+Computed: removing
  # the block stops managing the field, so the provider's read adopts the
  # remote value and the plan stays clean without sending a clear mask. The
  # server-side clear contract is proven by the explicit PATCH below (and by
  # the unit/conformance suites).
  export TF_VAR_mutation=cleared
  apply_phase "apply without message_storage_policy"
  assert_no_api_errors
  plan_no_changes "plan after removing message_storage_policy reports no changes"

  echo "== clear contract (explicit REST PATCH)"
  curl -sf -X PATCH -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' \
    --data "{\"topic\":{\"name\":\"projects/$PROJECT/topics/$TOPIC\"}}" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC?updateMask=messageStoragePolicy" \
    >"$WORK/clear.json" 2>/dev/null || true
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "clear read-back (no response from topic)"
  elif [[ "$body" == *'"messageStoragePolicy"'* ]]; then
    fail "messageStoragePolicy still present after clear: $body"
  else
    pass "messageStoragePolicy cleared by update_mask"
  fi

  destroy_phase
}
