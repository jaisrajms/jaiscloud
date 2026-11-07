# shellcheck shell=bash
#
# Case: pubsub — the lifecycle paths a Terraform user performs on a topic and a
# subscription.
#
#   apply -> import (topic + subscription, clean plan) -> refresh-only
#   -> -replace the subscription (destroy+create) -> ForceNew rename (old
#   subscription 404) -> out-of-band label drift on the topic (plan shows the
#   managed key, the unmanaged key is removed because Pub/Sub's update resolves
#   the whole labels map) -> label import artifact (classified) -> negative paths
#   -> destroy
#
# Differences from the storage case that make this worth its own resource:
#   - Pub/Sub's UpdateTopic/UpdateSubscription carry an updateMask; the drift
#     check exercises the served `updateMask=labels` path and the emulator's
#     unsupported-mask 400.
#   - Subscription identity is its resource name, so a ForceNew rename is a
#     clean DELETE + CREATE and the old subscription is a real 404.
#   - A topic and its subscription are two resources with a dependency, so the
#     destroy order and the re-import of both are exercised.

TOPIC="lc-ps-topic"
SUB="lc-ps-sub"
SUB_RENAMED="lc-ps-sub-renamed"

PROVIDER_HCL='provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false
  add_terraform_attribution_label = false

  pubsub_custom_endpoint = "${var.endpoint}/v1/"
}'

# write_main <subscription-name> <topic-labels-hcl> generates the root module.
# The first phase declares no labels so the import plan is strict; the drift
# phase passes a labels block and the subscription name changes for the ForceNew
# rename.
write_main() {
  local sub="$1" labels="${2:-}"
  cat >"$WORK/main.tf" <<EOF
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
  type    = string
  default = "$ENDPOINT"
}

variable "project" {
  type    = string
  default = "$PROJECT"
}

$PROVIDER_HCL

resource "google_pubsub_topic" "strict" {
  name                       = "$TOPIC"
  message_retention_duration = "86600s"
  $labels
}

resource "google_pubsub_subscription" "strict" {
  name                 = "$sub"
  topic                = google_pubsub_topic.strict.name
  ack_deadline_seconds = 20

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}
EOF
}

case_main() {
  write_main "$SUB"
  init_phase
  plan_phase "plan (topic + subscription)"
  apply_phase
  assert_no_api_errors "$LAST_APPLY_LOG" "apply"

  echo "== spot checks (create)"
  check "topic read-back" "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" \
    "\"name\":\"projects/$PROJECT/topics/$TOPIC\"" \
    '"messageRetentionDuration":"86600s"'
  check "subscription read-back" "$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB" \
    "\"name\":\"projects/$PROJECT/subscriptions/$SUB\"" \
    "\"topic\":\"projects/$PROJECT/topics/$TOPIC\"" \
    '"ackDeadlineSeconds":20' \
    '"minimumBackoff":"10s"' \
    '"maximumBackoff":"600s"'
  plan_no_changes "re-plan after create reports no changes"

  echo "== refresh"
  # The empty-labels normalization (labels = {}) is the same provider artifact
  # as in the storage case; any other changed attribute fails.
  refresh_phase labels-artifact "refresh-only plan reports no drift (empty-labels normalization classified)"

  echo "== import"
  import_phase google_pubsub_topic.strict "projects/$PROJECT/topics/$TOPIC"
  import_phase google_pubsub_subscription.strict "projects/$PROJECT/subscriptions/$SUB"

  echo "== replace (-replace forces destroy+create)"
  replace_phase google_pubsub_subscription.strict "replace the subscription"
  check "replaced subscription is readable" \
    "$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB" \
    "\"name\":\"projects/$PROJECT/subscriptions/$SUB\""

  echo "== ForceNew rename (old subscription must be gone)"
  write_main "$SUB_RENAMED"
  apply_phase "apply (subscription name is ForceNew)"
  check_status "the old subscription is gone" \
    "$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB" 404
  check_status "the renamed subscription exists" \
    "$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB_RENAMED" 200
  plan_no_changes "re-plan after the rename reports no changes"

  echo "== out-of-band drift"
  # Declare labels on the topic so they are inside Terraform's managed fields.
  write_main "$SUB_RENAMED" 'labels = { env = "lifecycle" }'
  apply_phase "apply (add topic labels)"
  plan_no_changes "re-plan after adding labels reports no changes"
  out_of_band "PATCH the topic's labels outside Terraform" PATCH \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC?updateMask=labels" \
    '{"labels":{"env":"drifted","drift":"yes"}}'
  # The Pub/Sub resource uses the same tpgresource.SetLabels path as the bucket:
  # only keys already in state are planned, and the update sends the refreshed
  # effective map back, so an unmanaged extra key is preserved rather than
  # removed. The managed key must be detected and converged.
  plan_expect_diff "plan shows the managed out-of-band label" '"drifted"'
  apply_phase "apply (converge the drift)"
  plan_no_changes "re-plan after drift convergence reports no changes"
  check "topic labels converged to configuration" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" '"env":"lifecycle"'
  check "the unmanaged label key is preserved (SetLabels filters it)" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" '"drift":"yes"'

  echo "== import with labels (provider artifact, classified)"
  import_phase google_pubsub_topic.strict "projects/$PROJECT/topics/$TOPIC" label-artifact
  apply_phase "apply (reconstruct the labels after import)"
  plan_no_changes "re-plan after the label reconstruction reports no changes"

  echo "== negative paths"
  expect_http_error "duplicate topic create" PUT \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" '{}' 409
  expect_http_error "unsupported update mask" PATCH \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC?updateMask=name" \
    '{"name":"ignored"}' 400
  expect_http_error "invalid message retention duration" PUT \
    "$ENDPOINT/v1/projects/$PROJECT/topics/lc-ps-invalid" \
    '{"messageRetentionDuration":"not-a-duration"}' 400
  curl -s -o /dev/null -X PUT -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' -d '{}' \
    "$ENDPOINT/v1/projects/$PROJECT/topics/lc-ps-dup" 2>/dev/null || true
  provider_error_create "duplicate topic create surfaces through the provider" \
    "$PROVIDER_HCL" \
    'resource "google_pubsub_topic" "dup" {
  name = "lc-ps-dup"
}' \
    "409"
  curl -s -o /dev/null -X DELETE -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/lc-ps-dup" 2>/dev/null || true

  destroy_phase
  check_status "destroy leaves the topic 404" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" 404
  check_status "destroy leaves the subscription 404" \
    "$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB_RENAMED" 404
}
