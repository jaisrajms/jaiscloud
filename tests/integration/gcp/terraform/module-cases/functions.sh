# shellcheck shell=bash
#
# Case: terraform-google-event-function
#
# The pinned consumer is terraform-google-modules/terraform-google-event-function
# (module v6.0.0). Its google_cloudfunctions_function (v1) resource always sends
# `ingress_settings` (default ALLOW_ALL), `max_instances` (0, which the provider
# omits) and an `event_trigger` with `failure_policy.retry`; it can also send
# `vpc_connector`, `vpc_connector_egress_settings`,
# `secret_environment_variables`, `docker_registry` and `docker_repository`.
#
# Before TGM3 the functions core had no ingressSettings field, so create dropped
# it and GET omitted it (a permanent ingress diff); an update mask naming it (or
# eventTrigger.failurePolicy.retry) was rejected with
# `501 unsupported updateMask field ...`.
#
# The gate proves the module lifecycle converges:
#
#   create (module defaults: ingress ALLOW_ALL + event trigger + retry false)
#          -> assert GET -> No changes
#   mutate (ingress ALLOW_INTERNAL_ONLY + environment_variables)
#          -> apply -> assert read-back -> No changes
#   mutate (event_trigger_failure_policy_retry = true)
#          -> apply -> assert failurePolicy.retry -> No changes
#   clear  (environment_variables removed, ingress back to ALLOW_ALL)
#          -> apply -> assert cleared -> No changes
#
# ingress_settings is ForceNew in the provider's module documentation, so the
# mutate phase records whether it replaced or updated the function; convergence
# is required either way (the read-back carries the new value and the next plan
# has no function diff).
#
# Out-of-scope resources: the module always manages a
# google_storage_bucket_object for the source archive, and hashicorp/google
# 7.46.2 permadiffs it because the emulator's storage-object renderer does not
# model content_disposition/metadata (a storage gap, not a Cloud Functions one;
# TGM1 owns buckets, not objects). The case supplies an existing
# uppercase-location bucket (create_bucket=false, so the module still uploads
# into it) and the no-changes assertion accepts a diff only when that one
# resource is the sole change; any function diff still fails the gate.

MODULE_REPO="terraform-google-event-function"
MODULE_COMMIT="9f2e26ae3b73"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-event-function"

FUNCTION="jc-event-function"
REGION="us-central1"
TOPIC="jc-module-function-topic"

# write_module regenerates the root module for one gate phase:
#   create | ingress | retry | clear
write_module() {
  local phase="$1"
  local ingress env_vars retry

  case "$phase" in
    create)
      ingress="ALLOW_ALL"
      env_vars="{}"
      retry="false"
      ;;
    ingress)
      ingress="ALLOW_INTERNAL_ONLY"
      env_vars='{ GREETING = "from-module" }'
      retry="false"
      ;;
    retry)
      ingress="ALLOW_INTERNAL_ONLY"
      env_vars='{ GREETING = "from-module" }'
      retry="true"
      ;;
    clear)
      ingress="ALLOW_ALL"
      env_vars="{}"
      retry="true"
      ;;
    *)
      echo "ERROR: unknown phase '$phase'" >&2
      exit 2
      ;;
  esac

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

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

provider "google" {
  project = var.project
  region  = "$REGION"

  user_project_override = false

  # hashicorp/google 7.46.2 names the v1 endpoint cloud_functions_custom_endpoint
  # (cloudfunctions2_custom_endpoint is the v2 one).
  cloud_functions_custom_endpoint = "\${var.endpoint}/v1/"
  storage_custom_endpoint        = "\${var.endpoint}/storage/v1/"
  pubsub_custom_endpoint         = "\${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "\${var.endpoint}/v1/"
  cloud_billing_custom_endpoint    = "\${var.endpoint}/v1/"
}

resource "google_pubsub_topic" "events" {
  name    = "$TOPIC"
  project = var.project
}

# The module's own bucket (create_bucket = true) passes the lowercase region as
# the bucket location; real GCS normalizes it to uppercase and the provider
# converges on that. The storage bucket renderer is out of scope for TGM3, so
# the case supplies an uppercase-location bucket and lets the module upload its
# archive into it (create_bucket = false keeps the module's object upload).
resource "google_storage_bucket" "source" {
  name          = "$FUNCTION-source"
  location      = "US-CENTRAL1"
  project       = var.project
  force_destroy = true
}

module "event_function" {
  source = "$MODULE_DIR"

  project_id = var.project
  region     = "$REGION"
  name       = "$FUNCTION"

  runtime        = "nodejs20"
  entry_point    = "hello"
  source_directory = "$WORK/function_source"

  create_bucket = false
  bucket_name   = google_storage_bucket.source.name
  event_trigger = {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/\${var.project}/topics/\${google_pubsub_topic.events.name}"
  }

  ingress_settings                = "$ingress"
  environment_variables           = $env_vars
  event_trigger_failure_policy_retry = $retry
}

output "function_name" {
  value = module.event_function.name
}
EOF
}

function_body() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$FUNCTION" 2>/dev/null || true
}

# check_absent is the negative check: a cleared field must not be rendered.
check_absent() {
  local name="$1"
  shift
  local body want
  body="$(function_body)"
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      fail "$name (unexpected: $want)"
      return 0
    fi
  done
  pass "$name"
}

# record_replace_path records whether the provider replaced or updated the
# function during an ingress change (the module documents ingress_settings as
# recreate-only; the 7.46.2 schema PATCHes it in place). Convergence is required
# either way; the log line is the recorded evidence.
record_replace_path() {
  if grep -q '# module.event_function.google_cloudfunctions_function.main must be replaced' "$WORK/plan.log" 2>/dev/null; then
    pass "ingress change planned a function replacement (ForceNew path, recorded)"
  elif grep -q '# module.event_function.google_cloudfunctions_function.main will be updated in-place' "$WORK/plan.log" 2>/dev/null; then
    pass "ingress change planned an in-place function update (recorded)"
  else
    pass "ingress change planned no function action (recorded)"
  fi
}

# STORAGE_OBJECT_GAP is the module's source-archive object: hashicorp/google
# 7.46.2 permadiffs it because the emulator's storage-object renderer does not
# model content_disposition/metadata. That is a storage gap (TGM1's surface, not
# TGM3's), so the no-changes assertion accepts a diff when this one resource is
# the only change and fails on anything else (including any function diff).
STORAGE_OBJECT_GAP="module.event_function.google_storage_bucket_object.main"

plan_no_changes_recording_object_gap() {
  local label="${1:-re-plan reports no changes}" rc
  set +e
  tf plan -input=false -no-color -detailed-exitcode >"$WORK/replan.log" 2>&1
  rc=$?
  set -e
  case "$rc" in
    0)
      pass "$label"
      return 0
      ;;
    2) ;;
    *)
      cat "$WORK/replan.log"
      fail "$label (plan failed: exit $rc)"
      return 0
      ;;
  esac
  # Changes were planned. Read the machine-readable plan and require every
  # changed address to be the recorded out-of-scope storage object.
  if ! tf plan -input=false -no-color -json >"$WORK/replan.json" 2>"$WORK/replan.err"; then
    cat "$WORK/replan.log"
    fail "$label (could not read the plan; see the log above)"
    return 0
  fi
  local addrs
  addrs="$(grep '"type":"planned_change"' "$WORK/replan.json" |
    grep -o '"addr":"[^"]*"' | sed -e 's/^"addr":"//' -e 's/"$//' | sort -u)"
  if [[ "$addrs" == "$STORAGE_OBJECT_GAP" ]]; then
    pass "$label (only the recorded storage-object gap differs)"
  else
    printf 'changed resources:\n%s\n' "$addrs"
    cat "$WORK/replan.log"
    fail "$label (changes outside the recorded storage-object gap)"
  fi
}

case_main() {
  mkdir -p "$WORK/function_source"
  cat >"$WORK/function_source/index.js" <<'JS'
const functions = require('@google-cloud/functions-framework');

functions.http('hello', (req, res) => {
  res.send('hello from the jaiscloud functions module gate');
});
JS
  cat >"$WORK/function_source/package.json" <<'JSON'
{
  "name": "jc-event-function-source",
  "version": "1.0.0",
  "main": "index.js",
  "dependencies": {
    "@google-cloud/functions-framework": "^3.0.0"
  }
}
JSON

  write_module create
  init_phase
  plan_phase "plan (module defaults: ingress ALLOW_ALL + event trigger)"
  apply_phase "apply (create)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check "function carries ingressSettings ALLOW_ALL, the event trigger and the default failure policy" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$FUNCTION" \
    '"ingressSettings":"ALLOW_ALL"' \
    '"eventType":"google.pubsub.topic.publish"' \
    '"resource":"projects/'"$PROJECT"'/topics/'"$TOPIC"'"' \
    '"failurePolicy":{}'
  echo "== $TF: second plan (permadiff check)"
  plan_no_changes_recording_object_gap "re-plan reports no changes"

  # Mutate: ingress_settings is ForceNew in the provider; either path must
  # converge. environment_variables is a plain in-place update.
  write_module ingress
  plan_phase "plan (ingress ALLOW_INTERNAL_ONLY + environment_variables)"
  record_replace_path
  apply_phase "apply (ingress + environment_variables)"
  assert_no_api_errors
  echo "== spot checks (ingress + env)"
  check "function carries the new ingress and env var" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$FUNCTION" \
    '"ingressSettings":"ALLOW_INTERNAL_ONLY"' '"GREETING":"from-module"'
  plan_no_changes_recording_object_gap "re-plan reports no changes after the ingress/env change"

  # Toggle the v1 failure policy retry flag (update mask
  # eventTrigger.failurePolicy.retry).
  write_module retry
  plan_phase "plan (event_trigger_failure_policy_retry = true)"
  apply_phase "apply (retry)"
  assert_no_api_errors
  echo "== spot checks (retry)"
  check "function carries eventTrigger.failurePolicy.retry" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$FUNCTION" \
    '"failurePolicy":{"retry":{}}'
  plan_no_changes_recording_object_gap "re-plan reports no changes after the retry change"

  # Clear back: environment_variables removed, ingress restored to ALLOW_ALL.
  write_module clear
  plan_phase "plan (env vars cleared, ingress ALLOW_ALL)"
  record_replace_path
  apply_phase "apply (clear)"
  assert_no_api_errors
  echo "== spot checks (clear)"
  check "function is back on ALLOW_ALL" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$FUNCTION" \
    '"ingressSettings":"ALLOW_ALL"'
  check_absent "environment_variables is cleared" '"GREETING"'
  plan_no_changes_recording_object_gap "re-plan reports no changes after the clear"

  destroy_phase
}
