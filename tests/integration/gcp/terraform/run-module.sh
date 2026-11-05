#!/usr/bin/env bash
#
# run-module.sh — opt-in gate that drives a *third-party* Terraform module
# against jaiscloud-gcp, rather than the in-repo fixture (run.sh).
#
# Why: the emulator's supported surface is defined by real consumers, not by
# fixtures we wrote ourselves. The pinned consumer here is
# terraform-google-modules/terraform-google-pubsub, whose `main.tf` reads
# `data "google_project"` on every plan to build the default Pub/Sub service
# agent e-mail from `data.google_project.project.number`. That data source also
# exposes `billing_account`, so the plan performs
# `GET /v1/projects/{p}/billingInfo` — the call that used to 404 with
# `UnknownService` and blocked the module (plan CB1/CB3).
#
# The script generates a minimal root module in a temp dir (provider block with
# the per-service custom endpoints + a module block naming the local module
# path), then runs:
#
#   init -> plan -> apply -> spot checks -> second plan (permadiff)
#        -> update phase 1 (ack/labels/retention) -> plan
#        -> update phase 2 (ack/labels/retention) -> plan
#        -> clear-back (labels {}, ack/retention unset) -> plan
#        -> destroy
#
# The update phases are the PS2 acceptance run: the hashicorp/google provider
# PATCHes `subscriptions.patch?updateMask=<changed>,bigqueryConfig` on every
# re-apply, so the emulator must accept those masks (and the absent trailing
# bigqueryConfig is a clear, not an unknown path). The second plan after each
# apply must report `No changes`; an apply log carrying `unsupported update_mask
# path` or an HTTP 400 fails the run.
#
# It never clones anything: point TF_MODULE_DIR (or $3) at a checkout you made
# yourself. When that directory does not exist it prints SKIP and exits 0, so it
# is safe to invoke unconditionally.
#
# Usage:
#   tests/integration/gcp/terraform/run-module.sh [ENDPOINT] [PROJECT] [MODULE_DIR]
#
#     ENDPOINT     base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#     PROJECT      project id (default: test-project)
#     MODULE_DIR   local checkout of the consumer module
#                  (default: $TF_MODULE_DIR, else
#                   $HOME/.cache/terraform-modules/terraform-google-pubsub)
#
# Env:
#   TF_MODULE_DIR  the module checkout (see above)
#   TF_BIN         terraform binary to use (default: terraform; TF_BIN=tofu works)
#
# Prefer the Makefile target, which starts/stops the emulator for you:
#   make test-gcp-terraform-pubsub

set -euo pipefail

ENDPOINT="${1:-http://localhost:8080}"
PROJECT="${2:-test-project}"
MODULE_DIR="${3:-${TF_MODULE_DIR:-$HOME/.cache/terraform-modules/terraform-google-pubsub}}"
TF="${TF_BIN:-terraform}"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  exit 0
fi

if [[ ! -d "$MODULE_DIR" ]]; then
  echo "SKIP: consumer module not found at $MODULE_DIR"
  echo "      clone it once (network needed only for this), then re-run:"
  echo "        git clone https://github.com/terraform-google-modules/terraform-google-pubsub \"$MODULE_DIR\""
  echo "        git -C \"$MODULE_DIR\" checkout 6cf1860"
  echo "      and/or point TF_MODULE_DIR at your checkout."
  exit 0
fi

MODULE_DIR="$(cd "$MODULE_DIR" && pwd)"
if [[ ! -f "$MODULE_DIR/main.tf" ]]; then
  echo "ERROR: $MODULE_DIR does not look like the terraform-google-pubsub module (no main.tf)"
  exit 1
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/jc-tf-module.XXXXXX")"
DESTROYED=0
destroy() {
  [[ "$DESTROYED" == "1" ]] && return 0
  DESTROYED=1
  "$TF" -chdir="$WORK" destroy -input=false -auto-approve -no-color >/dev/null 2>&1 || true
}
cleanup() {
  destroy
  rm -rf "$WORK"
}
trap 'cleanup' EXIT INT TERM

PASS=0
FAIL=0
pass() {
  printf '  [PASS] %s\n' "$1"
  PASS=$((PASS + 1))
}
fail() {
  printf '  [FAIL] %s\n' "$1"
  FAIL=$((FAIL + 1))
}

TOPIC="jc-module-topic"
SUB="jc-module-sub"

# ── the generated root module ─────────────────────────────────────────────────
# The provider block carries exactly the custom endpoints the consumer module
# touches (one trailing slash is mandatory for the provider's endpoint regexp):
#   pubsub          -> the topic/subscription the module creates
#   resource_manager-> data "google_project"
#   cloud_billing   -> the data source's billing_account read (the CB1 gap)
#   iam             -> the module's service-agent IAM member
#   service_usage   -> the provider's project-service lookups
#
# write_root_module LABELS_HCL ACK RETENTION rewrites the module block so each
# acceptance phase changes the three subscription inputs the provider PATCHes:
# subscription_labels is module-wide, ack_deadline_seconds and
# message_retention_duration are per pull_subscriptions entry. An empty ACK or
# RETENTION omits the key (the module then uses its own default/leave-unset).
write_root_module() {
  local labels_hcl="$1" ack="$2" retention="$3"
  local sub_extra=""
  if [[ -n "$ack" ]]; then
    sub_extra+="      ack_deadline_seconds       = ${ack}"$'\n'
  fi
  if [[ -n "$retention" ]]; then
    sub_extra+="      message_retention_duration = \"${retention}\""$'\n'
  fi
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

  subscription_labels = ${labels_hcl}

  pull_subscriptions = [
    {
      name = "$SUB"
${sub_extra}    },
  ]
}

output "topic" {
  value = module.pubsub.topic
}

output "subscription_names" {
  value = module.pubsub.subscription_names
}
EOF
}

# apply_module LABEL LOGNAME runs apply, fails the run on a provider 400 or an
# `unsupported update_mask path` in the log, and PASSes otherwise.
apply_module() {
  local label="$1" log="$WORK/apply-$2.log"
  if ! "$TF" -chdir="$WORK" apply -input=false -auto-approve -no-color >"$log" 2>&1; then
    cat "$log"
    fail "$label (see the log above)"
    return 1
  fi
  if grep -q "unsupported update_mask path" "$log"; then
    cat "$log"
    fail "$label (apply log reports 'unsupported update_mask path')"
    return 1
  fi
  if grep -q "Error 400" "$log"; then
    cat "$log"
    fail "$label (apply log reports an HTTP 400)"
    return 1
  fi
  pass "$label"
}

# plan_no_changes LABEL LOGNAME asserts plan -detailed-exitcode exits 0.
plan_no_changes() {
  local label="$1" log="$WORK/plan-$2.log" rc=0
  set +e
  "$TF" -chdir="$WORK" plan -input=false -no-color -detailed-exitcode >"$log" 2>&1
  rc=$?
  set -e
  case "$rc" in
    0) pass "$label" ;;
    2)
      cat "$log"
      fail "$label (a permadiff: plan reports changes)"
      ;;
    *)
      cat "$log"
      fail "$label (plan failed, exit $rc)"
      ;;
  esac
}

echo "== $TF: consumer module $MODULE_DIR (endpoint=$ENDPOINT project=$PROJECT)"

write_root_module '{ team = "platform", managed-by = "terraform" }' "" ""

if ! "$TF" -chdir="$WORK" init -input=false -no-color >"$WORK/init.log" 2>&1; then
  cat "$WORK/init.log"
  fail "init (see the log above)"
  echo "== summary: $PASS passed, $FAIL failed"
  exit 1
fi
pass "init (provider resolved from the registry/local cache)"

if "$TF" -chdir="$WORK" plan -input=false -no-color >"$WORK/plan.log" 2>&1; then
  pass "plan (data.google_project.number read, topic + subscription + IAM member planned)"
else
  cat "$WORK/plan.log"
  fail "plan (see the log above: the consumer module could not be planned)"
fi

if [[ "$FAIL" -gt 0 ]]; then
  echo "== summary: $PASS passed, $FAIL failed"
  exit 1
fi

if ! apply_module "apply (create with subscription_labels)" create; then
  echo "== summary: $PASS passed, $FAIL failed"
  exit 1
fi

# Spot checks through the emulator's own REST API: `apply` exiting 0 is not
# enough, the module's resources must be readable, and the billingInfo singleton
# the data source read must answer the no-account default (the CB1 gap).
check() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

# check_absent NAME URL WANT... asserts the read body contains none of the WANT
# strings (used for the clear-back: the module's label keys must be gone).
check_absent() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "$name (the subscription could not be read)"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      fail "$name (still present: $want)"
      return 0
    fi
  done
  pass "$name"
}

SUB_URL="$ENDPOINT/v1/projects/$PROJECT/subscriptions/$SUB"

echo "== spot checks"
check "module created the topic" \
  "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC" "\"name\":\"projects/$PROJECT/topics/$TOPIC\""
check "module created the pull subscription" \
  "$SUB_URL" \
  "\"name\":\"projects/$PROJECT/subscriptions/$SUB\"" \
  "\"topic\":\"projects/$PROJECT/topics/$TOPIC\""
# PS1: the module's subscription_labels round-trip on the create response AND the
# read-back. Before the fix the subscription GET omitted `labels` entirely.
check "create stored the module's subscription_labels" \
  "$SUB_URL" '"team":"platform"' '"managed-by":"terraform"'
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
plan_no_changes "re-plan reports no changes" permadiff

# ── PS2 acceptance: the provider's update masks ───────────────────────────────
# Each phase changes ack_deadline_seconds, message_retention_duration and the
# label map, which is exactly the "change an ack deadline, label and retention,
# apply again" sequence recorded against the real provider. The PATCH masks are
# `ackDeadlineSeconds,bigqueryConfig`, `messageRetentionDuration,bigqueryConfig`
# and `labels,bigqueryConfig`; the trailing bigqueryConfig is absent from the
# body (a clear). A 400 aborts the apply and fails the phase.

echo "== $TF: update phase 1 (ack 20, retention 1200s, extra label)"
write_root_module '{ team = "platform", managed-by = "terraform", environment = "staging" }' 20 "1200s"
if ! apply_module "update apply (ack 20, retention 1200s, extra label)" update1; then
  echo "== summary: $PASS passed, $FAIL failed"
  exit 1
fi
check "update 1 wrote labels/ack/retention" "$SUB_URL" \
  '"team":"platform"' '"environment":"staging"' \
  '"ackDeadlineSeconds":20' '"messageRetentionDuration":"1200s"'
plan_no_changes "re-plan after update 1 reports no changes" update1

echo "== $TF: update phase 2 (ack 30, retention 2400s, changed/extra label)"
write_root_module '{ team = "data", managed-by = "terraform", environment = "staging", cost-center = "cc-42" }' 30 "2400s"
if ! apply_module "update apply (ack 30, retention 2400s, changed/extra label)" update2; then
  echo "== summary: $PASS passed, $FAIL failed"
  exit 1
fi
check "update 2 wrote labels/ack/retention" "$SUB_URL" \
  '"team":"data"' '"cost-center":"cc-42"' \
  '"ackDeadlineSeconds":30' '"messageRetentionDuration":"2400s"'
plan_no_changes "re-plan after update 2 reports no changes" update2

echo "== $TF: clear-back (labels {}, ack/retention unset)"
write_root_module '{}' "" ""
if ! apply_module "clear-back apply (labels {}, ack/retention unset)" clear; then
  echo "== summary: $PASS passed, $FAIL failed"
  exit 1
fi
check_absent "clear-back removed the module's labels" "$SUB_URL" \
  '"team"' '"managed-by"' '"environment"' '"cost-center"'
check "clear-back reset retention to the 7-day default" "$SUB_URL" \
  '"messageRetentionDuration":"604800s"'
plan_no_changes "re-plan after clear-back reports no changes" clear

echo "== destroy"
if "$TF" -chdir="$WORK" destroy -input=false -auto-approve -no-color >"$WORK/destroy.log" 2>&1; then
  DESTROYED=1
  pass "destroy"
else
  cat "$WORK/destroy.log"
  fail "destroy (see the log above)"
fi

echo "== summary: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
