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
#   init -> plan -> apply -> plan -detailed-exitcode (permadiff) -> destroy
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

echo "== $TF: consumer module $MODULE_DIR (endpoint=$ENDPOINT project=$PROJECT)"

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

if "$TF" -chdir="$WORK" apply -input=false -auto-approve -no-color >"$WORK/apply.log" 2>&1; then
  pass "apply"
else
  cat "$WORK/apply.log"
  fail "apply (see the log above)"
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
set +e
"$TF" -chdir="$WORK" plan -input=false -no-color -detailed-exitcode >"$WORK/replan.log" 2>&1
RC=$?
set -e
case "$RC" in
  0) pass "re-plan reports no changes" ;;
  2)
    cat "$WORK/replan.log"
    fail "re-plan reports changes (a permadiff)"
    ;;
  *)
    cat "$WORK/replan.log"
    fail "re-plan failed (exit $RC)"
    ;;
esac

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
