#!/usr/bin/env bash
#
# run-module.sh — driver for the pinned third-party Terraform module gates.
#
# Why: the emulator's supported surface is defined by real consumers, not by
# fixtures we wrote ourselves. Each case pins one official
# terraform-google-modules repo, generates a throwaway root module around it,
# and requires the module's lifecycle to converge:
#
#   init -> plan -> apply -> (assert read-back) -> plan -detailed-exitcode
#        -> mutate -> apply -> plan -detailed-exitcode -> clear -> plan -> destroy
#
# The gate's acceptance is the `plan -detailed-exitcode` exit code 0 ("No
# changes"), not the apply exit code: a resource the provider stores and reads
# back differently is exactly what makes a module unusable.
#
# A case is one file: module-cases/<case>.sh. It sets
#
#   MODULE_REPO    terraform-google-modules repo name (also the default checkout
#                  directory name under $HOME/.cache/terraform-modules)
#   MODULE_COMMIT  the pinned commit printed in the SKIP/clone hint and README
#   MODULE_GIT     optional clone URL (defaults to the CFT GitHub URL)
#   MODULE_SUBDIR  optional module path inside the checkout, for a repo that is
#                  a collection of modules (e.g.
#                  terraform-google-cloud-operations/modules/simple-uptime-check);
#                  case_main then sees $MODULE_DIR as that module directory
#   case_main      the gate body; it runs with $WORK, $MODULE_DIR, $ENDPOINT,
#                  $PROJECT and the helpers below in scope
#
# and defines nothing at source time. Cases never clone from the network: point
# TF_MODULE_DIR at a checkout you made yourself. When that directory does not
# exist the script prints SKIP and exits 0, so it is safe to invoke
# unconditionally. It also SKIPs when the Terraform binary is absent.
#
# Usage:
#   tests/integration/gcp/terraform/run-module.sh <case> [ENDPOINT] [PROJECT]
#
#     case         name under module-cases/ (pubsub, storage, ...); run with no
#                  argument to list them
#     ENDPOINT     base URL of jaiscloud-gcp REST (default: $TF_ENDPOINT, else
#                  http://localhost:8080)
#     PROJECT      project id (default: $TF_PROJECT, else test-project)
#
# Env:
#   TF_MODULE_DIR  the module checkout for the named case (default:
#                  $HOME/.cache/terraform-modules/<MODULE_REPO>)
#   TF_BIN         terraform binary to use (default: terraform; TF_BIN=tofu works)
#
# Prefer the Makefile target, which starts/stops the emulator for you:
#   make test-gcp-terraform-pubsub
#   make test-gcp-terraform-storage

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASES_DIR="$SCRIPT_DIR/module-cases"

list_cases() {
  local f
  for f in "$CASES_DIR"/*.sh; do
    [[ -e "$f" ]] || continue
    basename "$f" .sh
  done | sort | paste -sd' ' -
}

usage() {
  echo "usage: $(basename "$0") <case> [ENDPOINT] [PROJECT]"
  echo "cases: $(list_cases)"
}

CASE="${1:-}"
if [[ -z "$CASE" ]]; then
  usage
  exit 2
fi
case "$CASE" in
  -h | --help)
    usage
    exit 0
    ;;
esac

CASE_FILE="$CASES_DIR/$CASE.sh"
if [[ ! -f "$CASE_FILE" ]]; then
  echo "ERROR: unknown case '$CASE' (expected one of: $(list_cases))"
  exit 2
fi

ENDPOINT="${2:-${TF_ENDPOINT:-http://localhost:8080}}"
PROJECT="${3:-${TF_PROJECT:-test-project}}"
TF="${TF_BIN:-terraform}"

MODULE_REPO=""
MODULE_COMMIT=""
MODULE_GIT=""
MODULE_SUBDIR=""
case_main() { :; }
# shellcheck source=/dev/null
source "$CASE_FILE"

if [[ -z "$MODULE_REPO" ]]; then
  echo "ERROR: case '$CASE' does not set MODULE_REPO"
  exit 2
fi
MODULE_GIT="${MODULE_GIT:-https://github.com/terraform-google-modules/$MODULE_REPO}"
MODULE_DIR="${TF_MODULE_DIR:-$HOME/.cache/terraform-modules/$MODULE_REPO}"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  exit 0
fi

if [[ ! -d "$MODULE_DIR" ]]; then
  echo "SKIP: consumer module not found at $MODULE_DIR"
  echo "      clone it once (network needed only for this), then re-run:"
  echo "        git clone $MODULE_GIT \"$MODULE_DIR\""
  if [[ -n "$MODULE_COMMIT" ]]; then
    echo "        git -C \"$MODULE_DIR\" checkout $MODULE_COMMIT"
  fi
  echo "      and/or point TF_MODULE_DIR at your checkout."
  exit 0
fi

MODULE_DIR="$(cd "$MODULE_DIR" && pwd)"
if [[ -n "$MODULE_SUBDIR" ]]; then
  MODULE_DIR="$MODULE_DIR/$MODULE_SUBDIR"
fi
if [[ ! -f "$MODULE_DIR/main.tf" ]]; then
  echo "ERROR: $MODULE_DIR does not look like $MODULE_REPO (no main.tf)"
  exit 1
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/jc-tf-module-$CASE.XXXXXX")"
DESTROYED=0

# tf runs Terraform against the generated root module.
tf() { "$TF" -chdir="$WORK" "$@"; }

# endpoint_for <service> prints the provider custom endpoint for one emulated
# service from terraform-endpoints.yaml (the single map the fixture and the
# corpus tool consume too). New cases should use
#
#   kms_custom_endpoint = "$(endpoint_for kms)"
#
# in their generated provider block; existing cases keep their literals until
# their own session migrates them.
TF_CORPUS_BIN="${TF_CORPUS_BIN:-}"
endpoint_for() {
  local svc="$1" root
  if [[ -z "$TF_CORPUS_BIN" ]]; then
    root="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
    TF_CORPUS_BIN="$root/bin/tfcorpus"
    if ! (cd "$root" && go build -o "$TF_CORPUS_BIN" ./tools/tfcorpus); then
      echo "ERROR: could not build tools/tfcorpus" >&2
      return 1
    fi
  fi
  "$TF_CORPUS_BIN" -print-endpoint "$svc" -endpoint "$ENDPOINT" -endpoints "$SCRIPT_DIR/terraform-endpoints.yaml"
}

destroy_work() {
  [[ "$DESTROYED" == "1" ]] && return 0
  DESTROYED=1
  tf destroy -input=false -auto-approve -no-color >/dev/null 2>&1 || true
}
cleanup() {
  destroy_work
  # JC_TF_KEEP=1 keeps the work dir (debug logs, captured request bodies) for
  # local investigation; the default removes it.
  if [[ "${JC_TF_KEEP:-0}" != "1" ]]; then
    rm -rf "$WORK"
  else
    echo "kept work dir: $WORK"
  fi
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

summary() {
  echo "== summary: $PASS passed, $FAIL failed"
  [[ "$FAIL" -eq 0 ]]
}

# init_phase runs `terraform init`; a failure is fatal (nothing can follow).
init_phase() {
  if tf init -input=false -no-color >"$WORK/init.log" 2>&1; then
    pass "init (provider resolved from the registry/local cache)"
    # FC6: the provider's own schema is the definition the capture is joined to.
    if ! tf providers schema -json >"$WORK/schema.json" 2>"$WORK/schema.err"; then
      rm -f "$WORK/schema.json"
    fi
    # Record every outgoing provider request for the capture gate. TF_LOG_PATH
    # keeps the plugin log out of the phase logs the gates grep.
    export TF_LOG=DEBUG TF_LOG_PATH="$WORK/tf-debug.log"
  else
    cat "$WORK/init.log"
    fail "init (see the log above)"
    summary
    exit 1
  fi
}

# plan_phase runs a plain plan and reports whether the module could be planned.
plan_phase() {
  if tf plan -input=false -no-color >"$WORK/plan.log" 2>&1; then
    pass "${1:-plan}"
  else
    cat "$WORK/plan.log"
    fail "${1:-plan} (see the log above)"
  fi
}

# apply_phase applies the generated module; a failure is fatal because every
# later phase builds on the applied state.
apply_phase() {
  if tf apply -input=false -auto-approve -no-color >"$WORK/apply.log" 2>&1; then
    pass "${1:-apply}"
  else
    cat "$WORK/apply.log"
    fail "${1:-apply} (see the log above)"
    summary
    exit 1
  fi
}

# assert_no_api_errors fails when the last apply reported an HTTP 400/501 or an
# unsupported update-mask path. The gate accepts "No changes", not just a clean
# exit code: a field the provider PATCHes and the emulator silently ignores is
# still a broken module.
assert_no_api_errors() {
  local log="${1:-$WORK/apply.log}"
  if grep -qE 'unsupported update_mask|Error 400:|Error 501:|"code": ?(400|501)' "$log"; then
    grep -nE 'unsupported update_mask|Error 400:|Error 501:|"code": ?(400|501)' "$log" | head -5
    fail "apply reported an API error (see above)"
  else
    pass "apply reported no 400/501"
  fi
}

# plan_no_changes is the acceptance: a second plan after apply must exit 0.
plan_no_changes() {
  local label="${1:-re-plan reports no changes}" rc
  set +e
  tf plan -input=false -no-color -detailed-exitcode >"$WORK/replan.log" 2>&1
  rc=$?
  set -e
  case "$rc" in
    0) pass "$label" ;;
    2)
      cat "$WORK/replan.log"
      fail "$label (a permadiff; see the log above)"
      ;;
    *)
      cat "$WORK/replan.log"
      fail "$label (plan failed: exit $rc)"
      ;;
  esac
}

# check asserts that a bucket/API body carries each expected substring.
check() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

destroy_phase() {
  echo "== destroy"
  if [[ "$DESTROYED" == "0" ]] && tf destroy -input=false -auto-approve -no-color >"$WORK/destroy.log" 2>&1; then
    DESTROYED=1
    pass "destroy"
  else
    cat "$WORK/destroy.log" 2>/dev/null || true
    fail "destroy (see the log above)"
  fi
}

# capture_phase joins the provider's outgoing request bodies to its schema and
# the field catalog (FC6). JC_TF_CAPTURE=0 disables it for local debugging.
capture_phase() {
  [[ "${JC_TF_CAPTURE:-1}" == "1" ]] || return 0
  if [[ ! -s "$WORK/schema.json" ]]; then
    fail "capture (no provider schema at $WORK/schema.json; terraform providers schema -json failed)"
    return 0
  fi
  if [[ ! -s "$WORK/tf-debug.log" ]]; then
    fail "capture (no TF_LOG output at $WORK/tf-debug.log)"
    return 0
  fi
  local root bin
  root="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
  bin="${TFCAPTURE_BIN:-$root/bin/tfcapture}"
  # Rebuild every run: a stale binary would silently miss new flags/rules.
  if [[ -z "${TFCAPTURE_BIN:-}" ]]; then
    if ! (cd "$root" && go build -o "$bin" ./tools/tfcapture); then
      fail "capture (could not build tools/tfcapture)"
      return 0
    fi
  fi
  # Cases whose provider resources are in the capture map must actually
  # exercise the join; the others record the capture without requirement.
  local require=""
  case "$CASE" in
    storage | pubsub | pubsub-topic | kms | firestore | clouddns | functions | memorystore | cloud-operations | cloudrun) require="-require-resources" ;;
  esac
  echo "== capture: provider request bodies vs the field catalog"
  if "$bin" -case "$CASE" $require -log "$WORK/tf-debug.log" -schema "$WORK/schema.json" \
      -mapping "$root/docs/fidelity/provider-attributes.csv" \
      -catalog "$root/docs/fidelity/field-coverage.csv"; then
    pass "capture (every sent provider attribute is served)"
  else
    fail "capture (see the report above)"
  fi
}

echo "== $TF: consumer module $MODULE_DIR (case=$CASE endpoint=$ENDPOINT project=$PROJECT)"

case_main
capture_phase

summary
