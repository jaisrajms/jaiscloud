#!/usr/bin/env bash
#
# run-lifecycle.sh — lifecycle-depth gate for the Terraform/OpenTofu suite
# (wave plan TF5).
#
# Why: the fixture and the module gates prove apply -> re-plan -> update ->
# destroy. They do not exercise the operations a real Terraform user performs on
# existing state: importing resources, planning with `-refresh-only`, forcing a
# `-replace`, noticing an out-of-band change, or recovering from an API error.
# Import is the strongest read-shape test in the set: it feeds the emulator's
# GET straight back into Terraform state and then plans against it.
#
# This driver runs one case file per representative service under
# lifecycle-cases/. A case is sourced, defines `case_main`, and calls the phase
# helpers below. The cases are deliberately not the pinned third-party modules:
# import, replace and negative paths need control over a resource's address and
# its ForceNew attributes, which a module hides.
#
# Coverage (the representative set, expanded as a service gains a case):
#
#   storage   import, refresh, replace (-replace), ForceNew rename (old id 404),
#             out-of-band label drift, negative paths, label-import artifact
#   pubsub    import (topic + subscription), refresh, replace, ForceNew rename,
#             out-of-band label drift, negative paths
#   kms       import (key ring + crypto key), refresh, out-of-band label drift,
#             non-deletable delete semantics, negative paths, the upstream
#             "replace is unsupported" classification
#   iam-parallel  10 service accounts + 10 bindings at -parallelism=10, policy
#             read-back, no lost updates
#   tftest    committed .tftest.hcl provider-visible assertions (`terraform test`)
#
# Usage:
#   tests/integration/gcp/terraform/run-lifecycle.sh [ENDPOINT] [PROJECT]
#   LIFECYCLE_CASE=storage tests/integration/gcp/terraform/run-lifecycle.sh ...
#
#     ENDPOINT     base URL of jaiscloud-gcp REST (default: $TF_ENDPOINT, else
#                  http://localhost:8080)
#     PROJECT      project id (default: $TF_PROJECT, else test-project)
#
# Env:
#   TF_BIN       terraform (default) or tofu
#   CASES        whitespace-separated subset of cases (default: every file)
#   STRICT=1     a SKIP is a failure (the nightly uses this)
#   LOG_DIR      per-case logs + summary.md/summary.tsv/summary.json
#                (default: /tmp/jaiscloud-gcp-terraform-lifecycle)
#   JC_TF_KEEP=1 keep each case's temp work dir for investigation
#
# Prefer the Makefile target, which starts/stops the emulator:
#   make test-gcp-terraform-lifecycle
#
# Exit: 0 all cases passed (or SKIP without STRICT); 1 a failure or no case ran.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASES_DIR="$SCRIPT_DIR/lifecycle-cases"
ENDPOINT="${1:-${TF_ENDPOINT:-http://localhost:8080}}"
PROJECT="${2:-${TF_PROJECT:-test-project}}"
TF="${TF_BIN:-terraform}"
STRICT="${STRICT:-0}"
LOG_DIR="${LOG_DIR:-/tmp/jaiscloud-gcp-terraform-lifecycle}"
# The aggregate spawns one child per case; export the log dir so the per-case
# import tables land beside the aggregate summary even when LOG_DIR was set as a
# plain shell variable.
export LOG_DIR

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  [ "$STRICT" = "1" ] && exit 1
  exit 0
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

all_cases() {
  local f
  for f in "$CASES_DIR"/*.sh; do
    [[ -e "$f" ]] || continue
    basename "$f" .sh
  done | sort
}

# ─────────────────────────────────────────────────────────────────────────────
# One case
# ─────────────────────────────────────────────────────────────────────────────
run_case() {
  # CASE is global on purpose: the EXIT trap (cleanup -> write_import_table)
  # runs after this function returns.
  CASE="$1"
  local CASE_FILE="$CASES_DIR/$CASE.sh"
  if [[ ! -f "$CASE_FILE" ]]; then
    echo "ERROR: unknown lifecycle case '$CASE' (expected: $(all_cases | paste -sd' ' -))"
    return 2
  fi

  WORK="$(mktemp -d "${TMPDIR:-/tmp}/jc-tf-lifecycle-$CASE.XXXXXX")"
  DESTROYED=0
  PASS=0
  FAIL=0
  IMPORT_ROWS=()

  # tf runs Terraform against the case's generated root module.
  tf() { "$TF" -chdir="$WORK" "$@"; }

  pass() {
    printf '  [PASS] %s\n' "$1"
    PASS=$((PASS + 1))
  }
  fail() {
    printf '  [FAIL] %s\n' "$1"
    FAIL=$((FAIL + 1))
  }
  # note records a classification that is not a pass/fail assertion (an
  # upstream limitation, a recorded gap). It is printed and copied into the
  # scorecard by hand, never counted as a green.
  note() { printf '  [NOTE] %s\n' "$1"; }

  summary() {
    echo "== summary: $PASS passed, $FAIL failed"
    [[ "$FAIL" -eq 0 ]]
  }

  # record_import feeds the import-support table (one row per imported address).
  record_import() {
    IMPORT_ROWS+=("$1|$2|$3|$4")
  }
  write_import_table() {
    [[ "${#IMPORT_ROWS[@]}" -eq 0 ]] && return 0
    {
      printf 'resource\trest_id\tplan_after_import\tclassification\n'
      local row
      for row in "${IMPORT_ROWS[@]}"; do
        printf '%s\n' "${row//|/$'\t'}"
      done
    } >"$LOG_DIR/$CASE-import.tsv"
  }

  destroy_work() {
    [[ "$DESTROYED" == "1" ]] && return 0
    DESTROYED=1
    tf destroy -input=false -auto-approve -no-color >/dev/null 2>&1 || true
  }
  cleanup() {
    destroy_work
    write_import_table
    if [[ "${JC_TF_KEEP:-0}" != "1" ]]; then
      rm -rf "$WORK"
    else
      echo "kept work dir: $WORK"
    fi
  }
  trap 'cleanup' EXIT INT TERM

  # ── phase helpers ──────────────────────────────────────────────────────────

  # init_phase runs `terraform init`; a failure is fatal (nothing can follow).
  init_phase() {
    if tf init -input=false -no-color >"$WORK/init.log" 2>&1; then
      pass "init (provider resolved from the registry/local cache)"
      export TF_LOG=DEBUG TF_LOG_PATH="$WORK/tf-debug.log"
    else
      cat "$WORK/init.log"
      fail "init (see the log above)"
      summary
      exit 1
    fi
  }

  plan_phase() {
    if tf plan -input=false -no-color >"$WORK/plan.log" 2>&1; then
      pass "${1:-plan}"
    else
      cat "$WORK/plan.log"
      fail "${1:-plan} (see the log above)"
    fi
  }

  apply_phase() {
    local label="${1:-apply}" extra=("${@:2}")
    LAST_APPLY_LOG="$WORK/apply-${label//[^a-zA-Z0-9]/-}.log"
    if tf apply -input=false -auto-approve -no-color "${extra[@]}" >"$LAST_APPLY_LOG" 2>&1; then
      pass "$label"
    else
      cat "$LAST_APPLY_LOG"
      fail "$label (see the log above)"
      summary
      exit 1
    fi
  }

  assert_no_api_errors() {
    local log="${1:-$LAST_APPLY_LOG}" label="${2:-apply}"
    if [[ ! -s "$log" ]]; then
      fail "$label (no apply log at $log)"
      return 0
    fi
    if grep -qE 'unsupported update_mask|Error 400:|Error 501:|"code": ?(400|501)' "$log" 2>/dev/null; then
      grep -nE 'unsupported update_mask|Error 400:|Error 501:|"code": ?(400|501)' "$log" | head -5
      fail "$label reported an API error (see above)"
    else
      pass "$label reported no 400/501"
    fi
  }

  # plan_expect_diff requires a non-empty plan (exit 2) that contains every
  # given substring. It is the drift assertion: the plan must notice exactly the
  # out-of-band change, not ignore it and not turn it into a replacement.
  plan_expect_diff() {
    local label="$1"
    shift
    local rc want
    set +e
    tf plan -input=false -no-color -detailed-exitcode >"$WORK/diff.log" 2>&1
    rc=$?
    set -e
    if [[ "$rc" -ne 2 ]]; then
      cat "$WORK/diff.log"
      fail "$label (plan exit $rc, want 2: the out-of-band change was not noticed)"
      return 0
    fi
    if grep -q 'must be replaced' "$WORK/diff.log"; then
      fail "$label (drift proposed a replacement, not an in-place update)"
      return 0
    fi
    for want in "$@"; do
      if ! grep -qF "$want" "$WORK/diff.log"; then
        cat "$WORK/diff.log"
        fail "$label (the diff does not mention '$want')"
        return 0
      fi
    done
    pass "$label"
  }

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

  # refresh_phase is `plan -refresh-only -detailed-exitcode`: Terraform re-reads
  # every object and the plan must be empty. Any drift here means the emulator's
  # read does not match what Terraform last wrote.
  #
  # `labels-artifact` accepts a refresh diff whose only changed attribute is the
  # provider's normalization of an absent labels map to {} (google_storage_bucket
  # does this for a bucket with no managed labels; real GCS follows the same wire
  # shape). Any other changed attribute fails.
  refresh_phase() {
    local mode="${1:-strict}" label="${2:-refresh-only plan reports no drift}" rc
    set +e
    tf plan -input=false -no-color -refresh-only -detailed-exitcode >"$WORK/refresh.log" 2>&1
    rc=$?
    set -e
    case "$rc" in
      0) pass "$label" ;;
      2)
        if [[ "$mode" == "labels-artifact" ]] && plan_diff_is_labels_only "$WORK/refresh.log"; then
          pass "$label (only the provider's empty-labels normalization)"
        else
          sed -n '/Objects have changed/,/──/p' "$WORK/refresh.log" | head -40
          fail "$label (out-of-band state change detected)"
        fi
        ;;
      *)
        cat "$WORK/refresh.log"
        fail "$label (plan failed: exit $rc)"
        ;;
    esac
  }

  # plan_diff_is_labels_only accepts the provider's import-time label artifact:
  # tpgresource.SetLabels filters the API labels against the labels already in
  # state, and an import starts with an empty map, so `labels`/`terraform_labels`
  # are reconstructed on the next plan. Any other changed attribute is an
  # emulator read-shape gap and fails.
  plan_diff_is_labels_only() {
    local log="$1" attrs attr
    attrs="$(grep -E '^[[:space:]]+[~+-][[:space:]]+[a-z0-9_]+[[:space:]]+=' "$log" |
      sed -E 's/^[[:space:]]+[~+-][[:space:]]+([a-z0-9_]+).*/\1/' || true)"
    [[ -n "$attrs" ]] || return 1
    for attr in $attrs; do
      case "$attr" in
        labels | terraform_labels | effective_labels) ;;
        *) return 1 ;;
      esac
    done
    return 0
  }

  # import_phase removes an address from state, imports it by its REST id, and
  # requires a clean plan. `label-artifact` is the classified provider case
  # above; `strict` (the default) allows no diff at all.
  import_phase() {
    local addr="$1" rest_id="$2" mode="${3:-strict}"
    local label="${4:-import $addr}"
    local rc
    if ! tf state rm "$addr" >"$WORK/import-rm.log" 2>&1; then
      cat "$WORK/import-rm.log"
      fail "$label (state rm failed)"
      record_import "$addr" "$rest_id" "state-rm-failed" "FAILED"
      return 0
    fi
    if ! tf import "$addr" "$rest_id" >"$WORK/import.log" 2>&1; then
      cat "$WORK/import.log"
      fail "$label (import failed)"
      record_import "$addr" "$rest_id" "import-failed" "FAILED"
      return 0
    fi
    set +e
    tf plan -input=false -no-color -detailed-exitcode >"$WORK/import-plan.log" 2>&1
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
      pass "$label (plan is clean)"
      record_import "$addr" "$rest_id" "clean" "supported"
      return 0
    fi
    if [[ "$mode" == "label-artifact" && "$rc" -eq 2 ]] && plan_diff_is_labels_only "$WORK/import-plan.log"; then
      pass "$label (only the provider's label reconstruction)"
      record_import "$addr" "$rest_id" "labels-only" "provider-label-artifact"
      return 0
    fi
    cat "$WORK/import-plan.log"
    fail "$label (plan after import: exit $rc; see the log above)"
    record_import "$addr" "$rest_id" "diff-exit-$rc" "FAILED"
  }

  # reimport_phase handles the recovery path after an expected replace
  # failure: the failed create removed the address from state but the object
  # still exists upstream, so import it back (no state rm) and let the case
  # converge the plan.
  reimport_phase() {
    local addr="$1" rest_id="$2"
    if tf import "$addr" "$rest_id" >"$WORK/reimport.log" 2>&1; then
      pass "re-import $addr after the expected failure"
    else
      cat "$WORK/reimport.log"
      fail "re-import $addr after the expected failure"
    fi
  }

  # replace_phase forces destroy+create of an address and then requires a clean
  # plan. The case owns the identity assertion (a replaced object must be
  # re-created, not reused): it captures a server-side marker before and after.
  replace_phase() {
    local addr="$1" label="${2:-replace $addr}"
    local rc
    set +e
    tf apply -replace="$addr" -input=false -auto-approve -no-color >"$WORK/replace.log" 2>&1
    rc=$?
    set -e
    if [[ "$rc" -ne 0 ]]; then
      cat "$WORK/replace.log"
      fail "$label (apply -replace failed: exit $rc)"
      return 0
    fi
    if grep -q 'Destruction complete' "$WORK/replace.log" && grep -q 'Creation complete' "$WORK/replace.log"; then
      pass "$label (destroyed and re-created)"
    else
      cat "$WORK/replace.log"
      fail "$label (apply did not report a destroy+create)"
    fi
    plan_no_changes "re-plan after $label reports no changes"
  }

  # apply_fails_phase asserts that an apply fails for the expected reason and
  # that the failure is a surfaced API error (never a provider panic or a 5xx).
  apply_fails_phase() {
    local name="$1" want="$2" log="$3"
    shift 3
    local rc
    set +e
    tf apply -input=false -auto-approve -no-color "$@" >"$log" 2>&1
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
      cat "$log"
      fail "$name (apply unexpectedly succeeded)"
      return 0
    fi
    if grep -qE 'panic:|SIGSEGV|Plugin did not respond|"code": ?5[0-9][0-9]|Error 5[0-9][0-9]:' "$log"; then
      cat "$log"
      fail "$name (provider panic or server error)"
      return 0
    fi
    if grep -qF "$want" "$log"; then
      pass "$name (surfaced: $want)"
    else
      cat "$log"
      fail "$name (failure did not mention '$want')"
    fi
  }

  # provider_error_create runs a throwaway root module whose single resource the
  # case pre-created out-of-band. It asserts the apply fails for the expected API
  # reason (the error crosses the wire into the provider) and that it is a
  # surfaced error, never a panic or a 5xx.
  provider_error_create() {
    local name="$1" provider_hcl="$2" resource_hcl="$3" want="$4"
    local neg="$WORK/negative" rc
    rm -rf "$neg"
    mkdir -p "$neg"
    cat >"$neg/main.tf" <<EOF
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

$provider_hcl
$resource_hcl
EOF
    if ! "$TF" -chdir="$neg" init -input=false -no-color >"$neg/init.log" 2>&1; then
      cat "$neg/init.log"
      fail "$name (negative module init failed)"
      return 0
    fi
    set +e
    "$TF" -chdir="$neg" apply -input=false -auto-approve -no-color >"$neg/apply.log" 2>&1
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
      cat "$neg/apply.log"
      fail "$name (apply unexpectedly succeeded)"
      return 0
    fi
    if grep -qE 'panic:|SIGSEGV|Plugin did not respond|Error 5[0-9][0-9]:|"code": ?5[0-9][0-9]' "$neg/apply.log"; then
      cat "$neg/apply.log"
      fail "$name (provider panic or server error)"
      return 0
    fi
    if grep -qF "$want" "$neg/apply.log"; then
      pass "$name (surfaced: $want)"
    else
      cat "$neg/apply.log"
      fail "$name (failure did not mention '$want')"
    fi
  }

  # expect_http_error asserts the emulator returns the documented status for a
  # direct REST request and that the body is a structured error (not a panic or
  # a 5xx). It is the negative-path counterpart of `check`.
  expect_http_error() {
    local name="$1" method="$2" url="$3" body="$4" want="$5"
    local out code
    out="$(mktemp "${TMPDIR:-/tmp}/jc-tf-http.XXXXXX")"
    if [[ -n "$body" ]]; then
      code="$(curl -s -o "$out" -w '%{http_code}' -X "$method" \
        -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
        -H 'Content-Type: application/json' -d "$body" "$url" 2>/dev/null || true)"
    else
      code="$(curl -s -o "$out" -w '%{http_code}' -X "$method" \
        -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
    fi
    if [[ "$code" != "$want" ]]; then
      fail "$name (HTTP $code, want $want: $(head -c 240 "$out"))"
    elif grep -qE 'panic:|SIGSEGV|"code": ?5[0-9][0-9]' "$out"; then
      fail "$name (server error body: $(head -c 240 "$out"))"
    elif ! grep -q '"error"' "$out"; then
      fail "$name (HTTP $want without a structured error body: $(head -c 240 "$out"))"
    else
      pass "$name (HTTP $want)"
    fi
    rm -f "$out"
  }

  # check asserts that a REST body carries each expected substring.
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

  check_status() {
    local name="$1" url="$2" want="$3" code
    code="$(curl -s -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
    if [[ "$code" == "$want" ]]; then
      pass "$name (HTTP $want)"
    else
      fail "$name (HTTP $code, want $want)"
    fi
  }

  # check_post is check for a POST custom method (the project IAM read).
  check_post() {
    local name="$1" url="$2"
    shift 2
    local body want
    body="$(curl -sf -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
      -H 'Content-Type: application/json' -d '{}' "$url" 2>/dev/null || true)"
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

  # check_absent fails when a REST body carries any of the substrings.
  check_absent() {
    local name="$1" url="$2"
    shift 2
    local body want
    body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
    if [[ -z "$body" ]]; then
      fail "$name (no response from $url)"
      return 0
    fi
    for want in "$@"; do
      if [[ "$body" == *"$want"* ]]; then
        fail "$name (unexpected: $want)"
        return 0
      fi
    done
    pass "$name"
  }

  # check_post_absent is check_absent for a POST custom method.
  check_post_absent() {
    local name="$1" url="$2"
    shift 2
    local body want
    body="$(curl -sf -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
      -H 'Content-Type: application/json' -d '{}' "$url" 2>/dev/null || true)"
    if [[ -z "$body" ]]; then
      fail "$name (no response from $url)"
      return 0
    fi
    for want in "$@"; do
      if [[ "$body" == *"$want"* ]]; then
        fail "$name (unexpected: $want)"
        return 0
      fi
    done
    pass "$name"
  }

  # out_of_band applies a direct REST change to a resource Terraform manages.
  # The following plan must show the change and the apply must converge it.
  out_of_band() {
    local name="$1" method="$2" url="$3" body="$4" code
    code="$(curl -s -o /dev/null -w '%{http_code}' -X "$method" \
      -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
      -H 'Content-Type: application/json' -d "$body" "$url" 2>/dev/null || true)"
    if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
      pass "$name (HTTP $code)"
    else
      fail "$name (HTTP $code)"
    fi
  }

  destroy_phase() {
    local extra=("$@")
    echo "== destroy"
    if [[ "$DESTROYED" == "0" ]] && tf destroy -input=false -auto-approve -no-color "${extra[@]}" >"$WORK/destroy.log" 2>&1; then
      DESTROYED=1
      pass "destroy"
    else
      cat "$WORK/destroy.log" 2>/dev/null || true
      fail "destroy (see the log above)"
    fi
  }

  echo "== $TF: lifecycle case $CASE (endpoint=$ENDPOINT project=$PROJECT)"

  # shellcheck source=/dev/null
  source "$CASE_FILE"
  if [[ "$(type -t case_main)" != "function" ]]; then
    echo "ERROR: $CASE_FILE does not define case_main"
    return 2
  fi
  case_main
  write_import_table

  # An aggregate is green only when every case ran its phases; a case with zero
  # assertions would otherwise pass vacuously.
  if [[ "$PASS" -eq 0 ]]; then
    fail "$CASE ran no assertions"
  fi

  summary
}

# ─────────────────────────────────────────────────────────────────────────────
# One case (internal mode) or every case (aggregate)
# ─────────────────────────────────────────────────────────────────────────────
if [[ -n "${LIFECYCLE_CASE:-}" ]]; then
  # The case dir is written per case; make sure LOG_DIR exists for the import
  # table even when the driver is called directly for one case.
  mkdir -p "$LOG_DIR"
  run_case "$LIFECYCLE_CASE"
  exit $?
fi

mkdir -p "$LOG_DIR"

if [[ -n "${CASES:-}" ]]; then
  # shellcheck disable=SC2206
  CASE_LIST=(${CASES})
else
  # shellcheck disable=SC2207
  CASE_LIST=($(all_cases))
fi

if [[ "${#CASE_LIST[@]}" -eq 0 ]]; then
  echo "ERROR: no lifecycle cases found under $CASES_DIR" >&2
  exit 1
fi

PASS=0
FAIL=0
SKIP=0
ROWS=()

run_one() {
  local name="$1"
  local log="$LOG_DIR/$name.log" start end rc verdict secs
  start="$(date +%s)"
  LIFECYCLE_CASE="$name" "$0" "$ENDPOINT" "$PROJECT" >"$log" 2>&1
  rc=$?
  end="$(date +%s)"
  secs=$((end - start))
  if [[ "$rc" -eq 0 ]] && grep -q '^SKIP' "$log"; then
    verdict="SKIP"
    SKIP=$((SKIP + 1))
  elif [[ "$rc" -eq 0 ]]; then
    verdict="PASS"
    PASS=$((PASS + 1))
  else
    verdict="FAIL"
    FAIL=$((FAIL + 1))
  fi
  printf '  [%-4s] %-14s %4ss  %s\n' "$verdict" "$name" "$secs" "$log"
  ROWS+=("$name|$verdict|$secs|$log")
}

echo "== terraform lifecycle (endpoint=$ENDPOINT project=$PROJECT tf=$TF strict=$STRICT)"
echo "   logs: $LOG_DIR"

for case in "${CASE_LIST[@]}"; do
  run_one "$case"
done

{
  printf '# Terraform lifecycle summary\n\n'
  printf -- '- date: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf -- '- endpoint: %s\n' "$ENDPOINT"
  printf -- '- project: %s\n' "$PROJECT"
  printf -- '- binary: %s\n' "$("$TF" version 2>/dev/null | head -1)"
  printf -- '- strict: %s\n\n' "$STRICT"
  printf '| case | result | seconds | log |\n'
  printf '|---|---|---|---|\n'
  local_row=""
  for local_row in "${ROWS[@]}"; do
    IFS='|' read -r n v s l <<<"$local_row"
    printf '| %s | %s | %s | `%s` |\n' "$n" "$v" "$s" "$l"
  done
  printf '\n**%d passed, %d failed, %d skipped**\n' "$PASS" "$FAIL" "$SKIP"
} >"$LOG_DIR/summary.md"

{
  printf 'case\tresult\tseconds\tlog\n'
  local_row=""
  for local_row in "${ROWS[@]}"; do
    printf '%s\n' "${local_row//|/$'\t'}"
  done
} >"$LOG_DIR/summary.tsv"

echo
echo "== summary ($PASS passed, $FAIL failed, $SKIP skipped)"
cat "$LOG_DIR/summary.md"

if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
if [[ "$STRICT" = "1" && "$SKIP" -gt 0 ]]; then
  echo "STRICT=1: $SKIP case(s) skipped (a missing tool/module is a failure in CI)" >&2
  exit 1
fi
exit 0
