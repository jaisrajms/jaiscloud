#!/usr/bin/env bash
#
# lib.sh — shared driver for the Terraform composition patterns (TF4).
#
# Why a third driver. `run.sh` is a raw fixture (a bag of resources) and
# `run-module.sh` drives one pinned third-party module. Neither proves a *stack*
# a user would actually provision: create a project, link billing, enable APIs,
# grant IAM, and put storage/queues/secrets/keys in it. A pattern under
# `patterns/<NN-name>/` is a small root module written here (main.tf +
# outputs.tf), plus a `run.sh` that sources this file and follows the standard
# contract:
#
#   init -> apply -> (REST read-back asserts) -> plan -detailed-exitcode == 0
#        -> (optional in-place mutation -> apply -> plan) -> destroy
#        -> (post-destroy absence asserts)
#
# A pattern may add a targeted-destroy phase between the re-plan and the full
# destroy (see targeted_destroy below) when the stack's partial-teardown
# behaviour is part of what it proves. A multi-environment pattern may run the
# whole contract once per workspace (see workspace_select/destroy_workspace
# below) when isolation between states is part of what it proves.
#
# The acceptance is the clean re-plan and the post-destroy state, not the apply
# exit code: a stack whose cross-resource references do not round-trip is a
# stack no consumer can use.
#
# The generated root module is assembled in a throwaway work dir: the shared
# `patterns/provider.tf` (the provider block + the offline endpoint defaults)
# plus the pattern's own `*.tf`. The per-service custom endpoints are emitted
# from `terraform-endpoints.yaml` by tools/tfcorpus, so this driver cannot drift
# from the fixture, the module driver or the corpus tool.
#
# Usage (from a pattern's run.sh):
#
#   PATTERN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   PATTERN_NAME="00-bootstrap"
#   # shellcheck source=../lib.sh
#   source "$PATTERN_DIR/../lib.sh"
#
#   ENVIRONMENT=... # optional
#   TF_VARS+=(-var "bootstrap_project=$bootstrap_project")
#   init_phase
#   apply_phase
#   ...
#   summary
#
# Environment:
#   TF_BIN        terraform (default) or tofu
#   TF_ENDPOINT   default endpoint when the positional argument is omitted
#   TF_PROJECT    default project when the positional argument is omitted
#   STRICT=1      a SKIP is a failure (the nightly uses this)
#   JC_TF_KEEP=1  keep the work dir (logs, generated tfvars) after the run

set -euo pipefail

PATTERNS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERN_DIR="${PATTERN_DIR:?PATTERN_DIR must be set before sourcing patterns/lib.sh}"
PATTERN_NAME="${PATTERN_NAME:-$(basename "$PATTERN_DIR")}"
REPO_ROOT="$(cd "$PATTERNS_DIR/../../../../.." && pwd)"

ENDPOINT="${1:-${TF_ENDPOINT:-http://localhost:8080}}"
PROJECT="${2:-${TF_PROJECT:-test-project}}"
TF="${TF_BIN:-terraform}"
STRICT="${STRICT:-0}"
REGION="${TF_REGION:-us-central1}"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  [ "$STRICT" = "1" ] && exit 1
  exit 0
fi

if [[ ! -f "$PATTERN_DIR/main.tf" ]]; then
  echo "ERROR: $PATTERN_DIR does not look like a composition pattern (no main.tf)"
  exit 1
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/jc-tf-pattern-$PATTERN_NAME.XXXXXX")"
cp "$PATTERNS_DIR/provider.tf" "$WORK/provider.tf"
for f in "$PATTERN_DIR"/*.tf; do
  cp "$f" "$WORK/"
done

# tf runs Terraform against the assembled root module in $WORK.
tf() { "$TF" -chdir="$WORK" "$@"; }

# TF_VARS carries the per-run variables. The shared ones are always set; a
# pattern appends its own before its first phase.
TF_VARS=(-var "endpoint=$ENDPOINT" -var "project=$PROJECT" -var "region=$REGION")

# emit_endpoints generates the `endpoints` tfvars from the shared map. Without
# Go (or when tools/tfcorpus cannot be built) the pattern still runs: the
# provider.tf locals carry the same defaults derived from var.endpoint.
emit_endpoints() {
  local bin="${TF_CORPUS_BIN:-$REPO_ROOT/bin/tfcorpus}"
  if [[ -z "${TF_CORPUS_BIN:-}" ]]; then
    if ! command -v go >/dev/null 2>&1; then
      echo "  note: go not found; using patterns/provider.tf's endpoint defaults"
      return 0
    fi
    if ! (cd "$REPO_ROOT" && go build -o "$bin" ./tools/tfcorpus); then
      echo "ERROR: could not build tools/tfcorpus" >&2
      return 1
    fi
  fi
  "$bin" -emit-tfvars "$WORK/endpoints.auto.tfvars.json" \
    -endpoint "$ENDPOINT" -endpoints "$PATTERNS_DIR/../terraform-endpoints.yaml"
}

# ── Local echo sink (compositions that prove a delivery) ────────────────────
# A pattern that asserts a Pub/Sub push or an Eventarc httpEndpoint delivery
# needs a far end to observe. start_echo_sink builds and starts the shared
# helpers/echo-sink: every request is appended to ECHO_SINK_LOG as one JSON
# line (method, path, headers, body) and ECHO_SINK_URL is the base URL the
# pattern passes to Terraform. Requires Go (this driver already builds
# tools/tfcorpus with it); a missing Go is a setup failure, not a SKIP.
ECHO_SINK_PID=""
ECHO_SINK_URL=""
ECHO_SINK_LOG=""
ECHO_SINK_BODY=""

start_echo_sink() {
  local bin="$WORK/echo-sink" addr_file="$WORK/echo-sink.addr" i
  ECHO_SINK_LOG="${1:-$WORK/echo-sink.jsonl}"
  ECHO_SINK_BODY="$WORK/echo-sink.last-body"
  : >"$ECHO_SINK_LOG"
  : >"$ECHO_SINK_BODY"
  if ! command -v go >/dev/null 2>&1; then
    echo "ERROR: go is required to build the composition echo sink" >&2
    return 1
  fi
  if ! (cd "$REPO_ROOT" && go build -o "$bin" ./tests/integration/gcp/terraform/patterns/helpers/echo-sink); then
    echo "ERROR: could not build the composition echo sink" >&2
    return 1
  fi
  rm -f "$addr_file"
  "$bin" -addr 127.0.0.1:0 -out "$ECHO_SINK_LOG" -addr-file "$addr_file" \
    -raw-latest "$ECHO_SINK_BODY" >"$WORK/echo-sink.out" 2>&1 &
  ECHO_SINK_PID=$!
  for ((i = 0; i < 100; i++)); do
    if [[ -s "$addr_file" ]]; then
      ECHO_SINK_URL="$(tr -d '\n' <"$addr_file")"
      return 0
    fi
    if ! kill -0 "$ECHO_SINK_PID" 2>/dev/null; then
      echo "ERROR: the composition echo sink exited:" >&2
      cat "$WORK/echo-sink.out" >&2 || true
      return 1
    fi
    sleep 0.1
  done
  echo "ERROR: the composition echo sink did not report an address" >&2
  return 1
}

stop_echo_sink() {
  [[ -n "$ECHO_SINK_PID" ]] || return 0
  kill "$ECHO_SINK_PID" 2>/dev/null || true
  wait "$ECHO_SINK_PID" 2>/dev/null || true
  ECHO_SINK_PID=""
}

# sink_count FILE — number of deliveries recorded so far.
sink_count() {
  [[ -f "$1" ]] || {
    echo 0
    return 0
  }
  grep -c . "$1" 2>/dev/null || true
}

# wait_for_sink FILE WANT [TIMEOUT_SECONDS] — poll until FILE holds WANT lines.
wait_for_sink() {
  local file="$1" want="$2" timeout="${3:-20}" i limit
  limit=$((timeout * 10))
  for ((i = 0; i < limit; i++)); do
    [[ "$(sink_count "$file")" -ge "$want" ]] && return 0
    sleep 0.1
  done
  return 1
}

# sink_reset FILE — forget the deliveries recorded so far (the sink keeps its
# append handle, so the next delivery lands at offset 0).
sink_reset() {
  : >"$1"
  [[ -n "$ECHO_SINK_BODY" ]] && : >"$ECHO_SINK_BODY"
  return 0
}

# ── Workspaces (patterns that prove multi-environment isolation) ────────────
# A workspace is a separate state over one configuration. WORKSPACES records the
# ones a pattern selected so cleanup destroys each of them: an environment left
# behind is a state full of resources left behind.
WORKSPACES=()
DESTROYED_WORKSPACES=()

# workspace_select NAME selects NAME, creating it when absent.
workspace_select() {
  local name="$1" w seen=0 list
  # `workspace list` marks the selected one with "* " and indents the others.
  # Capture it first: a failing list must not look like "workspace absent" and
  # silently turn into a `workspace new` that then aborts on an existing name.
  if ! list="$(tf workspace list -no-color 2>&1)"; then
    echo "ERROR: terraform workspace list failed:" >&2
    printf '%s\n' "$list" >&2
    return 1
  fi
  if printf '%s\n' "$list" | sed -E 's/^(\* )? *//' | grep -qxF -- "$name"; then
    tf workspace select -no-color "$name" >/dev/null
  else
    tf workspace new -no-color "$name" >/dev/null
  fi
  for w in "${WORKSPACES[@]:-}"; do
    [[ -n "$w" && "$w" == "$name" ]] && seen=1
  done
  [[ "$seen" == "1" ]] || WORKSPACES+=("$name")
}

# destroy_workspace NAME destroys the stack in NAME, leaving it selected. Each
# workspace has its own state, so this only tears down that environment.
destroy_workspace() {
  local name="$1" log
  if ! workspace_select "$name"; then
    fail "destroy ($name): could not select the workspace"
    return 0
  fi
  log="$WORK/destroy-$name.log"
  if tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >"$log" 2>&1; then
    DESTROYED_WORKSPACES+=("$name")
    pass "destroy ($name)"
  else
    cat "$log" 2>/dev/null || true
    fail "destroy ($name) (see the log above)"
  fi
}

DESTROYED=0
destroy_work() {
  local w d skip
  if [[ "${#WORKSPACES[@]}" -gt 0 ]]; then
    for w in "${WORKSPACES[@]}"; do
      skip=0
      for d in "${DESTROYED_WORKSPACES[@]:-}"; do
        [[ -n "$d" && "$d" == "$w" ]] && skip=1
      done
      [[ "$skip" == "1" ]] && continue
      workspace_select "$w" >/dev/null 2>&1 || true
      if tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null 2>&1; then
        DESTROYED_WORKSPACES+=("$w")
      fi
    done
    tf workspace select default >/dev/null 2>&1 || true
    DESTROYED=1
    return 0
  fi
  [[ "$DESTROYED" == "1" ]] && return 0
  DESTROYED=1
  tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null 2>&1 || true
}
cleanup() {
  stop_echo_sink
  destroy_work
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

# init_phase runs `terraform init`; nothing can follow a failed init.
init_phase() {
  emit_endpoints
  if tf init -input=false -no-color >"$WORK/init.log" 2>&1; then
    pass "init (provider resolved from the registry/local cache)"
  else
    cat "$WORK/init.log"
    fail "init (see the log above)"
    summary
    exit 1
  fi
}

# apply_phase applies the stack; every later phase builds on it.
apply_phase() {
  if tf apply -input=false -auto-approve -no-color "${TF_VARS[@]}" >"$WORK/apply.log" 2>&1; then
    pass "${1:-apply}"
  else
    cat "$WORK/apply.log"
    fail "${1:-apply} (see the log above)"
    summary
    exit 1
  fi
}

# assert_no_api_errors fails when the apply reported an HTTP 400/501 or an
# unsupported update-mask path: a field the provider sends and the emulator
# ignores is a broken stack even when Terraform exits 0.
assert_no_api_errors() {
  local log="${1:-$WORK/apply.log}"
  if grep -qE 'unsupported update_mask|Error 400:|Error 501:|"code": ?(400|501)' "$log"; then
    grep -nE 'unsupported update_mask|Error 400:|Error 501:|"code": ?(400|501)' "$log" | head -5
    fail "apply reported an API error (see above)"
  else
    pass "apply reported no 400/501"
  fi
}

# plan_no_changes is the acceptance: a second plan exits 0 (no permadiff).
plan_no_changes() {
  local label="${1:-re-plan reports no changes}" rc
  set +e
  tf plan -input=false -no-color -detailed-exitcode "${TF_VARS[@]}" >"$WORK/replan.log" 2>&1
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

destroy_phase() {
  echo "== destroy"
  if [[ "$DESTROYED" == "0" ]] &&
    tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >"$WORK/destroy.log" 2>&1; then
    DESTROYED=1
    pass "destroy"
  else
    cat "$WORK/destroy.log" 2>/dev/null || true
    fail "destroy (see the log above)"
  fi
}

# targeted_destroy LABEL ADDRESS... — destroy only the named addresses, for a
# phase that proves what a partial teardown leaves behind (a pattern can then
# still run its full destroy). Terraform's destroy graph keeps each target's
# dependencies, so targeting a virtual cluster does not take its GKE cluster
# with it. ADDRESS is the `-target=...` flag, passed through verbatim.
targeted_destroy() {
  local label="$1" slug log
  shift
  slug="$(printf '%s' "$label" | tr ' ' '-')"
  log="$WORK/targeted-destroy-$slug.log"
  if tf destroy -input=false -auto-approve -no-color "$@" "${TF_VARS[@]}" >"$log" 2>&1; then
    pass "targeted destroy: $label"
  else
    cat "$log"
    fail "targeted destroy: $label (see the log above)"
  fi
}

# tf_output reads one root-module output (empty when it is absent).
tf_output() {
  tf output -raw "$1" 2>/dev/null || true
}

# check NAME URL SUBSTRING [SUBSTRING...] — GET the URL and require every
# substring in the body.
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

# check_post is check with a POST body, for the :getIamPolicy custom methods.
check_post() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' "$url" 2>/dev/null || true)"
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

# check_status NAME URL CODE — require exactly that HTTP status (404 after
# destroy, 200 for a documented provider artifact that survives it).
check_status() {
  local name="$1" url="$2" want="$3" got
  got="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  if [[ "$got" == "$want" ]]; then
    pass "$name"
  else
    fail "$name (HTTP $got, want $want)"
  fi
}

# check_post_absent NAME URL NEEDLE — POST (the :getIamPolicy verbs) and require
# NEEDLE is NOT in the body. For a metadata-only resource whose policy outlives
# its parent: the grant must be gone even when the endpoint still answers.
check_post_absent() {
  local name="$1" url="$2" needle="$3" body
  body="$(curl -sf -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' "$url" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  if [[ "$body" == *"$needle"* ]]; then
    fail "$name (found: $needle)"
  else
    pass "$name"
  fi
}

# check_absent NAME URL NEEDLE — GET the URL and require NEEDLE is NOT in the
# body (orphan assertions over a list endpoint).
check_absent() {
  local name="$1" url="$2" needle="$3" body
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  if [[ "$body" == *"$needle"* ]]; then
    fail "$name (found orphan: $needle)"
  else
    pass "$name"
  fi
}

echo "== $TF: composition pattern $PATTERN_NAME (endpoint=$ENDPOINT project=$PROJECT)"
