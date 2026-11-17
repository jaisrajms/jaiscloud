#!/usr/bin/env bash
#
# dual-substrate-gate.sh — prove the regression this topology exists to prevent:
# resetting one substrate must not touch the other's state.
#
# What it does:
#   1. writes real state into the GCP substrate (a GCS bucket + object) and
#      records that the `jaiscloud_gcp` database and the GCP PVCs exist;
#   2. runs `deploy/deploy.sh --reset --yes` (the AWS-only, database-scoped
#      reset);
#   3. asserts the GCP object still reads back byte-for-byte, the GCP pod is
#      Ready, `jaiscloud_gcp` still exists, and the GCP blobs PVC survived;
#   4. asserts the AWS substrate came back (database + health).
#
# Nothing here needs Istio; it talks to the ClusterIP Services with
# `kubectl port-forward`, so it works on docker-desktop, k3d and kind.
#
# Usage:
#   GATE_ALLOW_RESET=1 scripts/dual-substrate-gate.sh
#   GATE_ALLOW_RESET=1 scripts/dual-substrate-gate.sh --keep-state
#
# The gate is destructive by design (it proves an AWS reset does not touch GCP),
# so it refuses to run unless GATE_ALLOW_RESET=1: it drops the AWS database and
# deletes the `jaiscloud` namespace (its blobs and state PVCs). GCP state is the
# thing under test and is expected to survive.
#
# Env:
#   KUBE_CONTEXT        kubectl context (default: the current one)
#   NS_AWS / NS_GCP / NS_DATA   namespaces (default jaiscloud / jaiscloud-gcp /
#                       jaiscloud-data)
#   PROJECT             GCP project id for the probe (default dpl-gate)
#   BUCKET              probe bucket (default dpl-gate-<pid>)
#   GATE_TIMEOUT        seconds to wait for a rollout/port-forward (default 240)
#
# Exit: 0 only when every assertion holds.
set -euo pipefail

PROG="$(basename "$0")"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NS_AWS="${NS_AWS:-jaiscloud}"
NS_GCP="${NS_GCP:-jaiscloud-gcp}"
NS_DATA="${NS_DATA:-jaiscloud-data}"
PROJECT="${PROJECT:-dpl-gate}"
BUCKET="${BUCKET:-dpl-gate-$$}"
GATE_TIMEOUT="${GATE_TIMEOUT:-240}"
KEEP_STATE=0
GATE_ALLOW_RESET="${GATE_ALLOW_RESET:-0}"

for arg in "$@"; do
  case "$arg" in
    --keep-state) KEEP_STATE=1 ;;
    --help|-h)
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0 ;;
    *) echo "$PROG: unknown argument: $arg (try --help)" >&2; exit 2 ;;
  esac
done

TMP="$(mktemp -d "${TMPDIR:-/tmp}/jaiscloud-dual-gate.XXXXXX")"
BODY="$TMP/body"
PIDS=()
cleanup() {
  local i
  for i in "${!PIDS[@]}"; do
    kill "${PIDS[$i]}" >/dev/null 2>&1 || true
    wait "${PIDS[$i]}" >/dev/null 2>&1 || true
  done
  rm -rf "$TMP"
}
trap cleanup EXIT

say()  { printf '%s\n' "$*"; }
head_() { printf '\n== %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); printf '  PASS  %s\n' "$*"; }
fail() { FAILED=$((FAILED + 1)); printf '  FAIL  %s\n' "$*" >&2; }
die()  { printf '%s: error: %s\n' "$PROG" "$*" >&2; exit 2; }
PASSED=0
FAILED=0

command -v kubectl >/dev/null 2>&1 || die "kubectl is required"
command -v curl    >/dev/null 2>&1 || die "curl is required"

# KUBECTL is an argv prefix, not a function: `kube ... &` would background a
# subshell running the function, so `$!` would be the subshell's pid and killing
# it would orphan the real kubectl (which keeps the local port bound).
KUBECTL=(kubectl)
if [[ -n "$KUBE_CONTEXT" ]]; then
  KUBECTL=(kubectl --context "$KUBE_CONTEXT")
fi
kube() { "${KUBECTL[@]}" "$@"; }

# wait_port <host> <port> — poll until the port accepts a TCP connection.
wait_port() {
  local host=$1 port=$2 i
  # sleep 0.5 per iteration, so 2*GATE_TIMEOUT iterations ≈ GATE_TIMEOUT seconds.
  for i in $(seq 1 $((GATE_TIMEOUT * 2))); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 3>&- || true
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# forward <namespace> <service> <local-port> <remote-port> — start a
# port-forward and wait for it. Sets FORWARD_PID and joins the cleanup list; it
# must run in the current shell (not a command substitution) so the pid is
# tracked.
FORWARD_PID=""
forward() {
  local ns=$1 svc=$2 lport=$3 rport=$4 log="$TMP/pf-$3.log"
  "${KUBECTL[@]}" -n "$ns" port-forward "svc/$svc" "$lport:$rport" --address 127.0.0.1 >"$log" 2>&1 &
  FORWARD_PID=$!
  PIDS+=("$FORWARD_PID")
  if ! wait_port 127.0.0.1 "$lport"; then
    sed 's/^/    /' "$log" >&2 || true
    die "port-forward $ns/svc/$svc -> 127.0.0.1:$lport did not come up"
  fi
}

# stop_forward <pid> <local-port> — stop one forward (the AWS one also dies with
# its namespace) and wait for its local port to be released, so the next
# forward on that port cannot silently bind to a half-dead listener.
stop_forward() {
  local pid=$1 port=$2 i
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  for i in $(seq 1 25); do
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      return 0
    fi
    exec 3>&- || true
    sleep 0.2
  done
  warn_local "local port $port is still bound after stopping the forward"
}

warn_local() { printf '  WARN  %s\n' "$*" >&2; }
http_status() { # http_status <url> [curl args...]
  local url=$1
  shift
  curl -sS --max-time 30 -o "$BODY" -w '%{http_code}' "$url" "$@"
}

# gcs_seed <host:port> <bucket> <object> <payload> — CREATE path only.
gcs_seed() {
  local base=$1 bucket=$2 object=$3 payload=$4 code
  code=$(http_status "http://$base/storage/v1/b?project=$PROJECT" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$bucket\"}")
  if [[ "$code" != "200" && "$code" != "201" && "$code" != "409" ]]; then
    fail "creating bucket '$bucket' returned HTTP $code"
    sed -n '1,5p' "$BODY" | sed 's/^/        /' >&2
    return 1
  fi
  code=$(http_status "http://$base/upload/storage/v1/b/$bucket/o?uploadType=media&name=$object" \
    -H 'Content-Type: application/octet-stream' --data-binary "$payload")
  if [[ "$code" != "200" ]]; then
    fail "uploading object '$object' returned HTTP $code"
    sed -n '1,5p' "$BODY" | sed 's/^/        /' >&2
    return 1
  fi
  return 0
}

# gcs_read <host:port> <bucket> <object> — READ path only (never re-creates the
# object, so a reset that lost it cannot be masked).
gcs_read() {
  local base=$1 bucket=$2 object=$3 code
  code=$(http_status "http://$base/storage/v1/b/$bucket/o/$object?alt=media")
  if [[ "$code" != "200" ]]; then
    fail "reading object '$object' returned HTTP $code"
    return 1
  fi
  cat "$BODY"
}

# db_exists <db> — 1 when the database exists in the shared Postgres.
db_exists() {
  kube -n "$NS_DATA" exec deploy/postgres -- \
    psql -U jaiscloud -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$1'" 2>/dev/null \
    | tr -d '[:space:]'
}

# db_sql <db> <sql> — run SQL in one database and print the scalar result.
db_sql() {
  kube -n "$NS_DATA" exec deploy/postgres -- \
    psql -U jaiscloud -d "$1" -tAc "$2" 2>/dev/null | tr -d '[:space:]'
}

# ── prerequisites ───────────────────────────────────────────────────────────
head_ "prerequisites"
kube get namespace "$NS_AWS" >/dev/null 2>&1 || die "namespace $NS_AWS not found — deploy the AWS substrate first"
kube get namespace "$NS_GCP" >/dev/null 2>&1 || die "namespace $NS_GCP not found — deploy the GCP substrate first"
kube get namespace "$NS_DATA" >/dev/null 2>&1 || die "namespace $NS_DATA not found — deploy the shared store first"
kube -n "$NS_GCP" get svc jaiscloud-gcp >/dev/null 2>&1 || die "svc/$NS_GCP/jaiscloud-gcp not found"
kube -n "$NS_AWS" get svc jaiscloud >/dev/null 2>&1 || die "svc/$NS_AWS/jaiscloud not found"
kube -n "$NS_DATA" rollout status deployment/postgres --timeout="${GATE_TIMEOUT}s" >/dev/null
kube -n "$NS_GCP" rollout status deployment/jaiscloud-gcp --timeout="${GATE_TIMEOUT}s" >/dev/null
kube -n "$NS_AWS" rollout status deployment/jaiscloud --timeout="${GATE_TIMEOUT}s" >/dev/null
say "  three namespaces, both substrates and the shared store are up"

# Refuse before writing anything: the gate destroys AWS state (by design).
if [[ "$GATE_ALLOW_RESET" != "1" ]]; then
  say ""
  say "  REFUSED: this gate drops the AWS database and deletes the"
  say "  $NS_AWS namespace (its blobs and state PVCs). GCP state is the"
  say "  thing under test and is expected to survive."
  say ""
  say "  Re-run with GATE_ALLOW_RESET=1 (or: make test-dual-substrate-k8s ALLOW_RESET=1)"
  say "  when this cluster's AWS state is disposable."
  exit 2
fi


GCP_PF_PORT="${GCP_PF_PORT:-18461}"
AWS_PF_PORT="${AWS_PF_PORT:-18462}"
forward "$NS_GCP" jaiscloud-gcp "$GCP_PF_PORT" 8080
GCP_PID="$FORWARD_PID"
say "  GCP substrate on 127.0.0.1:$GCP_PF_PORT"

# ── 1. write real GCP state ─────────────────────────────────────────────────
head_ "1. write GCP state (a GCS object) and record the store facts"
OBJECT="gate/marker.txt"
PAYLOAD="dual-substrate-gate $$ $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if gcs_seed "127.0.0.1:$GCP_PF_PORT" "$BUCKET" "$OBJECT" "$PAYLOAD" \
   && [[ "$(gcs_read "127.0.0.1:$GCP_PF_PORT" "$BUCKET" "$OBJECT")" == "$PAYLOAD" ]]; then
  pass "wrote and read back gs://$BUCKET/$OBJECT"
else
  fail "the GCS object did not round-trip before the reset"
fi

if [[ "$(db_exists jaiscloud_gcp)" == "1" ]]; then
  pass "database jaiscloud_gcp exists"
else
  fail "database jaiscloud_gcp does not exist"
fi
kube -n "$NS_GCP" get pvc jaiscloud-gcp-blobs-pvc >/dev/null 2>&1 \
  && pass "pvc/$NS_GCP/jaiscloud-gcp-blobs-pvc exists" \
  || fail "pvc/$NS_GCP/jaiscloud-gcp-blobs-pvc is missing"

forward "$NS_AWS" jaiscloud "$AWS_PF_PORT" 4566
AWS_PID="$FORWARD_PID"
if [[ "$(http_status "http://127.0.0.1:$AWS_PF_PORT/_jaiscloud/health")" == "200" ]]; then
  pass "AWS substrate healthy before the reset"
else
  fail "AWS substrate is not healthy before the reset"
fi
stop_forward "$AWS_PID" "$AWS_PF_PORT"

# Positive control: leave a marker IN the AWS database. The reset must actually
# drop/recreate it, so a regressed no-op reset cannot pass this gate.
db_sql jaiscloud "CREATE TABLE IF NOT EXISTS public.dpl_gate_probe (id int); INSERT INTO public.dpl_gate_probe VALUES (1)" >/dev/null
if [[ "$(db_sql jaiscloud "SELECT count(*) FROM public.dpl_gate_probe")" == "1" ]]; then
  pass "AWS database carries a pre-reset probe row"
else
  fail "could not write the AWS probe row"
fi

# ── 2. the reset under test ─────────────────────────────────────────────────
head_ "2. deploy/deploy.sh --reset (AWS-only, database-scoped)"
( cd "$REPO_ROOT" && SKIP_IMAGE_BUILD=1 ./deploy/deploy.sh --reset --yes ) \
  || fail "deploy.sh --reset exited non-zero"

# ── 3. GCP state must be untouched ──────────────────────────────────────────
head_ "3. GCP state survived the AWS reset"
# The GCP pod should never have restarted; check readiness, then re-read state
# through the still-open forward (it died only if the GCP Service changed).
if ! kube -n "$NS_GCP" rollout status deployment/jaiscloud-gcp --timeout="${GATE_TIMEOUT}s" >/dev/null; then
  fail "deployment/jaiscloud-gcp is not Ready after the AWS reset"
else
  stop_forward "$GCP_PID" "$GCP_PF_PORT"
  forward "$NS_GCP" jaiscloud-gcp "$GCP_PF_PORT" 8080
  GCP_PID="$FORWARD_PID"
  if [[ "$(gcs_read "127.0.0.1:$GCP_PF_PORT" "$BUCKET" "$OBJECT")" == "$PAYLOAD" ]]; then
    pass "gs://$BUCKET/$OBJECT still reads back identically"
  else
    fail "the GCS object changed or disappeared after the AWS reset"
  fi
fi

if [[ "$(db_exists jaiscloud_gcp)" == "1" ]]; then
  pass "database jaiscloud_gcp survived"
else
  fail "database jaiscloud_gcp was dropped by the AWS reset"
fi
if kube -n "$NS_GCP" get pvc jaiscloud-gcp-blobs-pvc >/dev/null 2>&1; then
  pass "pvc/$NS_GCP/jaiscloud-gcp-blobs-pvc survived"
else
  fail "the GCP blobs PVC was deleted by the AWS reset"
fi
if kube -n "$NS_GCP" get pvc jaiscloud-gcp-state-pvc >/dev/null 2>&1; then
  pass "pvc/$NS_GCP/jaiscloud-gcp-state-pvc survived"
else
  fail "the GCP state PVC was deleted by the AWS reset"
fi

# ── 4. the AWS substrate must be back ───────────────────────────────────────
head_ "4. AWS substrate came back, pointing at the recreated database"
kube -n "$NS_AWS" rollout status deployment/jaiscloud --timeout="${GATE_TIMEOUT}s" >/dev/null \
  && pass "deployment/$NS_AWS/jaiscloud is Ready" \
  || fail "deployment/$NS_AWS/jaiscloud is not Ready after the reset"
if [[ "$(db_exists jaiscloud)" == "1" ]]; then
  pass "database jaiscloud was recreated"
else
  fail "database jaiscloud is missing after the reset"
fi
if [[ "$(db_sql jaiscloud "SELECT to_regclass('public.dpl_gate_probe') IS NOT NULL")" == "f" ]]; then
  pass "the AWS reset really dropped the old database (probe row gone)"
else
  fail "pre-reset data is still in the AWS database — the reset did not drop it"
fi
forward "$NS_AWS" jaiscloud "$AWS_PF_PORT" 4566
AWS_PID="$FORWARD_PID"
if [[ "$(http_status "http://127.0.0.1:$AWS_PF_PORT/_jaiscloud/health")" == "200" ]]; then
  pass "AWS substrate healthy after the reset"
else
  fail "AWS substrate is not healthy after the reset"
fi
stop_forward "$AWS_PID" "$AWS_PF_PORT"

# ── 5. housekeeping ─────────────────────────────────────────────────────────
if [[ "$KEEP_STATE" -eq 0 ]]; then
  head_ "5. clean up the probe object"
  http_status "http://127.0.0.1:$GCP_PF_PORT/storage/v1/b/$BUCKET/o/$OBJECT" -X DELETE >/dev/null || true
  http_status "http://127.0.0.1:$GCP_PF_PORT/storage/v1/b/$BUCKET" -X DELETE >/dev/null || true
  say "  (probe object and bucket deleted; pass --keep-state to keep them)"
fi

# ── summary ─────────────────────────────────────────────────────────────────
head_ "summary"
say "  passed: $PASSED   failed: $FAILED"
if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi
say "  one substrate reset left the other substrate's state intact"
exit 0
