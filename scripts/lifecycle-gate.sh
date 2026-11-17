#!/usr/bin/env bash
#
# lifecycle-gate.sh — park / resume / switch must be compute-destructive and
# data-retentive. It proves the retention contract end to end:
#
#   seed       a GCS object (GCP) and an S3 object (AWS), record both instances'
#              IDs, and create one labelled namespace the sweeps must not delete
#   park aws   the GCP object still reads; the AWS database, PVCs and namespace
#              survive; the AWS Deployment is scaled to 0 and no AWS-labelled
#              namespace has a running Pod
#   resume aws the S3 object and the AWS instance ID are intact
#   switch gcp / switch aws
#              each direction parks one substrate and resumes the other; the
#              seeded objects, both instance IDs and the labelled namespace
#              survive every transition
#
# Assumes both substrates are already installed (deploy.sh and deploy.sh --gcp)
# with images that carry POST /_jaiscloud/park and /resume. Nothing is applied
# or rebuilt here; the only mutations are the lifecycle commands and probes.
#
# Usage:
#   GATE_ALLOW_LIFECYCLE=1 scripts/lifecycle-gate.sh
#
# Env:
#   KUBE_CONTEXT                       kubectl context (default: current)
#   NS_AWS / NS_GCP / NS_DATA          namespaces (default jaiscloud /
#                                      jaiscloud-gcp / jaiscloud-data)
#   PROJECT                            GCP project for the probe (default lifecycle-gate)
#   GATE_TIMEOUT                       rollout/port-forward seconds (default 240)
#
# Exit: 0 only when every assertion holds.
set -euo pipefail

PROG="$(basename "$0")"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NS_AWS="${NS_AWS:-jaiscloud}"
NS_GCP="${NS_GCP:-jaiscloud-gcp}"
NS_DATA="${NS_DATA:-jaiscloud-data}"
PROJECT="${PROJECT:-lifecycle-gate}"
GATE_TIMEOUT="${GATE_TIMEOUT:-240}"
GATE_ALLOW_LIFECYCLE="${GATE_ALLOW_LIFECYCLE:-0}"
FIXTURE_NS="gcp-lifecycle-gate-$$"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/jaiscloud-lifecycle-gate.XXXXXX")"
BODY="$TMP/body"
PIDS=()
cleanup() {
  local i
  for i in "${!PIDS[@]}"; do
    kill "${PIDS[$i]}" >/dev/null 2>&1 || true
    wait "${PIDS[$i]}" >/dev/null 2>&1 || true
  done
  # A failure before the housekeeping section must not leak the fixture.
  kube delete namespace "$FIXTURE_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
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

KUBECTL=(kubectl)
if [[ -n "$KUBE_CONTEXT" ]]; then
  KUBECTL=(kubectl --context "$KUBE_CONTEXT")
fi
kube() { "${KUBECTL[@]}" "$@"; }

# wait_port <host> <port> — poll until the port accepts a TCP connection.
wait_port() {
  local host=$1 port=$2 i
  for i in $(seq 1 $((GATE_TIMEOUT * 2))); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 3>&- || true
      return 0
    fi
    sleep 0.5
  done
  return 1
}

FORWARD_PID=""
# forward <namespace> <service> <local-port> <remote-port> — start a
# port-forward and wait for it; sets FORWARD_PID and joins the cleanup list.
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
  printf '  WARN  local port %s is still bound after stopping the forward\n' "$port" >&2
}

http_status() { # http_status <url> [curl args...]
  local url=$1
  shift
  curl -sS --max-time 30 -o "$BODY" -w '%{http_code}' "$url" "$@"
}

# ── S3 (AWS) probes — path-style, no SDK ────────────────────────────────────
s3_seed() { # s3_seed <host:port> <bucket> <key> <payload>
  local base=$1 bucket=$2 key=$3 payload=$4 code
  code=$(http_status "http://$base/$bucket" -X PUT)
  if [[ "$code" != "200" ]]; then
    fail "creating S3 bucket '$bucket' returned HTTP $code"
    sed -n '1,5p' "$BODY" | sed 's/^/        /' >&2
    return 1
  fi
  code=$(http_status "http://$base/$bucket/$key" -X PUT --data-binary "$payload")
  if [[ "$code" != "200" ]]; then
    fail "writing s3://$bucket/$key returned HTTP $code"
    sed -n '1,5p' "$BODY" | sed 's/^/        /' >&2
    return 1
  fi
}

s3_read() { # s3_read <host:port> <bucket> <key> — read path only
  local base=$1 bucket=$2 key=$3 code
  code=$(http_status "http://$base/$bucket/$key")
  if [[ "$code" != "200" ]]; then
    fail "reading s3://$bucket/$key returned HTTP $code"
    return 1
  fi
  cat "$BODY"
}

# ── GCS probes — REST as the dual-substrate gate ────────────────────────────
gcs_seed() { # gcs_seed <host:port> <bucket> <object> <payload>
  local base=$1 bucket=$2 object=$3 payload=$4 code
  code=$(http_status "http://$base/storage/v1/b?project=$PROJECT" \
    -H 'Content-Type: application/json' -d "{\"name\":\"$bucket\"}")
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
}

gcs_read() {
  local base=$1 bucket=$2 object=$3 code
  code=$(http_status "http://$base/storage/v1/b/$bucket/o/$object?alt=media")
  if [[ "$code" != "200" ]]; then
    fail "reading object '$object' returned HTTP $code"
    return 1
  fi
  cat "$BODY"
}

# ── cluster facts ───────────────────────────────────────────────────────────
db_exists() {
  kube -n "$NS_DATA" exec deploy/postgres -- \
    psql -U jaiscloud -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$1'" 2>/dev/null \
    | tr -d '[:space:]'
}

instance_id() { # instance_id <namespace> <deployment>
  kube -n "$1" exec "deploy/$2" -- cat /data/state/instance-id 2>/dev/null | tr -d '[:space:]'
}

replicas() { # replicas <namespace> <deployment>
  kube -n "$1" get deploy "$2" -o jsonpath='{.spec.replicas}' 2>/dev/null || true
}

ns_annotation() { # ns_annotation <namespace> <key>
  kube get namespace "$1" \
    -o jsonpath="{.metadata.annotations['$2']}" 2>/dev/null || true
}

# pods_in_labelled_namespaces <cloud> — total Pods in every namespace labelled
# as the given substrate's executor namespace (including the substrate itself).
pods_in_labelled_namespaces() {
  local cloud=$1 ns total=0 count
  for ns in $(kube get namespaces \
      -l "jaiscloud.io/managed-by=jaiscloud,jaiscloud.io/substrate=$cloud" \
      -o name 2>/dev/null | sed 's|^namespace/||'); do
    count=$(kube -n "$ns" get pods --no-headers 2>/dev/null | wc -l | tr -d '[:space:]')
    total=$((total + count))
  done
  printf '%s' "$total"
}

# ── prerequisites ───────────────────────────────────────────────────────────
head_ "prerequisites"
kube get namespace "$NS_AWS" >/dev/null 2>&1 || die "namespace $NS_AWS not found — deploy the AWS substrate first"
kube get namespace "$NS_GCP" >/dev/null 2>&1 || die "namespace $NS_GCP not found — deploy the GCP substrate first"
kube get namespace "$NS_DATA" >/dev/null 2>&1 || die "namespace $NS_DATA not found — deploy the shared store first"
kube -n "$NS_AWS" get svc jaiscloud >/dev/null 2>&1 || die "svc/$NS_AWS/jaiscloud not found"
kube -n "$NS_GCP" get svc jaiscloud-gcp >/dev/null 2>&1 || die "svc/$NS_GCP/jaiscloud-gcp not found"
kube -n "$NS_DATA" rollout status deployment/postgres --timeout="${GATE_TIMEOUT}s" >/dev/null
kube -n "$NS_AWS" rollout status deployment/jaiscloud --timeout="${GATE_TIMEOUT}s" >/dev/null
kube -n "$NS_GCP" rollout status deployment/jaiscloud-gcp --timeout="${GATE_TIMEOUT}s" >/dev/null
say "  both substrates and the shared store are up"

if [[ "$GATE_ALLOW_LIFECYCLE" != "1" ]]; then
  say ""
  say "  REFUSED: this gate parks and resumes both substrates (compute only;"
  say "  data is expected to survive, but the running workloads go away and come"
  say "  back). Pass GATE_ALLOW_LIFECYCLE=1 to run it:"
  say "    GATE_ALLOW_LIFECYCLE=1 make test-lifecycle-k8s"
  exit 2
fi

GCP_PF_PORT="${GCP_PF_PORT:-18471}"
AWS_PF_PORT="${AWS_PF_PORT:-18472}"
forward "$NS_GCP" jaiscloud-gcp "$GCP_PF_PORT" 8080
GCP_PID="$FORWARD_PID"
forward "$NS_AWS" jaiscloud "$AWS_PF_PORT" 4566
AWS_PID="$FORWARD_PID"
say "  GCP on 127.0.0.1:$GCP_PF_PORT, AWS on 127.0.0.1:$AWS_PF_PORT"

# ── 1. seed state and record identity ───────────────────────────────────────
head_ "1. seed one object per substrate, record instance IDs, create a fixture namespace"
GCS_BUCKET="lifecycle-gate-$$"
GCS_OBJECT="gate/gcs-marker.txt"
GCS_PAYLOAD="lifecycle-gate gcs $$ $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if gcs_seed "127.0.0.1:$GCP_PF_PORT" "$GCS_BUCKET" "$GCS_OBJECT" "$GCS_PAYLOAD" \
   && [[ "$(gcs_read "127.0.0.1:$GCP_PF_PORT" "$GCS_BUCKET" "$GCS_OBJECT")" == "$GCS_PAYLOAD" ]]; then
  pass "wrote and read back gs://$GCS_BUCKET/$GCS_OBJECT"
else
  fail "the GCS object did not round-trip before parking"
fi

S3_BUCKET="lifecycle-gate-$$"
S3_KEY="gate/s3-marker.txt"
S3_PAYLOAD="lifecycle-gate s3 $$ $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if s3_seed "127.0.0.1:$AWS_PF_PORT" "$S3_BUCKET" "$S3_KEY" "$S3_PAYLOAD" \
   && [[ "$(s3_read "127.0.0.1:$AWS_PF_PORT" "$S3_BUCKET" "$S3_KEY")" == "$S3_PAYLOAD" ]]; then
  pass "wrote and read back s3://$S3_BUCKET/$S3_KEY"
else
  fail "the S3 object did not round-trip before parking"
fi

AWS_INSTANCE="$(instance_id "$NS_AWS" jaiscloud)"
GCP_INSTANCE="$(instance_id "$NS_GCP" jaiscloud-gcp)"
[[ -n "$AWS_INSTANCE" ]] && pass "AWS instance ID recorded ($AWS_INSTANCE)" \
  || fail "could not read /data/state/instance-id from the AWS pod"
[[ -n "$GCP_INSTANCE" ]] && pass "GCP instance ID recorded ($GCP_INSTANCE)" \
  || fail "could not read /data/state/instance-id from the GCP pod"

# A labelled namespace stands in for the per-cluster namespaces Dataproc/Managed
# Kafka create: a park sweeps its workloads but must never delete it.
cat <<EOF | kube apply -f - >/dev/null
apiVersion: v1
kind: Namespace
metadata:
  name: $FIXTURE_NS
  labels:
    jaiscloud.io/managed-by: jaiscloud
    jaiscloud.io/substrate: gcp
    jaiscloud.io/instance-id: $GCP_INSTANCE
    app.kubernetes.io/part-of: lifecycle-gate
EOF
pass "created labelled fixture namespace $FIXTURE_NS"

stop_forward "$AWS_PID" "$AWS_PF_PORT"

# ── 2. park AWS ─────────────────────────────────────────────────────────────
head_ "2. deploy.sh --park aws"
( cd "$REPO_ROOT" && ./deploy/deploy.sh --park aws --yes ) || fail "deploy.sh --park aws exited non-zero"

if [[ "$(gcs_read "127.0.0.1:$GCP_PF_PORT" "$GCS_BUCKET" "$GCS_OBJECT")" == "$GCS_PAYLOAD" ]]; then
  pass "the GCP object still reads while AWS is parked"
else
  fail "the GCP object changed or disappeared during the AWS park"
fi
[[ "$(db_exists jaiscloud)" == "1" ]] && pass "database jaiscloud survived" \
  || fail "database jaiscloud disappeared"
kube -n "$NS_AWS" get pvc jaiscloud-blobs-pvc >/dev/null 2>&1 \
  && pass "pvc/$NS_AWS/jaiscloud-blobs-pvc survived" \
  || fail "the AWS blobs PVC disappeared"
kube -n "$NS_AWS" get pvc jaiscloud-state-pvc >/dev/null 2>&1 \
  && pass "pvc/$NS_AWS/jaiscloud-state-pvc survived" \
  || fail "the AWS state PVC disappeared"
kube get namespace "$NS_AWS" >/dev/null 2>&1 \
  && pass "namespace $NS_AWS survived" || fail "namespace $NS_AWS was deleted"
[[ "$(replicas "$NS_AWS" jaiscloud)" == "0" ]] \
  && pass "deployment/$NS_AWS/jaiscloud is scaled to 0" \
  || fail "the AWS Deployment is not parked (replicas=$(replicas "$NS_AWS" jaiscloud))"
[[ "$(ns_annotation "$NS_AWS" jaiscloud.io/state)" == "parked" ]] \
  && pass "namespace $NS_AWS is annotated state=parked" \
  || fail "namespace $NS_AWS is not annotated state=parked"
if [[ "$(pods_in_labelled_namespaces aws)" == "0" ]]; then
  pass "no Pods run in any AWS-labelled namespace"
else
  fail "AWS-labelled namespaces still run $(pods_in_labelled_namespaces aws) Pod(s)"
fi

# ── 3. resume AWS ───────────────────────────────────────────────────────────
head_ "3. deploy.sh --resume aws"
( cd "$REPO_ROOT" && ./deploy/deploy.sh --resume aws --yes ) || fail "deploy.sh --resume aws exited non-zero"
forward "$NS_AWS" jaiscloud "$AWS_PF_PORT" 4566
AWS_PID="$FORWARD_PID"
if [[ "$(s3_read "127.0.0.1:$AWS_PF_PORT" "$S3_BUCKET" "$S3_KEY")" == "$S3_PAYLOAD" ]]; then
  pass "s3://$S3_BUCKET/$S3_KEY still reads after resume"
else
  fail "the S3 object changed or disappeared across park/resume"
fi
if [[ "$(instance_id "$NS_AWS" jaiscloud)" == "$AWS_INSTANCE" ]]; then
  pass "the AWS instance ID survived park/resume"
else
  fail "the AWS instance ID changed across park/resume"
fi
[[ "$(replicas "$NS_AWS" jaiscloud)" == "1" ]] \
  && pass "deployment/$NS_AWS/jaiscloud is running again" \
  || fail "the AWS Deployment did not resume"
[[ "$(ns_annotation "$NS_AWS" jaiscloud.io/state)" == "running" ]] \
  && pass "namespace $NS_AWS is annotated state=running" \
  || fail "namespace $NS_AWS is not annotated state=running"
[[ -z "$(ns_annotation "$NS_AWS" jaiscloud.io/parked-image-id)" ]] \
  && pass "the parked-* annotations were cleared on resume" \
  || fail "the parked-* annotations were left behind"

# ── 4. switch to GCP (park AWS, resume GCP) ─────────────────────────────────
head_ "4. deploy.sh --switch gcp"
( cd "$REPO_ROOT" && ./deploy/deploy.sh --switch gcp --yes ) || fail "deploy.sh --switch gcp exited non-zero"

[[ "$(replicas "$NS_AWS" jaiscloud)" == "0" ]] \
  && pass "switch parked the AWS Deployment" || fail "the AWS Deployment still runs after --switch gcp"
[[ "$(replicas "$NS_GCP" jaiscloud-gcp)" == "1" ]] \
  && pass "switch resumed the GCP Deployment" || fail "the GCP Deployment is not running after --switch gcp"
if [[ "$(gcs_read "127.0.0.1:$GCP_PF_PORT" "$GCS_BUCKET" "$GCS_OBJECT")" == "$GCS_PAYLOAD" ]]; then
  pass "the GCP object survived the switch to GCP"
else
  fail "the GCP object disappeared across --switch gcp"
fi
# The parked pod is gone, so read the state PVC rather than exec into it; the
# ID itself is asserted after the next resume.
kube -n "$NS_AWS" get pvc jaiscloud-state-pvc >/dev/null 2>&1 \
  && pass "the AWS state PVC (instance ID) survived --switch gcp" \
  || fail "the AWS state PVC disappeared while parked"
if [[ "$(instance_id "$NS_GCP" jaiscloud-gcp)" == "$GCP_INSTANCE" ]]; then
  pass "the GCP instance ID survived --switch gcp"
else
  fail "the GCP instance ID changed across --switch gcp"
fi
kube get namespace "$FIXTURE_NS" >/dev/null 2>&1 \
  && pass "the labelled namespace survived --switch gcp" \
  || fail "the labelled namespace was deleted by --switch gcp"

stop_forward "$AWS_PID" "$AWS_PF_PORT"

# ── 5. switch back to AWS (park GCP, resume AWS) ────────────────────────────
head_ "5. deploy.sh --switch aws"
( cd "$REPO_ROOT" && ./deploy/deploy.sh --switch aws --yes ) || fail "deploy.sh --switch aws exited non-zero"
forward "$NS_AWS" jaiscloud "$AWS_PF_PORT" 4566
AWS_PID="$FORWARD_PID"

[[ "$(replicas "$NS_GCP" jaiscloud-gcp)" == "0" ]] \
  && pass "switch parked the GCP Deployment" || fail "the GCP Deployment still runs after --switch aws"
[[ "$(replicas "$NS_AWS" jaiscloud)" == "1" ]] \
  && pass "switch resumed the AWS Deployment" || fail "the AWS Deployment is not running after --switch aws"
if [[ "$(s3_read "127.0.0.1:$AWS_PF_PORT" "$S3_BUCKET" "$S3_KEY")" == "$S3_PAYLOAD" ]]; then
  pass "the S3 object survived both switches"
else
  fail "the S3 object disappeared across --switch aws"
fi
if [[ "$(instance_id "$NS_AWS" jaiscloud)" == "$AWS_INSTANCE" ]]; then
  pass "the AWS instance ID survived both switches"
else
  fail "the AWS instance ID changed across --switch aws"
fi
if [[ "$(instance_id "$NS_AWS" jaiscloud)" == "$AWS_INSTANCE" ]]; then
  pass "the AWS instance ID survived both switches"
else
  fail "the AWS instance ID changed across --switch aws"
fi
# GCP is parked at this point; its pod is gone, so assert the state PVC.
kube -n "$NS_GCP" get pvc jaiscloud-gcp-state-pvc >/dev/null 2>&1 \
  && pass "the GCP state PVC (instance ID) survived the switch cycle" \
  || fail "the GCP state PVC disappeared while parked"
[[ "$(db_exists jaiscloud_gcp)" == "1" ]] && pass "database jaiscloud_gcp survived" \
  || fail "database jaiscloud_gcp disappeared"
kube -n "$NS_GCP" get pvc jaiscloud-gcp-blobs-pvc >/dev/null 2>&1 \
  && pass "pvc/$NS_GCP/jaiscloud-gcp-blobs-pvc survived" \
  || fail "the GCP blobs PVC disappeared"
kube -n "$NS_GCP" get pvc jaiscloud-gcp-state-pvc >/dev/null 2>&1 \
  && pass "pvc/$NS_GCP/jaiscloud-gcp-state-pvc survived" \
  || fail "the GCP state PVC disappeared"
kube get namespace "$FIXTURE_NS" >/dev/null 2>&1 \
  && pass "the labelled namespace survived --switch aws" \
  || fail "the labelled namespace was deleted by --switch aws"

# ── 6. housekeeping ─────────────────────────────────────────────────────────
head_ "6. clean up the fixture namespace and probe objects"
kube delete namespace "$FIXTURE_NS" --ignore-not-found --timeout=120s >/dev/null 2>&1 \
  || printf '  WARN  could not delete the fixture namespace %s\n' "$FIXTURE_NS" >&2
http_status "http://127.0.0.1:$GCP_PF_PORT/storage/v1/b/$GCS_BUCKET/o/$GCS_OBJECT" -X DELETE >/dev/null || true
http_status "http://127.0.0.1:$GCP_PF_PORT/storage/v1/b/$GCS_BUCKET" -X DELETE >/dev/null || true
http_status "http://127.0.0.1:$AWS_PF_PORT/$S3_BUCKET/$S3_KEY" -X DELETE >/dev/null || true
http_status "http://127.0.0.1:$AWS_PF_PORT/$S3_BUCKET" -X DELETE >/dev/null || true
say "  fixture namespace and probe objects removed"

# ── summary ─────────────────────────────────────────────────────────────────
head_ "summary"
say "  passed: $PASSED   failed: $FAILED"
if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi
say "  park is compute-destructive and data-retentive; resume and both switch directions kept"
say "  the seeded objects, both instance IDs, both databases, the PVCs and the namespace."
exit 0
