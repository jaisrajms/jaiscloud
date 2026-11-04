#!/usr/bin/env bash
#
# run.sh — the gate for the Dataproc-on-GKE lifecycle module.
#
# Drives this module against a running jaiscloud-gcp and asserts the result
# through the emulator's REST API: init -> validate -> plan -> apply -> spot
# checks -> a second plan (permadiff) -> destroy -> "nothing is left".
#
# Deliberately Terraform-only. The demo's other legs are a Java job and a Helm
# installer; both arrive in later sessions and neither is exercised here. This
# gate answers one question: does the module the demo ships still create the
# infrastructure the later legs read from, cleanly and idempotently?
#
# It never installs tooling. When no terraform binary is on PATH it prints SKIP
# and exits 0 (the `make test-gcp-terraform` precedent), because a checkout
# without terraform is not a failure. Everything else it needs — an emulator —
# it builds and starts itself, and stops again on the way out; an emulator that
# was already healthy at ENDPOINT is left alone.
#
# The gate removes the saved plan, and the state files after a destroy that left
# nothing behind, so a stale tfstate can never make the next run look green. The
# module's .gitignore covers all of them; .terraform and its lock file are kept
# (they are the init cache) so a re-run does not re-download the provider, and a
# run that failed keeps its state for inspection and a manual destroy.
#
# Usage:
#   demo/dpgke-lifecycle/terraform/run.sh [ENDPOINT]
#
#     ENDPOINT  base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#
# Env:
#   TF_BIN    terraform binary to use (default: terraform)
#   PROJECT   project id (default: dpgke-demo, the module's own default)

set -euo pipefail

ENDPOINT="${1:-http://localhost:8080}"
ENDPOINT="${ENDPOINT%/}"
TF="${TF_BIN:-terraform}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
PROJECT="${PROJECT:-dpgke-demo}"
PLAN="$DIR/dpgke.tfplan"
LOG="${TMPDIR:-/tmp}/jaiscloud-gcp-dpgke-terraform.log"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed — nothing to gate here"
  exit 0
fi

PASS=0
FAIL=0

# check NAME URL SUBSTRING [SUBSTRING...] — GET the URL and require every
# substring to be present. Records a failure (does not abort) so one run
# reports every divergence.
check() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      printf '  [FAIL] %s (missing: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

# check_absent NAME URL SUBSTRING... — GET the URL and require every substring to
# be absent. Used for input-only fields real GCP never echoes, and for the
# post-destroy listings (a name that is no longer there).
check_absent() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      printf '  [FAIL] %s (unexpected: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

# check_status NAME URL WANT — require the HTTP status of a GET to be WANT. The
# post-destroy assertions expect 404, which `curl -sf` cannot distinguish from a
# wrong URL; the same URL was proven to answer 200 before the destroy.
check_status() {
  local name="$1" url="$2" want="$3" got
  got="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  if [[ "$got" == "$want" ]]; then
    printf '  [PASS] %s\n' "$name"
    PASS=$((PASS + 1))
  else
    printf '  [FAIL] %s (HTTP %s, want %s)\n' "$name" "$got" "$want"
    FAIL=$((FAIL + 1))
  fi
}

tf() { "$TF" -chdir="$DIR" "$@"; }

# out NAME — a string output, empty when the output is missing.
out() { tf output -raw "$1" 2>/dev/null || true; }

# bucket NAME — one entry of the `buckets` output. It is an object, so -raw
# cannot read it; terraform pretty-prints -json, hence the collapse first.
bucket() {
  tf output -json buckets 2>/dev/null | tr -d '\n' |
    sed -nE 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p'
}

DESTROYED=0
EMULATOR_PID=""
TF_VARS=()

destroy() {
  if [[ "$DESTROYED" == "1" ]]; then
    return 0
  fi
  DESTROYED=1
  if [[ ${#TF_VARS[@]} -eq 0 ]]; then
    return 0
  fi
  tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null 2>&1 || true
}

stop_emulator() {
  if [[ -n "$EMULATOR_PID" ]]; then
    kill "$EMULATOR_PID" 2>/dev/null || true
    wait "$EMULATOR_PID" 2>/dev/null || true
  fi
}

clean_artifacts() {
  rm -f "${DIR:?}/dpgke.tfplan"
  # Keep the init cache (.terraform + lock) always, and keep state unless this run
  # destroyed everything: a failed run's state is what lets the next step clean up.
  if [[ "$DESTROYED" == "1" && -z "$(tf state list 2>/dev/null || true)" ]]; then
    rm -f "${DIR:?}/terraform.tfstate" "${DIR:?}/terraform.tfstate.backup"
  fi
}

on_exit() {
  local rc=$?
  destroy
  stop_emulator
  clean_artifacts
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ── The container.* host ──────────────────────────────────────────────────────
# GKE shares /v1/projects/{p}/locations/{l}/clusters with Managed Kafka on a bare
# origin. The emulator tells the two apart — clusterId means Kafka, a create body
# with a GKE `cluster` and no clusterId means GKE, otherwise it probes which
# service holds a cluster at that project+location — so nothing is silently
# wrong, but it is a heuristic. GKE's own host token removes the question, and it
# is also the only form the provider can express: it strips a custom endpoint's
# path and keeps scheme+host. provider.tf derives the same host in HCL; this
# script derives it again only so its REST spot checks reach GKE directly, and it
# passes the result to the module as container_endpoint so the two can never
# disagree: a module that drifted back to the bare origin would be leaning on the
# heuristic, and the checks below would fail to find the cluster instead of
# silently passing against Managed Kafka's empty list.
CONTAINER_ORIGIN="$(printf '%s' "$ENDPOINT" | sed -E 's#^(https?://)([^/]+)#\1container.\2#')"
CONTAINER_ENDPOINT="$CONTAINER_ORIGIN/container/"
CONTAINER_BASE="$CONTAINER_ORIGIN/container/v1"
CONTAINER_HOST="$(printf '%s' "$CONTAINER_ORIGIN" | sed -E 's#^https?://##; s#[:/].*##')"
if ! getent hosts "$CONTAINER_HOST" >/dev/null 2>&1; then
  echo "ERROR: $CONTAINER_HOST does not resolve — the GKE half of this module needs a container.* host"
  echo "       (the provider drops the custom-endpoint path, so the bare origin's shared"
  echo "        clusters collection would have to be told apart from Managed Kafka's)."
  echo "       Fix the host:   add '127.0.0.1 container.localhost' to the hosts file, or"
  echo "       point ENDPOINT at a host whose container.* sibling resolves. When running the"
  echo "       module by hand, the same host goes in the module's container_endpoint variable"
  echo "       (terraform.tfvars) — this script passes it as -var container_endpoint=… and"
  echo "       therefore needs the alias on the host itself."
  exit 1
fi

# ── Emulator ─────────────────────────────────────────────────────────────────
# Reuse a healthy emulator (someone else's: the trap will not kill it), else
# build and start one. The port comes from ENDPOINT so a non-default ENDPOINT is
# honoured; the default is the documented 8080/8081 pair.
if curl -sf "$ENDPOINT/_jaiscloud/health" >/dev/null 2>&1; then
  echo "== emulator: reusing the one already healthy at $ENDPOINT"
else
  ENDPOINT_HOST="$(printf '%s' "$ENDPOINT" | sed -E 's#^https?://##; s#[:/].*##')"
  case "$ENDPOINT_HOST" in
    localhost | 127.0.0.1 | ::1) ;;
    *)
      echo "ERROR: no emulator is healthy at $ENDPOINT and $ENDPOINT_HOST is not local"
      echo "       start one there, or point ENDPOINT at a local emulator"
      exit 1
      ;;
  esac
  PORT="$(printf '%s' "$ENDPOINT" | sed -nE 's#^https?://[^/]*:([0-9]+).*#\1#p')"
  PORT="${PORT:-8080}"
  GRPC_PORT=$((PORT + 1))

  # go lives outside PATH in some checkouts; prepend its directory only when it
  # is missing, never shadow a go that is already there.
  if ! command -v go >/dev/null 2>&1; then
    export PATH="/tmp/opencode/go/bin:$PATH"
  fi
  echo "== emulator: building ./jaiscloud-gcp from $ROOT"
  (cd "$ROOT" && go build -o ./jaiscloud-gcp ./cmd/jaiscloud-gcp/)

  echo "== emulator: starting on :$PORT (grpc :$GRPC_PORT, log $LOG)"
  "$ROOT/jaiscloud-gcp" start --port "$PORT" --grpc-port "$GRPC_PORT" --ephemeral >"$LOG" 2>&1 &
  EMULATOR_PID=$!

  READY=0
  for _ in $(seq 1 30); do
    if curl -sf "$ENDPOINT/_jaiscloud/health" >/dev/null 2>&1; then
      READY=1
      break
    fi
    if ! kill -0 "$EMULATOR_PID" 2>/dev/null; then
      echo "ERROR: jaiscloud-gcp exited while starting; last log lines:"
      tail -n 20 "$LOG"
      exit 1
    fi
    sleep 1
  done
  if [[ "$READY" != "1" ]]; then
    echo "ERROR: emulator did not become healthy within 30s; last log lines:"
    tail -n 20 "$LOG"
    exit 1
  fi
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"
export TF_IN_AUTOMATION=1
# Reuse terraform's provider cache when the host has one, so a gate run does not
# depend on reaching the registry for a provider it already has.
if [[ -z "${TF_PLUGIN_CACHE_DIR:-}" && -d "$HOME/.terraform.d/plugin-cache" ]]; then
  export TF_PLUGIN_CACHE_DIR="$HOME/.terraform.d/plugin-cache"
fi

TF_VARS=(-var="endpoint=$ENDPOINT" -var="project=$PROJECT" -var="container_endpoint=$CONTAINER_ENDPOINT")

echo "== $TF: init / validate / plan / apply (endpoint=$ENDPOINT project=$PROJECT)"
tf init -input=false -no-color >/dev/null
tf validate -no-color >/dev/null
tf plan -input=false -no-color -out="$PLAN" "${TF_VARS[@]}" >/dev/null
tf apply -input=false -auto-approve -no-color "$PLAN" >/dev/null

# ── The second plan ──────────────────────────────────────────────────────────
# A permadiff is a FAIL, not an abort: the emulator has to answer a refresh with
# exactly what the provider stored, or a copied module never converges. -1 and
# other non-zero codes are plan errors and fail the same way, with the output.
echo "== $TF: second plan (permadiff check)"
PLAN_RC=0
PLAN_OUT="$(tf plan -input=false -no-color -detailed-exitcode "${TF_VARS[@]}" 2>&1)" || PLAN_RC=$?
if [[ "$PLAN_RC" -eq 0 ]]; then
  printf '  [PASS] re-plan reports no changes\n'
  PASS=$((PASS + 1))
elif [[ "$PLAN_RC" -eq 2 ]]; then
  printf '  [FAIL] re-plan reports changes (a permadiff)\n'
  FAIL=$((FAIL + 1))
  printf '%s\n' "$PLAN_OUT" | tail -n 40
else
  printf '  [FAIL] re-plan failed (exit %s)\n' "$PLAN_RC"
  FAIL=$((FAIL + 1))
  printf '%s\n' "$PLAN_OUT" | tail -n 40
fi

GKE_CLUSTER="$(out gke_cluster_name)"
DP_CLUSTER="$(out dataproc_cluster_name)"
REGION="$(out region)"
GKE_LOCATION="$(out gke_location)"
STORAGE_PROJECT="$(out storage_project)"
SA_EMAIL="$(out service_account_email)"
BUCKET_INPUT="$(bucket input)"
BUCKET_OUTPUT="$(bucket output)"
BUCKET_STAGING="$(bucket staging)"

echo "== spot checks"

# ── GKE: the zonal cluster ───────────────────────────────────────────────────
# The module's default target is the quickstart shape, region us-central1 with a
# cluster in us-central1-a. Assert the zone in the response: a silently rewritten
# location would break the Dataproc target below, and it is the field the
# provider diffs.
check "GKE cluster is zonal at $GKE_LOCATION" \
  "$CONTAINER_BASE/projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER" \
  "\"location\":\"$GKE_LOCATION\"" \
  "\"name\":\"$GKE_CLUSTER\""
# google_container_cluster's read dereferences legacyAbac.enabled,
# shieldedNodes.enabled and defaultMaxPodsConstraint.maxPodsPerNode
# unconditionally: a read that omits one SIGSEGVs the provider during refresh
# instead of reporting a diff, so apply surviving is not enough — the wire shape
# is asserted here.
check "GKE cluster read renders legacyAbac, shieldedNodes, defaultMaxPodsConstraint" \
  "$CONTAINER_BASE/projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER" \
  '"legacyAbac":{"enabled":false}' \
  '"shieldedNodes":{"enabled":true}' \
  '"defaultMaxPodsConstraint":{"maxPodsPerNode":"110"}'

# ── Dataproc on GKE: the virtual cluster ─────────────────────────────────────
DP_URL="$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$DP_CLUSTER"
check "Dataproc-on-GKE virtual cluster is RUNNING" "$DP_URL" '"state":"RUNNING"'
# The target must be the zonal cluster actually created above, not a regional
# spelling of it and not the region.
check "virtual cluster targets projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER" \
  "$DP_URL" \
  "\"gkeClusterTarget\":\"projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER\""
# Both reconciled pool ids are expanded by the provider into full resource names
# and echoed back by the read; the label on each pool names the Dataproc cluster.
check "virtual cluster echoes both reconciled node pools" \
  "$DP_URL" "\"nodePool\":\"projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER/nodePools/$DP_CLUSTER-controller\"" \
  "\"nodePool\":\"projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER/nodePools/$DP_CLUSTER-spark\""
# nodePoolConfig is input-only on real GCP ("It will not be returned by the API").
# Echoing it would make the Google provider plan the pool target as a change on
# every refresh; the re-plan above also proves the shape is stable, and this
# names the field.
check_absent "virtual cluster read omits input-only nodePoolConfig" "$DP_URL" 'nodePoolConfig'

# ── GKE node pools: the two Dataproc reconciled ──────────────────────────────
# Dataproc reconciles the pools into the GKE cluster through the GKE API, so they
# are read here, not on the Dataproc side. The machine type and autoscaler bounds
# are the module's variables; the label is Dataproc's own marker.
GKE_POOLS="$CONTAINER_BASE/projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER/nodePools"
for pool in controller spark; do
  check "$pool node pool adopted the module shape (e2-small, max 3, Dataproc label)" \
    "$GKE_POOLS/$DP_CLUSTER-$pool" \
    '"machineType":"e2-small"' \
    '"maxNodeCount":3' \
    "\"goog-dataproc-cluster-name\":\"$DP_CLUSTER\""
done

# ── Buckets: input / output / staging ────────────────────────────────────────
# The three buckets live in local.storage_project, which defaults to the cluster
# project; the listing is checked against that project, not blindly against
# $PROJECT, so the cross-project shape is exercised when it is set.
for b in "$BUCKET_INPUT" "$BUCKET_OUTPUT" "$BUCKET_STAGING"; do
  check "GCS bucket $b exists" "$ENDPOINT/storage/v1/b/$b" "\"name\":\"$b\""
  # DGD10: the bucket read reports the uniform bucket-level access the module
  # set, which is what lets the re-plan above converge instead of showing a
  # permanent diff. The deprecated bucketPolicyOnly alias mirrors it.
  check "GCS bucket $b reports uniform bucket-level access" "$ENDPOINT/storage/v1/b/$b" \
    '"uniformBucketLevelAccess":{"enabled":true}' \
    '"bucketPolicyOnly":{"enabled":true}'
done
check "bucket listing for project $STORAGE_PROJECT contains all three" \
  "$ENDPOINT/storage/v1/b?project=$STORAGE_PROJECT" \
  "\"name\":\"$BUCKET_INPUT\"" "\"name\":\"$BUCKET_OUTPUT\"" "\"name\":\"$BUCKET_STAGING\""

# ── Service account ──────────────────────────────────────────────────────────
check "job service account exists" \
  "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/$SA_EMAIL" \
  "\"email\":\"$SA_EMAIL\""

# ── Bucket IAM: shape, not a control ─────────────────────────────────────────
# DGD9: bucket IAM on this emulator is metadata only — the binding is stored and
# echoed but never enforced. This asserts the grant the module writes is the one
# real GCP needs; it proves nothing about authorization.
check "input bucket IAM names the job GSA as objectViewer" \
  "$ENDPOINT/storage/v1/b/$BUCKET_INPUT/iam" \
  '"role":"roles/storage.objectViewer"' \
  "serviceAccount:$SA_EMAIL"

# ── Destroy ──────────────────────────────────────────────────────────────────
echo "== destroy"
tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null
DESTROYED=1

# ── Nothing is left ──────────────────────────────────────────────────────────
# Each of these is its own PASS/FAIL: a resource that survives a destroy is the
# failure this gate exists to catch, and "state says empty" alone does not prove
# the emulator let go of it.
echo "== nothing is left"
STATE_LIST="$(tf state list 2>/dev/null || true)"
if [[ -z "${STATE_LIST//[[:space:]]/}" ]]; then
  printf '  [PASS] terraform state is empty\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] terraform state is not empty:\n%s\n' "$STATE_LIST"
  FAIL=$((FAIL + 1))
fi
check_absent "GKE cluster list no longer contains $GKE_CLUSTER" \
  "$CONTAINER_BASE/projects/$PROJECT/locations/-/clusters" \
  "\"name\":\"$GKE_CLUSTER\""
check_absent "Dataproc list no longer contains $DP_CLUSTER" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters" \
  "\"clusterName\":\"$DP_CLUSTER\""
# Deleting the virtual cluster leaves its node pools behind on real Dataproc;
# deleting the GKE cluster is what reclaims them. Both must now be gone.
for pool in controller spark; do
  check_status "$pool node pool is gone (404)" \
    "$GKE_POOLS/$DP_CLUSTER-$pool" 404
done
for b in "$BUCKET_INPUT" "$BUCKET_OUTPUT" "$BUCKET_STAGING"; do
  check_status "GCS bucket $b is gone (404)" "$ENDPOINT/storage/v1/b/$b" 404
done

echo "== summary: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
