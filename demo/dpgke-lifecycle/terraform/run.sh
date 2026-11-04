#!/usr/bin/env bash
#
# run.sh — the gate for the Dataproc-on-GKE lifecycle module.
#
# Drives this module against a running jaiscloud-gcp and asserts the result
# through the emulator's REST API: init -> validate -> plan -> apply -> spot
# checks (including the submitted PySpark job) -> a second plan (permadiff) ->
# a targeted destroy that proves the virtual cluster goes before the GKE
# cluster -> full destroy -> "nothing is left".
#
# Deliberately Terraform-only. The demo's other legs are a Java job and a Helm
# installer; both arrive in later sessions and neither is exercised here. This
# gate answers one question: does the module the demo ships still create the
# infrastructure and submit the job the later legs read from, cleanly and
# idempotently?
#
# The job check is honest about the backend: the emulator's default Dataproc
# executor is mock, so the job reaches DONE and carries a driverOutputResourceUri
# without a Spark driver running. This gate asserts the provider's read-back and
# explicitly asserts that no Spark output bytes exist. Real execution belongs to
# DGD6's k3d gate.
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

# check_value NAME GOT WANT — compare two strings. The provider's read-back is
# read from terraform outputs (state), not from a REST body, so it needs a check
# that is not a URL GET.
check_value() {
  local name="$1" got="$2" want="$3"
  if [[ "$got" == "$want" ]]; then
    printf '  [PASS] %s\n' "$name"
    PASS=$((PASS + 1))
  else
    printf '  [FAIL] %s (got %q, want %q)\n' "$name" "$got" "$want"
    FAIL=$((FAIL + 1))
  fi
}

# check_prefix NAME GOT PREFIX — require GOT to start with PREFIX and be non-empty.
check_prefix() {
  local name="$1" got="$2" prefix="$3"
  if [[ -n "$got" && "$got" == "$prefix"* ]]; then
    printf '  [PASS] %s\n' "$name"
    PASS=$((PASS + 1))
  else
    printf '  [FAIL] %s (got %q, want prefix %q)\n' "$name" "$got" "$prefix"
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

# object_url BUCKET KEY — the GCS JSON API path for one object. An object name is
# a path segment there, so its slashes are percent-encoded; a raw key would hit
# the wrong route.
object_url() {
  printf '%s/storage/v1/b/%s/o/%s' "$ENDPOINT" "$1" "${2//\//%2F}"
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
JOB_ID="$(out job_id)"
JOB_STATE="$(out job_state)"
JOB_CLUSTER="$(out job_placement_cluster_name)"
JOB_CLUSTER_UUID="$(out job_placement_cluster_uuid)"
JOB_DRIVER_URI="$(out job_driver_output_resource_uri)"
JOB_MAIN_URI="$(out job_main_python_file_uri)"
JOB_INPUT_URI="$(out job_input_uri)"
JOB_OUTPUT_URI="$(out job_output_uri)"

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

# ── The job: google_dataproc_job's create/read path ──────────────────────────
# The job is the resource this gate exists for: nothing else in the repo submits
# through the provider. The emulator's default executor is mock, so DONE and the
# driverOutputResourceUri below come from the state machine, not from a Spark
# driver — the final check in this block asserts that no Spark output was
# written, rather than implying it was. Real execution is DGD6's k3d gate.
JOB_URL="$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/jobs/$JOB_ID"

check "Dataproc job $JOB_ID reached DONE" \
  "$JOB_URL" '"state":"DONE"' '"done":true'
check_value "provider read-back stores the job state DONE" "$JOB_STATE" "DONE"

# placement.clusterName is the virtual cluster, not a resolved GKE target: the
# provider sends the name the module gave it, and the read echoes it.
check "job placement names the virtual cluster" \
  "$JOB_URL" "\"clusterName\":\"$DP_CLUSTER\""
check_value "provider read-back placement is the virtual cluster" \
  "$JOB_CLUSTER" "$DP_CLUSTER"

# placement.clusterUuid is output-only and captured at submit. Assert it matches
# the virtual cluster's own uuid: a job submitted against a different cluster
# would still read DONE but carry the wrong uuid.
DP_UUID="$(curl -s -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$DP_URL" 2>/dev/null |
  sed -nE 's/.*"clusterUuid":"([^"]+)".*/\1/p')"
check_prefix "virtual cluster read has a clusterUuid" "$DP_UUID" ""
check_value "job placement captured the virtual cluster uuid" \
  "$JOB_CLUSTER_UUID" "$DP_UUID"

# The driver output URI is allocated in the virtual cluster's staging bucket at
# submit, so it proves the job inherited the module's stagingBucket. The object
# itself is the emulator's synthetic terminal log (mock mode), not Spark's
# stdout — the name says "driveroutput", the bytes do not.
check_prefix "provider read-back driver output URI is in the staging bucket" \
  "$JOB_DRIVER_URI" "gs://$BUCKET_STAGING/"
check "wire driver output URI matches the provider read-back" \
  "$JOB_URL" "\"driverOutputResourceUri\":\"$JOB_DRIVER_URI\""
check "driver output URI is scoped to job $JOB_ID" \
  "$JOB_URL" "/jobs/$JOB_ID/driveroutput"

# The job runs the object Terraform uploaded, and its two args are the seeded
# input object and the output bucket prefix — the read/write contract the demo
# narrates. Assert both the provider read-back and the wire body.
check_value "job runs the uploaded PySpark object" \
  "$JOB_MAIN_URI" "gs://$BUCKET_INPUT/code/wordcount.py"
check "PySpark file object exists in the input bucket" \
  "$(object_url "$BUCKET_INPUT" code/wordcount.py)" \
  '"name":"code/wordcount.py"'
check "job args name the seeded input and the output prefix" \
  "$JOB_URL" "\"$JOB_INPUT_URI\"" "\"$JOB_OUTPUT_URI\""

# The URI is a prefix; the emulator materializes <uri>.000000000 when the mock
# job reaches a terminal state. Assert the advertised location resolves.
DRIVER_KEY="${JOB_DRIVER_URI#gs://$BUCKET_STAGING/}"
check "driver output object exists in the staging bucket" \
  "$(object_url "$BUCKET_STAGING" "$DRIVER_KEY.000000000")" \
  "\"name\":\"$DRIVER_KEY.000000000\""

# Honesty check: mock mode never starts the driver, so the output bucket must be
# empty. If this ever changes, the gate should be updated deliberately (DGD6
# asserts real output), not silently start claiming Spark ran here.
check_absent "mock executor wrote no Spark output (output bucket is empty)" \
  "$ENDPOINT/storage/v1/b/$BUCKET_OUTPUT/o" '"name":"'

# ── Destroy order: job -> virtual cluster -> GKE cluster ─────────────────────
# The point of DGD2's ordering case. Deleting the virtual cluster leaves its two
# reconciled node pools behind — real Dataproc does not own them, and the
# emulator only removes the pools with the GKE cluster that stores them. Target
# the virtual cluster first, prove the pools and the GKE cluster survive, then
# finish the destroy and prove the pools are gone. If a targeted destroy took
# the GKE cluster with it, the pool checks below would 404 and the summary would
# fail; nothing here waits on a resource that might never appear.
echo "== destroy order: virtual cluster before the GKE cluster"
TARGET_RC=0
tf destroy -input=false -auto-approve -no-color \
  -target=google_dataproc_cluster.gke "${TF_VARS[@]}" >/dev/null 2>&1 || TARGET_RC=$?
check_value "targeted destroy of the virtual cluster succeeded" "$TARGET_RC" "0"

check_status "virtual cluster is gone after the targeted destroy (404)" "$DP_URL" 404
check_status "job is gone after the targeted destroy (404)" "$JOB_URL" 404
check "targeted destroy left the GKE cluster in place" \
  "$CONTAINER_BASE/projects/$PROJECT/locations/$GKE_LOCATION/clusters/$GKE_CLUSTER" \
  "\"name\":\"$GKE_CLUSTER\""
for pool in controller spark; do
  check "targeted destroy left the $pool node pool in place" \
    "$GKE_POOLS/$DP_CLUSTER-$pool" "\"name\":\"$DP_CLUSTER-$pool\""
done

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
check_status "Dataproc job $JOB_ID is gone (404)" "$JOB_URL" 404
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
