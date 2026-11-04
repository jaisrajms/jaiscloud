#!/usr/bin/env bash
#
# e2e-k3d.sh — DGD6, the one gate that runs the whole Dataproc-on-GKE lifecycle.
#
# The other demo legs each answer one question: ../terraform/run.sh proves the
# module creates the infrastructure and the provider can submit a job,
# ../java/run.sh proves the official Java clients can drive that lifecycle, and
# ../helm/render.sh proves the chart renders Terraform's outputs into the app's
# environment. None of them runs the customer story end to end. This gate does:
#
#   Terraform (on the host)  ->  submitter image  ->  Helm  ->  the Java app
#   (in the cluster)         ->  assertions       ->  helm uninstall
#                            ->  terraform destroy ->  nothing is left
#
# Two clients, two network paths, on purpose:
#
#   * Terraform runs on the host. It reaches the emulator through a kubectl
#     port-forward, and the module derives the `container.` host from that
#     endpoint (see ../terraform/provider.tf): the provider strips a custom
#     endpoint's path, so the GKE half needs its own host rather than the bare
#     origin. `container.localhost` resolves to loopback on this box, so a
#     port-forward is enough; the script refuses to run if it does not resolve.
#   * The Java submitter runs inside the cluster and reaches the emulator over
#     the Service DNS name (`jaiscloud-gcp.<ns>.svc.cluster.local`), REST on
#     :8080 for the Storage/STS clients and gRPC on :8081 for Dataproc/GKE.
#     Nothing in the pod depends on the host-side port-forward.
#
# Real Spark, not the mock executor: the emulator runs Dataproc jobs on its
# Kubernetes Spark executor (`JAISCLOUD_SPARK_EXECUTOR_MODE=k8s`) with the same
# `spark-gcs` image the other k3d Dataproc suites use, so the job actually reads
# the input object and writes the word count. The gate asserts the bytes, not
# just the job state. If the deployed emulator was left in `mock` mode by an
# earlier target (test-e2e-scheduler-k8s does exactly that), this script sets it
# back rather than letting the gate pass without Spark.
#
# Honesty: a missing tool or cluster is a SKIP, not a pass and not a failure —
# the `make test-gcp-terraform` precedent. Once every tool is present and an
# assertion runs, a failure is a failure. Nothing is installed here.
#
# Teardown is a trap on every path (success, assertion failure, interrupt):
# helm uninstall -> terraform destroy -> wait for the emulator to reclaim the
# Dataproc namespace. The submitter image tag is removed from the registry when
# the registry supports deletes. The Terraform workspace, plan and state are
# removed only after the destroy left no state, so a failed run keeps what the
# next operator needs to finish the teardown by hand.
#
# Usage:
#   demo/dpgke-lifecycle/scripts/e2e-k3d.sh
#
# Env (all optional):
#   K8S_NAMESPACE              namespace the emulator runs in (default jaiscloud)
#   DPGKE_NAMESPACE            Dataproc workload + Helm release namespace (dpgke-demo)
#   PROJECT                    runner project, cluster + job (dpgke-demo)
#   STORAGE_PROJECT            project that owns the buckets (dpgke-demo-storage)
#   REGION / GKE_LOCATION      Dataproc region / zonal GKE location
#   GKE_CLUSTER / DATAPROC_CLUSTER   fixed demo resource names
#   DPGKE_OUTPUT_PREFIX        Java job's output prefix (wordcount)
#   DPGKE_TF_OUTPUT_PREFIX     Terraform-submitted job's prefix (wordcount-tf)
#   DPGKE_PORT                 host port-forward port for REST (18080)
#   DPGKE_IMAGE_MODE           registry (push; default) or import (k3d image import)
#   GCP_REGISTRY               registry reachable by the k3d node
#                              (default 10.0.100.21:5050)
#   DPGKE_IMAGE_REPO           submitter image repository
#                              (default $GCP_REGISTRY/dpgke-lifecycle-java;
#                               jaiscloud/dpgke-lifecycle-java in import mode)
#   DPGKE_IMAGE_TAG            submitter image tag (default unique per run)
#   DPGKE_K3D_CLUSTER          k3d cluster for import mode (default from the
#                              k3d-<name> kubectl context)
#   DPGKE_GCP_IMAGE            emulator image for _refresh-gcp-image
#   DPGKE_DOCKER_CONTEXT       docker context for build/push (default: active)
#   DPGKE_SPARK_IMAGE          override JAISCLOUD_K8S_SPARK_IMAGE on the deployment
#   DPGKE_JOB_TIMEOUT          seconds to wait for the submitter Job (900)
#   SKIP_GCP_IMAGE_REBUILD=1   reuse the deployed emulator image
#   TF_BIN / HELM / KUBECTL / DOCKER   tool overrides

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT="$(cd "$DEMO_DIR/../.." && pwd)"
TF_DIR="$DEMO_DIR/terraform"
CHART_DIR="$DEMO_DIR/helm"
JAVA_DIR="$DEMO_DIR/java"

TF_BIN="${TF_BIN:-terraform}"
HELM="${HELM:-helm}"
KUBECTL="${KUBECTL:-kubectl}"
DOCKER="${DOCKER:-docker}"

K8S_NAMESPACE="${K8S_NAMESPACE:-jaiscloud}"
DPGKE_NAMESPACE="${DPGKE_NAMESPACE:-dpgke-demo}"
RELEASE="${DPGKE_RELEASE:-dpgke-demo}"
PROJECT="${PROJECT:-dpgke-demo}"
STORAGE_PROJECT="${STORAGE_PROJECT:-${PROJECT}-storage}"
REGION="${REGION:-us-central1}"
GKE_LOCATION="${GKE_LOCATION:-us-central1-a}"
GKE_CLUSTER="${GKE_CLUSTER:-dpgke-gke}"
DATAPROC_CLUSTER="${DATAPROC_CLUSTER:-dpgke-dataproc}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-small}"
INPUT_OBJECT="${DPGKE_INPUT_OBJECT:-input/words.txt}"
CODE_OBJECT="${DPGKE_CODE_OBJECT:-code/wordcount.py}"
OUTPUT_PREFIX="${DPGKE_OUTPUT_PREFIX:-wordcount}"
TF_OUTPUT_PREFIX="${DPGKE_TF_OUTPUT_PREFIX:-wordcount-tf}"
REST_PORT="${DPGKE_PORT:-18080}"
ENDPOINT="http://localhost:${REST_PORT}"
WORKSPACE="${DPGKE_WORKSPACE:-dpgke-k3d}"
JOB_TIMEOUT="${DPGKE_JOB_TIMEOUT:-900}"
SPARK_IMAGE_OVERRIDE="${DPGKE_SPARK_IMAGE:-}"

GCP_REGISTRY="${GCP_REGISTRY:-10.0.100.21:5050}"
# How the node gets the submitter image: `registry` builds and pushes to
# GCP_REGISTRY (works when k3d is on another Docker host); `import` builds and
# `k3d image import`s it into DPGKE_K3D_CLUSTER (a local k3d without a
# registry, and the CI job). Both end up as the same chart override.
IMAGE_MODE="${DPGKE_IMAGE_MODE:-registry}"
if [[ "$IMAGE_MODE" == "import" ]]; then
  DEFAULT_IMAGE_REPO="jaiscloud/dpgke-lifecycle-java"
else
  DEFAULT_IMAGE_REPO="${GCP_REGISTRY}/dpgke-lifecycle-java"
fi
IMAGE_REPO="${DPGKE_IMAGE_REPO:-$DEFAULT_IMAGE_REPO}"
IMAGE_TAG="${DPGKE_IMAGE_TAG:-dgd6-$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo local)-$(date +%s)}"
IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"
EMULATOR_IMAGE="${DPGKE_GCP_IMAGE:-${GCP_REGISTRY}/jaiscloud-gcp:compat}"
SKIP_GCP_IMAGE_REBUILD="${SKIP_GCP_IMAGE_REBUILD:-0}"

export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-dpgke-demo}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"
export TF_IN_AUTOMATION=1

TMP="$(mktemp -d "${TMPDIR:-/tmp}/dpgke-k3d.XXXXXX")"
PLAN="$TMP/dpgke.tfplan"
# Outside $TMP on purpose: the transcript is the evidence for every in-cluster
# assertion, and it is worth keeping after a failed run (the trap removes $TMP).
SUBMITTER_LOG="${DPGKE_SUBMITTER_LOG:-${TMPDIR:-/tmp}/jaiscloud-gcp-dpgke-k3d-submitter.log}"
PORT_FORWARD_LOG="$TMP/port-forward.log"
: >"$SUBMITTER_LOG"

PASS=0
FAIL=0
PF_PID=""
HELM_INSTALLED=0
TF_APPLIED=0
PORT_FORWARD_STARTED=0
IMAGE_PUSHED=""
IMAGE_BUILT=""
JAVA_JOB_ID=""
# Declared before any use: the EXIT trap reads it even when the run fails during
# the emulator stage, and `set -u` must not turn a teardown into a script error.
TF_VARS=()

pass() { printf '  [PASS] %s\n' "$*"; PASS=$((PASS + 1)); }
fail() { printf '  [FAIL] %s\n' "$*"; FAIL=$((FAIL + 1)); }
log()  { printf '== %s\n' "$*"; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# ── Helpers ──────────────────────────────────────────────────────────────────

docker_() {
  if [[ -n "${DPGKE_DOCKER_CONTEXT:-}" ]]; then
    "$DOCKER" --context "$DPGKE_DOCKER_CONTEXT" "$@"
  else
    "$DOCKER" "$@"
  fi
}

# api URL [curl args...] — an authenticated GET against the emulator's REST API.
api() {
  local url="$1"
  shift
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$@" "$url" 2>/dev/null || true
}

http_status() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check NAME URL SUBSTRING... — require every substring in the response body.
check() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(api "$url")"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

# check_absent NAME URL SUBSTRING... — require every substring to be absent.
check_absent() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(api "$url")"
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      fail "$name (unexpected: $want)"
      return 0
    fi
  done
  pass "$name"
}

check_status() {
  local name="$1" url="$2" want="$3" got
  got="$(http_status "$url")"
  if [[ "$got" == "$want" ]]; then
    pass "$name"
  else
    fail "$name (HTTP $got, want $want)"
  fi
}

check_value() {
  local name="$1" got="$2" want="$3"
  if [[ "$got" == "$want" ]]; then
    pass "$name"
  else
    fail "$name (got '$got', want '$want')"
  fi
}

# assert_log LINE — a PASS line the in-cluster app printed. The app is the
# witness for anything that must happen from inside the pod (the gRPC path to
# the Service, the STS exchange, the 403s), so its transcript is the evidence.
assert_log() {
  local line="$1"
  if grep -Fq "[PASS] $line" "$SUBMITTER_LOG"; then
    pass "$line (asserted by the in-cluster submitter)"
  else
    fail "the in-cluster submitter did not print '[PASS] $line'"
  fi
}

# object_url BUCKET NAME [QUERY] — the GCS JSON API path for one object.
object_url() {
  local bucket="$1" name="$2" query="${3:-}"
  printf '%s/storage/v1/b/%s/o/%s%s' "$ENDPOINT" "$bucket" "${name//\//%2F}" "$query"
}

# object_names URL — the object names in a listing, one per line.
object_names() {
  api "$1" | grep -oE '"name":"[^"]*"' | sed -e 's/^"name":"//' -e 's/"$//' || true
}

tf() { "$TF_BIN" -chdir="$TF_DIR" "$@"; }

out() { tf output -raw "$1" 2>/dev/null || true; }

# bucket NAME — one entry of the module's `buckets` output object.
bucket() {
  tf output -json buckets 2>/dev/null | tr -d '\n' |
    sed -nE 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p'
}

# ensure_project PROJECT — create the bucket-owning project the way the
# Terraform gate does (Cloud Resource Manager `POST /v1/projects`, the call
# `gcloud projects create` makes). Real GCP refuses a bucket in a project that
# does not exist; the module cannot create one (google_project reads Cloud
# Billing, which this provider cannot reach), so the gate creates it and asserts
# it is really listed rather than merely synthesized by the emulator.
ensure_project() {
  local p="$1" code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' \
    --data "{\"projectId\":\"$p\",\"name\":\"$p\"}" \
    "$ENDPOINT/v1/projects" 2>/dev/null || true)"
  if [[ "$code" != "200" && "$code" != "409" ]]; then
    die "could not create the bucket-owning project $p (HTTP $code)"
  fi
  if api "$ENDPOINT/v1/projects/$p" | grep -q '"lifecycleState":"DELETE_REQUESTED"'; then
    curl -s -o /dev/null -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
      "$ENDPOINT/v1/projects/$p:undelete" 2>/dev/null || true
  fi
  check "storage project $p exists and is ACTIVE" "$ENDPOINT/v1/projects/$p" \
    '"lifecycleState":"ACTIVE"'
  check "storage project $p is listed in projects.list" "$ENDPOINT/v1/projects" \
    "\"projectId\":\"$p\""
}

# ── Preflight: a missing tool or cluster is a SKIP ───────────────────────────
# Sourcing SDKMAN is the same courtesy ../java/run.sh extends: the harness shell
# is non-interactive and ~/.bashrc returns early for it. Nothing is installed.
if ! command -v java >/dev/null 2>&1 || ! command -v mvn >/dev/null 2>&1; then
  if [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]]; then
    set +u
    # shellcheck disable=SC1091
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    set -u
  fi
fi

missing=()
for tool in "$TF_BIN" "$HELM" "$KUBECTL" java mvn; do
  command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
command -v curl >/dev/null 2>&1 || missing+=("curl")
# Import mode is the only mode that shells out to k3d itself; registry mode
# needs only Docker and a kube context.
if [[ "$IMAGE_MODE" == "import" ]]; then
  command -v k3d >/dev/null 2>&1 || missing+=("k3d (DPGKE_IMAGE_MODE=import)")
fi
if ! docker_ info >/dev/null 2>&1; then
  missing+=("$DOCKER daemon")
fi
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "SKIP: missing ${missing[*]} — the Dataproc-on-GKE k3d demo needs terraform, helm, kubectl, java, mvn and a Docker daemon"
  echo "      (see demo/dpgke-lifecycle/README.md; nothing is installed by this gate)"
  exit 0
fi

if ! "$KUBECTL" get namespace "$K8S_NAMESPACE" >/dev/null 2>&1; then
  echo "SKIP: namespace '$K8S_NAMESPACE' not found — start the k3d cluster and apply deploy/k8s (see deploy/k8s/README)"
  exit 0
fi
if ! "$KUBECTL" -n "$K8S_NAMESPACE" get svc jaiscloud-gcp >/dev/null 2>&1; then
  echo "SKIP: svc/jaiscloud-gcp not found in '$K8S_NAMESPACE' — kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml"
  exit 0
fi
if ! "$KUBECTL" get nodes -o name 2>/dev/null | grep -q '^node/k3d-'; then
  echo "SKIP: the current kubectl context is not a k3d cluster (no node/k3d-* found)"
  exit 0
fi

# ── Teardown ─────────────────────────────────────────────────────────────────
# Every exit path runs this. The order matters: the Helm release must be gone
# before the Dataproc namespace disappears under it, and the Terraform destroy
# must go last because it is what removes the clusters, buckets and namespace.
stop_port_forward() {
  if [[ "$PORT_FORWARD_STARTED" == "1" && -n "$PF_PID" ]]; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    PF_PID=""
    PORT_FORWARD_STARTED=0
  fi
}

wait_namespace_gone() {
  local i
  for i in $(seq 1 30); do
    "$KUBECTL" get namespace "$DPGKE_NAMESPACE" >/dev/null 2>&1 || return 0
    sleep 2
  done
  return 1
}

cleanup_registry_tag() {
  # A tag left behind is harmless but not "nothing left"; drop it when the
  # registry advertises manifest deletion. A registry that refuses is reported,
  # never fatal — the tag is a build cache entry, not a cloud resource.
  local reg repo digest
  reg="${IMAGE_REPO%%/*}"
  repo="${IMAGE_REPO#*/}"
  digest="$(curl -sI -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    "http://$reg/v2/$repo/manifests/$IMAGE_TAG" 2>/dev/null |
    awk 'tolower($1)=="docker-content-digest:"{print $2}' | tr -d '\r')"
  if [[ -n "$digest" ]] && curl -sf -o /dev/null -X DELETE \
    "http://$reg/v2/$repo/manifests/$digest" 2>/dev/null; then
    echo "== submitter image tag $IMAGE_TAG removed from $reg"
  else
    echo "== note: could not remove $IMAGE_TAG from $reg (registry deletion disabled); it is a build tag, not a cluster resource"
  fi
  docker_ rmi "$IMAGE" >/dev/null 2>&1 || true
}

cleanup() {
  local rc=$?
  stop_port_forward

  if [[ "$HELM_INSTALLED" == "1" ]]; then
    echo "== teardown: helm uninstall $RELEASE"
    "$HELM" uninstall "$RELEASE" -n "$DPGKE_NAMESPACE" --wait --timeout 120s >/dev/null 2>&1 || true
  fi

  if [[ "$TF_APPLIED" == "1" ]]; then
    echo "== teardown: terraform destroy"
    tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null 2>&1 || true
  fi

  if wait_namespace_gone; then
    :
  else
    echo "WARN: namespace $DPGKE_NAMESPACE is still present after teardown; inspect with: $KUBECTL get ns $DPGKE_NAMESPACE"
  fi

  # The image is only ours to remove if this run built it.
  if [[ -n "${IMAGE_PUSHED:-}" ]]; then
    cleanup_registry_tag
  elif [[ "$IMAGE_MODE" == "import" && -n "${IMAGE_BUILT:-}" ]]; then
    docker_ rmi "$IMAGE" >/dev/null 2>&1 || true
    echo "== note: $IMAGE removed from the Docker daemon; the k3d node keeps its imported copy (containerd has no cheap tag delete)"
  fi

  # Keep state when the destroy did not empty it: that is what lets the next
  # operator finish the teardown. Drop the workspace and state only when clean.
  if [[ "$TF_APPLIED" == "1" && -z "$(tf state list 2>/dev/null || true)" ]]; then
    tf workspace select default >/dev/null 2>&1 || true
    tf workspace delete "$WORKSPACE" >/dev/null 2>&1 || true
    rm -f "$TF_DIR/terraform.tfstate" "$TF_DIR/terraform.tfstate.backup" "$PLAN"
  fi

  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ── Preflight: no half-finished lifecycle from an earlier run ────────────────
# The demo uses fixed resource names, so a second lifecycle on top of a
# leftover one cannot converge. A trap cleans up on failure; a hard kill
# (SIGKILL, CI timeout) can still leave state. Refuse loudly instead of
# adopting or deleting someone else's clusters.
tf init -input=false -no-color >/dev/null
tf workspace select -or-create "$WORKSPACE" >/dev/null
if [[ -n "$(tf state list 2>/dev/null || true)" ]]; then
  echo "ERROR: Terraform workspace '$WORKSPACE' still holds state from an earlier run:"
  tf state list | sed 's/^/       /'
  echo "       tear it down first: terraform -chdir=$TF_DIR destroy -var ... (see demo/dpgke-lifecycle/terraform/README.md)"
  exit 1
fi
if "$KUBECTL" get namespace "$DPGKE_NAMESPACE" >/dev/null 2>&1; then
  echo "ERROR: namespace '$DPGKE_NAMESPACE' is left over from an earlier run."
  echo "       delete it first (it is the Dataproc workload namespace): $KUBECTL delete ns $DPGKE_NAMESPACE"
  exit 1
fi

# ── Emulator: real Spark, and an image built from this tree ──────────────────
log "emulator: apply deploy/k8s/rbac.yaml (per-cluster namespace create/delete)"
"$KUBECTL" apply -f "$ROOT/deploy/k8s/rbac.yaml" >/dev/null

if [[ "$SKIP_GCP_IMAGE_REBUILD" == "1" ]]; then
  log "emulator: SKIP_GCP_IMAGE_REBUILD=1 — reusing deployed $EMULATOR_IMAGE"
else
  log "emulator: rebuilding $EMULATOR_IMAGE (set SKIP_GCP_IMAGE_REBUILD=1 to reuse the deployed image)"
  if ! make -C "$ROOT" --no-print-directory _refresh-gcp-image \
    K8S_NAMESPACE="$K8S_NAMESPACE" GCP_IMAGE="$EMULATOR_IMAGE" \
    >"$TMP/emulator-refresh.log" 2>&1; then
    tail -n 40 "$TMP/emulator-refresh.log" >&2 || true
    die "rebuilding the emulator image failed"
  fi
fi
"$KUBECTL" -n "$K8S_NAMESPACE" rollout status deployment/jaiscloud-gcp --timeout=180s >/dev/null

# The deployed manifest sets k8s mode, but test-e2e-scheduler-k8s flips this
# deployment to mock and leaves it there. Put it back before the run so a green
# gate never means "the mock executor happened to look like Spark".
current_mode="$("$KUBECTL" -n "$K8S_NAMESPACE" get deploy jaiscloud-gcp \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAISCLOUD_SPARK_EXECUTOR_MODE")].value}' 2>/dev/null || true)"
current_gcs="$("$KUBECTL" -n "$K8S_NAMESPACE" get deploy jaiscloud-gcp \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAISCLOUD_GCS_EMULATOR_ENDPOINT")].value}' 2>/dev/null || true)"
want_gcs="http://jaiscloud-gcp.$K8S_NAMESPACE.svc.cluster.local:8080"
if [[ "$current_mode" != "k8s" || ( -n "$SPARK_IMAGE_OVERRIDE" ) || "$current_gcs" != "$want_gcs" ]]; then
  log "emulator: pinning executor mode=k8s and the in-cluster GCS endpoint"
  env_args=(JAISCLOUD_SPARK_EXECUTOR_MODE=k8s JAISCLOUD_GCS_EMULATOR_ENDPOINT="$want_gcs")
  if [[ -n "$SPARK_IMAGE_OVERRIDE" ]]; then
    env_args+=(JAISCLOUD_K8S_SPARK_IMAGE="$SPARK_IMAGE_OVERRIDE")
  fi
  "$KUBECTL" -n "$K8S_NAMESPACE" set env deployment/jaiscloud-gcp "${env_args[@]}" >/dev/null
  "$KUBECTL" -n "$K8S_NAMESPACE" rollout status deployment/jaiscloud-gcp --timeout=180s >/dev/null
fi
MODE_NOW="$("$KUBECTL" -n "$K8S_NAMESPACE" get deploy jaiscloud-gcp \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAISCLOUD_SPARK_EXECUTOR_MODE")].value}' 2>/dev/null || true)"
SPARK_IMAGE_NOW="$("$KUBECTL" -n "$K8S_NAMESPACE" get deploy jaiscloud-gcp \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAISCLOUD_K8S_SPARK_IMAGE")].value}' 2>/dev/null || true)"
check_value "emulator JAISCLOUD_SPARK_EXECUTOR_MODE" "$MODE_NOW" "k8s"
if [[ -n "$SPARK_IMAGE_NOW" ]]; then
  pass "emulator has a real Spark executor image ($SPARK_IMAGE_NOW)"
else
  fail "emulator has no JAISCLOUD_K8S_SPARK_IMAGE; a k8s Dataproc job would fail at submit"
fi

# ── Host-side REST path: port-forward, and the container.* host ──────────────
log "host path: port-forward svc/jaiscloud-gcp $REST_PORT:8080"
"$KUBECTL" -n "$K8S_NAMESPACE" port-forward svc/jaiscloud-gcp "$REST_PORT:8080" \
  >"$PORT_FORWARD_LOG" 2>&1 &
PF_PID=$!
PORT_FORWARD_STARTED=1
ready=0
for _ in $(seq 1 30); do
  if curl -sf "$ENDPOINT/_jaiscloud/health" >/dev/null 2>&1; then
    ready=1
    break
  fi
  kill -0 "$PF_PID" 2>/dev/null || break
  sleep 1
done
[[ "$ready" == "1" ]] || die "port-forward to svc/jaiscloud-gcp never became ready (see $PORT_FORWARD_LOG)"
pass "host reaches the emulator through the port-forward"

CONTAINER_HOST="container.$(printf '%s' "$ENDPOINT" | sed -E 's#^https?://##; s#[:/].*##')"
if ! getent hosts "$CONTAINER_HOST" >/dev/null 2>&1; then
  die "$CONTAINER_HOST does not resolve — the module derives the GKE host from the endpoint, and the provider keeps only scheme+host. Add '127.0.0.1 $CONTAINER_HOST' to the hosts file, or point the script at a host whose container.* sibling resolves."
fi
pass "the module's derived GKE host $CONTAINER_HOST resolves"

ensure_project "$STORAGE_PROJECT"

# ── Terraform (host) ─────────────────────────────────────────────────────────
TF_VARS=(
  -var="endpoint=$ENDPOINT"
  -var="project=$PROJECT"
  -var="storage_project=$STORAGE_PROJECT"
  -var="region=$REGION"
  -var="gke_location=$GKE_LOCATION"
  -var="gke_cluster_name=$GKE_CLUSTER"
  -var="dataproc_cluster_name=$DATAPROC_CLUSTER"
  -var="namespace=$DPGKE_NAMESPACE"
  -var="machine_type=$MACHINE_TYPE"
  -var="job_input_object=$INPUT_OBJECT"
  -var="job_output_prefix=$TF_OUTPUT_PREFIX"
)

log "terraform: plan and apply (runner $PROJECT, bucket owner $STORAGE_PROJECT)"
TF_APPLIED=1
tf validate -no-color >/dev/null
tf plan -input=false -no-color -out="$PLAN" "${TF_VARS[@]}" >/dev/null
tf apply -input=false -auto-approve -no-color "$PLAN" >/dev/null

GKE_OUT="$(out gke_cluster_name)"
DP_OUT="$(out dataproc_cluster_name)"
REGION_OUT="$(out region)"
GKE_LOC_OUT="$(out gke_location)"
OWNER_OUT="$(out storage_project)"
BUCKET_INPUT="$(bucket input)"
BUCKET_OUTPUT="$(bucket output)"
BUCKET_STAGING="$(bucket staging)"
TF_JOB_ID="$(out job_id)"
TF_JOB_STATE="$(out job_state)"
TF_JOB_URI="$(out job_driver_output_resource_uri)"
SPARK_RELEASE_OUT="$(out spark_release)"

check_value "Terraform created the GKE cluster $GKE_CLUSTER" "$GKE_OUT" "$GKE_CLUSTER"
check "Terraform's virtual cluster is RUNNING" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION_OUT/clusters/$DP_OUT" '"state":"RUNNING"'
check "Terraform-submitted job reached DONE under the real k8s executor" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION_OUT/jobs/$TF_JOB_ID" '"state":"DONE"'
check_value "provider read-back stores the Terraform job state" "$TF_JOB_STATE" "DONE"

if "$KUBECTL" get namespace "$DPGKE_NAMESPACE" >/dev/null 2>&1; then
  pass "the emulator provisioned the workload namespace $DPGKE_NAMESPACE"
else
  fail "namespace $DPGKE_NAMESPACE was not provisioned (per-cluster namespace RBAC missing?)"
fi

# Cross-project shape (DGD8): the buckets belong to the owner, the runner has
# no shadow bucket of its own.
check "bucket listing for $OWNER_OUT contains all three" \
  "$ENDPOINT/storage/v1/b?project=$OWNER_OUT" \
  "\"name\":\"$BUCKET_INPUT\"" "\"name\":\"$BUCKET_OUTPUT\"" "\"name\":\"$BUCKET_STAGING\""
check_absent "runner project $PROJECT lists no bucket (no shadow copy)" \
  "$ENDPOINT/storage/v1/b?project=$PROJECT" '"name":"'

# The Terraform job's own bytes: the k8s executor really ran Spark for the
# provider path, not just for the Java app.
tf_part="$(
  object_names "$ENDPOINT/storage/v1/b/$BUCKET_OUTPUT/o?prefix=$TF_OUTPUT_PREFIX/" |
    grep -E '/part-[0-9]+$' | head -1 || true
)"
if [[ -n "$tf_part" ]]; then
  pass "Terraform job wrote a Spark part file ($tf_part)"
  count="$(api "$(object_url "$BUCKET_OUTPUT" "$tf_part" '?alt=media')" |
    grep -Fxc "$(printf 'the\t4')" || true)"
  check_value "the Terraform job's part file is the real word count" "$count" "1"
else
  fail "Terraform job wrote no part file under gs://$BUCKET_OUTPUT/$TF_OUTPUT_PREFIX/ (mock executor?)"
fi

TF_DRIVER_KEY="${TF_JOB_URI#gs://$BUCKET_STAGING/}"
TF_DRIVER_BYTES="$(api "$(object_url "$BUCKET_STAGING" "$TF_DRIVER_KEY.000000000" '?alt=media')" | wc -c | tr -d ' ')"
if [[ "${TF_DRIVER_BYTES:-0}" -gt 0 ]]; then
  pass "the Terraform job's driver output object is non-empty ($TF_DRIVER_BYTES bytes)"
else
  fail "the Terraform job's driverOutputResourceUri object is missing or empty"
fi

# ── The submitter image ──────────────────────────────────────────────────────
log "submitter: mvn package and docker build $IMAGE"
( cd "$JAVA_DIR" && mvn -q -e -DskipTests package )
docker_ build -q -t "$IMAGE" -f "$JAVA_DIR/Dockerfile" "$JAVA_DIR" >/dev/null
IMAGE_BUILT=1
if [[ "$IMAGE_MODE" == "import" ]]; then
  command -v k3d >/dev/null 2>&1 || die "DPGKE_IMAGE_MODE=import needs k3d on PATH (or use the default registry mode)"
  K3D_CLUSTER="${DPGKE_K3D_CLUSTER:-$("$KUBECTL" config current-context 2>/dev/null | sed -nE 's/^k3d-(.+)$/\1/p')}"
  [[ -n "$K3D_CLUSTER" ]] || die "DPGKE_IMAGE_MODE=import needs DPGKE_K3D_CLUSTER (the current context is not k3d-<name>)"
  k3d image import -k "$K3D_CLUSTER" "$IMAGE" >/dev/null
  pass "submitter image built and imported into k3d cluster $K3D_CLUSTER"
else
  docker_ push -q "$IMAGE" >/dev/null
  IMAGE_PUSHED=1
  pass "submitter image built and pushed to $GCP_REGISTRY"
fi

# ── Helm (the submitter runs in the cluster) ─────────────────────────────────
# The endpoints are the in-cluster Service, never the host port-forward; the
# image is the exact tag this run pushed, with IfNotPresent so the node uses
# what we just pushed instead of pulling a same-named image.
log "helm: install $RELEASE into $DPGKE_NAMESPACE"
HELM_INSTALLED=1
"$HELM" install "$RELEASE" "$CHART_DIR" \
  --namespace "$DPGKE_NAMESPACE" \
  --set-string project="$PROJECT" \
  --set-string region="$REGION_OUT" \
  --set-string storageProject="$OWNER_OUT" \
  --set-string gkeLocation="$GKE_LOC_OUT" \
  --set-string gkeCluster="$GKE_OUT" \
  --set-string dataprocCluster="$DP_OUT" \
  --set-string namespace="$DPGKE_NAMESPACE" \
  --set-string buckets.input="$BUCKET_INPUT" \
  --set-string buckets.output="$BUCKET_OUTPUT" \
  --set-string buckets.staging="$BUCKET_STAGING" \
  --set-string sparkRelease="$SPARK_RELEASE_OUT" \
  --set-string machineType="$MACHINE_TYPE" \
  --set-string endpoints.rest="http://jaiscloud-gcp.$K8S_NAMESPACE.svc.cluster.local:8080" \
  --set-string endpoints.grpc="jaiscloud-gcp.$K8S_NAMESPACE.svc.cluster.local" \
  --set endpoints.grpcPort=8081 \
  --set-string image.repository="$IMAGE_REPO" \
  --set-string image.tag="$IMAGE_TAG" \
  --set image.pullPolicy=IfNotPresent \
  --set-string credentials.accessToken="$GOOGLE_OAUTH_ACCESS_TOKEN" \
  --set job.teardown=false \
  --set-string job.codeObject="$CODE_OBJECT" \
  --set-string job.inputObject="$INPUT_OBJECT" \
  --set-string job.outputPrefix="$OUTPUT_PREFIX" \
  --set-string 'job.extraEnv[0].name=DGD3_POLL_ATTEMPTS' \
  --set-string 'job.extraEnv[0].value=150' >/dev/null

SUBMITTER_JOB="$("$KUBECTL" -n "$DPGKE_NAMESPACE" get job \
  -l "app.kubernetes.io/instance=$RELEASE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
[[ -n "$SUBMITTER_JOB" ]] || die "helm installed no Job labelled app.kubernetes.io/instance=$RELEASE"
CONFIGMAP="$("$KUBECTL" -n "$DPGKE_NAMESPACE" get configmap \
  -l "app.kubernetes.io/instance=$RELEASE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
SECRET_NAME="$("$KUBECTL" -n "$DPGKE_NAMESPACE" get secret \
  -l "app.kubernetes.io/instance=$RELEASE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
pass "helm installed the in-cluster submitter Job $SUBMITTER_JOB"

# ── The submitter runs (and the Job is the thing being timed) ───────────────
# kubectl wait reports a failed Job as an error; the log is captured either way
# so the assertions below can explain a red run instead of just reddening it.
log "submitter: waiting for Job $SUBMITTER_JOB (timeout ${JOB_TIMEOUT}s)"
JOB_WAIT_RC=0
"$KUBECTL" -n "$DPGKE_NAMESPACE" wait --for=condition=complete "job/$SUBMITTER_JOB" \
  --timeout="${JOB_TIMEOUT}s" >/dev/null 2>&1 || JOB_WAIT_RC=$?
"$KUBECTL" -n "$DPGKE_NAMESPACE" logs "job/$SUBMITTER_JOB" >"$SUBMITTER_LOG" 2>&1 || true
if [[ "$JOB_WAIT_RC" == "0" ]]; then
  pass "the in-cluster submitter Job completed"
else
  fail "the in-cluster submitter Job did not complete (kubectl wait exit $JOB_WAIT_RC)"
  echo "---- submitter log (tail) ----"
  tail -n 40 "$SUBMITTER_LOG" || true
  echo "-----------------------------"
fi

# The pod log is the evidence for everything that must happen in the cluster.
grep -q "dataproc+gke transport=gRPC plaintext jaiscloud-gcp.$K8S_NAMESPACE.svc.cluster.local:8081" \
  "$SUBMITTER_LOG" &&
  pass "the submitter used the in-cluster gRPC Service DNS (not the host port-forward)" ||
  fail "the submitter did not report the in-cluster gRPC target"
grep -q "storage transport=REST http://jaiscloud-gcp.$K8S_NAMESPACE.svc.cluster.local:8080" \
  "$SUBMITTER_LOG" &&
  pass "the submitter used the in-cluster REST Service DNS (not the host port-forward)" ||
  fail "the submitter did not report the in-cluster REST target"

JAVA_JOB_ID="$(sed -nE 's/.*submitted job ([^: ]+):.*/\1/p' "$SUBMITTER_LOG" | tail -1)"
if [[ -n "$JAVA_JOB_ID" ]]; then
  pass "the submitter submitted Dataproc job $JAVA_JOB_ID"
else
  fail "the submitter log names no submitted job"
fi

assert_log "job $JAVA_JOB_ID reached DONE"

# The denial case, in the in-cluster run. The app prints the witness (plain
# token, same object, 200) next to the 403s so a denial cannot be a typo.
assert_log "output read with the plain token is 200"
assert_log "output read under the output boundary is 403"
assert_log "output read under the input boundary is 403"
assert_log "output write under the input boundary is 403"
assert_log "input write under the output boundary is 403"
assert_log "the downscope probe is cleared so the job's output prefix starts empty"

# ── The Java job's output, from GCS ──────────────────────────────────────────
if [[ -n "$JAVA_JOB_ID" ]]; then
  JAVA_JOB="$ENDPOINT/v1/projects/$PROJECT/regions/$REGION_OUT/jobs/$JAVA_JOB_ID"
  check "the Java job is DONE" "$JAVA_JOB" '"state":"DONE"'

  JAVA_DRIVER_URI="$(api "$JAVA_JOB" | sed -nE 's/.*"driverOutputResourceUri":"([^"]+)".*/\1/p')"
  if [[ "$JAVA_DRIVER_URI" == gs://"$BUCKET_STAGING"/* ]]; then
    pass "the Java job's driverOutputResourceUri is in the staging bucket"
  else
    fail "driverOutputResourceUri '$JAVA_DRIVER_URI' is not under gs://$BUCKET_STAGING/"
  fi
  JAVA_DRIVER_KEY="${JAVA_DRIVER_URI#gs://$BUCKET_STAGING/}"
  JAVA_DRIVER_BYTES="$(api "$(object_url "$BUCKET_STAGING" "$JAVA_DRIVER_KEY.000000000" '?alt=media')" | wc -c | tr -d ' ')"
  if [[ "${JAVA_DRIVER_BYTES:-0}" -gt 0 ]]; then
    pass "the driverOutputResourceUri object exists and is non-empty ($JAVA_DRIVER_BYTES bytes)"
  else
    fail "the driverOutputResourceUri object is missing or empty"
  fi

  java_part="$(
    object_names "$ENDPOINT/storage/v1/b/$BUCKET_OUTPUT/o?prefix=$OUTPUT_PREFIX/" |
      grep -E '/part-[0-9]+$' | head -1 || true
  )"
  if [[ -n "$java_part" ]]; then
    pass "the Java job wrote a Spark part file ($java_part)"
    part_body="$(api "$(object_url "$BUCKET_OUTPUT" "$java_part" '?alt=media')")"
    expected=($'dataproc\t2' $'gke\t2' $'on\t2' $'the\t4' $'count\t1')
    for line in "${expected[@]}"; do
      if printf '%s\n' "$part_body" | grep -Fxq "$line"; then
        pass "the part file contains '$(printf '%s' "$line" | tr '\t' ' ')'"
      else
        fail "the part file is missing the word-count line '$(printf '%s' "$line" | tr '\t' ' ')'"
      fi
    done
    check "the output prefix has Spark's _SUCCESS marker" \
      "$ENDPOINT/storage/v1/b/$BUCKET_OUTPUT/o?prefix=$OUTPUT_PREFIX/" '_SUCCESS'
  else
    fail "the Java job wrote no part file under gs://$BUCKET_OUTPUT/$OUTPUT_PREFIX/ (mock executor?)"
  fi
fi

# ── helm uninstall removes the release objects ──────────────────────────────
log "helm: uninstall $RELEASE"
"$HELM" uninstall "$RELEASE" -n "$DPGKE_NAMESPACE" --wait --timeout 120s >/dev/null
HELM_INSTALLED=0
[[ -z "$("$KUBECTL" -n "$DPGKE_NAMESPACE" get job "$SUBMITTER_JOB" --ignore-not-found -o name 2>/dev/null)" ]] &&
  pass "helm uninstall removed the Job $SUBMITTER_JOB" ||
  fail "Job $SUBMITTER_JOB survived helm uninstall"
[[ -z "$("$KUBECTL" -n "$DPGKE_NAMESPACE" get configmap "$CONFIGMAP" --ignore-not-found -o name 2>/dev/null)" ]] &&
  pass "helm uninstall removed the ConfigMap $CONFIGMAP" ||
  fail "ConfigMap $CONFIGMAP survived helm uninstall"
[[ -z "$("$KUBECTL" -n "$DPGKE_NAMESPACE" get secret "$SECRET_NAME" --ignore-not-found -o name 2>/dev/null)" ]] &&
  pass "helm uninstall removed the Secret $SECRET_NAME" ||
  fail "Secret $SECRET_NAME survived helm uninstall"
[[ -z "$("$KUBECTL" -n "$DPGKE_NAMESPACE" get pods -l "app.kubernetes.io/instance=$RELEASE" --no-headers 2>/dev/null)" ]] &&
  pass "helm uninstall left no submitter pods" ||
  fail "submitter pods survived helm uninstall"

# ── terraform destroy, and nothing is left ──────────────────────────────────
log "terraform: destroy"
tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null
TF_APPLIED=0

log "nothing is left"
check_value "terraform state is empty" "$(tf state list 2>/dev/null | wc -l | tr -d ' ')" "0"
check_absent "GKE cluster list no longer contains $GKE_OUT" \
  "http://$CONTAINER_HOST:$REST_PORT/container/v1/projects/$PROJECT/locations/-/clusters" \
  "\"name\":\"$GKE_OUT\""
check_absent "Dataproc cluster list no longer contains $DP_OUT" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION_OUT/clusters" "\"clusterName\":\"$DP_OUT\""
check_status "the Terraform job is gone (404)" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION_OUT/jobs/$TF_JOB_ID" 404
# The Java job's record is NOT asserted gone: real Dataproc keeps job history as
# a regional resource after its cluster is deleted (a job submitted by a
# customer is still listable for audit), and the emulator models that. The
# resource teardown the demo owns — cluster, buckets, Kubernetes namespace and
# its pods — is what the assertions above and below cover. The Terraform job is
# 404 because its own google_dataproc_job resource was destroyed, not because
# deleting the cluster swept it.

# "Both projects" means both listings, and the runner listing is checked as a
# whole: a shadow bucket the job's staging path created in the runner would
# otherwise survive behind the three-name check.
check_absent "runner project $PROJECT has no buckets after the destroy" \
  "$ENDPOINT/storage/v1/b?project=$PROJECT" '"name":"'
check_absent "bucket owner $OWNER_OUT has no buckets after the destroy" \
  "$ENDPOINT/storage/v1/b?project=$OWNER_OUT" '"name":"'

if wait_namespace_gone; then
  pass "the Dataproc namespace $DPGKE_NAMESPACE is gone"
else
  fail "namespace $DPGKE_NAMESPACE survived the destroy"
fi
if "$KUBECTL" get pods -A --no-headers 2>/dev/null | grep -q " $DPGKE_NAMESPACE "; then
  fail "pods remain in $DPGKE_NAMESPACE after the destroy"
else
  pass "no pods remain in $DPGKE_NAMESPACE"
fi
if "$KUBECTL" get pvc -A --no-headers 2>/dev/null | grep -q " $DPGKE_NAMESPACE "; then
  fail "PVCs remain in $DPGKE_NAMESPACE after the destroy"
else
  pass "no PVCs remain in $DPGKE_NAMESPACE"
fi
if "$KUBECTL" get namespaces --no-headers 2>/dev/null | awk '{print $1}' | grep -qx "$DPGKE_NAMESPACE"; then
  fail "namespace $DPGKE_NAMESPACE is still listed"
else
  pass "namespace $DPGKE_NAMESPACE is not listed"
fi

echo
echo "== summary: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
