#!/usr/bin/env bash
# deploy.sh — one-click build + deploy of the JaisCloud substrates to local Kubernetes
#
# Topology (see deploy/k8s/namespace-*.yaml; docs in README-GCP.md and
# DEVELOPER_GUIDE.md, Setup 3):
#   jaiscloud        AWS substrate — Deployment jaiscloud, PVC jaiscloud-blobs-pvc
#                    (deploy/k8s/namespace-aws.yaml)
#   jaiscloud-gcp    GCP substrate — Deployment jaiscloud-gcp, PVC jaiscloud-gcp-blobs-pvc
#                    (deploy/k8s/namespace-gcp.yaml)
#   jaiscloud-data   PostgreSQL — Deployment postgres, PVC postgres-pvc
#                    databases: jaiscloud (AWS), jaiscloud_gcp (GCP)
#                    (deploy/k8s/namespace-data.yaml)
#
# Each install applies only its own substrate's namespace and RBAC: an AWS-only
# install never creates a GCP namespace, ServiceAccount or binding, and vice
# versa. The store (namespace-data.yaml + postgres.yaml) and the namespace
# admission policy are the only shared objects.
#
# Requirements:
#   - Docker Desktop with Kubernetes enabled (or any cluster `kubectl` points at)
#   - kubectl pointing at that cluster
#   - Kubernetes >= 1.30: the two substrate ServiceAccounts hold cluster-wide
#     namespace delete, so namespace isolation is enforced by the
#     ValidatingAdmissionPolicy in deploy/k8s/rbac-admission-policy.yaml (VAP is
#     GA in 1.30). deploy.sh refuses an older cluster before creating anything.
#
# Usage:
#   ./deploy/deploy.sh              # build image + apply the AWS substrate manifests
#   ./deploy/deploy.sh --gcp        # apply the GCP substrate manifests (no image build)
#   ./deploy/deploy.sh --migrate-store
#                                   # one-time: move the pre-split store from
#                                   # ns jaiscloud into jaiscloud-data, and the
#                                   # old GCP object bytes into jaiscloud-gcp
#   ./deploy/deploy.sh --delete [aws|gcp|both]
#                                   # remove workloads only (default aws); keep every PVC and
#                                   # PostgreSQL database. `both` does each substrate.
#   ./deploy/deploy.sh --park <aws|gcp>
#                                   # deliberate, compute-destructive, data-retentive: the
#                                   # emulator cancels jobs and stops broker/revision
#                                   # workloads, then the Deployment scales to 0; the
#                                   # database, PVCs, state PVC (instance ID), namespaces,
#                                   # CA and resource records survive
#   ./deploy/deploy.sh --resume <aws|gcp>
#                                   # scale back to 1, recreate the engine runtimes, and warn
#                                   # when the binary digest changed while parked
#   ./deploy/deploy.sh --switch <aws|gcp>
#                                   # park the other substrate (when installed), then resume
#                                   # this one; `--switch both` is invalid
#   ./deploy/deploy.sh --reset      # AWS-only, database-scoped: drop the `jaiscloud` db +
#                                   # the AWS namespace/PVC; GCP (`jaiscloud_gcp`, its PVC)
#                                   # and Postgres survive
#   ./deploy/deploy.sh --gcp-reset  # GCP-only, database-scoped: drop the `jaiscloud_gcp`
#                                   # db + the GCP blobs PVC; AWS and Postgres survive
#   ./deploy/deploy.sh --check      # READ-ONLY inspection: cluster state, legacy
#                                   # detection, store, substrates, mesh, next steps
#   ./deploy/deploy.sh --check --json # same as machine-readable JSON (--strict: exit 1
#                                   # when the intended shape does not hold)
#   ./deploy/deploy.sh --check --substrate aws
#                                   # intent-aware check: treat AWS as the intended
#                                   # install (--desired overrides the states)
#   ./deploy/deploy.sh --plan       # print the commands a mode would run, then exit
#   ./deploy/deploy.sh --skip-build # do not run `docker build`; use the image already
#                                   # imported into the cluster (SKIP_IMAGE_BUILD=1)
#   ./deploy/deploy.sh --yes        # non-interactive (ASSUME_YES=1)
#   ./deploy/deploy.sh --help       # this text
#
# Install record:
#   Every install/delete/reset path merges ConfigMap jaiscloud-install in
#   jaiscloud-data (schema, installed, desired). It lives with the store, so a
#   substrate reset never loses the record of what was installed and what the
#   operator wants running; the preflight uses it to scope its advice.
#
# Every prompt can be skipped non-interactively with --yes / ASSUME_YES=1.
# The image build can be skipped with --skip-build / SKIP_IMAGE_BUILD=1, for a
# cluster whose images were imported (e.g. `k3d image import`, as CI does).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
K8S_DIR="$REPO_ROOT/deploy/k8s"
IMAGE="jaiscloud:latest"
# The tag deploy/k8s/jaiscloud.yaml actually runs (imagePullPolicy: IfNotPresent),
# so a pre-imported image must carry it when the build is skipped.
AWS_MANIFEST_IMAGE="jaisraj/jaiscloud-aws:latest"

# One namespace per concern, so deleting a substrate never destroys the store.
NAMESPACE_AWS="jaiscloud"
NAMESPACE_GCP="jaiscloud-gcp"
NAMESPACE_DATA="jaiscloud-data"

# PVCs owned by the AWS substrate. `--delete` keeps these (and everything in the
# other two namespaces); `--reset` removes them with the AWS namespace.
AWS_PVCS=("jaiscloud-blobs-pvc")
GCP_PVC="jaiscloud-gcp-blobs-pvc"
# The name comes from deploy/k8s/postgres.yaml (Service + Deployment).
POSTGRES_DEPLOYMENT="deployment/postgres"
POSTGRES_PVC="postgres-pvc"

# Database names, one per cloud (schema isolation means one db = one instance).
AWS_DB="jaiscloud"
GCP_DB="jaiscloud_gcp"

ASSUME_YES="${ASSUME_YES:-0}"
SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-0}"

# ── helpers ──────────────────────────────────────────────────────────────────

info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[1;32m[OK]\033[0m    %s\n' "$*"; }
warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
die()   { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
deploy.sh — build and deploy the JaisCloud substrates (AWS + GCP) to local Kubernetes

Topology (three namespaces, one Postgres server):
  jaiscloud       AWS substrate   Deployment jaiscloud, PVC jaiscloud-blobs-pvc
  jaiscloud-gcp   GCP substrate   Deployment jaiscloud-gcp, PVC jaiscloud-gcp-blobs-pvc
  jaiscloud-data  PostgreSQL      Deployment postgres, PVC postgres-pvc
                                  databases: jaiscloud (AWS), jaiscloud_gcp (GCP)

Requires Kubernetes >= 1.30: both substrate ServiceAccounts hold cluster-wide
namespace delete, so the namespace boundary between them is enforced by the
ValidatingAdmissionPolicy in deploy/k8s/rbac-admission-policy.yaml. deploy.sh
applies that policy (with the namespace-*/rbac-* manifests) on every install and
reset path.

Usage:
  ./deploy/deploy.sh              Build the image and apply the AWS substrate
                                  (namespace-data.yaml, postgres.yaml,
                                  namespace-aws.yaml, rbac-shared.yaml, rbac-aws.yaml,
                                  jaiscloud.yaml)
  ./deploy/deploy.sh --gcp        Apply the GCP substrate (namespace-data.yaml,
                                  postgres.yaml, namespace-gcp.yaml, rbac-shared.yaml,
                                  rbac-gcp.yaml, jaiscloud-gcp.yaml), creating the
                                  `jaiscloud_gcp` database BEFORE the pod starts,
                                  and wait for its rollout
  ./deploy/deploy.sh --migrate-store
                                  ONE-TIME upgrade from the pre-split layout: dump
                                  database `jaiscloud` from ns jaiscloud/deployment/
                                  postgres and restore it into the new store in
                                  jaiscloud-data (both databases, so pre-split GCP
                                  state travels). If the old GCP emulator ran in
                                  the jaiscloud namespace, it is stopped, its
                                  object bytes are copied to
                                  jaiscloud-gcp/jaiscloud-gcp-blobs-pvc and the old
                                  workload is removed (SKIP_BLOB_MIGRATE=1 copies
                                  by hand; BLOB_MIGRATE_IMAGE overrides busybox).
                                  Keeps both old PVCs as rollback copies, and
                                  refuses when the new postgres-pvc already exists.
  ./deploy/deploy.sh --delete [aws|gcp|both]
                                  Remove workloads only (default aws): the Deployment,
                                  Service and DSN Secret (AWS keeps its ConfigMap); every
                                  PVC and database is kept, as is the other substrate.
  ./deploy/deploy.sh --park <aws|gcp>
                                  Deliberate, compute-destructive, data-retentive park:
                                  call POST /_jaiscloud/park (cancel jobs, stop Managed
                                  Kafka brokers and Cloud Run revisions), scale the
                                  substrate Deployment to 0, delete executor Pods/Jobs
                                  (and the ECR proxy) in the substrate and its labelled
                                  `gcp-*` namespaces, and annotate the namespace parked.
                                  Keeps: database, blobs/state PVCs (instance ID), the
                                  namespaces, the CA and every resource record.
  ./deploy/deploy.sh --resume <aws|gcp>
                                  Scale the substrate back to 1, wait for its rollout,
                                  POST /_jaiscloud/resume (recreate brokers/revisions),
                                  and warn if the running image digest changed while
                                  parked (a store migration may run).
  ./deploy/deploy.sh --switch <aws|gcp>
                                  Park the other substrate (when installed), then resume
                                  this one. There is no `--switch both`.
  ./deploy/deploy.sh --reset      AWS-only, database-scoped reset: drop/recreate the
                                  `jaiscloud` database, delete the jaiscloud namespace (its
                                  blobs and state PVCs), then re-apply the AWS manifests
                                  (namespace-aws.yaml, rbac-shared.yaml, rbac-aws.yaml,
                                  jaiscloud.yaml). jaiscloud-data, postgres-pvc,
                                  jaiscloud-gcp and `jaiscloud_gcp` survive.
  ./deploy/deploy.sh --gcp-reset  GCP-only, database-scoped reset: scale jaiscloud-gcp to 0,
                                  delete jaiscloud-gcp-blobs-pvc, drop/recreate
                                  `jaiscloud_gcp`, then re-apply namespace-gcp.yaml,
                                  rbac-shared.yaml, rbac-gcp.yaml and jaiscloud-gcp.yaml.
                                  The AWS substrate and jaiscloud-data are never touched.
  --check                         READ-ONLY: inspect the cluster (namespaces, legacy
                                  layout, store, substrates, mesh, install intent) and
                                  print the next safe step. Alias for
                                  scripts/integration-preflight.sh.
  --json                          With --check: machine-readable JSON.
  --strict                        With --check: exit 1 unless the intended shape holds
                                  (no non-optional recommendation, no unexpected
                                  footprint from a cloud outside the intent).
  --substrate aws|gcp|both        With --check: override the recorded intent.
  --desired aws=running,gcp=parked
                                  With --check: override the desired states
                                  (running|parked|absent).
  --plan                          Print the kubectl/docker/psql commands this mode would
                                  run, then exit 0 without executing them.
  --skip-build                    Do not run `docker build`; deploy the image already
                                  imported into the cluster (SKIP_IMAGE_BUILD=1). Useful
                                  with `k3d image import` and other CI paths.
  -y, --yes                       Non-interactive: answer yes to every prompt
                                  (same as ASSUME_YES=1)
  -h, --help                      Show this help and exit 0

Environment:
  ASSUME_YES=1                    Non-interactive, same as --yes
  SKIP_IMAGE_BUILD=1              Skip `docker build`, same as --skip-build
  PARK_FORCE=1                    With --park/--resume: warn and continue when the
                                  emulator's /_jaiscloud hook returns a non-2xx
                                  (the scale/teardown still runs; the endpoint is
                                  reached through a free port in 18080-18089)

Reset semantics (both are database-scoped, never store-wide):
  --reset      drops only the AWS database `jaiscloud` (and the AWS blobs PVC)
  --gcp-reset  drops only the GCP database `jaiscloud_gcp` (and the GCP blobs PVC)
  Neither deletes the jaiscloud-data namespace or postgres-pvc, so the other
  substrate's state always survives.
EOF
}

require() {
  command -v "$1" &>/dev/null || die "'$1' not found — please install it first"
}

# confirm <prompt> — honours --yes / ASSUME_YES=1, otherwise asks (default: no).
confirm() {
  local prompt=$1
  if [[ "$ASSUME_YES" == "1" ]]; then
    info "$prompt — assuming yes (--yes / ASSUME_YES=1)"
    return 0
  fi
  local yn
  if ! read -rp "$prompt [y/N] " yn; then
    info "No answer (no TTY) — aborted; re-run with --yes / ASSUME_YES=1 to skip prompts."
    exit 1
  fi
  [[ "${yn,,}" == "y" ]] || { info "Aborted."; exit 0; }
}

wait_for_rollout() {
  local resource=$1 namespace=$2 timeout=${3:-120s}
  info "Waiting for $resource in $namespace to be ready..."
  if kubectl rollout status "$resource" -n "$namespace" --timeout="$timeout"; then
    return 0
  fi
  # A failed rollout is almost always an image or a store problem; the events say
  # which, so print them instead of a bare timeout.
  warn "rollout of $resource in $namespace did not complete; recent pod state:"
  kubectl -n "$namespace" get pods 2>&1 | tail -n 5 >&2 || true
  kubectl -n "$namespace" describe pods 2>&1 | sed -n '/Events:/,$p' | head -n 20 >&2 || true
  return 1
}

# apply_gcp_image_override — when JAISCLOUD_GCP_IMAGE is set, point the GCP
# Deployment at that tag (your own build) and use IfNotPresent so a preloaded
# image is not pulled from a registry.
apply_gcp_image_override() {
  [[ -n "${JAISCLOUD_GCP_IMAGE:-}" ]] || return 0
  info "Using GCP image $JAISCLOUD_GCP_IMAGE"
  kubectl -n "$NAMESPACE_GCP" set image deployment/jaiscloud-gcp jaiscloud-gcp="$JAISCLOUD_GCP_IMAGE" >/dev/null
  kubectl -n "$NAMESPACE_GCP" patch deployment jaiscloud-gcp --type=json \
    -p '[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' >/dev/null
}

# import_image_into_cluster <image> — a locally built image is invisible to a
# kind/k3d node (they have their own containerd), so the default deploy would
# run the published image instead of the build. Load it when we recognise the
# context; other clusters need their own registry.
import_image_into_cluster() {
  local image=$1 context
  context=$(kubectl config current-context 2>/dev/null || true)
  case "$context" in
    kind-*)
      if command -v kind >/dev/null 2>&1; then
        info "Loading $image into kind cluster '${context#kind-}'..."
        kind load docker-image "$image" --name "${context#kind-}"
      else
        warn "kind context '$context' but 'kind' is not on PATH — load $image yourself"
      fi ;;
    k3d-*)
      if command -v k3d >/dev/null 2>&1; then
        info "Importing $image into k3d cluster '${context#k3d-}'..."
        k3d image import -c "${context#k3d-}" "$image"
      else
        warn "k3d context '$context' but 'k3d' is not on PATH — import $image yourself"
      fi ;;
  esac
}

postgres_deployed() {
  kubectl -n "$NAMESPACE_DATA" get deployment postgres &>/dev/null
}

# scale_to_zero <namespace> <deployment> <pod-label-selector>
#   Scales a deployment to 0 and waits for its pods to disappear. `kubectl scale`
#   has no --ignore-not-found, so a missing deployment is handled explicitly.
scale_to_zero() {
  local ns=$1 deployment=$2 selector=$3
  if ! kubectl -n "$ns" get deployment "$deployment" &>/dev/null; then
    return 0
  fi
  kubectl -n "$ns" scale deployment "$deployment" --replicas=0 --timeout=120s >/dev/null
  kubectl -n "$ns" wait --for=delete pod -l "$selector" --timeout=120s >/dev/null 2>&1 ||     warn "$deployment pods in $ns did not terminate within 120s"
}

# ── lifecycle helpers (park / resume / switch) ──────────────────────────────
# Park is compute-destructive and data-retentive: the emulator quiesces its own
# engine compute through POST /_jaiscloud/park before the Deployment is scaled
# to 0, and everything persisted — databases, PVCs (blobs + state/instance ID),
# namespaces, CA and resource records — survives. Resume brings the compute back
# without re-applying a manifest, so exactly one rollout happens per direction.

# substrate_vars <aws|gcp> — set the per-cloud names the helpers below use.
substrate_vars() {
  case "$1" in
    aws)
      NS="$NAMESPACE_AWS"; DEPLOY="jaiscloud"; SVC="jaiscloud"
      PORT=4566; SELECTOR="app=jaiscloud"; DB="$AWS_DB"
      BLOBS_PVC="${AWS_PVCS[0]}"; STATE_PVC="jaiscloud-state-pvc" ;;
    gcp)
      NS="$NAMESPACE_GCP"; DEPLOY="jaiscloud-gcp"; SVC="jaiscloud-gcp"
      PORT=8080; SELECTOR="app=jaiscloud-gcp"; DB="$GCP_DB"
      BLOBS_PVC="$GCP_PVC"; STATE_PVC="jaiscloud-gcp-state-pvc" ;;
    *) die "unknown substrate '$1' (want aws or gcp)" ;;
  esac
}

# Annotation reads use jsonpath bracket notation: annotation keys contain dots
# and slashes, which the dot-path form cannot express reliably. A missing key
# reads as empty.
ns_annotation() { # ns_annotation <namespace> <key>
  kubectl get namespace "$1" \
    -o jsonpath="{.metadata.annotations['$2']}" 2>/dev/null || true
}

# free_local_port — the first unbound port in 18080-18089. The check runs in a
# subshell so a failed /dev/tcp open cannot leak an fd into this shell.
free_local_port() {
  local p
  for p in $(seq 18080 18089); do
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      printf '%s' "$p"
      return 0
    fi
  done
  return 1
}

PORT_FORWARD_PIDS=()
PORT_FORWARD_PORT=""

stop_port_forwards() {
  local pid
  for pid in ${PORT_FORWARD_PIDS+"${PORT_FORWARD_PIDS[@]}"}; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  PORT_FORWARD_PIDS=()
  PORT_FORWARD_PORT=""
}

# port_forward <namespace> <service> <remote-port> — forward to a free local
# port and wait for /_jaiscloud/health. Sets PORT_FORWARD_PORT; every spawned
# pid is killed by the EXIT trap and by stop_port_forwards.
port_forward() {
  local ns=$1 svc=$2 remote=$3 local_port log i
  local_port=$(free_local_port) || die "no free local port in 18080-18089 — stop the process using one"
  log=$(mktemp "${TMPDIR:-/tmp}/jaiscloud-portforward.XXXXXX.log")
  trap 'stop_port_forwards' EXIT
  info "Port-forwarding $ns/svc/$svc -> 127.0.0.1:$local_port ..."
  kubectl -n "$ns" port-forward "svc/$svc" "$local_port:$remote" --address 127.0.0.1 >"$log" 2>&1 &
  PORT_FORWARD_PIDS+=("$!")
  for i in $(seq 1 60); do
    if curl -sf --max-time 2 "http://127.0.0.1:$local_port/_jaiscloud/health" >/dev/null 2>&1; then
      PORT_FORWARD_PORT=$local_port
      rm -f "$log"
      return 0
    fi
    sleep 0.5
  done
  warn "port-forward $ns/svc/$svc did not answer /_jaiscloud/health:"
  sed 's/^/    /' "$log" >&2 || true
  stop_port_forwards
  rm -f "$log"
  return 1
}

# admin_post <local-port> <park|resume> — POST the lifecycle hook and report the
# body. A 404 means the running image predates the endpoint; any other non-2xx
# is fatal unless PARK_FORCE=1.
admin_post() {
  local port=$1 path=$2 body code
  body=$(mktemp "${TMPDIR:-/tmp}/jaiscloud-hook.XXXXXX.json")
  code=$(curl -sS --max-time 120 -o "$body" -w '%{http_code}' \
    -X POST "http://127.0.0.1:$port/_jaiscloud/$path" || true)
  if [[ "$code" == "404" ]]; then
    sed 's/^/    /' "$body" >&2 || true
    rm -f "$body"
    die "POST /_jaiscloud/$path returned 404: the running image predates the park/resume API. Rebuild it and redeploy (or k3d image import/kind load + --skip-build), then retry"
  fi
  if [[ "$code" != 2* ]]; then
    warn "POST /_jaiscloud/$path returned HTTP ${code:-<none>}:"
    sed 's/^/    /' "$body" >&2 || true
    rm -f "$body"
    if [[ "${PARK_FORCE:-0}" == "1" ]]; then
      warn "PARK_FORCE=1 — continuing after the failed '$path' hook"
      return 0
    fi
    die "the emulator's '$path' hook failed; fix it and retry (PARK_FORCE=1 continues anyway)"
  fi
  info "POST /_jaiscloud/$path -> HTTP $code"
  sed 's/^/    /' "$body" || true
  rm -f "$body"
}

# ensure_park_annotations <cloud> <image> <image-id> <version> — the namespace
# record the preflight and a later resume read. Empty values are not stored.
ensure_park_annotations() {
  local cloud=$1 image=$2 image_id=$3 version=$4
  substrate_vars "$cloud"
  local args=(
    "jaiscloud.io/state=parked"
    "jaiscloud.io/parked-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  )
  [[ -n "$image" ]] && args+=("jaiscloud.io/parked-image=$image")
  [[ -n "$image_id" ]] && args+=("jaiscloud.io/parked-image-id=$image_id")
  [[ -n "$version" ]] && args+=("jaiscloud.io/parked-version=$version")
  kubectl annotate namespace "$NS" "${args[@]}" --overwrite >/dev/null
}

# delete_executor_workloads <cloud> — remove executor-created Pods/Jobs inside
# the substrate namespace and inside every namespace it owns. Namespaces and
# Services stay (a parked substrate keeps its addressing and its records).
delete_executor_workloads() {
  local cloud=$1 ns
  kubectl -n "$NS" delete jobs --all --ignore-not-found
  kubectl -n "$NS" delete pods --all --ignore-not-found
  # The ECR pull-through proxy is a JaisCloud-managed workload, not the substrate.
  kubectl -n "$NS" delete deployments -l managed-by=jaiscloud --ignore-not-found
  for ns in $(kubectl get namespaces \
      -l "jaiscloud.io/managed-by=jaiscloud,jaiscloud.io/substrate=$cloud" \
      -o name 2>/dev/null | sed 's|^namespace/||'); do
    [[ "$ns" == "$NS" ]] && continue
    info "Terminating executor workloads in $ns (namespace and Services survive)..."
    kubectl -n "$ns" delete jobs,pods,deployments,statefulsets --all --ignore-not-found
  done
}

# park_substrate <cloud> — quiesce compute, scale to 0, keep everything else.
park_substrate() {
  local cloud=$1 other replicas image image_id version
  substrate_vars "$cloud"
  if [[ "$cloud" == "aws" ]]; then other=gcp; else other=aws; fi

  if ! kubectl get namespace "$NS" &>/dev/null; then
    info "namespace $NS does not exist — nothing to park for '$cloud'."
    return 0
  fi
  if ! kubectl -n "$NS" get deployment "$DEPLOY" &>/dev/null; then
    info "deployment/$DEPLOY is absent in $NS (the $cloud substrate is not installed here);"
    info "cleaning labelled executor workloads only — this is a delete, not a park."
    delete_executor_workloads "$cloud"
    return 0
  fi

  replicas=$(kubectl -n "$NS" get deployment "$DEPLOY" -o jsonpath='{.spec.replicas}')
  image=$(kubectl -n "$NS" get deployment "$DEPLOY" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)
  if [[ "$replicas" == "0" ]]; then
    info "deployment/$DEPLOY is already scaled to 0 — ensuring the park record."
    ensure_park_annotations "$cloud" "$image" \
      "$(ns_annotation "$NS" "jaiscloud.io/parked-image-id")" \
      "$(ns_annotation "$NS" "jaiscloud.io/parked-version")"
    record_intent "$cloud" parked
    return 0
  fi

  image_id=$(kubectl -n "$NS" get pods -l "$SELECTOR" \
    -o jsonpath='{.items[0].status.containerStatuses[0].imageID}' 2>/dev/null || true)
  port_forward "$NS" "$SVC" "$PORT"
  version=$(curl -sS --max-time 10 \
    "http://127.0.0.1:$PORT_FORWARD_PORT/_jaiscloud/doctor" 2>/dev/null \
    | sed -n 's/.*"version":"\([^"]*\)".*/\1/p' || true)
  info "Parking $cloud: cancelling jobs and stopping engine runtimes..."
  admin_post "$PORT_FORWARD_PORT" park
  stop_port_forwards

  info "Scaling deployment/$DEPLOY to 0 in $NS..."
  kubectl -n "$NS" scale deployment "$DEPLOY" --replicas=0 --timeout=120s >/dev/null
  kubectl -n "$NS" wait --for=delete pod -l "$SELECTOR" --timeout=120s >/dev/null 2>&1 \
    || warn "the $DEPLOY pod did not terminate within 120s"

  info "Terminating executor workloads (Pods, Jobs, ECR proxy) in $NS..."
  delete_executor_workloads "$cloud"

  ensure_park_annotations "$cloud" "$image" "$image_id" "$version"
  record_intent "$cloud" parked

  ok "$cloud substrate parked. Survives: database '$DB', pvc/$BLOBS_PVC, pvc/$STATE_PVC,"
  ok "instance ID, namespaces, CA and every resource record. Terminated: the substrate"
  ok "pod, Dataproc jobs, Managed Kafka brokers, Cloud Run revisions and executor pods."
  echo ""
  echo "  Resume with: $0 --resume $cloud"
  echo "  Switch with: $0 --switch $other"
  echo ""
}

# resume_substrate <cloud> — scale to 1, recreate compute, warn on image change.
resume_substrate() {
  local cloud=$1 state parked_image parked_id new_id
  substrate_vars "$cloud"

  if ! kubectl get namespace "$NS" &>/dev/null; then
    die "namespace $NS does not exist — install the $cloud substrate first (see $0 --help)"
  fi
  if ! kubectl -n "$NS" get deployment "$DEPLOY" &>/dev/null; then
    die "deployment/$DEPLOY is missing in $NS — run deploy.sh to install the $cloud substrate"
  fi

  state=$(ns_annotation "$NS" "jaiscloud.io/state")
  parked_image=$(ns_annotation "$NS" "jaiscloud.io/parked-image")
  parked_id=$(ns_annotation "$NS" "jaiscloud.io/parked-image-id")
  if [[ -z "$state" && -z "$parked_image" && -z "$parked_id" ]]; then
    warn "namespace $NS is not marked parked — resuming anyway"
  fi

  info "Scaling deployment/$DEPLOY to 1 in $NS..."
  kubectl -n "$NS" scale deployment "$DEPLOY" --replicas=1 >/dev/null
  wait_for_rollout "deployment/$DEPLOY" "$NS"

  port_forward "$NS" "$SVC" "$PORT"
  info "Resuming $cloud: recreating engine runtimes..."
  admin_post "$PORT_FORWARD_PORT" resume
  new_id=$(kubectl -n "$NS" get pods -l "$SELECTOR" \
    -o jsonpath='{.items[0].status.containerStatuses[0].imageID}' 2>/dev/null || true)
  stop_port_forwards

  if [[ -n "$parked_id" && -n "$new_id" && "$parked_id" != "$new_id" ]]; then
    warn "binary changed while parked: store migrations may run"
    warn "  parked: $parked_id"
    warn "  now:    $new_id"
  fi

  kubectl annotate namespace "$NS" "jaiscloud.io/state=running" --overwrite >/dev/null
  kubectl annotate namespace "$NS" \
    jaiscloud.io/parked-at- jaiscloud.io/parked-image- \
    jaiscloud.io/parked-image-id- jaiscloud.io/parked-version- \
    --overwrite >/dev/null
  record_intent "$cloud" running
  ok "$cloud substrate resumed in $NS."
  print_endpoints
}

# switch_substrate <target> — park the other substrate when installed, resume target.
switch_substrate() {
  local target=$1 other
  [[ "$target" == "both" ]] && die "--switch takes aws or gcp, not both"
  substrate_vars "$target" # validates the target
  if [[ "$target" == "aws" ]]; then other=gcp; else other=aws; fi
  confirm "Switch to the $target substrate (park $other, then resume $target)?"
  substrate_vars "$other"
  if kubectl get namespace "$NS" &>/dev/null; then
    park_substrate "$other"
  else
    info "the $other substrate is not installed — nothing to park"
  fi
  resume_substrate "$target"
}

# delete_substrate <cloud> — remove the workloads only; every PVC and database stays.
delete_substrate() {
  local cloud=$1
  substrate_vars "$cloud"
  if ! kubectl get namespace "$NS" &>/dev/null; then
    info "namespace $NS does not exist — nothing to delete for '$cloud'."
    return 0
  fi
  info "Removing the $cloud workloads in $NS (persisted data is preserved)..."
  if [[ "$cloud" == "aws" ]]; then
    kubectl delete deployment jaiscloud -n "$NS" --ignore-not-found
    kubectl delete service    jaiscloud -n "$NS" --ignore-not-found
    kubectl delete configmap  jaiscloud-config -n "$NS" --ignore-not-found
    kubectl delete secret     jaiscloud-dsn    -n "$NS" --ignore-not-found
  else
    # No ConfigMap: the GCP substrate reads its configuration from env/DSN.
    kubectl delete deployment jaiscloud-gcp -n "$NS" --ignore-not-found
    kubectl delete service    jaiscloud-gcp -n "$NS" --ignore-not-found
    kubectl delete secret     jaiscloud-gcp-dsn -n "$NS" --ignore-not-found
  fi
  record_intent "$cloud" absent 0
  ok "$cloud workloads removed. Persisted data retained:"
  kubectl get pvc "$BLOBS_PVC" -n "$NS" --no-headers 2>/dev/null \
    && echo "    $NS/pvc/$BLOBS_PVC" || true
  kubectl get pvc "$STATE_PVC" -n "$NS" --no-headers 2>/dev/null \
    && echo "    $NS/pvc/$STATE_PVC" || true
  kubectl get pvc "$POSTGRES_PVC" -n "$NAMESPACE_DATA" --no-headers 2>/dev/null \
    && echo "    $NAMESPACE_DATA/pvc/$POSTGRES_PVC" || true
}

# migrate_blobs <src-ns> <src-pvc> <dst-ns> <dst-pvc>
#   Streams every file from one claim into another with two short-lived helper
#   pods, because a PersistentVolumeClaim cannot be mounted across namespaces.
#   Verifies the file count and removes the helpers. SKIP_BLOB_MIGRATE=1 skips
#   the copy (the source claim is left in place).
migrate_blobs() {
  local src_ns=$1 src_pvc=$2 dst_ns=$3 dst_pvc=$4
  local image="${BLOB_MIGRATE_IMAGE:-busybox:1.36}" src_count dst_count
  if [[ "${SKIP_BLOB_MIGRATE:-0}" == "1" ]]; then
    warn "SKIP_BLOB_MIGRATE=1 — not copying $src_ns/pvc/$src_pvc (left in place)"
    return 0
  fi
  info "Copying object bytes: $src_ns/pvc/$src_pvc -> $dst_ns/pvc/$dst_pvc"
  kubectl -n "$src_ns" delete pod dpl-blob-src --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$dst_ns" delete pod dpl-blob-dst --ignore-not-found >/dev/null 2>&1 || true
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: dpl-blob-src
  namespace: $src_ns
spec:
  restartPolicy: Never
  containers:
    - name: copy
      image: $image
      command: ["sleep", "3600"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $src_pvc}
---
apiVersion: v1
kind: Pod
metadata:
  name: dpl-blob-dst
  namespace: $dst_ns
spec:
  restartPolicy: Never
  containers:
    - name: copy
      image: $image
      command: ["sleep", "3600"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $dst_pvc}
EOF
  if ! kubectl -n "$src_ns" wait --for=condition=Ready pod/dpl-blob-src --timeout=180s >/dev/null 2>&1 \
     || ! kubectl -n "$dst_ns" wait --for=condition=Ready pod/dpl-blob-dst --timeout=180s >/dev/null 2>&1; then
    kubectl -n "$src_ns" delete pod dpl-blob-src --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "$dst_ns" delete pod dpl-blob-dst --ignore-not-found >/dev/null 2>&1 || true
    die "a blob-copy helper pod did not become Ready (image '$image' not pullable?) — preload the image or set SKIP_BLOB_MIGRATE=1"
  fi
  kubectl -n "$src_ns" exec dpl-blob-src -- tar cf - -C /data . \
    | kubectl -n "$dst_ns" exec -i dpl-blob-dst -- tar xf - -C /data
  src_count=$(kubectl -n "$src_ns" exec dpl-blob-src -- sh -c 'find /data -type f | wc -l' | tr -d '[:space:]')
  dst_count=$(kubectl -n "$dst_ns" exec dpl-blob-dst -- sh -c 'find /data -type f | wc -l' | tr -d '[:space:]')
  kubectl -n "$src_ns" delete pod dpl-blob-src --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$dst_ns" delete pod dpl-blob-dst --ignore-not-found >/dev/null 2>&1 || true
  if [[ -z "$src_count" || "$src_count" != "$dst_count" ]]; then
    die "blob copy incomplete: $src_count file(s) in the source, $dst_count in the destination — the source claim is untouched"
  fi
  ok "Copied $src_count file(s) into $dst_ns/pvc/$dst_pvc."
}

# psql_admin <psql args...> — run psql as the postgres superuser inside the
# jaiscloud-data Postgres pod. The database is chosen with -d by the caller.
psql_admin() {
  kubectl -n "$NAMESPACE_DATA" exec "$POSTGRES_DEPLOYMENT" -- \
    psql -U jaiscloud -v ON_ERROR_STOP=1 -d postgres "$@"
}

# reset_database <db> — DROP (FORCE terminates sessions) + CREATE the database,
# keeping the Postgres server, its PVC and every other database intact.
reset_database() {
  local db=$1
  if ! postgres_deployed; then
    if kubectl -n "$NAMESPACE_DATA" get pvc "$POSTGRES_PVC" &>/dev/null; then
      die "deployment/postgres is missing in $NAMESPACE_DATA but pvc/$POSTGRES_PVC exists — deploy the stack first (./deploy/deploy.sh) so the '$db' database can be dropped"
    fi
    info "No postgres deployment or volume yet — nothing to drop for '$db'."
    return 0
  fi
  wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  info "Dropping and recreating database '$db' (server, PVC and other databases keep running)..."
  psql_admin \
    -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" \
    -c "CREATE DATABASE $db OWNER jaiscloud"
  ok "Database '$db' recreated."
}

# ensure_database <db> — deploy/k8s/postgres.yaml creates only the AWS database
# (`POSTGRES_DB=jaiscloud`, on a fresh volume) and declares no initdb script, so
# every other database is created explicitly. For `jaiscloud_gcp` this must
# happen before the GCP pod starts: a missing DSN target is fatal at startup.
# Idempotent.
ensure_database() {
  local db=$1 exists
  if ! postgres_deployed; then
    warn "deployment/postgres not found in $NAMESPACE_DATA — skipping database bootstrap for '$db'."
    return 0
  fi
  wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  exists=$(psql_admin -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | tr -d '[:space:]')
  if [[ "$exists" == "1" ]]; then
    ok "Database '$db' already exists."
    return 0
  fi
  info "Creating database '$db' (postgres.yaml creates only the AWS database)..."
  psql_admin -c "CREATE DATABASE $db OWNER jaiscloud"
  ok "Database '$db' created."
}

check_context() {
  require kubectl
  local context
  context=$(kubectl config current-context 2>/dev/null || true)
  if [[ "$context" != "docker-desktop" ]]; then
    warn "Current kubectl context is '$context', not 'docker-desktop'."
    warn "If you have a different local cluster (minikube, kind, rancher-desktop),"
    warn "make sure it's the active context before continuing."
    confirm "Continue anyway?"
  fi
}

# server_minor — the Kubernetes server's minor version ("30" for v1.30.x), or ""
# when it cannot be read. `kubectl version -o jsonpath` is not supported by every
# kubectl ("--output must be 'yaml' or 'json'"), so the JSON is parsed with sed;
# the raw /version endpoint is the fallback.
server_minor() {
  local raw minor
  raw=$(kubectl version -o json 2>/dev/null || true)
  minor=$(printf '%s' "$raw" | tr -d '[:space:]' \
    | sed -n 's/.*"serverVersion":{[^}]*"minor":"\([^"]*\)".*/\1/p')
  if [[ -z "$minor" ]]; then
    raw=$(kubectl get --raw /version 2>/dev/null || true)
    minor=$(printf '%s' "$raw" | tr -d '[:space:]' | sed -n 's/.*"minor":"\([^"]*\)".*/\1/p')
  fi
  printf '%s' "${minor//[!0-9]/}"
}

# require_isolation_prereqs — refuse to install where the dual-substrate boundary
# cannot be enforced. Both substrate ServiceAccounts hold cluster-wide namespace
# delete (per-cluster namespaces, Managed Kafka brokers), and RBAC cannot scope a
# delete by label; the ValidatingAdmissionPolicy in
# deploy/k8s/rbac-admission-policy.yaml is what keeps one substrate out of the
# other's namespaces. It needs Kubernetes >= 1.30 (VAP GA), so this fails before
# anything is created instead of installing a cluster with no boundary.
require_isolation_prereqs() {
  local minor api
  minor=$(server_minor)
  if [[ -z "$minor" ]]; then
    die "cannot read the Kubernetes server version — dual-substrate isolation requires Kubernetes >= 1.30 (ValidatingAdmissionPolicy)"
  fi
  if (( minor < 30 )); then
    die "JaisCloud dual-substrate isolation requires Kubernetes >= 1.30 (the server is 1.$minor): ValidatingAdmissionPolicy cannot enforce namespace isolation — upgrade the cluster, or run each substrate on its own cluster"
  fi
  api=$(kubectl get --raw /apis/admissionregistration.k8s.io/v1 2>/dev/null || true)
  if [[ "$api" != *validatingadmissionpolicies* ]]; then
    die "the cluster's admissionregistration.k8s.io/v1 API does not serve validatingadmissionpolicies — JaisCloud dual-substrate isolation requires Kubernetes >= 1.30 with ValidatingAdmissionPolicy enabled"
  fi
}

# apply_isolation_policy — idempotent apply of the ValidatingAdmissionPolicy and
# its binding. Applies before any namespace/executor object so every namespace a
# substrate creates from now on is admitted under the boundary.
apply_isolation_policy() {
  kubectl apply -f "$K8S_DIR/rbac-admission-policy.yaml"
}

# retire_legacy_namespace_binding <cloud> — a pre-split install bound one
# jaiscloud-namespace-admin ClusterRoleBinding to BOTH substrate
# ServiceAccounts. The split manifests replace it with per-substrate bindings.
# Delete the combined binding only when that cannot strand the other substrate:
# its own binding already exists, or its namespace is absent. Otherwise keep it
# (and warn) until the other substrate's install migrates it.
retire_legacy_namespace_binding() {
  local cloud=$1 other other_ns
  if [[ "$cloud" == "aws" ]]; then other=gcp; other_ns=$NAMESPACE_GCP; else other=aws; other_ns=$NAMESPACE_AWS; fi
  kubectl get clusterrolebinding jaiscloud-namespace-admin &>/dev/null || return 0
  if kubectl get clusterrolebinding "jaiscloud-namespace-admin-$other" &>/dev/null \
     || ! kubectl get namespace "$other_ns" &>/dev/null; then
    info "Removing the pre-split combined clusterrolebinding jaiscloud-namespace-admin (superseded by the per-substrate bindings)..."
    kubectl delete clusterrolebinding jaiscloud-namespace-admin --ignore-not-found >/dev/null
  else
    warn "keeping the pre-split combined clusterrolebinding jaiscloud-namespace-admin: the $other substrate is present but has no per-substrate binding yet; run the $other install to migrate it"
  fi
}

# apply_aws — the store first (namespace-data.yaml + postgres.yaml), then the
# AWS namespace and its RBAC (rbac-shared.yaml declares the ClusterRole the
# bindings reference; rbac-aws.yaml declares the ServiceAccount and both
# bindings), and only then the deployment that runs as that identity. No GCP
# object is created by an AWS-only install.
apply_aws() {
  require_isolation_prereqs
  apply_isolation_policy
  kubectl apply -f "$K8S_DIR/namespace-data.yaml"
  kubectl apply -f "$K8S_DIR/postgres.yaml"
  kubectl apply -f "$K8S_DIR/namespace-aws.yaml"
  kubectl apply -f "$K8S_DIR/rbac-shared.yaml"
  kubectl apply -f "$K8S_DIR/rbac-aws.yaml"
  retire_legacy_namespace_binding aws
  kubectl apply -f "$K8S_DIR/jaiscloud.yaml"
}

# AWS reset re-applies only the AWS-side objects: postgres.yaml lives entirely
# in jaiscloud-data, which a --reset must never touch.
apply_aws_workloads() {
  kubectl apply -f "$K8S_DIR/namespace-aws.yaml"
  kubectl apply -f "$K8S_DIR/rbac-shared.yaml"
  kubectl apply -f "$K8S_DIR/rbac-aws.yaml"
  retire_legacy_namespace_binding aws
  kubectl apply -f "$K8S_DIR/jaiscloud.yaml"
  restore_aws_ca_configmap
}

# The front door's CA ConfigMap lives in the AWS namespace, so deleting that
# namespace (--reset) removes it. Restore it from the gateway's TLS secret when
# the mesh is installed, so managed workloads keep their CA source.
restore_aws_ca_configmap() {
  local istio_ns="istio-system" secret="jaiscloud-wildcard" ca
  if ! kubectl -n "$istio_ns" get secret "$secret" &>/dev/null; then
    return 0
  fi
  ca="$(kubectl -n "$istio_ns" get secret "$secret" -o jsonpath='{.data.ca\.crt}' 2>/dev/null | base64 -d || true)"
  [[ -n "$ca" ]] || return 0
  info "Restoring ConfigMap $NAMESPACE_AWS/jaiscloud-ca-cert from $istio_ns/$secret..."
  kubectl -n "$NAMESPACE_AWS" create configmap jaiscloud-ca-cert \
    --from-literal=ca.crt="$ca" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# ── install record (jaiscloud-data/jaiscloud-install) ────────────────────────
# The record lives with the store, so a substrate reset — which deletes only its
# own namespace — never erases what was installed and what the operator wants
# running. The preflight reads it to make its recommendations intent-relative.
# deploy.sh owns this object; no static manifest declares it, because applying
# a manifest would reset `installed`/`desired` to a fixed value.
INSTALL_CONFIGMAP="jaiscloud-install"

# install_record_field <installed|desired> — the stored value ("" when absent).
install_record_field() {
  kubectl -n "$NAMESPACE_DATA" get configmap "$INSTALL_CONFIGMAP" \
    -o jsonpath="{.data.$1}" 2>/dev/null || true
}

# csv_has <csv> <item>
csv_has() {
  local item
  IFS=',' read -r -a items <<<"$1"
  for item in ${items+"${items[@]}"}; do
    [[ "$item" == "$2" ]] && return 0
  done
  return 1
}

# csv_add <csv> <item> — item appended once, order preserved.
csv_add() {
  local item
  IFS=',' read -r -a items <<<"$1"
  for item in ${items+"${items[@]}"}; do
    [[ "$item" == "$2" ]] && { printf '%s' "$1"; return 0; }
  done
  if [[ -z "$1" ]]; then printf '%s' "$2"; else printf '%s,%s' "$1" "$2"; fi
}

# csv_set <csv cloud=state,...> <cloud> <state> — replace the cloud's entry (or
# add it), keeping the other cloud. Canonical order (aws, then gcp) so repeated
# runs do not churn the record.
csv_set() {
  local csv=$1 key=$2 value=$3 aws="" gcp="" item
  IFS=',' read -r -a items <<<"$csv"
  for item in ${items+"${items[@]}"}; do
    case "${item%%=*}" in
      aws) aws="aws=${item#*=}"; [[ "$key" == "aws" ]] && aws="aws=$value" ;;
      gcp) gcp="gcp=${item#*=}"; [[ "$key" == "gcp" ]] && gcp="gcp=$value" ;;
    esac
  done
  [[ -z "$aws" && "$key" == "aws" ]] && aws="aws=$value"
  [[ -z "$gcp" && "$key" == "gcp" ]] && gcp="gcp=$value"
  printf '%s' "${aws}${aws:+${gcp:+,}}${gcp}"
}

# record_intent <aws|gcp> <running|parked|absent> [add=1] — merge the cloud into
# `installed` (unless add=0, for a teardown of a cloud that was never installed)
# and set its `desired` state. Never creates the store namespace just to record
# intent; a cluster without jaiscloud-data gets a warning.
record_intent() {
  local cloud=$1 state=$2 add=${3:-1} installed desired
  if ! kubectl get namespace "$NAMESPACE_DATA" &>/dev/null; then
    warn "namespace $NAMESPACE_DATA does not exist — not recording install intent for '$cloud'"
    return 0
  fi
  installed=$(install_record_field installed)
  if [[ "$add" == "1" ]]; then
    installed=$(csv_add "$installed" "$cloud")
  elif ! csv_has "$installed" "$cloud"; then
    return 0
  fi
  desired=$(install_record_field desired)
  desired=$(csv_set "$desired" "$cloud" "$state")
  if ! kubectl -n "$NAMESPACE_DATA" create configmap "$INSTALL_CONFIGMAP" \
      --from-literal=schema=1 \
      --from-literal=installed="$installed" \
      --from-literal=desired="$desired" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null; then
    warn "could not write the install record $NAMESPACE_DATA/$INSTALL_CONFIGMAP — the preflight will fall back to inference"
    return 0
  fi
  info "Install record ($NAMESPACE_DATA/$INSTALL_CONFIGMAP): installed=$installed desired=$desired"
}

print_endpoints() {
  local aws_host="" context
  context=$(kubectl config current-context 2>/dev/null || true)
  case "$context" in
    # kind/k3d report the node's internal IP as the LoadBalancer address; the
    # host cannot reach it, so the port-forward is the real endpoint.
    kind-*|k3d-*) aws_host="" ;;
    *)
      aws_host=$(kubectl -n "$NAMESPACE_AWS" get svc jaiscloud \
        -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
      if [[ -z "$aws_host" ]]; then
        aws_host=$(kubectl -n "$NAMESPACE_AWS" get svc jaiscloud \
          -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
      fi ;;
  esac
  echo ""
  if [[ -n "$aws_host" ]]; then
    echo "  AWS substrate : http://$aws_host:4566   (namespace $NAMESPACE_AWS)"
  else
    echo "  AWS substrate : kubectl port-forward -n $NAMESPACE_AWS svc/jaiscloud 4566:4566"
    echo "                  (no host-reachable LoadBalancer on '$context')"
  fi
  # The GCP Service is a ClusterIP on purpose (it also carries the Thrift HMS
  # ports; the front door is the external HTTP surface).
  echo "  GCP substrate : kubectl port-forward -n $NAMESPACE_GCP svc/jaiscloud-gcp 8080:8080 8081:8081"
  echo "                  (namespace $NAMESPACE_GCP: REST :8080, gRPC :8081)"
  echo "  Postgres      : postgres.$NAMESPACE_DATA.svc.cluster.local:5432  (nodePort 30543)"
  echo ""
  echo "  Remove the AWS workloads (keep data) : $0 --delete"
  echo "  Reset the AWS substrate (its db+PVC): $0 --reset"
  echo "  Reset the GCP substrate (its db+PVC): $0 --gcp-reset"
  echo ""
}

# ── argument parsing ─────────────────────────────────────────────────────────

MODE="deploy"
PLAN=0
PREFLIGHT_JSON=0
PREFLIGHT_STRICT=0
PREFLIGHT_SUBSTRATE=""
PREFLIGHT_DESIRED=""
DELETE_TARGET="aws"
LIFECYCLE_TARGET=""
while (($#)); do
  case "$1" in
    --delete)        MODE="delete"
                     case "${2:-}" in aws|gcp|both) DELETE_TARGET="$2"; shift ;; esac ;;
    --delete=*)      MODE="delete"; DELETE_TARGET="${1#*=}" ;;
    --park)          [[ -n "${2:-}" ]] || die "--park needs a value (aws or gcp)"
                     MODE="park"; LIFECYCLE_TARGET="$2"; shift ;;
    --park=*)        MODE="park"; LIFECYCLE_TARGET="${1#*=}" ;;
    --resume)        [[ -n "${2:-}" ]] || die "--resume needs a value (aws or gcp)"
                     MODE="resume"; LIFECYCLE_TARGET="$2"; shift ;;
    --resume=*)      MODE="resume"; LIFECYCLE_TARGET="${1#*=}" ;;
    --switch)        [[ -n "${2:-}" ]] || die "--switch needs a value (aws or gcp)"
                     MODE="switch"; LIFECYCLE_TARGET="$2"; shift ;;
    --switch=*)      MODE="switch"; LIFECYCLE_TARGET="${1#*=}" ;;
    --reset)         MODE="reset" ;;
    --gcp)           MODE="gcp" ;;
    --gcp-reset)     MODE="gcp-reset" ;;
    --migrate-store) MODE="migrate-store" ;;
    --check)         MODE="check" ;;
    --json)          PREFLIGHT_JSON=1 ;;
    --strict)        PREFLIGHT_STRICT=1 ;;
    --substrate)     [[ -n "${2:-}" ]] || die "--substrate needs a value (aws, gcp or both)"
                     PREFLIGHT_SUBSTRATE="$2"; shift ;;
    --substrate=*)   PREFLIGHT_SUBSTRATE="${1#*=}" ;;
    --desired)       [[ -n "${2:-}" ]] || die "--desired needs a value (aws=running,gcp=parked)"
                     PREFLIGHT_DESIRED="$2"; shift ;;
    --desired=*)     PREFLIGHT_DESIRED="${1#*=}" ;;
    --plan)          PLAN=1 ;;
    -y|--yes)        ASSUME_YES=1 ;;
    --skip-build)    SKIP_IMAGE_BUILD=1 ;;
    -h|--help)       usage; exit 0 ;;
    *)               printf '\033[1;31m[ERROR]\033[0m unknown option: %s\n\n' "$1" >&2
                     usage >&2
                     exit 1 ;;
  esac
  shift
done

case "$MODE" in
  park|resume|switch)
    case "$LIFECYCLE_TARGET" in
      aws|gcp) ;;
      both)    die "--$MODE takes aws or gcp, not 'both'" ;;
      *)       die "--$MODE takes aws or gcp (got '${LIFECYCLE_TARGET:-}')" ;;
    esac ;;
  delete)
    case "$DELETE_TARGET" in
      aws|gcp|both) ;;
      *) die "--delete takes aws, gcp or both (got '$DELETE_TARGET')" ;;
    esac ;;
esac

# ── --check: read-only inspection (never mutates) ───────────────────────────

if [[ "$MODE" == "check" ]]; then
  args=()
  [[ "$PREFLIGHT_JSON" == "1" ]] && args+=(--json)
  [[ "$PREFLIGHT_STRICT" == "1" ]] && args+=(--strict)
  [[ -n "$PREFLIGHT_SUBSTRATE" ]] && args+=(--substrate "$PREFLIGHT_SUBSTRATE")
  [[ -n "$PREFLIGHT_DESIRED" ]] && args+=(--desired "$PREFLIGHT_DESIRED")
  exec "$REPO_ROOT/scripts/integration-preflight.sh" ${args+"${args[@]}"}
fi

# ── --plan: print what the selected mode would run, without running it ──────

plan() { printf '  + %s\n' "$*"; }

# plan_install_record <clouds to add to installed> <cloud=state pairs to set>
#   The record is merged with whatever is already stored, so the plan shows the
#   intent transition instead of a fixed document.
plan_install_record() {
  plan "kubectl -n $NAMESPACE_DATA get configmap $INSTALL_CONFIGMAP -o jsonpath='{.data}'   # merge installed += $1"
  plan "kubectl -n $NAMESPACE_DATA create configmap $INSTALL_CONFIGMAP --from-literal=schema=1 --from-literal=installed=<merged> --from-literal=desired=<merged, $2> --dry-run=client -o yaml | kubectl apply -f -"
}

# plan_lifecycle_park <cloud> / plan_lifecycle_resume <cloud> — the exact steps
# the lifecycle helpers run (no manifest is re-applied in either direction).
plan_lifecycle_park() {
  local cloud=$1
  substrate_vars "$cloud"
  plan "# park $cloud: compute-destructive, data-retentive"
  plan "kubectl get namespace $NS   # absent -> nothing to park; deployment absent -> clean executor workloads only"
  plan "kubectl -n $NS get deployment $DEPLOY -o jsonpath='{.spec.replicas}'   # 0 -> already parked"
  plan "kubectl -n $NS get deployment $DEPLOY -o jsonpath='{.spec.template.spec.containers[0].image}'"
  plan "kubectl -n $NS get pods -l $SELECTOR -o jsonpath='{.items[0].status.containerStatuses[0].imageID}'"
  plan "kubectl -n $NS port-forward svc/$SVC <free 18080-18089>:$PORT   # wait for /_jaiscloud/health"
  plan "curl -sS -X POST http://127.0.0.1:<port>/_jaiscloud/park   # jobs cancelled; broker/revision runtimes stopped"
  plan "kubectl -n $NS scale deployment $DEPLOY --replicas=0 --timeout=120s"
  plan "kubectl -n $NS wait --for=delete pod -l $SELECTOR --timeout=120s"
  plan "kubectl -n $NS delete jobs --all; delete pods --all; delete deployments -l managed-by=jaiscloud   # ECR proxy"
  plan "for ns in the namespaces labelled jaiscloud.io/managed-by=jaiscloud,jaiscloud.io/substrate=$cloud (other than $NS): kubectl -n \$ns delete jobs,pods,deployments,statefulsets --all"
  plan "kubectl annotate namespace $NS jaiscloud.io/state=parked jaiscloud.io/parked-at=... jaiscloud.io/parked-image=... jaiscloud.io/parked-image-id=... jaiscloud.io/parked-version=... --overwrite"
}

plan_lifecycle_resume() {
  local cloud=$1
  substrate_vars "$cloud"
  plan "# resume $cloud: one rollout, no manifest re-apply, no image check"
  plan "kubectl get namespace $NS   # missing -> die 'install it first'; deployment missing -> die 'run deploy.sh'"
  plan "kubectl get namespace $NS -o jsonpath=\"{.metadata.annotations['jaiscloud.io/state']}\"   # warn when not parked"
  plan "kubectl -n $NS scale deployment $DEPLOY --replicas=1"
  plan "kubectl -n $NS rollout status deployment/$DEPLOY --timeout=120s"
  plan "kubectl -n $NS port-forward svc/$SVC <free 18080-18089>:$PORT   # wait for /_jaiscloud/health"
  plan "curl -sS -X POST http://127.0.0.1:<port>/_jaiscloud/resume   # brokers/revisions recreated"
  plan "# compare the new pod imageID with jaiscloud.io/parked-image-id; warn when the binary changed"
  plan "kubectl annotate namespace $NS jaiscloud.io/state=running --overwrite; kubectl annotate namespace $NS jaiscloud.io/parked-at- jaiscloud.io/parked-image- jaiscloud.io/parked-image-id- jaiscloud.io/parked-version-"
}

print_plan() {
  local mode=$1
  info "Plan for mode '$mode' (nothing is executed)"
  case "$mode" in
    deploy)
      if [[ "$SKIP_IMAGE_BUILD" == "1" ]]; then
        plan "# docker build skipped (SKIP_IMAGE_BUILD=1)"
      else
        plan "docker build -t $IMAGE $REPO_ROOT"
        plan "docker tag $IMAGE $AWS_MANIFEST_IMAGE"
        plan "# kind load / k3d image import when the context is kind-*/k3d-*"
      fi
      plan "kubectl apply -f $K8S_DIR/rbac-admission-policy.yaml   # namespace isolation (K8s >= 1.30)"
      plan "kubectl apply -f $K8S_DIR/namespace-data.yaml"
      plan "kubectl apply -f $K8S_DIR/postgres.yaml"
      plan "kubectl apply -f $K8S_DIR/namespace-aws.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-shared.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-aws.yaml"
      plan "kubectl apply -f $K8S_DIR/jaiscloud.yaml"
      plan "kubectl -n $NAMESPACE_DATA rollout status deployment/postgres --timeout=120s"
      plan "kubectl -n $NAMESPACE_AWS rollout status deployment/jaiscloud --timeout=120s"
      plan_install_record "aws" "aws=running"
      ;;
    gcp)
      plan "kubectl apply -f $K8S_DIR/rbac-admission-policy.yaml   # namespace isolation (K8s >= 1.30)"
      plan "kubectl apply -f $K8S_DIR/namespace-data.yaml"
      plan "kubectl apply -f $K8S_DIR/postgres.yaml"
      plan "kubectl apply -f $K8S_DIR/namespace-gcp.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-shared.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-gcp.yaml"
      plan "kubectl -n $NAMESPACE_DATA rollout status deployment/postgres --timeout=120s"
      plan "kubectl -n $NAMESPACE_DATA exec deploy/postgres -- psql ... 'SELECT 1 ... jaiscloud_gcp' || CREATE DATABASE jaiscloud_gcp"
      plan "kubectl apply -f $K8S_DIR/jaiscloud-gcp.yaml"
      [[ -n "${JAISCLOUD_GCP_IMAGE:-}" ]] && plan "kubectl -n $NAMESPACE_GCP set image deployment/jaiscloud-gcp jaiscloud-gcp=$JAISCLOUD_GCP_IMAGE"
      plan "kubectl -n $NAMESPACE_GCP rollout status deployment/jaiscloud-gcp --timeout=180s"
      plan_install_record "gcp" "gcp=running"
      ;;
    migrate-store)
      local OLD_NS="${LEGACY_NAMESPACE:-$NAMESPACE_AWS}"
      plan "kubectl -n $NAMESPACE_AWS scale deployment jaiscloud --replicas=0   # when present"
      plan "kubectl -n $OLD_NS scale deployment postgres --replicas=0            # legacy GCP too, when present"
      plan "kubectl -n $OLD_NS exec deploy/postgres -- pg_dump -Fc -d jaiscloud > <tmp>"
      plan "kubectl apply -f $K8S_DIR/rbac-admission-policy.yaml   # namespace isolation (K8s >= 1.30)"
      plan "kubectl apply -f $K8S_DIR/namespace-data.yaml"
      plan "kubectl apply -f $K8S_DIR/postgres.yaml"
      plan "kubectl -n $NAMESPACE_DATA exec -i deploy/postgres -- pg_restore ... -d jaiscloud < <tmp>"
      plan "kubectl -n $NAMESPACE_DATA exec -i deploy/postgres -- pg_restore ... -d jaiscloud_gcp < <tmp>"
      plan "kubectl -n $OLD_NS delete deployment postgres; delete service postgres postgres-external"
      plan "# legacy GCP: apply namespace-gcp.yaml, rbac-shared.yaml, rbac-gcp.yaml and jaiscloud-gcp.yaml, scale to 0, tar-copy the blob PVC, scale to 1, delete the old Deployment/Service"
      plan "kubectl apply -f $K8S_DIR/jaiscloud.yaml"
      plan_install_record "aws" "aws=running (and gcp when the legacy GCP workload exists)"
      ;;
    reset)
      plan "kubectl -n $NAMESPACE_AWS scale deployment jaiscloud --replicas=0   # when present"
      plan "kubectl -n $NAMESPACE_DATA exec deploy/postgres -- psql -c 'DROP DATABASE IF EXISTS jaiscloud WITH (FORCE)' -c 'CREATE DATABASE jaiscloud OWNER jaiscloud'"
      plan "kubectl delete namespace $NAMESPACE_AWS"
      plan "kubectl apply -f $K8S_DIR/rbac-admission-policy.yaml   # namespace isolation (K8s >= 1.30)"
      plan "kubectl apply -f $K8S_DIR/namespace-data.yaml   # only when the store is missing"
      plan "kubectl apply -f $K8S_DIR/postgres.yaml         # only when the store is missing"
      plan "kubectl apply -f $K8S_DIR/namespace-aws.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-shared.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-aws.yaml"
      plan "kubectl apply -f $K8S_DIR/jaiscloud.yaml"
      plan_install_record "aws" "aws=running"
      ;;
    gcp-reset)
      plan "kubectl -n $NAMESPACE_GCP scale deployment jaiscloud-gcp --replicas=0   # when present"
      plan "kubectl -n $NAMESPACE_GCP delete pvc $GCP_PVC"
      plan "kubectl -n $NAMESPACE_DATA exec deploy/postgres -- psql -c 'DROP DATABASE IF EXISTS jaiscloud_gcp WITH (FORCE)' -c 'CREATE DATABASE jaiscloud_gcp OWNER jaiscloud'"
      plan "kubectl apply -f $K8S_DIR/rbac-admission-policy.yaml   # namespace isolation (K8s >= 1.30)"
      plan "kubectl apply -f $K8S_DIR/namespace-gcp.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-shared.yaml"
      plan "kubectl apply -f $K8S_DIR/rbac-gcp.yaml"
      plan "kubectl apply -f $K8S_DIR/jaiscloud-gcp.yaml"
      plan_install_record "gcp" "gcp=running"
      ;;
    delete)
      local target
      for target in $(if [[ "$DELETE_TARGET" == "both" ]]; then echo "aws gcp"; else echo "$DELETE_TARGET"; fi); do
        substrate_vars "$target"
        if [[ "$target" == "aws" ]]; then
          plan "kubectl -n $NS delete deployment jaiscloud service jaiscloud configmap jaiscloud-config secret jaiscloud-dsn"
        else
          plan "kubectl -n $NS delete deployment jaiscloud-gcp service jaiscloud-gcp secret jaiscloud-gcp-dsn"
        fi
        plan "kubectl -n $NAMESPACE_DATA get configmap $INSTALL_CONFIGMAP -o jsonpath='{.data}'   # keeps installed; sets desired $target=absent only when it was installed"
        plan "# keeps pvc/$BLOBS_PVC, pvc/$STATE_PVC, database '$DB', namespace $NS and the store"
      done
      ;;
    park)
      plan_lifecycle_park "$LIFECYCLE_TARGET"
      plan_install_record "$LIFECYCLE_TARGET" "$LIFECYCLE_TARGET=parked"
      plan "# keeps: database '$DB', pvc/$BLOBS_PVC, pvc/$STATE_PVC (instance ID), namespaces, CA, resource records"
      ;;
    resume)
      plan_lifecycle_resume "$LIFECYCLE_TARGET"
      plan_install_record "$LIFECYCLE_TARGET" "$LIFECYCLE_TARGET=running"
      ;;
    switch)
      local other
      if [[ "$LIFECYCLE_TARGET" == "aws" ]]; then other=gcp; else other=aws; fi
      plan "# switch to $LIFECYCLE_TARGET: park $other (when installed), then resume $LIFECYCLE_TARGET"
      plan_lifecycle_park "$other"
      plan_lifecycle_resume "$LIFECYCLE_TARGET"
      plan_install_record "$other,$LIFECYCLE_TARGET" "$other=parked,$LIFECYCLE_TARGET=running"
      ;;
    *) plan "# unknown mode: $mode" ;;
  esac
  info "Re-run without --plan to execute."
}

if [[ "$PLAN" == "1" ]]; then
  print_plan "$MODE"
  exit 0
fi

# ── --delete [aws|gcp|both]: remove workloads, keep every PVC ───────────────

if [[ "$MODE" == "delete" ]]; then
  check_context
  targets=(aws)
  if [[ "$DELETE_TARGET" == "both" ]]; then
    targets=(aws gcp)
  elif [[ "$DELETE_TARGET" == "gcp" ]]; then
    targets=(gcp)
  fi
  for target in "${targets[@]}"; do
    delete_substrate "$target"
    echo ""
  done
  echo "  Data is still on disk. Re-deploy a substrate with: $0 (AWS) / $0 --gcp (GCP)"
  echo "  To reset a substrate's database and namespace too: $0 --reset / $0 --gcp-reset"
  [[ "$DELETE_TARGET" == "both" ]] || echo "  The other substrate and Postgres were not touched."
  exit 0
fi

# ── --park / --resume / --switch: lifecycle without data loss ────────────────

if [[ "$MODE" == "park" || "$MODE" == "resume" || "$MODE" == "switch" ]]; then
  require kubectl
  require curl
  check_context
  if [[ "$MODE" == "park" ]]; then
    confirm "Park the $LIFECYCLE_TARGET substrate? This terminates all its jobs and engine compute but keeps its data."
  fi
  if [[ "$MODE" == "switch" ]]; then
    switch_substrate "$LIFECYCLE_TARGET"
  elif [[ "$MODE" == "park" ]]; then
    park_substrate "$LIFECYCLE_TARGET"
  else
    resume_substrate "$LIFECYCLE_TARGET"
  fi
  exit 0
fi

# ── --reset: AWS-scoped, database-scoped ─────────────────────────────────────

if [[ "$MODE" == "reset" ]]; then
  warn "AWS reset: this drops the '$AWS_DB' database and the $NAMESPACE_AWS namespace"
  warn "(including its blobs PVC). It does NOT touch $NAMESPACE_DATA, $POSTGRES_PVC,"
  warn "the '$GCP_DB' database, or the $NAMESPACE_GCP namespace/PVC."
  confirm "Are you sure?"
  check_context
  require_isolation_prereqs
  apply_isolation_policy

  info "Scaling the AWS deployment to zero..."
  scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud

  reset_database "$AWS_DB"

  info "Deleting namespace $NAMESPACE_AWS (removes the AWS blobs PVC)..."
  kubectl delete namespace "$NAMESPACE_AWS" --ignore-not-found --timeout=180s

  info "Recreating the AWS substrate..."
  # The store may not exist yet (e.g. --reset on a cluster that never ran the
  # full deploy); bring it up so the recreated AWS pod has a database.
  if ! postgres_deployed; then
    kubectl apply -f "$K8S_DIR/namespace-data.yaml"
    kubectl apply -f "$K8S_DIR/postgres.yaml"
    wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  fi
  apply_aws_workloads
  wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"
  record_intent aws running
  ok "AWS substrate reset. The GCP substrate and Postgres were left untouched."
  print_endpoints
  exit 0
fi

# ── --gcp-reset: GCP-scoped only ─────────────────────────────────────────────

if [[ "$MODE" == "gcp-reset" ]]; then
  warn "GCP reset: this drops the '$GCP_DB' database and pvc/$GCP_PVC in $NAMESPACE_GCP."
  warn "It does NOT touch the $NAMESPACE_AWS namespace, the '$AWS_DB' database,"
  warn "$NAMESPACE_DATA, or $POSTGRES_PVC."
  confirm "Are you sure?"
  check_context
  require_isolation_prereqs
  apply_isolation_policy

  info "Scaling the GCP deployment to zero..."
  # The pod must actually be gone before its RWO claim can be deleted (a PVC
  # held by a running pod stays Terminating forever), so this waits.
  scale_to_zero "$NAMESPACE_GCP" jaiscloud-gcp app=jaiscloud-gcp

  info "Deleting pvc/$GCP_PVC in $NAMESPACE_GCP..."
  kubectl -n "$NAMESPACE_GCP" delete pvc "$GCP_PVC" --ignore-not-found
  if kubectl -n "$NAMESPACE_GCP" get pvc "$GCP_PVC" &>/dev/null; then
    kubectl -n "$NAMESPACE_GCP" wait --for=delete "pvc/$GCP_PVC" --timeout=120s \
      || warn "pvc/$GCP_PVC is still present — the next apply will keep using it"
  fi

  reset_database "$GCP_DB"

  info "Re-applying the GCP substrate..."
  kubectl apply -f "$K8S_DIR/namespace-gcp.yaml"
  kubectl apply -f "$K8S_DIR/rbac-shared.yaml"
  kubectl apply -f "$K8S_DIR/rbac-gcp.yaml"
  retire_legacy_namespace_binding gcp
  kubectl apply -f "$K8S_DIR/jaiscloud-gcp.yaml"
  wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
  record_intent gcp running
  ok "GCP substrate reset. The AWS substrate and Postgres were left untouched."
  print_endpoints
  exit 0
fi

# ── --gcp: apply the GCP substrate only (no image build) ─────────────────────

if [[ "$MODE" == "gcp" ]]; then
  check_context
  require_isolation_prereqs
  info "Applying the namespace-isolation admission policy..."
  apply_isolation_policy
  info "Applying the shared store and the GCP substrate objects..."
  kubectl apply -f "$K8S_DIR/namespace-data.yaml"
  kubectl apply -f "$K8S_DIR/postgres.yaml"
  kubectl apply -f "$K8S_DIR/namespace-gcp.yaml"
  kubectl apply -f "$K8S_DIR/rbac-shared.yaml"
  kubectl apply -f "$K8S_DIR/rbac-gcp.yaml"
  retire_legacy_namespace_binding gcp
  # Create the database BEFORE starting the GCP pod: a missing DSN target is
  # fatal at startup, so applying the Deployment first would crash-loop it.
  ensure_database "$GCP_DB"
  info "Applying the GCP substrate manifest..."
  kubectl apply -f "$K8S_DIR/jaiscloud-gcp.yaml"
  apply_gcp_image_override
  wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
  record_intent gcp running
  ok "GCP substrate is ready in namespace $NAMESPACE_GCP."
  print_endpoints
  exit 0
fi

# ── --migrate-store: move a pre-split single-namespace store ─────────────────

if [[ "$MODE" == "migrate-store" ]]; then
  check_context
  require_isolation_prereqs
  apply_isolation_policy
  # Before the namespace split, Postgres ran as deployment/postgres in the AWS
  # namespace and held both clouds' schemas in database `jaiscloud`.
  OLD_NS="${LEGACY_NAMESPACE:-$NAMESPACE_AWS}"
  require kubectl
  kubectl -n "$OLD_NS" get deployment postgres &>/dev/null \
    || die "no legacy deployment/postgres in namespace $OLD_NS — nothing to migrate"
  if kubectl -n "$NAMESPACE_DATA" get pvc "$POSTGRES_PVC" &>/dev/null; then
    die "pvc/$POSTGRES_PVC already exists in $NAMESPACE_DATA — the new store is deployed; refusing to overwrite it (drop it first if this is a retry)"
  fi

  # A pre-split install ran the GCP emulator in the AWS namespace too, with its
  # object bytes on jaiscloud-gcp-blobs-pvc there. Detect it so the bytes are
  # migrated (a PVC cannot move namespaces) and the old workload is stopped.
  legacy_gcp=0
  legacy_gcp_pvc="${LEGACY_GCP_BLOBS_PVC:-jaiscloud-gcp-blobs-pvc}"
  gcp_blobs_pvc="${GCP_BLOBS_PVC:-jaiscloud-gcp-blobs-pvc}"
  if kubectl -n "$OLD_NS" get deployment jaiscloud-gcp &>/dev/null; then
    legacy_gcp=1
    info "Legacy GCP emulator found in $OLD_NS; its object bytes will be migrated."
  fi

  # Stop the writers BEFORE the dump: anything they write after the snapshot
  # would be silently lost. The legacy Postgres stays up (it is the dump source).
  info "Stopping the emulators that write to the legacy store..."
  scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud
  if [[ "$legacy_gcp" -eq 1 ]]; then
    scale_to_zero "$OLD_NS" jaiscloud-gcp app=jaiscloud-gcp
  fi

  info "Dumping database '$AWS_DB' from $OLD_NS/deployment/postgres..."
  DUMP_FILE="$(mktemp "${TMPDIR:-/tmp}/jaiscloud-store.XXXXXX.dump")"
  trap 'rm -f "$DUMP_FILE"' EXIT
  kubectl -n "$OLD_NS" exec "$POSTGRES_DEPLOYMENT" -- \
    pg_dump -U jaiscloud --no-owner --no-privileges -Fc -d "$AWS_DB" > "$DUMP_FILE"
  [[ -s "$DUMP_FILE" ]] || die "the dump is empty — refusing to continue"

  info "Stopping the legacy Postgres (the dump is complete)..."
  scale_to_zero "$OLD_NS" postgres app=postgres

  info "Starting the new store in $NAMESPACE_DATA..."
  kubectl apply -f "$K8S_DIR/namespace-data.yaml"
  kubectl apply -f "$K8S_DIR/postgres.yaml"
  wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  ensure_database "$GCP_DB"

  info "Restoring into '$AWS_DB' and '$GCP_DB' (pre-split GCP schema travels too)..."
  kubectl -n "$NAMESPACE_DATA" exec -i "$POSTGRES_DEPLOYMENT" -- \
    pg_restore -U jaiscloud --no-owner --no-privileges --clean --if-exists -d "$AWS_DB" < "$DUMP_FILE"
  kubectl -n "$NAMESPACE_DATA" exec -i "$POSTGRES_DEPLOYMENT" -- \
    pg_restore -U jaiscloud --no-owner --no-privileges --clean --if-exists -d "$GCP_DB" < "$DUMP_FILE"

  # The legacy workload must not keep running against the old volume.
  kubectl -n "$OLD_NS" delete deployment postgres --ignore-not-found
  kubectl -n "$OLD_NS" delete service postgres postgres-external --ignore-not-found

  if [[ "$legacy_gcp" -eq 1 ]]; then
    info "Bringing up the GCP substrate in $NAMESPACE_GCP (paused for the byte copy)..."
    kubectl apply -f "$K8S_DIR/namespace-gcp.yaml"
    kubectl apply -f "$K8S_DIR/rbac-shared.yaml"
    kubectl apply -f "$K8S_DIR/rbac-gcp.yaml"
    retire_legacy_namespace_binding gcp
    kubectl apply -f "$K8S_DIR/jaiscloud-gcp.yaml"
    apply_gcp_image_override
    scale_to_zero "$NAMESPACE_GCP" jaiscloud-gcp app=jaiscloud-gcp
    if kubectl -n "$OLD_NS" get pvc "$legacy_gcp_pvc" &>/dev/null; then
      migrate_blobs "$OLD_NS" "$legacy_gcp_pvc" "$NAMESPACE_GCP" "$gcp_blobs_pvc"
    else
      warn "no pvc/$legacy_gcp_pvc in $OLD_NS — no GCP object bytes to copy"
    fi
    kubectl apply -f "$K8S_DIR/jaiscloud-gcp.yaml"
    apply_gcp_image_override
    wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
    # The legacy GCP workload must not keep running in the AWS namespace.
    kubectl -n "$OLD_NS" delete deployment jaiscloud-gcp --ignore-not-found
    kubectl -n "$OLD_NS" delete service jaiscloud-gcp --ignore-not-found
  fi

  info "Recreating the AWS substrate against the migrated store..."
  apply_aws_workloads
  wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"

  record_intent aws running
  [[ "$legacy_gcp" -eq 1 ]] && record_intent gcp running
  ok "Store migrated from $OLD_NS to $NAMESPACE_DATA."
  echo ""
  echo "  Legacy volumes kept in $OLD_NS as rollback copies:"
  echo "    - pvc/$POSTGRES_PVC (the pre-split store)"
  if [[ "$legacy_gcp" -eq 1 ]]; then
    echo "    - pvc/$legacy_gcp_pvc (the pre-split GCP object bytes, now copied)"
  fi
  echo "  Verify both substrates, then delete them:"
  echo "    kubectl -n $OLD_NS delete pvc $POSTGRES_PVC $legacy_gcp_pvc --ignore-not-found"
  echo "  (NOTE: $0 --reset deletes the $OLD_NS namespace, and with it those copies.)"
  exit 0
fi

# ── default: build the image and deploy the AWS substrate ────────────────────

check_context

if [[ "$SKIP_IMAGE_BUILD" == "1" ]]; then
  info "SKIP_IMAGE_BUILD=1 — skipping the docker build."
  info "The AWS Deployment runs $AWS_MANIFEST_IMAGE (imagePullPolicy: IfNotPresent),"
  info "so a pre-imported image must carry that tag (e.g. 'k3d image import')."
else
  require docker
  info "Building Docker image: $IMAGE"
  docker build -t "$IMAGE" "$REPO_ROOT"
  # The Deployment runs AWS_MANIFEST_IMAGE with imagePullPolicy: IfNotPresent, so
  # tag the build under that name too — otherwise the cluster would keep running
  # whatever image happened to be there and the build would be dead weight.
  docker tag "$IMAGE" "$AWS_MANIFEST_IMAGE"
  import_image_into_cluster "$AWS_MANIFEST_IMAGE"
  ok "Image built and tagged as $AWS_MANIFEST_IMAGE."
fi

info "Applying the AWS substrate manifests..."
info "  $K8S_DIR/namespace-data.yaml  $K8S_DIR/postgres.yaml  $K8S_DIR/namespace-aws.yaml"
info "  $K8S_DIR/rbac-shared.yaml  $K8S_DIR/rbac-aws.yaml  $K8S_DIR/jaiscloud.yaml"
apply_aws

wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
ok "Postgres is ready."
wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"
ok "JaisCloud (AWS) is ready."
record_intent aws running

# ── smoke test ────────────────────────────────────────────────────────────────

info "Smoke-testing health endpoint..."
for i in $(seq 1 10); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:4566/_jaiscloud/health || true)
  if [[ "$STATUS" == "200" ]]; then
    ok "Health check passed (HTTP 200)."
    break
  fi
  if [[ $i -eq 10 ]]; then
    warn "Health check returned HTTP $STATUS after 10 attempts — check logs with:"
    warn "  kubectl logs -n $NAMESPACE_AWS deployment/jaiscloud"
  fi
  sleep 2
done

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
ok "AWS deployment complete."
print_endpoints
echo "  GCP substrate: $0 --gcp"
echo ""
echo "  NOTE: On non-Docker-Desktop clusters (minikube, kind) the LoadBalancer"
echo "  external IP may stay <pending>. Use port-forward instead:"
echo "    kubectl port-forward -n $NAMESPACE_AWS svc/jaiscloud 4566:4566"
echo ""
