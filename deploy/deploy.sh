#!/usr/bin/env bash
# deploy.sh — one-click build + deploy of the JaisCloud substrates to local Kubernetes
#
# Topology (see deploy/k8s/namespace.yaml; docs in README-GCP.md and
# DEVELOPER_GUIDE.md, Setup 3):
#   jaiscloud        AWS substrate — Deployment jaiscloud, PVC jaiscloud-blobs-pvc
#   jaiscloud-gcp    GCP substrate — Deployment jaiscloud-gcp, PVC jaiscloud-gcp-blobs-pvc
#   jaiscloud-data   PostgreSQL — Deployment postgres, PVC postgres-pvc
#                    databases: jaiscloud (AWS), jaiscloud_gcp (GCP)
#
# Requirements:
#   - Docker Desktop with Kubernetes enabled (or any cluster `kubectl` points at)
#   - kubectl pointing at that cluster
#
# Usage:
#   ./deploy/deploy.sh              # build image + apply the AWS substrate manifests
#   ./deploy/deploy.sh --gcp        # apply the GCP substrate manifests (no image build)
#   ./deploy/deploy.sh --migrate-store
#                                   # one-time: move the pre-split store from
#                                   # ns jaiscloud into jaiscloud-data
#   ./deploy/deploy.sh --delete     # remove the AWS workloads; keep every PVC and Postgres
#   ./deploy/deploy.sh --reset      # AWS-only, database-scoped: drop the `jaiscloud` db +
#                                   # the AWS namespace/PVC; GCP (`jaiscloud_gcp`, its PVC)
#                                   # and Postgres survive
#   ./deploy/deploy.sh --gcp-reset  # GCP-only, database-scoped: drop the `jaiscloud_gcp`
#                                   # db + the GCP blobs PVC; AWS and Postgres survive
#   ./deploy/deploy.sh --skip-build # do not run `docker build`; use the image already
#                                   # imported into the cluster (SKIP_IMAGE_BUILD=1)
#   ./deploy/deploy.sh --yes        # non-interactive (ASSUME_YES=1)
#   ./deploy/deploy.sh --help       # this text
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

Usage:
  ./deploy/deploy.sh              Build the image and apply the AWS substrate
                                  (namespace.yaml, postgres.yaml, rbac.yaml, jaiscloud.yaml)
  ./deploy/deploy.sh --gcp        Apply the GCP substrate (namespace.yaml, postgres.yaml,
                                  rbac.yaml, jaiscloud-gcp.yaml), creating the
                                  `jaiscloud_gcp` database BEFORE the pod starts,
                                  and wait for its rollout
  ./deploy/deploy.sh --migrate-store
                                  ONE-TIME upgrade from the pre-split layout: dump
                                  database `jaiscloud` from ns jaiscloud/deployment/
                                  postgres and restore it into the new store in
                                  jaiscloud-data (both databases, so pre-split GCP
                                  state travels). Stops the old Postgres and keeps
                                  its PVC as a rollback copy. Refuses when the new
                                  postgres-pvc already exists.
  ./deploy/deploy.sh --delete     Remove the AWS workloads; keep ALL PVCs and Postgres
  ./deploy/deploy.sh --reset      AWS-only, database-scoped reset: drop/recreate the
                                  `jaiscloud` database, delete the jaiscloud namespace (its
                                  blobs and state PVCs), then re-apply the AWS manifests.
                                  jaiscloud-data,
                                  postgres-pvc, jaiscloud-gcp and `jaiscloud_gcp` survive.
  ./deploy/deploy.sh --gcp-reset  GCP-only, database-scoped reset: scale jaiscloud-gcp to 0,
                                  delete jaiscloud-gcp-blobs-pvc, drop/recreate
                                  `jaiscloud_gcp`, then re-apply jaiscloud-gcp.yaml. The AWS
                                  substrate and jaiscloud-data are never touched.
  --skip-build                    Do not run `docker build`; deploy the image already
                                  imported into the cluster (SKIP_IMAGE_BUILD=1). Useful
                                  with `k3d image import` and other CI paths.
  -y, --yes                       Non-interactive: answer yes to every prompt
                                  (same as ASSUME_YES=1)
  -h, --help                      Show this help and exit 0

Environment:
  ASSUME_YES=1                    Non-interactive, same as --yes
  SKIP_IMAGE_BUILD=1              Skip `docker build`, same as --skip-build

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
  kubectl rollout status "$resource" -n "$namespace" --timeout="$timeout"
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

# ensure_database <db> — the initdb script only runs on a FRESH postgres volume
# (deploy/k8s/postgres.yaml, ConfigMap postgres-init), so create it explicitly
# when the volume already existed. Idempotent.
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
  info "Creating database '$db' (the postgres volume predates postgres-init)..."
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

# apply_aws — rbac.yaml must be applied after namespace.yaml (it declares
# ServiceAccounts in both substrate namespaces and cluster-scoped bindings) and
# before the deployments that reference those identities.
apply_aws() {
  kubectl apply -f "$K8S_DIR/namespace.yaml"
  kubectl apply -f "$K8S_DIR/postgres.yaml"
  kubectl apply -f "$K8S_DIR/rbac.yaml"
  kubectl apply -f "$K8S_DIR/jaiscloud.yaml"
}

# AWS reset re-applies only the AWS-side objects: postgres.yaml lives entirely
# in jaiscloud-data, which a --reset must never touch.
apply_aws_workloads() {
  kubectl apply -f "$K8S_DIR/namespace.yaml"
  kubectl apply -f "$K8S_DIR/rbac.yaml"
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

print_endpoints() {
  echo ""
  echo "  AWS substrate : http://localhost:4566   (namespace $NAMESPACE_AWS)"
  echo "  GCP substrate : http://localhost:8080   (namespace $NAMESPACE_GCP, gRPC :8081)"
  echo "                  kubectl port-forward -n $NAMESPACE_GCP svc/jaiscloud-gcp 8080:8080"
  echo "  Postgres      : postgres.$NAMESPACE_DATA.svc.cluster.local:5432  (nodePort 30543)"
  echo ""
  echo "  Remove the AWS workloads (keep data) : $0 --delete"
  echo "  Reset the AWS substrate (its db+PVC): $0 --reset"
  echo "  Reset the GCP substrate (its db+PVC): $0 --gcp-reset"
  echo ""
}

# ── argument parsing ─────────────────────────────────────────────────────────

MODE="deploy"
while (($#)); do
  case "$1" in
    --delete)        MODE="delete" ;;
    --reset)         MODE="reset" ;;
    --gcp)           MODE="gcp" ;;
    --gcp-reset)     MODE="gcp-reset" ;;
    --migrate-store) MODE="migrate-store" ;;
    -y|--yes)        ASSUME_YES=1 ;;
    --skip-build)    SKIP_IMAGE_BUILD=1 ;;
    -h|--help)       usage; exit 0 ;;
    *)               printf '\033[1;31m[ERROR]\033[0m unknown option: %s\n\n' "$1" >&2
                     usage >&2
                     exit 1 ;;
  esac
  shift
done

# ── --delete: remove AWS workloads, keep every PVC ───────────────────────────

if [[ "$MODE" == "delete" ]]; then
  check_context
  info "Removing the AWS workloads in $NAMESPACE_AWS (persisted data is preserved)..."
  kubectl delete deployment jaiscloud -n "$NAMESPACE_AWS" --ignore-not-found
  kubectl delete service    jaiscloud -n "$NAMESPACE_AWS" --ignore-not-found
  kubectl delete configmap  jaiscloud-config -n "$NAMESPACE_AWS" --ignore-not-found
  kubectl delete secret     jaiscloud-dsn    -n "$NAMESPACE_AWS" --ignore-not-found
  ok "AWS workloads removed. Persisted data retained:"
  for pvc in "${AWS_PVCS[@]}"; do
    kubectl get pvc "$pvc" -n "$NAMESPACE_AWS" --no-headers 2>/dev/null \
      && echo "    $NAMESPACE_AWS/pvc/$pvc" || true
  done
  kubectl get pvc "$GCP_PVC" -n "$NAMESPACE_GCP" --no-headers 2>/dev/null \
    && echo "    $NAMESPACE_GCP/pvc/$GCP_PVC" || true
  kubectl get pvc "$POSTGRES_PVC" -n "$NAMESPACE_DATA" --no-headers 2>/dev/null \
    && echo "    $NAMESPACE_DATA/pvc/$POSTGRES_PVC" || true
  echo ""
  echo "  Data is still on disk. Re-deploy the AWS substrate with: $0"
  echo "  To reset the AWS substrate too, run:                      $0 --reset"
  echo "  The GCP substrate and Postgres were not touched."
  exit 0
fi

# ── --reset: AWS-scoped, database-scoped ─────────────────────────────────────

if [[ "$MODE" == "reset" ]]; then
  warn "AWS reset: this drops the '$AWS_DB' database and the $NAMESPACE_AWS namespace"
  warn "(including its blobs PVC). It does NOT touch $NAMESPACE_DATA, $POSTGRES_PVC,"
  warn "the '$GCP_DB' database, or the $NAMESPACE_GCP namespace/PVC."
  confirm "Are you sure?"
  check_context

  info "Scaling the AWS deployment to zero..."
  scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud

  reset_database "$AWS_DB"

  info "Deleting namespace $NAMESPACE_AWS (removes the AWS blobs PVC)..."
  kubectl delete namespace "$NAMESPACE_AWS" --ignore-not-found --timeout=180s

  info "Recreating the AWS substrate..."
  # The store may not exist yet (e.g. --reset on a cluster that never ran the
  # full deploy); bring it up so the recreated AWS pod has a database.
  if ! postgres_deployed; then
    kubectl apply -f "$K8S_DIR/namespace.yaml"
    kubectl apply -f "$K8S_DIR/postgres.yaml"
    wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  fi
  apply_aws_workloads
  wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"
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
  kubectl apply -f "$K8S_DIR/jaiscloud-gcp.yaml"
  wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
  ok "GCP substrate reset. The AWS substrate and Postgres were left untouched."
  print_endpoints
  exit 0
fi

# ── --gcp: apply the GCP substrate only (no image build) ─────────────────────

if [[ "$MODE" == "gcp" ]]; then
  check_context
  info "Applying the shared namespaces, Postgres and RBAC..."
  kubectl apply -f "$K8S_DIR/namespace.yaml"
  kubectl apply -f "$K8S_DIR/postgres.yaml"
  kubectl apply -f "$K8S_DIR/rbac.yaml"
  # Create the database BEFORE starting the GCP pod: a missing DSN target is
  # fatal at startup, so applying the Deployment first would crash-loop it.
  ensure_database "$GCP_DB"
  info "Applying the GCP substrate manifest..."
  kubectl apply -f "$K8S_DIR/jaiscloud-gcp.yaml"
  wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
  ok "GCP substrate is ready in namespace $NAMESPACE_GCP."
  print_endpoints
  exit 0
fi

# ── --migrate-store: move a pre-split single-namespace store ─────────────────

if [[ "$MODE" == "migrate-store" ]]; then
  check_context
  # Before the namespace split, Postgres ran as deployment/postgres in the AWS
  # namespace and held both clouds' schemas in database `jaiscloud`.
  OLD_NS="${LEGACY_NAMESPACE:-$NAMESPACE_AWS}"
  require kubectl
  kubectl -n "$OLD_NS" get deployment postgres &>/dev/null \
    || die "no legacy deployment/postgres in namespace $OLD_NS — nothing to migrate"
  if kubectl -n "$NAMESPACE_DATA" get pvc "$POSTGRES_PVC" &>/dev/null; then
    die "pvc/$POSTGRES_PVC already exists in $NAMESPACE_DATA — the new store is deployed; refusing to overwrite it (drop it first if this is a retry)"
  fi

  # Stop the AWS writer BEFORE the dump: anything it writes after the snapshot
  # would be silently lost. The legacy Postgres stays up (it is the dump source).
  info "Stopping the AWS emulator (single writer to the legacy store)..."
  scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud

  info "Dumping database '$AWS_DB' from $OLD_NS/deployment/postgres..."
  DUMP_FILE="$(mktemp "${TMPDIR:-/tmp}/jaiscloud-store.XXXXXX.dump")"
  trap 'rm -f "$DUMP_FILE"' EXIT
  kubectl -n "$OLD_NS" exec "$POSTGRES_DEPLOYMENT" -- \
    pg_dump -U jaiscloud --no-owner --no-privileges -Fc -d "$AWS_DB" > "$DUMP_FILE"
  [[ -s "$DUMP_FILE" ]] || die "the dump is empty — refusing to continue"

  info "Stopping the legacy Postgres (the dump is complete)..."
  scale_to_zero "$OLD_NS" postgres app=postgres

  info "Starting the new store in $NAMESPACE_DATA..."
  kubectl apply -f "$K8S_DIR/namespace.yaml"
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

  info "Recreating the AWS substrate against the migrated store..."
  apply_aws_workloads
  wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"

  ok "Store migrated from $OLD_NS to $NAMESPACE_DATA."
  echo ""
  echo "  The legacy pvc/postgres-pvc was kept in $OLD_NS as a rollback copy."
  echo "  Verify both substrates, then delete it:"
  echo "    kubectl -n $OLD_NS delete pvc $POSTGRES_PVC --ignore-not-found"
  echo "  (NOTE: $0 --reset deletes the $OLD_NS namespace, and with it that copy.)"
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
  ok "Image built and tagged as $AWS_MANIFEST_IMAGE."
fi

info "Applying the AWS substrate manifests..."
info "  $K8S_DIR/namespace.yaml  $K8S_DIR/postgres.yaml  $K8S_DIR/rbac.yaml  $K8S_DIR/jaiscloud.yaml"
apply_aws

wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
ok "Postgres is ready."
wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"
ok "JaisCloud (AWS) is ready."

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
