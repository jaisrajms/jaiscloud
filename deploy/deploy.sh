#!/usr/bin/env bash
# deploy.sh — one-click build + deploy of the JaisCloud substrates to local Kubernetes
#
# Topology (see deploy/k8s/namespace-*.yaml; docs in README-GCP.md and
# DEVELOPER_GUIDE.md, Setup 3):
#   jaiscloud        AWS substrate — Deployment jaiscloud, PVC jaiscloud-blobs-pvc
#                    (deploy/k8s/namespace-aws.yaml), ServiceAccount jaiscloud
#   jaiscloud-gcp    GCP substrate — Deployment jaiscloud-gcp, PVC jaiscloud-gcp-blobs-pvc
#                    (deploy/k8s/namespace-gcp.yaml), ServiceAccount jaiscloud-gcp
#   jaiscloud-data   PostgreSQL — Deployment postgres, PVC postgres-pvc
#                    databases: jaiscloud (AWS), jaiscloud_gcp (GCP)
#                    (deploy/k8s/namespace-data.yaml)
#
# Each install applies only its own substrate's namespace and RBAC: an AWS-only
# install never creates a GCP namespace, ServiceAccount or binding, and vice
# versa. The store (namespace-data.yaml + postgres.yaml) and the namespace
# admission policy are the only shared objects.
#
# Custom names: the manifests above are the DEFAULT base. NS_AWS, NS_GCP, NS_DATA,
# SA_AWS and SA_GCP select another layout; deploy.sh copies the base into a
# throwaway kustomize overlay tree and patches the Namespace names, the
# ServiceAccount name and every reference to it, the DSN hostnames, the
# JAISCLOUD_K8S_* env values and the admission policy's identity expression, then
# applies it with `kubectl apply -k` (see render_tree). Nothing under deploy/k8s
# is edited, so the shipped manifests stay directly appliable for the defaults:
#
#   NS_AWS=team-aws NS_GCP=team-gcp NS_DATA=team-data \
#   SA_AWS=team-sa-aws SA_GCP=team-sa-gcp deploy/deploy.sh --yes
#
# --render-only DIR writes that tree and exits without touching the cluster.
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
#                                   # old GCP object bytes into jaiscloud-gcp.
#                                   # The source object must carry
#                                   # jaiscloud.io/managed-by=jaiscloud (or
#                                   # LEGACY_CONFIRM=1 for a pre-split build).
#                                   # The legacy workload is SCALED TO 0 and kept
#                                   # (Deployment/StatefulSet, Services, PVCs);
#                                   # --delete-legacy removes it too.
#   ./deploy/deploy.sh --migrate-store --delete-legacy
#                                   # also delete the legacy postgres workload +
#                                   # Services (only when it carries the JaisCloud
#                                   # marker; LEGACY_DELETE_FORCE=1 overrides a
#                                   # bare app=postgres), never a PVC
#   ./deploy/deploy.sh --store-dsn postgres://user:pass@host:5432/db
#                                   # bring your own store: no postgres.yaml, the
#                                   # databases are created via a psql pod and the
#                                   # substrate DSN Secrets point at this server
#                                   # (JAISCLOUD_STORE_DSN, JAISCLOUD_AWS_DSN,
#                                   # JAISCLOUD_GCP_DSN)
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
#                                   # scale back to 1, recreate the engine runtimes;
#                                   # refuses to change the binary while parked
#                                   # (pins the parked digest) unless --allow-upgrade
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
# The manifests name the default layout literally; NS_AWS/NS_GCP/NS_DATA and
# SA_AWS/SA_GCP let an install (or a gate script using a scratch namespace) pick
# another. They are not read by the manifests themselves: every mutating path
# renders a kustomize overlay that applies the names to a copy of the base (see
# render_tree), which is also what keeps deploy/k8s/*.yaml directly appliable
# for the defaults.
NAMESPACE_AWS="${NS_AWS:-jaiscloud}"
NAMESPACE_GCP="${NS_GCP:-jaiscloud-gcp}"
NAMESPACE_DATA="${NS_DATA:-jaiscloud-data}"
SA_AWS="${SA_AWS:-jaiscloud}"
SA_GCP="${SA_GCP:-jaiscloud-gcp}"

# ── the store: bundled Postgres in NAMESPACE_DATA, or an external DSN ────────
# --store-dsn / JAISCLOUD_STORE_DSN switches the install to "bring your own
# store": postgres.yaml is not applied, the databases are created through a
# short-lived psql pod, and the substrate DSN Secrets are overwritten with the
# external DSN. JAISCLOUD_AWS_DSN / JAISCLOUD_GCP_DSN override it per substrate.
STORE_DSN="${JAISCLOUD_STORE_DSN:-}"
STORE_MODE="bundled" # bundled|external, set from STORE_DSN after arg parsing
STORE_INIT_IMAGE="${STORE_INIT_IMAGE:-postgres:16-alpine}"

# ── legacy store migration (--migrate-store) ────────────────────────────────
# The tooling never deletes a workload it did not create unless the operator
# asks for it and the object carries the expected label. Deletion is opt-in.
DELETE_LEGACY="${DELETE_LEGACY:-0}"
# LEGACY_POSTGRES_WORKLOAD: auto (deployment/postgres, then statefulset/postgres),
# or an explicit "<kind>/<name>" such as statefulset/postgres.
LEGACY_POSTGRES_WORKLOAD="${LEGACY_POSTGRES_WORKLOAD:-auto}"
# LEGACY_NAMESPACE: where the pre-split store lives. Defaulting to the AWS
# namespace is the shipped layout; any other namespace must be confirmed with
# LEGACY_CONFIRM=1 (check_legacy_namespace), so a stray override can never aim the
# migration at a namespace this install does not own.
LEGACY_NAMESPACE="${LEGACY_NAMESPACE:-$NAMESPACE_AWS}"
LEGACY_PG_USER="${LEGACY_PG_USER:-jaiscloud}"
LEGACY_DSN="${LEGACY_DSN:-}"
MIGRATE_FORCE="${MIGRATE_FORCE:-0}"
# LEGACY_CONFIRM=1: this object/namespace really is the pre-split JaisCloud
# store. Required for an object that does not carry
# jaiscloud.io/managed-by=jaiscloud (a pre-split build predates the label) and
# for a LEGACY_NAMESPACE outside the three JaisCloud namespaces.
LEGACY_CONFIRM="${LEGACY_CONFIRM:-0}"
LEGACY_DELETE_FORCE="${LEGACY_DELETE_FORCE:-0}"
# ALLOW_UPGRADE=1 (--allow-upgrade): resume with whatever image the Deployment
# names, instead of pinning the parked binary's digest. Without it a resume
# refuses to change the binary and pins the parked digest when it is known.
ALLOW_UPGRADE="${ALLOW_UPGRADE:-0}"
# Blob claims: the pre-split claim in the legacy namespace and the claim the new
# substrate owns. The AWS pair only needs copying when the namespace changes
# (LEGACY_NAMESPACE != NAMESPACE_AWS); the GCP pair is a namespace move too.
LEGACY_AWS_BLOBS_PVC="${LEGACY_AWS_BLOBS_PVC:-jaiscloud-blobs-pvc}"
NEW_AWS_BLOBS_PVC="${NEW_AWS_BLOBS_PVC:-jaiscloud-blobs-pvc}"
LEGACY_GCP_BLOBS_PVC="${LEGACY_GCP_BLOBS_PVC:-jaiscloud-gcp-blobs-pvc}"
NEW_GCP_BLOBS_PVC="${NEW_GCP_BLOBS_PVC:-${GCP_BLOBS_PVC:-jaiscloud-gcp-blobs-pvc}}"

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

# b64decode — stdin base64 -> stdout. macOS's base64 (BSD) spells the decode flag
# -D, GNU coreutils spells it -d; openssl is the portable last resort.
b64decode() {
  base64 -d 2>/dev/null || base64 -D 2>/dev/null || openssl base64 -d -A 2>/dev/null || true
}

usage() {
  cat <<'EOF'
deploy.sh — build and deploy the JaisCloud substrates (AWS + GCP) to local Kubernetes

Topology (three namespaces, one Postgres server) — the defaults:
  jaiscloud       AWS substrate   Deployment jaiscloud, ServiceAccount jaiscloud,
                                  PVC jaiscloud-blobs-pvc
  jaiscloud-gcp   GCP substrate   Deployment jaiscloud-gcp, ServiceAccount jaiscloud-gcp,
                                  PVC jaiscloud-gcp-blobs-pvc
  jaiscloud-data  PostgreSQL      Deployment postgres, PVC postgres-pvc
                                  databases: jaiscloud (AWS), jaiscloud_gcp (GCP)

NS_AWS / NS_GCP / NS_DATA / SA_AWS / SA_GCP select another layout. deploy.sh
renders a throwaway kustomize overlay from deploy/k8s (the default base) and
applies it with `kubectl apply -k`, so no file is edited:
  NS_AWS=team-aws NS_GCP=team-gcp NS_DATA=team-data \
  SA_AWS=team-sa-aws SA_GCP=team-sa-gcp deploy/deploy.sh --yes
`--render-only DIR` writes that overlay tree and exits without touching the
cluster. Applying deploy/k8s/*.yaml directly always installs the defaults.

Requires Kubernetes >= 1.30: both substrate ServiceAccounts hold cluster-wide
namespace delete, so the namespace boundary between them is enforced by the
ValidatingAdmissionPolicy in deploy/k8s/rbac-admission-policy.yaml. deploy.sh
applies that policy (in every install and reset overlay) before any substrate
object.

Usage:
  ./deploy/deploy.sh              Build the image and apply the AWS substrate
                                  (overlays/data: namespace-data.yaml + postgres.yaml;
                                  overlays/aws: namespace-aws.yaml, rbac-shared.yaml,
                                  rbac-aws.yaml, jaiscloud.yaml)
  ./deploy/deploy.sh --gcp        Apply the GCP substrate (overlays/data, then
                                  overlays/gcp: namespace-gcp.yaml, rbac-shared.yaml,
                                  rbac-gcp.yaml, jaiscloud-gcp.yaml), creating the
                                  `jaiscloud_gcp` database BEFORE the pod starts,
                                  and wait for its rollout
  ./deploy/deploy.sh --migrate-store
                                  ONE-TIME upgrade from the pre-split layout: dump
                                  database `jaiscloud` from the legacy store
                                  (deployment/postgres, or statefulset/postgres,
                                  in LEGACY_NAMESPACE; or the LEGACY_DSN server)
                                  and restore it into the new store in
                                  jaiscloud-data (both databases, so pre-split GCP
                                  state travels). If the old GCP emulator ran in
                                  the legacy namespace, its object bytes are
                                  copied to jaiscloud-gcp/jaiscloud-gcp-blobs-pvc.
                                  The source object must be marked
                                  jaiscloud.io/managed-by=jaiscloud; a pre-split
                                  build predates that label, so confirm it once
                                  with LEGACY_CONFIRM=1 (LEGACY_DSN is an explicit
                                  source and needs no marker).
                                  By default the legacy Postgres is only SCALED
                                  TO 0: its Deployment/StatefulSet, Services and
                                  PVCs are kept, and the command prints how to
                                  remove them later. --delete-legacy (or
                                  DELETE_LEGACY=1) deletes the workload and the
                                  postgres/postgres-external Services after
                                  checking the JaisCloud marker; a bare
                                  app=postgres object needs LEGACY_DELETE_FORCE=1.
                                  A PVC is never deleted. SKIP_BLOB_MIGRATE=1
                                  copies by hand, BLOB_MIGRATE_IMAGE overrides
                                  busybox. Refuses when the new postgres-pvc
                                  already exists unless MIGRATE_FORCE=1.
  ./deploy/deploy.sh --migrate-store --delete-legacy
                                  With --migrate-store: opt into removing the
                                  legacy Postgres workload and Services.
  ./deploy/deploy.sh --store-dsn <dsn>
                                  Bring your own store (JAISCLOUD_STORE_DSN does
                                  the same): do not apply postgres.yaml, create
                                  the databases through a short-lived psql pod in
                                  jaiscloud-data, and overwrite the substrate DSN
                                  Secrets. The base DSN's database component is
                                  replaced with `jaiscloud` / `jaiscloud_gcp` for
                                  the Secrets (one database per cloud);
                                  JAISCLOUD_AWS_DSN / JAISCLOUD_GCP_DSN are used
                                  verbatim for an exact per-substrate DSN.
                                  --migrate-store with an external target is NOT
                                  supported (the pre-split migration needs the
                                  bundled store it is moving into).
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
                                  `gcp-*` namespaces, and record the park on the
                                  namespace under jaiscloud.io/<cloud>.* (per substrate,
                                  so co-located namespaces do not overwrite each other).
                                  Keeps: database, blobs/state PVCs (instance ID), the
                                  namespaces, the CA and every resource record.
                                  Managed Kafka broker topic data is NOT kept (the
                                  broker has no PVC); a real broker mode is warned about.
  ./deploy/deploy.sh --resume <aws|gcp>
                                  Scale the substrate back to 1, wait for its rollout,
                                  POST /_jaiscloud/resume (recreate brokers/revisions),
                                  and keep the parked binary: the Deployment image must
                                  still match the parked one, and the parked digest is
                                  pinned before scaling. --allow-upgrade opts into the
                                  new image (the store may migrate).
  ./deploy/deploy.sh --switch <aws|gcp>
                                  Park the other substrate (when installed), then resume
                                  this one. There is no `--switch both`.
  ./deploy/deploy.sh --reset      AWS-only, database-scoped reset: drop/recreate the
                                  `jaiscloud` database, delete the AWS namespace (its
                                  blobs and state PVCs), then re-apply the rendered AWS
                                  overlay (namespace-aws.yaml, rbac-shared.yaml,
                                  rbac-aws.yaml, jaiscloud.yaml). jaiscloud-data,
                                  postgres-pvc, the GCP namespace and `jaiscloud_gcp`
                                  survive.
  ./deploy/deploy.sh --gcp-reset  GCP-only, database-scoped reset: scale the GCP Deployment
                                  to 0, delete its blobs PVC, drop/recreate
                                  `jaiscloud_gcp`, then re-apply the rendered GCP overlay
                                  (namespace-gcp.yaml, rbac-shared.yaml, rbac-gcp.yaml,
                                  jaiscloud-gcp.yaml). The AWS substrate and the store
                                  are never touched.
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
  --render-only DIR               Render the kustomize overlay tree for the configured
                                  NS_*/SA_* names into DIR (every overlay of the
                                  install layout), print the resolved
                                  namespaces/ServiceAccounts and the overlay paths, and
                                  exit 0 without running kubectl. Nothing is written
                                  into the repository; DIR is left in place.
  --render-to DIR                 Alias for --render-only.
  --store-dsn <dsn>               Bring your own store (see above; also
                                  JAISCLOUD_STORE_DSN).
  --delete-legacy                 With --migrate-store: remove the legacy Postgres
                                  workload and Services (JaisCloud marker required;
                                  never a PVC).
  --legacy-confirm                Same as LEGACY_CONFIRM=1: confirm that the
                                  LEGACY_NAMESPACE/legacy object really is the
                                  pre-split JaisCloud store (needed for a pre-split
                                  build, which predates the managed-by label).
  --allow-upgrade                 With --resume/--switch: use whatever image the
                                  Deployment names instead of pinning the parked
                                  binary's digest (a store migration may run).
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
  JAISCLOUD_STORE_DSN             External store DSN, same as --store-dsn
                                  (its database component is replaced per cloud
                                  for the substrate Secrets)
  JAISCLOUD_AWS_DSN               Exact DSN for the AWS DSN Secret (verbatim,
                                  overrides the rewritten STORE_DSN)
  JAISCLOUD_GCP_DSN               Exact DSN for the GCP DSN Secret (verbatim)
  STORE_WORKLOAD=<kind>/<name>    With --check: the bundled store workload the
                                  preflight probes (default: deployment/postgres,
                                  then statefulset/postgres)
  PREFLIGHT_TIMEOUT_IMPL          With --check: auto (default), timeout,
                                  gtimeout, watchdog or none — the wall-clock
                                  helper used for the psql exec (macOS has no
                                  GNU timeout; the watchdog is the fallback)
  STORE_INIT_IMAGE                Image for the external-store psql pod
                                  (default postgres:16-alpine)
  LEGACY_NAMESPACE                Namespace holding the pre-split store
                                  (default the AWS namespace). Any other value
                                  must be confirmed with LEGACY_CONFIRM=1.
  LEGACY_POSTGRES_WORKLOAD        auto (default), deployment/postgres or
                                  statefulset/postgres
  LEGACY_DSN                      Dump this server from the NEW store pod instead
                                  of a legacy workload (nothing legacy is scaled or
                                  deleted); postgres:// or postgresql://. It is an
                                  explicit source, so it needs no managed-by marker.
  LEGACY_PG_USER                  Role for the legacy-workload dump (default jaiscloud)
  LEGACY_CONFIRM=1                Confirm that the legacy object/namespace is the
                                  pre-split JaisCloud store when it does not carry
                                  jaiscloud.io/managed-by=jaiscloud. Same as
                                  --legacy-confirm.
  LEGACY_GCP_BLOBS_PVC            Legacy GCP claim (default jaiscloud-gcp-blobs-pvc)
  NEW_GCP_BLOBS_PVC               New GCP claim (default jaiscloud-gcp-blobs-pvc)
  LEGACY_AWS_BLOBS_PVC            Legacy AWS claim (default jaiscloud-blobs-pvc)
  NEW_AWS_BLOBS_PVC               New AWS claim (default jaiscloud-blobs-pvc)
  DELETE_LEGACY=1                 Same as --delete-legacy
  LEGACY_DELETE_FORCE=1           Delete the legacy workload even when it only
                                  carries app=postgres and no JaisCloud marker
                                  (DANGEROUS)
  ALLOW_UPGRADE=1                 With --resume/--switch: do not pin the parked
                                  digest; the image may have changed and the store
                                  may migrate. Same as --allow-upgrade.
  MIGRATE_FORCE=1                 With --migrate-store: proceed when the new
                                  postgres-pvc already exists
  SKIP_BLOB_MIGRATE=1             Do not copy blob claims
  BLOB_MIGRATE_IMAGE              Helper image for the blob copy (default
                                  busybox:1.36)
  NS_AWS / NS_GCP / NS_DATA       Namespace names (defaults: jaiscloud,
                                  jaiscloud-gcp, jaiscloud-data)
  SA_AWS / SA_GCP                 ServiceAccount names (defaults: jaiscloud,
                                  jaiscloud-gcp). deploy.sh renders a kustomize
                                  overlay from the default base and patches the
                                  Namespace names, the ServiceAccount and every
                                  reference to it (Deployment serviceAccountName,
                                  RoleBinding/ClusterRoleBinding subjects, the
                                  JAISCLOUD_K8S_* env values), the DSN
                                  hostnames and the admission policy's
                                  ServiceAccount usernames. The manifests under
                                  deploy/k8s are never edited, so applying them
                                  directly always installs the defaults.

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
  [[ "$(printf '%s' "$yn" | tr '[:upper:]' '[:lower:]')" == "y" ]] || { info "Aborted."; exit 0; }
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

# park_key <cloud> <field> — the park record is per substrate
# (jaiscloud.io/<cloud>.state, .parked-at, .parked-image, .parked-image-id,
# .parked-version), so two substrates sharing one namespace cannot overwrite each
# other's record. The install record's `desired` is per cloud too.
park_key() { printf 'jaiscloud.io/%s.%s' "$1" "$2"; }

# Annotation reads use jsonpath bracket notation: annotation keys contain dots
# and slashes, which the dot-path form cannot express reliably. A missing key
# reads as empty.
ns_annotation() { # ns_annotation <namespace> <key>
  kubectl get namespace "$1" \
    -o jsonpath="{.metadata.annotations['$2']}" 2>/dev/null || true
}

# deployment_env <namespace> <deployment> <name> — the literal env value of one
# variable in the first container ("" when unset).
deployment_env() {
  kubectl -n "$1" get deployment "$2" \
    -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name==\"$3\")].value}" 2>/dev/null || true
}

# digest_from_image_id <imageID> — a repo@sha256:… reference, or "" when the
# value is not a digest (e.g. a bare tag). The container runtime reports it as
# docker-pullable://<repo>@sha256:<hash> or <repo>@sha256:<hash>.
digest_from_image_id() {
  local id=${1#docker-pullable://}
  case "$id" in
    *@sha256:*) printf '%s' "$id" ;;
    *) printf '' ;;
  esac
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
  # One EXIT trap per invocation: keep the rendered-overlay cleanup in the chain
  # so a mode that forwards a port still removes its temp tree.
  trap 'stop_port_forwards; render_cleanup' EXIT
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
    "$(park_key "$cloud" state)=parked"
    "$(park_key "$cloud" parked-at)=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  )
  [[ -n "$image" ]] && args+=("$(park_key "$cloud" parked-image)=$image")
  [[ -n "$image_id" ]] && args+=("$(park_key "$cloud" parked-image-id)=$image_id")
  [[ -n "$version" ]] && args+=("$(park_key "$cloud" parked-version)=$version")
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
  local cloud=$1 other replicas image image_id version kafka_mode
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
      "$(ns_annotation "$NS" "$(park_key "$cloud" parked-image-id)")" \
      "$(ns_annotation "$NS" "$(park_key "$cloud" parked-version)")"
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

  # Managed Kafka is the one engine whose data does not survive a park: the
  # broker has no PVC, so its topics go with the broker Pod while cluster/topic
  # metadata and the namespace stay. The /park response says so
  # (topicDataLost), and this warns before the pods disappear.
  if [[ "$cloud" == "gcp" ]]; then
    kafka_mode=$(deployment_env "$NS" "$DEPLOY" JAISCLOUD_KAFKA_BROKER_MODE)
    case "$kafka_mode" in
      k8s|docker|native)
        warn "Managed Kafka broker topic data is not persistent (the broker has no PVC); topics are lost on park. Cluster/topic metadata and the namespace survive." ;;
    esac
  fi

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

# resume_substrate <cloud> — scale to 1, recreate compute, keep the parked binary.
#   The Deployment image must still be the parked one; otherwise the resume could
#   silently swap the binary (and run the new binary's store migrations) with no
#   operator intent. Without --allow-upgrade this refuses before scaling, and it
#   pins the recorded parked digest, so a moving :latest tag cannot slip a new
#   build in behind the tag. --allow-upgrade is the explicit opt-in.
resume_substrate() {
  local cloud=$1 state parked_image parked_id deploy_image pin_id new_id
  substrate_vars "$cloud"

  if ! kubectl get namespace "$NS" &>/dev/null; then
    die "namespace $NS does not exist — install the $cloud substrate first (see $0 --help)"
  fi
  if ! kubectl -n "$NS" get deployment "$DEPLOY" &>/dev/null; then
    die "deployment/$DEPLOY is missing in $NS — run deploy.sh to install the $cloud substrate"
  fi

  state=$(ns_annotation "$NS" "$(park_key "$cloud" state)")
  parked_image=$(ns_annotation "$NS" "$(park_key "$cloud" parked-image)")
  parked_id=$(ns_annotation "$NS" "$(park_key "$cloud" parked-image-id)")
  if [[ -z "$state" && -z "$parked_image" && -z "$parked_id" ]]; then
    warn "namespace $NS is not marked parked — resuming anyway"
  fi
  deploy_image=$(kubectl -n "$NS" get deployment "$DEPLOY" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)

  # Guard, then pin, strictly BEFORE the scale: a refused resume leaves the
  # substrate parked and untouched.
  if [[ -n "$parked_image" && "$deploy_image" != "$parked_image" ]]; then
    if [[ "$ALLOW_UPGRADE" != "1" ]]; then
      die "the image changed while parked: parked '$parked_image', deployment '$deploy_image'; pass --allow-upgrade to use the new image, or restore the parked image first"
    fi
  fi
  if [[ "$ALLOW_UPGRADE" == "1" ]]; then
    warn "--allow-upgrade: not pinning the resumed image to the parked digest; if the image changed, the store may migrate"
  else
    pin_id=$(digest_from_image_id "$parked_id")
    if [[ -z "$pin_id" ]]; then
      warn "no parked binary digest recorded; cannot pin the resumed image"
    else
      if [[ "$deploy_image" != "$pin_id" ]]; then
        info "Pinning deployment/$DEPLOY to the parked digest (${pin_id##*@})..."
        kubectl -n "$NS" set image "deployment/$DEPLOY" "$DEPLOY=$pin_id" >/dev/null
      fi
    fi
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

  # Still useful when the digest was unknown at park time (nothing to pin).
  if [[ -n "$parked_id" && -n "$new_id" && "$parked_id" != "$new_id" ]]; then
    warn "binary changed while parked: store migrations may run"
    warn "  parked: $parked_id"
    warn "  now:    $new_id"
  fi

  kubectl annotate namespace "$NS" "$(park_key "$cloud" state)=running" --overwrite >/dev/null
  kubectl annotate namespace "$NS" \
    "$(park_key "$cloud" parked-at)-" "$(park_key "$cloud" parked-image)-" \
    "$(park_key "$cloud" parked-image-id)-" "$(park_key "$cloud" parked-version)-" \
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

# dsn_with_db <dsn> <database> — the same server/user/password/options with the
# database component replaced. psql takes the target database either as one
# conninfo string or as -d, not both, so psql_external rewrites the DSN instead
# of passing "-d" alongside it.
dsn_with_db() {
  local dsn=$1 db=$2 scheme rest authority path query=""
  scheme="${dsn%%://*}"
  rest="${dsn#*://}"
  authority="${rest%%/*}"
  path=""
  [[ "$rest" == */* ]] && path="${rest#*/}"
  if [[ "$path" == *"?"* ]]; then
    query="?${path#*\?}"
    path="${path%%\?*}"
  fi
  printf '%s://%s/%s%s' "$scheme" "$authority" "$db" "$query"
}

# psql_external [-d <db>] [--dsn <dsn>] <psql args...> — run psql from a
# short-lived pod in NAMESPACE_DATA (the substrate namespaces cannot reach an
# external server, and the store namespace already exists for the install
# record). The pod is removed on exit (--rm --restart=Never). The DSN defaults
# to STORE_DSN; --dsn carries the per-substrate override when one is configured.
psql_external() {
  local db="" dsn="$STORE_DSN" args=()
  while (($#)); do
    case "$1" in
      -d) db="${2:-}"; shift 2 || shift ;;
      -d*) db="${1#-d}"; shift ;;
      --dsn) dsn="${2:-}"; shift 2 || shift ;;
      --dsn=*) dsn="${1#--dsn=}"; shift ;;
      *) args+=("$1"); shift ;;
    esac
  done
  [[ -n "$db" ]] && dsn=$(dsn_with_db "$dsn" "$db")
  kubectl -n "$NAMESPACE_DATA" run "jaiscloud-store-init-$$" --rm -i --restart=Never \
    --labels=app=jaiscloud-store-init --image="$STORE_INIT_IMAGE" -- \
    psql "$dsn" -v ON_ERROR_STOP=1 ${args+"${args[@]}"}
}

# store_dsn_for_db <db> — the DSN that owns a database: the per-substrate
# override when set (JAISCLOUD_AWS_DSN / JAISCLOUD_GCP_DSN), else STORE_DSN.
store_dsn_for_db() {
  case "$1" in
    "$AWS_DB") external_dsn aws ;;
    "$GCP_DB") external_dsn gcp ;;
    *) printf '%s' "$STORE_DSN" ;;
  esac
}

# ensure_data_namespace — an external store still needs the store namespace: it
# holds the install record and the psql helper pod runs there. The module is
# applied in every install path anyway; this makes the reset paths explicit. The
# data-ns overlay is namespace-data.yaml alone, renamed for NS_DATA.
ensure_data_namespace() {
  apply_overlay data-ns
}

# external_dsn <aws|gcp> — the DSN that substrate's Secret must carry. A
# per-substrate override (JAISCLOUD_AWS_DSN / JAISCLOUD_GCP_DSN) is used verbatim;
# otherwise the base DSN's database component is replaced with that substrate's
# database, so the two substrates keep the one-database-per-cloud invariant on a
# shared external server.
external_dsn() {
  case "$1" in
    aws)
      if [[ -n "${JAISCLOUD_AWS_DSN:-}" ]]; then printf '%s' "$JAISCLOUD_AWS_DSN"
      else dsn_with_db "$STORE_DSN" "$AWS_DB"; fi ;;
    gcp)
      if [[ -n "${JAISCLOUD_GCP_DSN:-}" ]]; then printf '%s' "$JAISCLOUD_GCP_DSN"
      else dsn_with_db "$STORE_DSN" "$GCP_DB"; fi ;;
    *) die "external_dsn: unknown substrate '$1'" ;;
  esac
}

# external_host <dsn> — host[:port] only, never the credentials (for messages).
external_host() {
  local rest="${1#*://}"
  rest="${rest##*@}"
  printf '%s' "${rest%%[/?]*}"
}

# apply_external_dsn_secret <aws|gcp> — point the substrate's DSN Secret at the
# external server. Substrate manifests declare that Secret with the bundled DSN,
# so this runs AFTER the manifest apply and the Deployment is restarted: env
# values are resolved when the pod is created, so an in-place Secret update alone
# would leave a crash-looping pod on the old DSN.
apply_external_dsn_secret() {
  local cloud=$1 ns secret dsn dsn_var
  substrate_vars "$cloud"
  ns=$NS
  dsn_var=$(printf 'JAISCLOUD_%s_DSN' "$(printf '%s' "$cloud" | tr '[:lower:]' '[:upper:]')")
  if [[ "$cloud" == "aws" ]]; then secret="jaiscloud-dsn"; else secret="jaiscloud-gcp-dsn"; fi
  dsn=$(external_dsn "$cloud")
  [[ -n "$dsn" ]] || die "no external DSN for '$cloud' — set --store-dsn/JAISCLOUD_STORE_DSN (or $dsn_var)"
  case "$dsn" in
    postgres://*|postgresql://*) ;;
    *) die "$dsn_var must start with postgres:// or postgresql://" ;;
  esac
  info "Pointing $ns/secret/$secret at the external store..."
  kubectl -n "$ns" create secret generic "$secret" \
    --from-literal=dsn="$dsn" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n "$ns" annotate secret "$secret" jaiscloud.io/store=external --overwrite >/dev/null
  if kubectl -n "$ns" get deployment "$DEPLOY" &>/dev/null; then
    kubectl -n "$ns" rollout restart deployment/"$DEPLOY" >/dev/null
  fi
}

# ensure_store — bring up the store the install needs. Bundled: the data overlay
# (namespace-data + postgres.yaml) and wait for the rollout. External: only the
# store namespace (the external server is the operator's).
ensure_store() {
  if [[ "$STORE_MODE" == "external" ]]; then
    ensure_data_namespace
    info "External store configured — not applying postgres.yaml."
    return 0
  fi
  apply_overlay data
  wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
}

# reset_database <db> — DROP (FORCE terminates sessions) + CREATE the database,
# keeping the Postgres server, its PVC and every other database intact. With an
# external store the statements run from a helper pod against STORE_DSN.
reset_database() {
  local db=$1
  if [[ "$STORE_MODE" == "external" ]]; then
    ensure_data_namespace
    info "Dropping and recreating database '$db' on the external store (server, other databases intact)..."
    psql_external --dsn "$(store_dsn_for_db "$db")" -d postgres \
      -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" \
      -c "CREATE DATABASE $db"
    ok "Database '$db' recreated on the external store."
    return 0
  fi
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
# Idempotent. With an external store the check creates the database on that
# server and refuses when it is unreachable (a missing target stays fatal).
ensure_database() {
  local db=$1 exists
  if [[ "$STORE_MODE" == "external" ]]; then
    ensure_data_namespace
    exists=$(psql_external --dsn "$(store_dsn_for_db "$db")" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | tr -d '[:space:]') \
      || die "cannot reach the external store through STORE_DSN — is the server up and the DSN correct?"
    if [[ "$exists" == "1" ]]; then
      ok "Database '$db' already exists on the external store."
      return 0
    fi
    info "Creating database '$db' on the external store..."
    psql_external --dsn "$(store_dsn_for_db "$db")" -d postgres -c "CREATE DATABASE $db"
    ok "Database '$db' created on the external store."
    return 0
  fi
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

# ── rendered overlays: the default base plus the configured names ────────────
#
# deploy/k8s/*.yaml is the base and stays directly appliable: it installs
# jaiscloud / jaiscloud-gcp / jaiscloud-data with the ServiceAccounts jaiscloud /
# jaiscloud-gcp. NS_AWS, NS_GCP, NS_DATA, SA_AWS and SA_GCP select a custom
# layout. Instead of editing those files (which would break the defaults),
# deploy.sh copies the base it needs into a throwaway kustomize tree and patches
# what is name-dependent, then applies the result with `kubectl apply -k`:
#
#   <render>/overlays/data       namespace-data.yaml + postgres.yaml  (bundled store)
#   <render>/overlays/data-ns    namespace-data.yaml only            (external store,
#                                                                     install record)
#   <render>/overlays/aws        the AWS substrate + its RBAC
#   <render>/overlays/gcp        the GCP substrate + its RBAC
#   <render>/overlays/isolation  the cluster-scoped ValidatingAdmissionPolicy
#
# Each overlay carries its own copy of the base files it uses under base/:
# kustomize's load restrictor refuses a `../../base` escape from the overlay
# root, so the copies are per overlay and every overlay builds on its own
# (`kubectl kustomize <render>/overlays/aws`). The admission policy gets an
# overlay of its own because it is cluster-scoped — kustomize's namespace
# transformer would otherwise stamp a bogus metadata.namespace onto the
# ValidatingAdmissionPolicy and its binding.
#
# What each substrate overlay patches:
#   - the Namespace object's metadata.name (cluster-scoped: the namespace
#     transformer only renames the field on the resources inside it);
#   - the ServiceAccount's metadata.name and the Deployment's
#     spec.template.spec.serviceAccountName;
#   - the RoleBinding/ClusterRoleBinding subjects (kustomize does not rewrite
#     subjects, and a binding must name the real identity);
#   - the ConfigMap/env values the emulator uses to find its own namespace and
#     ServiceAccount (AWS: jaiscloud-config; GCP: the Deployment env), plus the
#     GCP Service FQDN in the endpoint env values;
#   - the DSN Secret's host, which embeds the store namespace;
#   - the admission policy's ServiceAccount usernames.
# Nothing under deploy/k8s is modified; --render-only DIR writes the tree for
# inspection (the render test and reviewers use it) without running kubectl.
RENDER_DIR=""

validate_dns1123_label() { # validate_dns1123_label <description> <value>
  local what=$1 value=$2
  if ! [[ "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || ((${#value} > 63)); then
    die "$what must be a lowercase DNS-1123 label (a-z, 0-9, '-'; at most 63 chars); got '$value'"
  fi
}

validate_names() {
  validate_dns1123_label "NS_AWS" "$NAMESPACE_AWS"
  validate_dns1123_label "NS_GCP" "$NAMESPACE_GCP"
  validate_dns1123_label "NS_DATA" "$NAMESPACE_DATA"
  validate_dns1123_label "SA_AWS" "$SA_AWS"
  validate_dns1123_label "SA_GCP" "$SA_GCP"
}

render_copy() { # render_copy <root> <overlay> <manifest>...
  local root=$1 overlay=$2 manifest
  shift 2
  for manifest in "$@"; do
    cp -f "$K8S_DIR/$manifest" "$root/overlays/$overlay/base/$manifest"
  done
}

render_tree() { # render_tree <directory>
  local root=$1 overlay
  [[ -n "$root" && "$root" != "/" ]] || die "refusing to render the overlay tree into '${root:-<empty>}'"
  mkdir -p "$root"
  rm -rf "$root/overlays"
  for overlay in data data-ns aws gcp isolation; do
    mkdir -p "$root/overlays/$overlay/base" "$root/overlays/$overlay/patches"
  done

  # ── overlays/data: the bundled PostgreSQL store ──────────────────────────
  render_copy "$root" data namespace-data.yaml postgres.yaml
  cat > "$root/overlays/data/patches/namespace-name.yaml" <<EOF
- op: replace
  path: /metadata/name
  value: $NAMESPACE_DATA
EOF
  cat > "$root/overlays/data/patches/postgres-dsn-host.yaml" <<EOF
- op: replace
  path: /stringData/dsn
  value: "postgresql://jaiscloud:jaiscloud@postgres.$NAMESPACE_DATA.svc.cluster.local:5432/jaiscloud"
EOF
  cat > "$root/overlays/data/kustomization.yaml" <<EOF
# Rendered by deploy/deploy.sh (NS_DATA=$NAMESPACE_DATA). Do not edit: rerun
# deploy.sh, or --render-only DIR for an inspectable copy.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $NAMESPACE_DATA
resources:
  - base/namespace-data.yaml
  - base/postgres.yaml
patches:
  # The Namespace object is cluster-scoped, so the namespace transformer does not
  # rename it; patch metadata.name instead.
  - path: patches/namespace-name.yaml
    target:
      kind: Namespace
      name: jaiscloud-data
  # The store's own DSN is a convenience value, but it must name the real
  # Service host: postgres.<NS_DATA>.svc.cluster.local.
  - path: patches/postgres-dsn-host.yaml
    target:
      kind: Secret
      name: postgres-secret
EOF

  # ── overlays/data-ns: the store namespace alone (external store) ─────────
  render_copy "$root" data-ns namespace-data.yaml
  cp -f "$root/overlays/data/patches/namespace-name.yaml" \
        "$root/overlays/data-ns/patches/namespace-name.yaml"
  cat > "$root/overlays/data-ns/kustomization.yaml" <<EOF
# Rendered by deploy/deploy.sh (NS_DATA=$NAMESPACE_DATA): the store namespace
# alone. Used for --store-dsn (nothing of postgres.yaml is applied) and anywhere
# only the install-record namespace must exist.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $NAMESPACE_DATA
resources:
  - base/namespace-data.yaml
patches:
  - path: patches/namespace-name.yaml
    target:
      kind: Namespace
      name: jaiscloud-data
EOF

  # ── overlays/aws: the AWS substrate ──────────────────────────────────────
  render_copy "$root" aws namespace-aws.yaml rbac-shared.yaml rbac-aws.yaml jaiscloud.yaml
  cat > "$root/overlays/aws/patches/namespace-name.yaml" <<EOF
- op: replace
  path: /metadata/name
  value: $NAMESPACE_AWS
EOF
  cat > "$root/overlays/aws/patches/serviceaccount-name.yaml" <<EOF
- op: replace
  path: /metadata/name
  value: $SA_AWS
EOF
  cat > "$root/overlays/aws/patches/deployment-serviceaccount.yaml" <<EOF
- op: replace
  path: /spec/template/spec/serviceAccountName
  value: $SA_AWS
EOF
  cat > "$root/overlays/aws/patches/rolebinding-subject.yaml" <<EOF
- op: replace
  path: /subjects/0/name
  value: $SA_AWS
- op: replace
  path: /subjects/0/namespace
  value: $NAMESPACE_AWS
EOF
  cat > "$root/overlays/aws/patches/clusterrolebinding-subject.yaml" <<EOF
- op: replace
  path: /subjects/0/name
  value: $SA_AWS
- op: replace
  path: /subjects/0/namespace
  value: $NAMESPACE_AWS
EOF
  cat > "$root/overlays/aws/patches/configmap-identity.yaml" <<EOF
- op: replace
  path: /data/JAISCLOUD_K8S_NAMESPACE
  value: $NAMESPACE_AWS
- op: replace
  path: /data/JAISCLOUD_K8S_SA
  value: $SA_AWS
EOF
  cat > "$root/overlays/aws/patches/dsn-host.yaml" <<EOF
- op: replace
  path: /stringData/dsn
  value: "postgres://jaiscloud:jaiscloud@postgres.$NAMESPACE_DATA.svc.cluster.local:5432/jaiscloud"
EOF
  cat > "$root/overlays/aws/kustomization.yaml" <<EOF
# Rendered by deploy/deploy.sh (NS_AWS=$NAMESPACE_AWS SA_AWS=$SA_AWS
# NS_DATA=$NAMESPACE_DATA). Do not edit: rerun deploy.sh with the NS_*/SA_*
# values, or --render-only DIR for an inspectable copy.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $NAMESPACE_AWS
resources:
  - base/namespace-aws.yaml
  - base/rbac-shared.yaml
  - base/rbac-aws.yaml
  - base/jaiscloud.yaml
patches:
  - path: patches/namespace-name.yaml
    target:
      kind: Namespace
      name: jaiscloud
  - path: patches/serviceaccount-name.yaml
    target:
      kind: ServiceAccount
      name: jaiscloud
  - path: patches/deployment-serviceaccount.yaml
    target:
      kind: Deployment
      name: jaiscloud
  - path: patches/rolebinding-subject.yaml
    target:
      kind: RoleBinding
      name: jaiscloud-executor
  - path: patches/clusterrolebinding-subject.yaml
    target:
      kind: ClusterRoleBinding
      name: jaiscloud-namespace-admin-aws
  # The emulator resolves its own namespace/ServiceAccount from here (the
  # executor RoleBindings it creates must bind the real identity).
  - path: patches/configmap-identity.yaml
    target:
      kind: ConfigMap
      name: jaiscloud-config
  # Secrets cannot cross namespaces; this one names the store Service host.
  - path: patches/dsn-host.yaml
    target:
      kind: Secret
      name: jaiscloud-dsn
EOF

  # ── overlays/gcp: the GCP substrate ──────────────────────────────────────
  render_copy "$root" gcp namespace-gcp.yaml rbac-shared.yaml rbac-gcp.yaml jaiscloud-gcp.yaml
  cat > "$root/overlays/gcp/patches/namespace-name.yaml" <<EOF
- op: replace
  path: /metadata/name
  value: $NAMESPACE_GCP
EOF
  cat > "$root/overlays/gcp/patches/serviceaccount-name.yaml" <<EOF
- op: replace
  path: /metadata/name
  value: $SA_GCP
EOF
  cat > "$root/overlays/gcp/patches/deployment-serviceaccount.yaml" <<EOF
- op: replace
  path: /spec/template/spec/serviceAccountName
  value: $SA_GCP
EOF
  # Strategic merge: EnvVar lists merge by name, so this replaces exactly these
  # entries and leaves every other env value to the base manifest.
  cat > "$root/overlays/gcp/patches/deployment-identity.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaiscloud-gcp
spec:
  template:
    spec:
      containers:
        - name: jaiscloud-gcp
          env:
            # The emulator places its own workloads in this namespace and binds
            # this identity in the namespaces it creates.
            - name: JAISCLOUD_K8S_NAMESPACE
              value: $NAMESPACE_GCP
            - name: JAISCLOUD_K8S_SA
              value: $SA_GCP
            - name: JAISCLOUD_K8S_SA_NAMESPACE
              value: $NAMESPACE_GCP
            # Pod-reachable Service addresses: the namespace is part of the FQDN.
            - name: JAISCLOUD_GCS_EMULATOR_ENDPOINT
              value: http://jaiscloud-gcp.$NAMESPACE_GCP.svc.cluster.local:8080
            - name: JAISCLOUD_DATAPROC_HMS_ENDPOINT
              value: jaiscloud-gcp.$NAMESPACE_GCP.svc.cluster.local:9083
EOF
  cat > "$root/overlays/gcp/patches/rolebinding-subject.yaml" <<EOF
- op: replace
  path: /subjects/0/name
  value: $SA_GCP
- op: replace
  path: /subjects/0/namespace
  value: $NAMESPACE_GCP
EOF
  cat > "$root/overlays/gcp/patches/clusterrolebinding-subject.yaml" <<EOF
- op: replace
  path: /subjects/0/name
  value: $SA_GCP
- op: replace
  path: /subjects/0/namespace
  value: $NAMESPACE_GCP
EOF
  cat > "$root/overlays/gcp/patches/dsn-host.yaml" <<EOF
- op: replace
  path: /stringData/dsn
  value: "postgres://jaiscloud:jaiscloud@postgres.$NAMESPACE_DATA.svc.cluster.local:5432/jaiscloud_gcp"
EOF
  cat > "$root/overlays/gcp/kustomization.yaml" <<EOF
# Rendered by deploy/deploy.sh (NS_GCP=$NAMESPACE_GCP SA_GCP=$SA_GCP
# NS_DATA=$NAMESPACE_DATA). Do not edit: rerun deploy.sh with the NS_*/SA_*
# values, or --render-only DIR for an inspectable copy.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $NAMESPACE_GCP
resources:
  - base/namespace-gcp.yaml
  - base/rbac-shared.yaml
  - base/rbac-gcp.yaml
  - base/jaiscloud-gcp.yaml
patches:
  - path: patches/namespace-name.yaml
    target:
      kind: Namespace
      name: jaiscloud-gcp
  - path: patches/serviceaccount-name.yaml
    target:
      kind: ServiceAccount
      name: jaiscloud-gcp
  - path: patches/deployment-serviceaccount.yaml
    target:
      kind: Deployment
      name: jaiscloud-gcp
  - path: patches/deployment-identity.yaml
    target:
      kind: Deployment
      name: jaiscloud-gcp
  - path: patches/rolebinding-subject.yaml
    target:
      kind: RoleBinding
      name: jaiscloud-executor
  - path: patches/clusterrolebinding-subject.yaml
    target:
      kind: ClusterRoleBinding
      name: jaiscloud-namespace-admin-gcp
  # Secrets cannot cross namespaces; this one names the store Service host.
  - path: patches/dsn-host.yaml
    target:
      kind: Secret
      name: jaiscloud-gcp-dsn
EOF

  # ── overlays/isolation: the cluster-scoped admission policy ──────────────
  render_copy "$root" isolation rbac-admission-policy.yaml
  cat > "$root/overlays/isolation/patches/match-usernames.yaml" <<EOF
- op: replace
  path: /spec/matchConditions/0/expression
  value: >-
    request.userInfo.username == 'system:serviceaccount:$NAMESPACE_AWS:$SA_AWS' ||
    request.userInfo.username == 'system:serviceaccount:$NAMESPACE_GCP:$SA_GCP'
# The policy derives the caller's substrate from the AWS username; the ternary
# must name the configured identity, not the shipped one, or an AWS request
# would be treated as the GCP substrate and denied its own namespaces.
- op: replace
  path: /spec/variables/0/expression
  value: >-
    request.userInfo.username == 'system:serviceaccount:$NAMESPACE_AWS:$SA_AWS'
    ? 'aws' : 'gcp'
EOF
  cat > "$root/overlays/isolation/kustomization.yaml" <<EOF
# Rendered by deploy/deploy.sh (AWS=$NAMESPACE_AWS/$SA_AWS GCP=$NAMESPACE_GCP/$SA_GCP).
# The ValidatingAdmissionPolicy and its binding are cluster-scoped, so this
# overlay sets no namespace: the namespace transformer would stamp one onto them.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - base/rbac-admission-policy.yaml
patches:
  # The policy matches ServiceAccount usernames, which embed both namespaces:
  # matchConditions selects the identities and variables derives the substrate.
  - path: patches/match-usernames.yaml
    target:
      group: admissionregistration.k8s.io
      version: v1
      kind: ValidatingAdmissionPolicy
      name: jaiscloud-namespace-isolation
EOF

  cat > "$root/README.md" <<EOF
# Rendered overlay tree (generated by deploy/deploy.sh)

Resolved layout:

| cloud | namespace | ServiceAccount |
|---|---|---|
| AWS | \`$NAMESPACE_AWS\` | \`$SA_AWS\` |
| GCP | \`$NAMESPACE_GCP\` | \`$SA_GCP\` |
| store | \`$NAMESPACE_DATA\` | — |

Overlays (each is self-contained; \`base/\` holds a copy of the manifests it
patches, because kustomize refuses resources outside an overlay's root):

- \`overlays/data\` — the bundled store (\`namespace-data.yaml\` + \`postgres.yaml\`)
- \`overlays/data-ns\` — the store namespace only (external store / install record)
- \`overlays/aws\` — the AWS substrate and its RBAC
- \`overlays/gcp\` — the GCP substrate and its RBAC
- \`overlays/isolation\` — the cluster-scoped namespace-isolation policy

Inspect with \`kubectl kustomize <overlay>\`; apply with
\`kubectl apply -k <overlay>\`. Re-running deploy.sh regenerates this tree.
EOF
}

render_cleanup() {
  if [[ -n "$RENDER_DIR" ]]; then
    rm -rf "$RENDER_DIR"
    RENDER_DIR=""
  fi
}

# render_ensure — build the tree once per invocation in a private temp dir and
# schedule its removal. Mutating paths call it before their first apply; the
# tree is rebuilt from the current NS_*/SA_* values, never cached.
render_ensure() {
  [[ -n "$RENDER_DIR" ]] && return 0
  validate_names
  RENDER_DIR="$(mktemp -d "${TMPDIR:-/tmp}/jaiscloud-render.XXXXXX")"
  trap 'render_cleanup' EXIT
  render_tree "$RENDER_DIR"
  info "Rendered kustomize overlays in $RENDER_DIR"
  info "  AWS: namespace $NAMESPACE_AWS, ServiceAccount $SA_AWS"
  info "  GCP: namespace $NAMESPACE_GCP, ServiceAccount $SA_GCP"
  info "  store: namespace $NAMESPACE_DATA"
}

# apply_overlay <data|data-ns|aws|gcp|isolation> — kubectl apply -k the rendered
# overlay. Every object of that group lands in one apply, so a namespace and its
# RBAC exist before the Deployment that uses them.
apply_overlay() {
  local overlay=$1
  render_ensure
  [[ -f "$RENDER_DIR/overlays/$overlay/kustomization.yaml" ]] \
    || die "no rendered overlay '$overlay' in $RENDER_DIR"
  info "Applying overlay '$overlay' (kubectl apply -k $RENDER_DIR/overlays/$overlay)..."
  kubectl apply -k "$RENDER_DIR/overlays/$overlay"
}

# check_legacy_namespace — the migration may read a pre-split store from a fourth
# namespace, but only deliberately: an unconfirmed LEGACY_NAMESPACE must not point
# at a namespace this install does not own (a typo would dump and then scale down
# a foreign Postgres).
check_legacy_namespace() {
  if [[ -n "$LEGACY_NAMESPACE" \
        && "$LEGACY_NAMESPACE" != "$NAMESPACE_AWS" \
        && "$LEGACY_NAMESPACE" != "$NAMESPACE_GCP" \
        && "$LEGACY_NAMESPACE" != "$NAMESPACE_DATA" \
        && "$LEGACY_CONFIRM" != "1" ]]; then
    die "LEGACY_NAMESPACE='$LEGACY_NAMESPACE' is not one of the JaisCloud namespaces ($NAMESPACE_AWS, $NAMESPACE_GCP, $NAMESPACE_DATA). The migration only reads a store this install owns; set LEGACY_CONFIRM=1 (--legacy-confirm) to confirm '$LEGACY_NAMESPACE' holds the pre-split JaisCloud store."
  fi
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
# its binding, from the rendered isolation overlay (the policy is cluster-scoped
# and matches the substrate ServiceAccount usernames, so it is patched with the
# configured names too). Applied before any namespace/executor object so every
# namespace a substrate creates from now on is admitted under the boundary.
apply_isolation_policy() {
  apply_overlay isolation
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

# apply_aws — the store first (the data overlay: namespace-data.yaml +
# postgres.yaml for a bundled store), then the AWS overlay (rbac-shared.yaml
# declares the ClusterRole the bindings reference; rbac-aws.yaml declares the
# ServiceAccount and both bindings; jaiscloud.yaml runs as that identity). No GCP
# object is created by an AWS-only install.
apply_aws() {
  require_isolation_prereqs
  apply_isolation_policy
  ensure_store
  ensure_database "$AWS_DB"
  apply_overlay aws
  retire_legacy_namespace_binding aws
  [[ "$STORE_MODE" == "external" ]] && apply_external_dsn_secret aws
  record_store "$STORE_MODE"
}

# AWS reset re-applies only the AWS-side objects: postgres.yaml lives entirely
# in the store namespace, which a --reset must never touch.
apply_aws_workloads() {
  apply_overlay aws
  retire_legacy_namespace_binding aws
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
  ca="$(kubectl -n "$istio_ns" get secret "$secret" -o jsonpath='{.data.ca\.crt}' 2>/dev/null | b64decode || true)"
  [[ -n "$ca" ]] || return 0
  info "Restoring ConfigMap $NAMESPACE_AWS/jaiscloud-ca-cert from $istio_ns/$secret..."
  kubectl -n "$NAMESPACE_AWS" create configmap jaiscloud-ca-cert \
    --from-literal=ca.crt="$ca" --labels=jaiscloud.io/issuer=mesh \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
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
# The `store` key (bundled|external) is preserved across intent writes and
# defaulted from STORE_MODE when the record does not carry it yet.
record_intent() {
  local cloud=$1 state=$2 add=${3:-1} installed desired store
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
  store=$(install_record_field store)
  [[ -n "$store" ]] || store="$STORE_MODE"
  write_install_record "$installed" "$desired" "$store"
}

# record_store <bundled|external> — record how the store is provided. Kept in the
# same ConfigMap as the intent so the preflight can explain an external store
# (no deployment/postgres, database probes skipped) instead of calling it missing.
record_store() {
  local mode=$1 installed desired
  if ! kubectl get namespace "$NAMESPACE_DATA" &>/dev/null; then
    return 0
  fi
  installed=$(install_record_field installed)
  desired=$(install_record_field desired)
  write_install_record "$installed" "$desired" "$mode"
}

# write_install_record <installed> <desired> <store> — merge the three keys into
# the install ConfigMap (create or patch; the record is deploy.sh's alone).
write_install_record() {
  local installed=$1 desired=$2 store=$3
  if ! kubectl -n "$NAMESPACE_DATA" create configmap "$INSTALL_CONFIGMAP" \
      --from-literal=schema=1 \
      --from-literal=installed="$installed" \
      --from-literal=desired="$desired" \
      --from-literal=store="$store" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null; then
    warn "could not write the install record $NAMESPACE_DATA/$INSTALL_CONFIGMAP — the preflight will fall back to inference"
    return 0
  fi
  info "Install record ($NAMESPACE_DATA/$INSTALL_CONFIGMAP): installed=${installed:--} desired=${desired:--} store=$store"
}

# print_endpoints — the endpoints and the next safe commands.
# store_mode_from_record — adopt the recorded store mode when this invocation did
# not configure one (park/resume/delete/switch), so the printed next steps do not
# describe a bundled Postgres on an external-store install.
store_mode_from_record() {
  [[ -n "$STORE_DSN" ]] && return 0
  local store
  store=$(install_record_field store 2>/dev/null || true)
  case "$store" in bundled|external) STORE_MODE="$store" ;; esac
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
  if [[ "$STORE_MODE" == "external" ]]; then
    echo "  Store         : external PostgreSQL (DSN from --store-dsn / JAISCLOUD_STORE_DSN)"
  else
    echo "  Postgres      : postgres.$NAMESPACE_DATA.svc.cluster.local:5432  (nodePort 30543)"
  fi
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
    --delete-legacy) DELETE_LEGACY=1 ;;
    --legacy-confirm) LEGACY_CONFIRM=1 ;;
    --allow-upgrade) ALLOW_UPGRADE=1 ;;
    --store-dsn)     [[ -n "${2:-}" ]] || die "--store-dsn needs a DSN (postgres:// or postgresql://)"
                     STORE_DSN="$2"; shift ;;
    --store-dsn=*)   STORE_DSN="${1#*=}"
                     [[ -n "$STORE_DSN" ]] || die "--store-dsn= needs a DSN (postgres:// or postgresql://)" ;;
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
    --render-only|--render-to)
                     [[ -n "${2:-}" ]] || die "--render-only needs a directory"
                     MODE="render-only"; RENDER_TO="$2"; shift ;;
    --render-only=*) MODE="render-only"; RENDER_TO="${1#*=}"
                     [[ -n "$RENDER_TO" ]] || die "--render-only= needs a directory" ;;
    --render-to=*)   MODE="render-only"; RENDER_TO="${1#*=}"
                     [[ -n "$RENDER_TO" ]] || die "--render-to= needs a directory" ;;
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

# ── store mode: the bundled Postgres, or a server you bring ─────────────────
if [[ -n "$STORE_DSN" ]]; then
  case "$STORE_DSN" in
    postgres://*|postgresql://*) ;;
    *) die "--store-dsn/JAISCLOUD_STORE_DSN must start with postgres:// or postgresql://" ;;
  esac
  STORE_MODE="external"
fi
if [[ "$DELETE_LEGACY" == "1" && "$MODE" != "migrate-store" ]]; then
  die "--delete-legacy is only valid with --migrate-store"
fi
if [[ "$MODE" == "migrate-store" && "$STORE_MODE" == "external" ]]; then
  die "--migrate-store moves a pre-split store INTO the bundled store in $NAMESPACE_DATA; an external store target is not supported yet. Run the migration without --store-dsn (or dump/restore the server yourself)."
fi

# ── --check: read-only inspection (never mutates) ───────────────────────────

if [[ "$MODE" == "check" ]]; then
  args=()
  [[ "$PREFLIGHT_JSON" == "1" ]] && args+=(--json)
  [[ "$PREFLIGHT_STRICT" == "1" ]] && args+=(--strict)
  [[ -n "$PREFLIGHT_SUBSTRATE" ]] && args+=(--substrate "$PREFLIGHT_SUBSTRATE")
  [[ -n "$PREFLIGHT_DESIRED" ]] && args+=(--desired "$PREFLIGHT_DESIRED")
  # The preflight reads NS_AWS/NS_GCP/NS_DATA from its environment; hand it the
  # resolved names so a check always inspects the layout deploy.sh would install.
  export NS_AWS="$NAMESPACE_AWS" NS_GCP="$NAMESPACE_GCP" NS_DATA="$NAMESPACE_DATA"
  export SA_AWS SA_GCP
  exec "$REPO_ROOT/scripts/integration-preflight.sh" ${args+"${args[@]}"}
fi

# ── --plan: print what the selected mode would run, without running it ──────

plan() { printf '  + %s\n' "$*"; }

# plan_install_record <clouds to add to installed> <cloud=state pairs to set>
#   The record is merged with whatever is already stored, so the plan shows the
#   intent transition instead of a fixed document.
plan_install_record() {
  plan "kubectl -n $NAMESPACE_DATA get configmap $INSTALL_CONFIGMAP -o jsonpath='{.data}'   # merge installed += $1"
  plan "kubectl -n $NAMESPACE_DATA create configmap $INSTALL_CONFIGMAP --from-literal=schema=1 --from-literal=installed=<merged> --from-literal=desired=<merged, $2> --from-literal=store=$STORE_MODE --dry-run=client -o yaml | kubectl apply -f -"
}

# plan_overlay <data|data-ns|aws|gcp|isolation> — one rendered overlay and the
# names it resolves to. --plan never renders: the tree is built at run time in a
# temp dir, so the plan shows the shape instead of a path that does not exist yet.
plan_overlay() {
  local overlay=$1 detail="" ns="" sa=""
  case "$overlay" in
    data)      ns=$NAMESPACE_DATA
               detail="namespace-data.yaml + postgres.yaml; DSN host postgres.$NAMESPACE_DATA.svc.cluster.local" ;;
    data-ns)   ns=$NAMESPACE_DATA
               detail="namespace-data.yaml only (no postgres.yaml: external store)" ;;
    aws)       ns=$NAMESPACE_AWS; sa=$SA_AWS
               detail="namespace-aws.yaml + rbac-shared.yaml + rbac-aws.yaml + jaiscloud.yaml; namespace $ns, ServiceAccount $sa, DSN host postgres.$NAMESPACE_DATA.svc.cluster.local" ;;
    gcp)       ns=$NAMESPACE_GCP; sa=$SA_GCP
               detail="namespace-gcp.yaml + rbac-shared.yaml + rbac-gcp.yaml + jaiscloud-gcp.yaml; namespace $ns, ServiceAccount $sa, service FQDN jaiscloud-gcp.$ns.svc.cluster.local, DSN host postgres.$NAMESPACE_DATA.svc.cluster.local" ;;
    isolation) ns="(cluster-scoped)"
               detail="rbac-admission-policy.yaml; matchConditions usernames system:serviceaccount:$NAMESPACE_AWS:$SA_AWS and system:serviceaccount:$NAMESPACE_GCP:$SA_GCP" ;;
  esac
  plan "# render overlay '$overlay' (<temp>/overlays/$overlay; namespace $ns${sa:+, ServiceAccount $sa})"
  plan "#   base: $detail"
  plan "kubectl apply -k <render>/overlays/$overlay"
}

# plan_store <cloud> — the store half of an install: bundled applies the data
# overlay and creates the database inside it; external applies the namespace only
# and runs the CREATE/DROP through a psql pod against the DSN.
plan_store() {
  local cloud=$1 ns secret
  if [[ "$cloud" == "aws" ]]; then ns=$NAMESPACE_AWS; secret="jaiscloud-dsn"
  else ns=$NAMESPACE_GCP; secret="jaiscloud-gcp-dsn"; fi
  if [[ "$STORE_MODE" == "external" ]]; then
    plan_overlay data-ns
    plan "kubectl -n $NAMESPACE_DATA run jaiscloud-store-init-<pid> --rm -i --restart=Never --image=$STORE_INIT_IMAGE -- psql <external DSN> -v ON_ERROR_STOP=1 -d postgres -c 'SELECT 1 FROM pg_database ...' || CREATE DATABASE"
    plan "# after the substrate overlay: kubectl -n $ns create secret generic $secret --from-literal=dsn=<external DSN> --dry-run=client -o yaml | kubectl apply -f - ; rollout restart"
  else
    plan_overlay data
    plan "kubectl -n $NAMESPACE_DATA rollout status deployment/postgres --timeout=120s"
    plan "kubectl -n $NAMESPACE_DATA exec deploy/postgres -- psql ... 'SELECT 1 ... database' || CREATE DATABASE"
  fi
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
  plan "curl -sS -X POST http://127.0.0.1:<port>/_jaiscloud/park   # jobs cancelled; broker/revision runtimes stopped; topicDataLost when a real broker stopped"
  if [[ "$cloud" == "gcp" ]]; then
    plan "kubectl -n $NS get deployment $DEPLOY -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"JAISCLOUD_KAFKA_BROKER_MODE\")].value}'   # k8s/docker/native -> warn: broker topics are lost (no broker PVC)"
  fi
  plan "kubectl -n $NS scale deployment $DEPLOY --replicas=0 --timeout=120s"
  plan "kubectl -n $NS wait --for=delete pod -l $SELECTOR --timeout=120s"
  plan "kubectl -n $NS delete jobs --all; delete pods --all; delete deployments -l managed-by=jaiscloud   # ECR proxy"
  plan "for ns in the namespaces labelled jaiscloud.io/managed-by=jaiscloud,jaiscloud.io/substrate=$cloud (other than $NS): kubectl -n \$ns delete jobs,pods,deployments,statefulsets --all"
  plan "kubectl annotate namespace $NS $(park_key "$cloud" state)=parked $(park_key "$cloud" parked-at)=... $(park_key "$cloud" parked-image)=... $(park_key "$cloud" parked-image-id)=... $(park_key "$cloud" parked-version)=... --overwrite"
  plan "# the park record is per substrate ($(park_key "$cloud" state) etc.), so a co-located namespace keeps both substrates' records"
}

plan_lifecycle_resume() {
  local cloud=$1
  substrate_vars "$cloud"
  plan "# resume $cloud: one rollout, no manifest re-apply, no silent binary change"
  plan "kubectl get namespace $NS   # missing -> die 'install it first'; deployment missing -> die 'run deploy.sh'"
  plan "kubectl get namespace $NS -o jsonpath=\"{.metadata.annotations['$(park_key "$cloud" state)']}\"   # warn when not parked"
  plan "kubectl -n $NS get deployment $DEPLOY -o jsonpath='{.spec.template.spec.containers[0].image}'   # compare with $(park_key "$cloud" parked-image)"
  if [[ "$ALLOW_UPGRADE" == "1" ]]; then
    plan "# ALLOW_UPGRADE=1: use the current Deployment image (no pin); warn that the store may migrate"
  else
    plan "# image differs from the parked one -> die BEFORE scaling ('pass --allow-upgrade ...'); never a silent upgrade"
    plan "kubectl -n $NS set image deployment/$DEPLOY $DEPLOY=<$(park_key "$cloud" parked-image-id) repo@sha256:...>   # pin the parked digest; skipped with no recorded digest (warn)"
  fi
  plan "kubectl -n $NS scale deployment $DEPLOY --replicas=1"
  plan "kubectl -n $NS rollout status deployment/$DEPLOY --timeout=120s"
  plan "kubectl -n $NS port-forward svc/$SVC <free 18080-18089>:$PORT   # wait for /_jaiscloud/health"
  plan "curl -sS -X POST http://127.0.0.1:<port>/_jaiscloud/resume   # brokers/revisions recreated"
  plan "# compare the new pod imageID with $(park_key "$cloud" parked-image-id); warn when the binary changed"
  plan "kubectl annotate namespace $NS $(park_key "$cloud" state)=running --overwrite; kubectl annotate namespace $NS $(park_key "$cloud" parked-at)- $(park_key "$cloud" parked-image)- $(park_key "$cloud" parked-image-id)- $(park_key "$cloud" parked-version)-"
}

print_plan() {
  local mode=$1
  info "Plan for mode '$mode' (nothing is executed)"
  info "Resolved names: AWS=$NAMESPACE_AWS/$SA_AWS GCP=$NAMESPACE_GCP/$SA_GCP store=$NAMESPACE_DATA"
  plan "# render the overlay tree from deploy/k8s (the default base) into a temp dir, patching:"
  plan "#   Namespace names, ServiceAccount name + Deployment serviceAccountName,"
  plan "#   RoleBinding/ClusterRoleBinding subjects, jaiscloud-config / GCP env identity,"
  plan "#   DSN hostnames, and the admission policy's ServiceAccount usernames"
  plan "# inspect it without touching the cluster: $0 --render-only <dir>"
  case "$mode" in
    deploy)
      if [[ "$SKIP_IMAGE_BUILD" == "1" ]]; then
        plan "# docker build skipped (SKIP_IMAGE_BUILD=1)"
      else
        plan "docker build -t $IMAGE $REPO_ROOT"
        plan "docker tag $IMAGE $AWS_MANIFEST_IMAGE"
        plan "# kind load / k3d image import when the context is kind-*/k3d-*"
      fi
      plan_overlay isolation
      plan_store "aws"
      plan_overlay aws
      if [[ "$STORE_MODE" == "external" ]]; then
        plan "kubectl -n $NAMESPACE_AWS create secret generic jaiscloud-dsn --from-literal=dsn=<external DSN> --dry-run=client -o yaml | kubectl apply -f -; kubectl -n $NAMESPACE_AWS rollout restart deployment/jaiscloud"
      fi
      plan "kubectl -n $NAMESPACE_AWS rollout status deployment/jaiscloud --timeout=120s"
      plan_install_record "aws" "aws=running"
      ;;
    gcp)
      plan_overlay isolation
      plan_store "gcp"
      plan "kubectl -n $NAMESPACE_DATA exec deploy/postgres -- psql ... CREATE DATABASE jaiscloud_gcp   # before the GCP pod starts"
      plan_overlay gcp
      if [[ "$STORE_MODE" == "external" ]]; then
        plan "kubectl -n $NAMESPACE_GCP create secret generic jaiscloud-gcp-dsn --from-literal=dsn=<external DSN> --dry-run=client -o yaml | kubectl apply -f -; kubectl -n $NAMESPACE_GCP rollout restart deployment/jaiscloud-gcp"
      fi
      [[ -n "${JAISCLOUD_GCP_IMAGE:-}" ]] && plan "kubectl -n $NAMESPACE_GCP set image deployment/jaiscloud-gcp jaiscloud-gcp=$JAISCLOUD_GCP_IMAGE"
      plan "kubectl -n $NAMESPACE_GCP rollout status deployment/jaiscloud-gcp --timeout=180s"
      plan_install_record "gcp" "gcp=running"
      ;;
    migrate-store)
      local OLD_NS="$LEGACY_NAMESPACE" legacy_workload confirm_state
      legacy_workload="$LEGACY_POSTGRES_WORKLOAD"
      [[ "$legacy_workload" == "auto" ]] && legacy_workload="<deployment|statefulset>/postgres"
      if [[ "$LEGACY_CONFIRM" == "1" ]]; then
        confirm_state="LEGACY_CONFIRM=1: an unmarked legacy object is accepted as the pre-split JaisCloud store"
      else
        confirm_state="LEGACY_CONFIRM=0: an unmarked legacy object is refused (--legacy-confirm confirms a pre-split build)"
      fi
      plan "# legacy namespace: $OLD_NS (default $NAMESPACE_AWS; any other value needs LEGACY_CONFIRM=1)"
      plan "# $confirm_state"
      plan "kubectl -n $NAMESPACE_AWS scale deployment jaiscloud --replicas=0   # when present (stops a writer)"
      plan "kubectl -n $OLD_NS scale deployment jaiscloud-gcp --replicas=0       # legacy GCP too, when marked (or LEGACY_CONFIRM=1)"
      plan "# legacy store source: $legacy_workload (auto = deployment/postgres, then statefulset/postgres)"
      plan "kubectl -n $OLD_NS get <kind>/<name> -o jsonpath=\"{.metadata.labels['jaiscloud.io/managed-by']}\"   # must be jaiscloud, else die (LEGACY_CONFIRM=1 overrides)"
      plan "# blob claims: legacy aws=$LEGACY_AWS_BLOBS_PVC gcp=$LEGACY_GCP_BLOBS_PVC -> new aws=$NEW_AWS_BLOBS_PVC gcp=$NEW_GCP_BLOBS_PVC"
      if [[ -n "$LEGACY_DSN" ]]; then
        plan_overlay data
        plan "kubectl -n $NAMESPACE_DATA exec deploy/postgres -- pg_dump -Fc '<LEGACY_DSN>' > <tmp>   # from the NEW store pod; no legacy workload touched"
      else
        plan "kubectl -n $OLD_NS exec $legacy_workload -- pg_dump -U $LEGACY_PG_USER -Fc -d jaiscloud > <tmp>"
        plan "kubectl -n $OLD_NS scale $legacy_workload --replicas=0   # kept, not deleted"
        plan_overlay data
      fi
      plan "kubectl -n $NAMESPACE_DATA exec -i deploy/postgres -- pg_restore ... -d jaiscloud < <tmp>"
      plan "kubectl -n $NAMESPACE_DATA exec -i deploy/postgres -- pg_restore ... -d jaiscloud_gcp < <tmp>"
      if [[ "$DELETE_LEGACY" == "1" ]]; then
        plan "kubectl -n $OLD_NS get <deployment|statefulset>/postgres -o jsonpath=\"{.metadata.labels}\"   # needs jaiscloud.io/managed-by=jaiscloud; a bare app=postgres needs LEGACY_DELETE_FORCE=1"
        plan "kubectl -n $OLD_NS delete <deployment|statefulset>/postgres; delete service postgres postgres-external   # PVCs kept"
      else
        plan "# legacy workload kept: scaled to 0; Deployment/StatefulSet, Services and PVCs stay (pass --delete-legacy to remove the workload)"
      fi
      plan "# legacy GCP: apply the GCP overlay, scale to 0, tar-copy the blob PVC, scale to 1"
      [[ "$OLD_NS" != "$NAMESPACE_AWS" ]] && plan "# AWS blobs move namespaces: tar-copy pvc/$LEGACY_AWS_BLOBS_PVC -> pvc/$NEW_AWS_BLOBS_PVC (migrate_blobs)"
      plan_overlay gcp
      plan_overlay aws
      plan_install_record "aws" "aws=running (and gcp when the legacy GCP workload exists)"
      ;;
    reset)
      plan "kubectl -n $NAMESPACE_AWS scale deployment jaiscloud --replicas=0   # when present"
      [[ "$STORE_MODE" == "external" ]] && plan_overlay data-ns
      plan "kubectl -n $NAMESPACE_DATA psql -c 'DROP DATABASE IF EXISTS jaiscloud WITH (FORCE)' -c 'CREATE DATABASE jaiscloud'   # bundled: inside deploy/postgres; external: -d postgres from a psql pod"
      plan "kubectl delete namespace $NAMESPACE_AWS"
      plan_overlay isolation
      [[ "$STORE_MODE" == "bundled" ]] && plan_overlay data   # only when the store is missing
      plan_overlay aws
      [[ "$STORE_MODE" == "external" ]] && plan "kubectl -n $NAMESPACE_AWS create secret generic jaiscloud-dsn --from-literal=dsn=<external DSN> --dry-run=client -o yaml | kubectl apply -f -; rollout restart"
      plan_install_record "aws" "aws=running"
      ;;
    gcp-reset)
      plan "kubectl -n $NAMESPACE_GCP scale deployment jaiscloud-gcp --replicas=0   # when present"
      plan "kubectl -n $NAMESPACE_GCP delete pvc $GCP_PVC"
      [[ "$STORE_MODE" == "external" ]] && plan_overlay data-ns
      plan "kubectl -n $NAMESPACE_DATA psql -c 'DROP DATABASE IF EXISTS jaiscloud_gcp WITH (FORCE)' -c 'CREATE DATABASE jaiscloud_gcp'   # bundled: deploy/postgres; external: -d postgres from a psql pod"
      plan_overlay isolation
      plan_overlay gcp
      [[ "$STORE_MODE" == "external" ]] && plan "kubectl -n $NAMESPACE_GCP create secret generic jaiscloud-gcp-dsn --from-literal=dsn=<external DSN> --dry-run=client -o yaml | kubectl apply -f -; rollout restart"
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
  if [[ "$STORE_MODE" == "external" ]]; then
    info "Store mode: external (postgres.yaml is not applied; databases via a psql pod; migrate-store is unsupported)"
  else
    info "Store mode: bundled (PostgreSQL in $NAMESPACE_DATA)"
  fi
  info "Re-run without --plan to execute."
}

if [[ "$PLAN" == "1" ]]; then
  print_plan "$MODE"
  exit 0
fi

# ── --render-only DIR: write the overlay tree and stop (no kubectl) ──────────
# Reviewing a custom layout (or the render test) needs the tree without a cluster.
# Names are validated, the tree is written to DIR and left in place, and nothing
# under the repository is touched.
if [[ "$MODE" == "render-only" ]]; then
  validate_names
  render_tree "$RENDER_TO"
  ok "Rendered the overlay tree for AWS=$NAMESPACE_AWS/$SA_AWS GCP=$NAMESPACE_GCP/$SA_GCP store=$NAMESPACE_DATA"
  echo ""
  echo "  AWS   namespace $NAMESPACE_AWS   ServiceAccount $SA_AWS"
  echo "  GCP   namespace $NAMESPACE_GCP   ServiceAccount $SA_GCP"
  echo "  store namespace $NAMESPACE_DATA"
  echo ""
  echo "  overlays:"
  for overlay in data data-ns aws gcp isolation; do
    echo "    $RENDER_TO/overlays/$overlay"
  done
  echo ""
  echo "  inspect: kubectl kustomize $RENDER_TO/overlays/<name>"
  echo "  apply:   kubectl apply -k $RENDER_TO/overlays/<name>   (deploy.sh does this for you)"
  exit 0
fi

# --check reads the overrides and never mutates; --delete/--park/--resume/--switch
# only address the configured names. The migration is the one mode that reads a
# namespace this install may not own, so it keeps the LEGACY_NAMESPACE guard.

# ── --delete [aws|gcp|both]: remove workloads, keep every PVC ───────────────

if [[ "$MODE" == "delete" ]]; then
  check_context
  store_mode_from_record
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
  store_mode_from_record
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
  if [[ "$STORE_MODE" == "external" ]]; then
    warn "The store is external: '$AWS_DB' is dropped on $(external_host "$(external_dsn aws)") — other databases there are kept."
  fi
  confirm "Are you sure?"
  check_context
  require_isolation_prereqs
  apply_isolation_policy

  info "Scaling the AWS deployment to zero..."
  scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud

  if [[ "$STORE_MODE" == "external" ]]; then
    # The psql helper pod runs in NAMESPACE_DATA; make sure it exists before the
    # drop (an external store has no Deployment to create it for us).
    ensure_data_namespace
  fi
  reset_database "$AWS_DB"

  info "Deleting namespace $NAMESPACE_AWS (removes the AWS blobs PVC)..."
  kubectl delete namespace "$NAMESPACE_AWS" --ignore-not-found --timeout=180s

  info "Recreating the AWS substrate..."
  # The store may not exist yet (e.g. --reset on a cluster that never ran the
  # full deploy); bring it up so the recreated AWS pod has a database. An
  # external store is the operator's, so only its namespace is ensured.
  if [[ "$STORE_MODE" == "external" ]]; then
    ensure_data_namespace
  elif ! postgres_deployed; then
    apply_overlay data
    wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  fi
  apply_aws_workloads
  [[ "$STORE_MODE" == "external" ]] && apply_external_dsn_secret aws
  wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"
  record_intent aws running
  record_store "$STORE_MODE"
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

  if [[ "$STORE_MODE" == "external" ]]; then
    ensure_data_namespace
  fi
  reset_database "$GCP_DB"

  info "Re-applying the GCP substrate..."
  apply_overlay gcp
  retire_legacy_namespace_binding gcp
  [[ "$STORE_MODE" == "external" ]] && apply_external_dsn_secret gcp
  wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
  record_intent gcp running
  record_store "$STORE_MODE"
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
  ensure_store
  # Create the database BEFORE starting the GCP pod: a missing DSN target is
  # fatal at startup, so applying the Deployment first would crash-loop it. The
  # store is up by now (ensure_store), so this needs no substrate namespace.
  ensure_database "$GCP_DB"
  info "Applying the GCP substrate overlay..."
  apply_overlay gcp
  retire_legacy_namespace_binding gcp
  [[ "$STORE_MODE" == "external" ]] && apply_external_dsn_secret gcp
  apply_gcp_image_override
  wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
  record_intent gcp running
  record_store "$STORE_MODE"
  ok "GCP substrate is ready in namespace $NAMESPACE_GCP."
  print_endpoints
  exit 0
fi

# ── --migrate-store: move a pre-split single-namespace store ─────────────────
#
# The migration is a data move, not a teardown. By default the legacy Postgres
# is only scaled to 0: its Deployment/StatefulSet, Services and PVCs stay as
# rollback copies. --delete-legacy opts into removing the workload and Services,
# and only after the object proves it is JaisCloud's (the managed-by marker or an
# explicit LEGACY_DELETE_FORCE=1 consent); a PVC is never deleted by this command.

# object_label <namespace> <kind> <name> <key> — one metadata label ("" when
# absent). Bracket notation: the label keys contain dots and a slash.
object_label() {
  kubectl -n "$1" get "$2" "$3" -o jsonpath="{.metadata.labels['$4']}" 2>/dev/null || true
}

# legacy_workload_marked <namespace> <kind> <name> — 0 when the object carries a
# JaisCloud marker, 1 otherwise. Accepted markers:
#   jaiscloud.io/managed-by=jaiscloud       the store's own marker (postgres.yaml)
#   app.kubernetes.io/managed-by=jaiscloud  the conventional equivalent
#   jaiscloud.io/substrate=<any>            what the pre-split GCP emulator carried
# `app=postgres` is NOT proof: any operator-supplied Postgres has it.
legacy_workload_marked() {
  local ns=$1 kind=$2 name=$3
  [[ "$(object_label "$ns" "$kind" "$name" jaiscloud.io/managed-by)" == "jaiscloud" ]] && return 0
  [[ "$(object_label "$ns" "$kind" "$name" app.kubernetes.io/managed-by)" == "jaiscloud" ]] && return 0
  [[ -n "$(object_label "$ns" "$kind" "$name" jaiscloud.io/substrate)" ]] && return 0
  return 1
}

# migrate_source_workload — resolve LEGACY_POSTGRES_WORKLOAD (auto: Deployment,
# then StatefulSet) into LEGACY_KIND/LEGACY_NAME and validate the object exists.
# The object must prove it is the JaisCloud store (legacy_workload_marked) or the
# operator must confirm it with LEGACY_CONFIRM=1: the tooling will not dump from,
# scale down or delete a workload it has no evidence it created. With
# DELETE_LEGACY=1 the deletion consent is validated up front too, so a refused
# deletion cannot leave a half-migrated cluster.
migrate_source_workload() {
  local old_ns=$1 label
  case "$LEGACY_POSTGRES_WORKLOAD" in
    auto)
      if kubectl -n "$old_ns" get deployment postgres &>/dev/null; then
        LEGACY_KIND=deployment; LEGACY_NAME=postgres
      elif kubectl -n "$old_ns" get statefulset postgres &>/dev/null; then
        LEGACY_KIND=statefulset; LEGACY_NAME=postgres
      else
        die "no legacy deployment/postgres or statefulset/postgres in namespace $old_ns — nothing to migrate. Point LEGACY_NAMESPACE at the old install, name the object with LEGACY_POSTGRES_WORKLOAD=<deployment|statefulset>/<name>, or set LEGACY_DSN for a store outside the cluster."
      fi ;;
    deployment/*|statefulset/*)
      LEGACY_KIND="${LEGACY_POSTGRES_WORKLOAD%%/*}"
      LEGACY_NAME="${LEGACY_POSTGRES_WORKLOAD#*/}"
      kubectl -n "$old_ns" get "$LEGACY_KIND" "$LEGACY_NAME" &>/dev/null \
        || die "LEGACY_POSTGRES_WORKLOAD=$LEGACY_POSTGRES_WORKLOAD does not exist in namespace $old_ns" ;;
    *)
      die "LEGACY_POSTGRES_WORKLOAD must be 'auto' or '<deployment|statefulset>/<name>' (got '$LEGACY_POSTGRES_WORKLOAD')" ;;
  esac
  LEGACY_WORKLOAD="$LEGACY_KIND/$LEGACY_NAME"
  label=$(object_label "$old_ns" "$LEGACY_KIND" "$LEGACY_NAME" app)
  if legacy_workload_marked "$old_ns" "$LEGACY_KIND" "$LEGACY_NAME"; then
    LEGACY_MARKED=1
    info "$old_ns/$LEGACY_WORKLOAD carries the JaisCloud managed-by marker"
  elif [[ "$LEGACY_CONFIRM" == "1" ]]; then
    LEGACY_MARKED=0
    warn "LEGACY_CONFIRM=1 — migrating $old_ns/$LEGACY_WORKLOAD although it is not marked jaiscloud.io/managed-by=jaiscloud (app='${label:-<none>}'); this is the operator's explicit confirmation that it is the pre-split JaisCloud store"
  else
    die "refusing to migrate $old_ns/$LEGACY_WORKLOAD: it is not marked jaiscloud.io/managed-by=jaiscloud (app='${label:-<none>}'); set LEGACY_CONFIRM=1 only if this is the pre-split JaisCloud store"
  fi
  if [[ "$DELETE_LEGACY" == "1" ]]; then
    if [[ "$LEGACY_MARKED" == "1" ]]; then
      :
    elif [[ "$label" == "postgres" && "$LEGACY_DELETE_FORCE" != "1" ]]; then
      die "refusing to delete $old_ns/$LEGACY_WORKLOAD: it carries app=postgres but not the JaisCloud managed-by marker, so this tooling cannot prove it created it. Set LEGACY_DELETE_FORCE=1 to delete it anyway; its PVC is never deleted."
    elif [[ "$label" != "postgres" && "$LEGACY_DELETE_FORCE" != "1" ]]; then
      die "refusing to delete $old_ns/$LEGACY_WORKLOAD: it carries neither jaiscloud.io/managed-by=jaiscloud nor label app=postgres (app='${label:-<none>}'), so this tooling cannot prove it created it. Set LEGACY_DELETE_FORCE=1 to delete it anyway; its PVC is never deleted."
    else
      warn "LEGACY_DELETE_FORCE=1 — deleting $old_ns/$LEGACY_WORKLOAD without the JaisCloud marker (app='${label:-<none>}')"
    fi
  fi
}

if [[ "$MODE" == "migrate-store" ]]; then
  check_context
  # The migration reads a namespace this install may not own; the guard must run
  # before anything is scaled, dumped or applied.
  check_legacy_namespace
  require_isolation_prereqs
  apply_isolation_policy
  # Before the namespace split, Postgres ran as deployment/postgres in the AWS
  # namespace and held both clouds' schemas in database `jaiscloud`.
  OLD_NS="$LEGACY_NAMESPACE"
  require kubectl
  LEGACY_KIND="" LEGACY_NAME="" LEGACY_WORKLOAD="" LEGACY_MARKED=0

  if kubectl -n "$NAMESPACE_DATA" get pvc "$POSTGRES_PVC" &>/dev/null; then
    if [[ "$MIGRATE_FORCE" == "1" ]]; then
      warn "pvc/$POSTGRES_PVC already exists in $NAMESPACE_DATA — MIGRATE_FORCE=1 continues; the restore overwrites the schema, not the volume"
    else
      die "pvc/$POSTGRES_PVC already exists in $NAMESPACE_DATA — the new store is deployed; refusing to overwrite it (drop it first if this is a retry, or set MIGRATE_FORCE=1)"
    fi
  fi

  # ── where the dump comes from ─────────────────────────────────────────────
  if [[ -n "$LEGACY_DSN" ]]; then
    case "$LEGACY_DSN" in
      postgres://*|postgresql://*) ;;
      *) die "LEGACY_DSN must start with postgres:// or postgresql://" ;;
    esac
    info "Legacy store source: LEGACY_DSN (dumped from the NEW store pod; no legacy store workload is scaled or deleted; an explicit source, so no managed-by marker applies)"
  else
    migrate_source_workload "$OLD_NS"
    info "Legacy store source: $OLD_NS/$LEGACY_WORKLOAD"
  fi

  # A pre-split install ran the GCP emulator in the AWS namespace too, with its
  # object bytes on jaiscloud-gcp-blobs-pvc there. Detect it so the bytes are
  # migrated (a PVC cannot move namespaces). The workload must show it is ours:
  # a Deployment that merely happens to be named jaiscloud-gcp is not evidence.
  legacy_gcp=0
  legacy_gcp_marked=0
  if kubectl -n "$OLD_NS" get deployment jaiscloud-gcp &>/dev/null; then
    if legacy_workload_marked "$OLD_NS" deployment jaiscloud-gcp; then
      legacy_gcp=1 legacy_gcp_marked=1
      info "Legacy GCP emulator found in $OLD_NS (marked); its object bytes will be migrated."
    elif [[ "$LEGACY_CONFIRM" == "1" ]]; then
      legacy_gcp=1
      warn "LEGACY_CONFIRM=1 — treating $OLD_NS/deployment/jaiscloud-gcp as the pre-split GCP emulator although it is not marked jaiscloud.io/managed-by=jaiscloud"
    else
      die "refusing to migrate $OLD_NS/deployment/jaiscloud-gcp: it is not marked jaiscloud.io/managed-by=jaiscloud; set LEGACY_CONFIRM=1 only if this is the pre-split JaisCloud GCP emulator"
    fi
    if [[ "$legacy_gcp" -eq 1 && "$legacy_gcp_marked" -eq 0 \
          && "$DELETE_LEGACY" == "1" && "$LEGACY_DELETE_FORCE" != "1" ]]; then
      die "refusing to delete $OLD_NS/deployment/jaiscloud-gcp: it is not marked jaiscloud.io/managed-by=jaiscloud, so this tooling cannot prove it created it. Set LEGACY_DELETE_FORCE=1 to delete it anyway (the migration still copies its bytes), or omit --delete-legacy."
    fi
  fi

  # Stop the writers BEFORE the dump: anything they write after the snapshot
  # would be silently lost. The legacy Postgres stays up (it is the dump source).
  info "Stopping the emulators that write to the legacy store..."
  scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud
  if [[ "$legacy_gcp" -eq 1 ]]; then
    scale_to_zero "$OLD_NS" jaiscloud-gcp app=jaiscloud-gcp
  fi

  DUMP_FILE="$(mktemp "${TMPDIR:-/tmp}/jaiscloud-store.XXXXXX.dump")"
  # Keep render_cleanup (the overlay tree) alongside the dump cleanup: only one
  # EXIT trap runs, so this one calls both.
  trap 'rm -f "$DUMP_FILE"; render_cleanup' EXIT

  if [[ -n "$LEGACY_DSN" ]]; then
    # The new store must be up: its pod (which ships pg_dump) is the dump client,
    # so the external server is reached without a legacy workload in the cluster.
    info "Starting the new store in $NAMESPACE_DATA (the dump client)..."
    apply_overlay data
    wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
    ensure_database "$GCP_DB"
    info "Dumping the LEGACY_DSN server from the new store pod..."
    kubectl -n "$NAMESPACE_DATA" exec "$POSTGRES_DEPLOYMENT" -- \
      pg_dump --no-owner --no-privileges -Fc "$LEGACY_DSN" > "$DUMP_FILE"
  else
    info "Dumping database '$AWS_DB' from $OLD_NS/$LEGACY_WORKLOAD..."
    kubectl -n "$OLD_NS" exec "$LEGACY_WORKLOAD" -- \
      pg_dump -U "$LEGACY_PG_USER" --no-owner --no-privileges -Fc -d "$AWS_DB" > "$DUMP_FILE"
    [[ -s "$DUMP_FILE" ]] || die "the dump is empty — refusing to continue"

    # The legacy workload is kept (a rollback copy); it simply must not keep
    # running against the old volume while the new store serves.
    info "Scaling the legacy Postgres to 0 (the dump is complete; kept for rollback)..."
    kubectl -n "$OLD_NS" scale "$LEGACY_WORKLOAD" --replicas=0 --timeout=120s >/dev/null
    kubectl -n "$OLD_NS" wait --for=delete pod -l app=postgres --timeout=120s >/dev/null 2>&1 \
      || warn "legacy Postgres pods in $OLD_NS did not terminate within 120s"

    info "Starting the new store in $NAMESPACE_DATA..."
    apply_overlay data
    wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
    ensure_database "$GCP_DB"
  fi
  [[ -s "$DUMP_FILE" ]] || die "the dump is empty — refusing to continue"

  info "Restoring into '$AWS_DB' and '$GCP_DB' (pre-split GCP schema travels too)..."
  kubectl -n "$NAMESPACE_DATA" exec -i "$POSTGRES_DEPLOYMENT" -- \
    pg_restore -U jaiscloud --no-owner --no-privileges --clean --if-exists -d "$AWS_DB" < "$DUMP_FILE"
  kubectl -n "$NAMESPACE_DATA" exec -i "$POSTGRES_DEPLOYMENT" -- \
    pg_restore -U jaiscloud --no-owner --no-privileges --clean --if-exists -d "$GCP_DB" < "$DUMP_FILE"

  # ── the legacy workloads: kept by default, removed only on request ────────
  if [[ -n "$LEGACY_WORKLOAD" ]]; then
    if [[ "$DELETE_LEGACY" == "1" ]]; then
      info "Deleting $OLD_NS/$LEGACY_WORKLOAD and the postgres Services (label verified; PVCs are kept)..."
      kubectl -n "$OLD_NS" delete "$LEGACY_KIND" "$LEGACY_NAME" --ignore-not-found
      kubectl -n "$OLD_NS" delete service postgres postgres-external --ignore-not-found
    else
      info "Legacy Postgres kept: $OLD_NS/$LEGACY_WORKLOAD is scaled to 0; its Services and volumes stay."
    fi
  fi

  if [[ "$legacy_gcp" -eq 1 ]]; then
    info "Bringing up the GCP substrate in $NAMESPACE_GCP (paused for the byte copy)..."
    apply_overlay gcp
    retire_legacy_namespace_binding gcp
    apply_gcp_image_override
    scale_to_zero "$NAMESPACE_GCP" jaiscloud-gcp app=jaiscloud-gcp
    if [[ "$OLD_NS" == "$NAMESPACE_GCP" ]]; then
      info "The legacy GCP claim already lives in $NAMESPACE_GCP — no namespace move needed."
    elif kubectl -n "$OLD_NS" get pvc "$LEGACY_GCP_BLOBS_PVC" &>/dev/null; then
      migrate_blobs "$OLD_NS" "$LEGACY_GCP_BLOBS_PVC" "$NAMESPACE_GCP" "$NEW_GCP_BLOBS_PVC"
    else
      warn "no pvc/$LEGACY_GCP_BLOBS_PVC in $OLD_NS — no GCP object bytes to copy"
    fi
    apply_overlay gcp
    apply_gcp_image_override
    wait_for_rollout deployment/jaiscloud-gcp "$NAMESPACE_GCP" 180s
    if [[ "$DELETE_LEGACY" == "1" ]]; then
      # The legacy GCP workload must not keep running in the legacy namespace.
      kubectl -n "$OLD_NS" delete deployment jaiscloud-gcp --ignore-not-found
      kubectl -n "$OLD_NS" delete service jaiscloud-gcp --ignore-not-found
    else
      info "Legacy GCP workload kept in $OLD_NS (scaled to 0): deployment/jaiscloud-gcp + service/jaiscloud-gcp."
    fi
  fi

  info "Recreating the AWS substrate against the migrated store..."
  apply_aws_workloads
  if [[ "$OLD_NS" != "$NAMESPACE_AWS" ]]; then
    # The claim is moving namespaces: pause the substrate, copy, resume. When the
    # legacy namespace IS the AWS namespace the claim never moved.
    if kubectl -n "$OLD_NS" get pvc "$LEGACY_AWS_BLOBS_PVC" &>/dev/null; then
      scale_to_zero "$NAMESPACE_AWS" jaiscloud app=jaiscloud
      migrate_blobs "$OLD_NS" "$LEGACY_AWS_BLOBS_PVC" "$NAMESPACE_AWS" "$NEW_AWS_BLOBS_PVC"
      kubectl -n "$NAMESPACE_AWS" scale deployment jaiscloud --replicas=1 >/dev/null
    else
      warn "no pvc/$LEGACY_AWS_BLOBS_PVC in $OLD_NS — no AWS object bytes to copy"
    fi
  fi
  wait_for_rollout deployment/jaiscloud "$NAMESPACE_AWS"

  record_intent aws running
  record_store "$STORE_MODE"
  [[ "$legacy_gcp" -eq 1 ]] && record_intent gcp running
  ok "Store migrated from $OLD_NS to $NAMESPACE_DATA."
  echo ""
  if [[ "$DELETE_LEGACY" == "1" ]]; then
    echo "  Legacy workloads removed. Retained volumes (rollback copies):"
  else
    echo "  Legacy workloads kept (scaled to 0). Retained volumes (rollback copies):"
  fi
  if [[ -z "$LEGACY_WORKLOAD" ]]; then
    echo "    - the LEGACY_DSN server itself (outside this cluster)"
  else
    echo "    - $OLD_NS/pvc/$POSTGRES_PVC (the pre-split store)"
  fi
  if [[ "$legacy_gcp" -eq 1 ]]; then
    echo "    - $OLD_NS/pvc/$LEGACY_GCP_BLOBS_PVC (the pre-split GCP object bytes, now copied)"
  fi
  if [[ "$OLD_NS" != "$NAMESPACE_AWS" ]] && kubectl -n "$OLD_NS" get pvc "$LEGACY_AWS_BLOBS_PVC" &>/dev/null; then
    echo "    - $OLD_NS/pvc/$LEGACY_AWS_BLOBS_PVC (the pre-split AWS object bytes, now copied)"
  fi
  echo "  Verify both substrates, then remove what is left:"
  [[ "$DELETE_LEGACY" == "1" ]] || echo "    kubectl -n $OLD_NS delete ${LEGACY_WORKLOAD:-deployment/postgres}"
  [[ "$legacy_gcp" -eq 1 ]] && [[ "$DELETE_LEGACY" != "1" ]] && echo "    kubectl -n $OLD_NS delete deployment jaiscloud-gcp service jaiscloud-gcp"
  [[ -n "$LEGACY_WORKLOAD" ]] && echo "    kubectl -n $OLD_NS delete service postgres postgres-external"
  echo "    kubectl -n $OLD_NS delete pvc $POSTGRES_PVC $LEGACY_GCP_BLOBS_PVC $LEGACY_AWS_BLOBS_PVC --ignore-not-found"
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

info "Applying the AWS substrate (rendered overlay; NS_AWS=$NAMESPACE_AWS SA_AWS=$SA_AWS)..."
if [[ "$STORE_MODE" == "external" ]]; then
  info "  overlays/data-ns: namespace-data.yaml (install record)"
  info "  overlays/aws: namespace-aws.yaml rbac-shared.yaml rbac-aws.yaml jaiscloud.yaml"
  info "  external store: postgres.yaml is NOT applied; the DSN Secret points at your server"
else
  info "  overlays/data: namespace-data.yaml postgres.yaml"
  info "  overlays/aws: namespace-aws.yaml rbac-shared.yaml rbac-aws.yaml jaiscloud.yaml"
fi
apply_aws

if [[ "$STORE_MODE" == "external" ]]; then
  info "External store configured — skipping the bundled Postgres rollout."
else
  wait_for_rollout "$POSTGRES_DEPLOYMENT" "$NAMESPACE_DATA"
  ok "Postgres is ready."
fi
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
