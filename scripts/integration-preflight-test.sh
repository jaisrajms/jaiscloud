#!/usr/bin/env bash
#
# integration-preflight-test.sh — offline tests for the two safety predicates in
# scripts/integration-preflight.sh, with a fake `kubectl` (no cluster, no
# mutations, no kubectl binary needed):
#
#   leaks   a cloud outside the intent is a leak only when it has real objects:
#           its Deployment, ServiceAccount, database or PVCs, or a namespace
#           labelled for that substrate. A namespace that merely exists (a shared,
#           long-lived namespace) is NOT a leak. This is the regression that
#           matters when an AWS-only install shares a cluster with other tenants.
#
#   legacy  deployment/statefulset `postgres` and deployment `jaiscloud-gcp` in
#           the AWS namespace are a *pending migration* only when they carry a
#           JaisCloud marker (jaiscloud.io/managed-by=jaiscloud, the conventional
#           app.kubernetes.io/managed-by=jaiscloud, or jaiscloud.io/substrate).
#           An unmarked object of the same name is legacy.candidate=true plus a
#           warning, never a migrate-store recommendation and never a --strict
#           failure — it may be someone else's Postgres.
#
#   kafka   a real Managed Kafka broker mode is reported (and warned about) so a
#           park's topic loss is never a surprise.
#
# The fixture is one fake cluster written as `kind|namespace|name|key=value;...`
# lines; the fake kubectl answers the read-only probes the preflight makes.
#
# Usage:
#   scripts/integration-preflight-test.sh          # quiet: one line per case
#   VERBOSE=1 scripts/integration-preflight-test.sh
#
# Requires: bash, awk, sed, jq. The real kubectl is never invoked.
# Exit: 0 only when every assertion holds.
set -uo pipefail

PROG="$(basename "$0")"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFLIGHT="$REPO_ROOT/scripts/integration-preflight.sh"
VERBOSE="${VERBOSE:-0}"

command -v jq >/dev/null 2>&1 || { echo "$PROG: jq is required for the JSON assertions" >&2; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/jaiscloud-preflight-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/fixtures"

PASSED=0
FAILED=0
pass() { PASSED=$((PASSED + 1)); printf '  PASS  %s\n' "$*"; }
fail() { FAILED=$((FAILED + 1)); printf '  FAIL  %s\n' "$*" >&2; }
head_() { printf '\n== %s\n' "$*"; }

# ── the fake kubectl ────────────────────────────────────────────────────────
# Every answer comes from $FIXTURE/resources.txt. A probe succeeds when its
# record exists, so the preflight's read-only logic runs unchanged.
cat > "$TMP/bin/kubectl" <<'SHIM'
#!/usr/bin/env bash
# fake kubectl for scripts/integration-preflight-test.sh — read-only.
set -u
set -f
fixture="${FIXTURE:?FIXTURE must point at a fixture directory}"
resources="$fixture/resources.txt"

args=()
while (($#)); do
  case "$1" in
    --context|--request-timeout) shift ;;
    --context=*|--request-timeout=*) ;;
    *) args+=("$1") ;;
  esac
  shift
done
set -- ${args+"${args[@]}"}

present() { # present <kind> <namespace> <name>
  awk -F'|' -v k="$1" -v n="$2" -v m="$3" \
    '$1==k && $3==m && ($2==n || $2=="-" || n=="") {found=1} END {exit found ? 0 : 1}' "$resources"
}

data_of() { # data_of <kind> <namespace> <name>
  awk -F'|' -v k="$1" -v n="$2" -v m="$3" \
    '$1==k && $3==m && ($2==n || $2=="-" || n=="") {print $4; exit}' "$resources"
}

field_val() { # field_val <data> <key> — the value of key= in a ;-separated record
  local data=$1 key=$2 entry
  local oldifs=$IFS
  IFS=';'
  for entry in $data; do
    if [[ "$entry" == "$key="* ]]; then
      IFS=$oldifs
      printf '%s' "${entry#*=}"
      return 0
    fi
  done
  IFS=$oldifs
  return 0
}

# answer_template <template> <data> — the value the preflight's go-template or
# jsonpath would produce for this record. Only the forms the preflight uses.
answer_template() {
  local template=$1 data=$2 key
  case "$template" in
    *'.metadata.labels'*)
      key=$(printf '%s' "$template" | sed -n 's/.*index \.metadata\.labels "\([^"]*\)".*/\1/p')
      [[ -n "$key" ]] || key=$(printf '%s' "$template" | sed -n "s/.*labels\['\([^']*\)'\].*/\1/p")
      field_val "$data" "label.$key" ;;
    *'.metadata.annotations'*)
      key=$(printf '%s' "$template" | sed -n "s/.*annotations\['\([^']*\)'\].*/\1/p")
      field_val "$data" "annotation.$key" ;;
    *'.data "installed"'*) field_val "$data" "data.installed" ;;
    *'.data "desired"'*)   field_val "$data" "data.desired" ;;
    *'.data "store"'*)     field_val "$data" "data.store" ;;
    *'.imagePullPolicy'*)  field_val "$data" "imagePullPolicy" ;;
    *'.image'*)            field_val "$data" "image" ;;
    *'.status.availableReplicas'*) field_val "$data" "available" ;;
    *'.status.containerStatuses'*) field_val "$data" "imageID" ;;
    *'.status.phase'*)     field_val "$data" "phase" ;;
    *'capacity "storage"'*) field_val "$data" "capacity" ;;
    *'.lastState.terminated'*) : ;;
    *'.status.loadBalancer.ingress'*) : ;;
    *'env[?(@.name=='*)
      key=$(printf '%s' "$template" | sed -n 's/.*@\.name=="\([^"]*\)".*/\1/p')
      field_val "$data" "env.$key" ;;
    *'.spec.replicas'*)    field_val "$data" "replicas" ;;
    *'containers[0].image'*) field_val "$data" "image" ;;
    *) : ;;
  esac
}

# kubectl accepts `-n NS` before the verb; the preflight always writes it there.
ns_pre=""
while (($#)); do
  case "$1" in
    -n|--namespace) ns_pre="${2:-}"; shift 2 || shift ;;
    *) break ;;
  esac
done
verb=${1:-}
shift || true
case "$verb" in
  config)
    case "${1:-}" in
      current-context) echo "fake-context" ;;
      view) echo "https://fake.example:6443" ;;
      *) : ;;
    esac
    exit 0 ;;
  cluster-info)
    echo "Kubernetes control plane is running at https://fake.example:6443"
    exit 0 ;;
  version)
    printf '{"serverVersion":{"major":"1","minor":"31","gitVersion":"v1.31.0"}}\n'
    exit 0 ;;
  exec)
    # db_exists: `exec deploy/postgres -- psql ... datname='<db>'`; the fake
    # store has a record per database.
    all="$*"
    db=$(printf '%s' "$all" | sed -n "s/.*datname='\([^']*\)'.*/\1/p")
    if [[ -n "$db" ]]; then
      present db jaiscloud-data "$db" && printf '1\n'
    fi
    exit 0 ;;
  get) : ;;
  *) exit 0 ;;
esac

if [[ "${1:-}" == "--raw" ]]; then
  printf '{"major":"1","minor":"31","gitVersion":"v1.31.0"}\n'
  exit 0
fi

ns="$ns_pre"
template=""
positional=()
while (($#)); do
  case "$1" in
    -n|--namespace) ns="${2:-}"; shift 2 || shift ;;
    -o) template="${2:-}"; shift 2 || shift ;;
    -l|--selector|--field-selector) shift 2 || shift ;;
    --no-headers|--ignore-not-found|-A|--all-namespaces|-o=*) shift ;;
    -*) shift ;;
    *) positional+=("$1"); shift ;;
  esac
done

kind="${positional[0]:-}"
name="${positional[1]:-}"
case "$kind" in
  */?*) name="${kind#*/}"; kind="${kind%%/*}" ;;
esac
case "$kind" in
  sa) kind=serviceaccount ;;
  svc) kind=service ;;
  pvc) kind=persistentvolumeclaim ;;
  cm) kind=configmap ;;
  ns) kind=namespace ;;
  deploy) kind=deployment ;;
  sts) kind=statefulset ;;
esac
[[ -n "$name" ]] || exit 0
present "$kind" "$ns" "$name" || exit 1
data=$(data_of "$kind" "$ns" "$name")
printf '%s' "$(answer_template "$template" "$data")"
exit 0
SHIM
chmod +x "$TMP/bin/kubectl"

# ── fixtures ────────────────────────────────────────────────────────────────
# base_fixture — a healthy AWS install: the store in jaiscloud-data (marked), the
# AWS substrate, its PVCs and ServiceAccount, the admission policy and the
# per-substrate namespace-admin binding. No GCP objects, no GCP database.
base_fixture() {
  cat <<'EOF'
namespace|jaiscloud|jaiscloud|label.jaiscloud.io/role=substrate;label.jaiscloud.io/substrate=aws
namespace|jaiscloud-data|jaiscloud-data|label.jaiscloud.io/role=data
deployment|jaiscloud|jaiscloud|image=jaisraj/jaiscloud-aws:latest;imagePullPolicy=IfNotPresent;replicas=1;available=1;imageID=docker-pullable://jaisraj/jaiscloud-aws@sha256:aaaa
serviceaccount|jaiscloud|jaiscloud|
persistentvolumeclaim|jaiscloud|jaiscloud-blobs-pvc|phase=Bound;capacity=5Gi
persistentvolumeclaim|jaiscloud|jaiscloud-state-pvc|phase=Bound;capacity=64Mi
deployment|jaiscloud-data|postgres|image=postgres:16-alpine;replicas=1;available=1;label.app=postgres;label.jaiscloud.io/managed-by=jaiscloud
persistentvolumeclaim|jaiscloud-data|postgres-pvc|phase=Bound;capacity=1Gi
db|jaiscloud-data|jaiscloud|
validatingadmissionpolicy|-|jaiscloud-namespace-isolation|
clusterrolebinding|-|jaiscloud-namespace-admin|
clusterrolebinding|-|jaiscloud-namespace-admin-aws|
EOF
}

# gcp_substrate — the objects an installed GCP substrate owns, appended to a
# fixture so a case can add them one at a time.
gcp_substrate() {
  cat <<'EOF'
namespace|jaiscloud-gcp|jaiscloud-gcp|label.jaiscloud.io/role=substrate;label.jaiscloud.io/substrate=gcp
deployment|jaiscloud-gcp|jaiscloud-gcp|image=gcr.io/example/jaiscloud-gcp:1;imagePullPolicy=IfNotPresent;replicas=1;available=1;imageID=docker-pullable://gcr.io/example/jaiscloud-gcp@sha256:bbbb;env.JAISCLOUD_K8S_SPARK_IMAGE=apache/spark:3.5.0;env.JAISCLOUD_KAFKA_BROKER_MODE=k8s
serviceaccount|jaiscloud-gcp|jaiscloud-gcp|
persistentvolumeclaim|jaiscloud-gcp|jaiscloud-gcp-blobs-pvc|phase=Bound;capacity=5Gi
persistentvolumeclaim|jaiscloud-gcp|jaiscloud-gcp-state-pvc|phase=Bound;capacity=64Mi
db|jaiscloud-data|jaiscloud_gcp|
clusterrolebinding|-|jaiscloud-namespace-admin-gcp|
EOF
}

new_fixture() { # new_fixture <name> — prints the fixture directory
  local dir="$TMP/fixtures/$1"
  mkdir -p "$dir"
  : > "$dir/resources.txt"
  printf '%s' "$dir"
}

run_preflight() { # run_preflight <fixture> <args...>; sets RUN_STATUS and RUN_OUT
  local fixture=$1
  shift
  RUN_OUT=$(PATH="$TMP/bin:$PATH" FIXTURE="$fixture" KUBE_CONTEXT= \
    NS_AWS=jaiscloud NS_GCP=jaiscloud-gcp NS_DATA=jaiscloud-data \
    "$PREFLIGHT" "$@" 2>&1)
  RUN_STATUS=$?
  [[ "$VERBOSE" == "1" ]] && { printf '%s\n' "$RUN_OUT" | sed 's/^/        /'; }
  return 0
}

expect_status() { # expect_status <label> <want>
  if [[ "$RUN_STATUS" == "$2" ]]; then
    pass "$1: exit $2"
  else
    fail "$1: exit $RUN_STATUS, want $2"
    printf '%s\n' "$RUN_OUT" | sed 's/^/        /' >&2
  fi
}

expect_json() { # expect_json <label> <jq filter> <want>
  local got
  got=$(printf '%s' "$RUN_OUT" | jq -r "$2" 2>/dev/null)
  if [[ "$got" == "$3" ]]; then
    pass "$1: $2 == $3"
  else
    fail "$1: $2 == '$got', want '$3'"
  fi
}

expect_out() { # expect_out <label> <extended regex>
  if printf '%s' "$RUN_OUT" | grep -Eq -- "$2"; then
    pass "$1"
  else
    fail "$1 (no match for /$2/)"
    printf '%s\n' "$RUN_OUT" | sed 's/^/        /' >&2
  fi
}

expect_no_out() { # expect_no_out <label> <extended regex>
  if printf '%s' "$RUN_OUT" | grep -Eq -- "$2"; then
    fail "$1 (unexpected match for /$2/)"
    printf '%s\n' "$RUN_OUT" | sed 's/^/        /' >&2
  else
    pass "$1"
  fi
}

# ── 1. a namespace that only exists is not a leak ───────────────────────────
head_ "1. AWS intent: the GCP namespace exists but has no GCP objects"
F=$(new_fixture "namespace-only")
{ base_fixture; echo "namespace|jaiscloud-gcp|jaiscloud-gcp|"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "namespace-only" 0
expect_json "namespace-only" '.warnings | map(select(test("unexpected gcp footprint"))) | length' 0
expect_json "namespace-only" '.legacy.candidate' false
expect_json "namespace-only" '.legacy.pending' false

# ── 2. a GCP ServiceAccount is a leak ───────────────────────────────────────
head_ "2. AWS intent: a GCP ServiceAccount in a shared namespace is a leak"
F=$(new_fixture "gcp-sa")
{ base_fixture; echo "namespace|jaiscloud-gcp|jaiscloud-gcp|"; echo "serviceaccount|jaiscloud-gcp|jaiscloud-gcp|"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "gcp-sa" 1
expect_out "gcp-sa" "unexpected gcp footprint"
expect_out "gcp-sa" "serviceaccount/jaiscloud-gcp"

# ── 3. a GCP Deployment is a leak ───────────────────────────────────────────
head_ "3. AWS intent: a GCP Deployment and its PVCs are a leak"
F=$(new_fixture "gcp-deployment")
{ base_fixture; gcp_substrate; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "gcp-deployment" 1
expect_out "gcp-deployment" "deployment/jaiscloud-gcp"
expect_out "gcp-deployment" "pvc/jaiscloud-gcp-blobs-pvc"

# ── 4. a GCP database alone is a leak ───────────────────────────────────────
head_ "4. AWS intent: the jaiscloud_gcp database alone is a leak"
F=$(new_fixture "gcp-db")
{ base_fixture; echo "db|jaiscloud-data|jaiscloud_gcp|"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "gcp-db" 1
expect_out "gcp-db" "its database exists"

# ── 5. an unmarked postgres is a candidate, not a migration ─────────────────
head_ "5. an unmarked postgres in the AWS namespace is a candidate"
F=$(new_fixture "legacy-candidate")
{ base_fixture; echo "deployment|jaiscloud|postgres|image=postgres:16-alpine;replicas=1;available=1;label.app=postgres"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "legacy-candidate" 0
expect_json "legacy-candidate" '.legacy.postgresInAws' true
expect_json "legacy-candidate" '.legacy.marked' false
expect_json "legacy-candidate" '.legacy.candidate' true
expect_json "legacy-candidate" '.legacy.pending' false
expect_json "legacy-candidate" '[.recommended[] | select(.action=="migrate-store")] | length' 0
expect_out "legacy-candidate" "not marked as a JaisCloud store"
expect_out "legacy-candidate" "LEGACY_CONFIRM=1"

# ── 6. a marked postgres is a pending migration ─────────────────────────────
head_ "6. a marked postgres in the AWS namespace is a pending migration"
F=$(new_fixture "legacy-marked")
{ base_fixture; echo "deployment|jaiscloud|postgres|image=postgres:16-alpine;replicas=1;available=1;label.app=postgres;label.jaiscloud.io/managed-by=jaiscloud"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "legacy-marked" 1
expect_json "legacy-marked" '.legacy.marked' true
expect_json "legacy-marked" '.legacy.candidate' false
expect_json "legacy-marked" '.legacy.pending' true
expect_json "legacy-marked" '[.recommended[] | select(.action=="migrate-store" and .optional==false)] | length' 1
expect_out "legacy-marked" "legacy pre-split layout detected"

# ── 7. the legacy GCP workload: substrate label counts as a marker ──────────
head_ "7. a legacy GCP workload is marked by jaiscloud.io/substrate"
F=$(new_fixture "legacy-gcp-marked")
{ base_fixture; echo "deployment|jaiscloud|jaiscloud-gcp|image=gcr.io/example/jaiscloud-gcp:1;replicas=1;available=1;label.app=jaiscloud-gcp;label.jaiscloud.io/substrate=gcp"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_json "legacy-gcp-marked" '.legacy.gcpInAws' true
expect_json "legacy-gcp-marked" '.legacy.gcpMarked' true
expect_json "legacy-gcp-marked" '.legacy.candidate' false
expect_json "legacy-gcp-marked" '.legacy.pending' true

# ── 8. a real Managed Kafka broker mode is surfaced ─────────────────────────
head_ "8. a real Kafka broker mode warns about topic loss on park"
F=$(new_fixture "kafka-mode")
{ base_fixture; gcp_substrate; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate both
expect_json "kafka-mode" '.substrates.gcp.kafkaBrokerMode' "k8s"
expect_out "kafka-mode" "parking the GCP substrate loses topics"
expect_out "kafka-mode" "broker has no PVC"

# ── summary ─────────────────────────────────────────────────────────────────
head_ "summary"
printf '  passed: %s   failed: %s\n' "$PASSED" "$FAILED"
[[ "$FAILED" -eq 0 ]] || exit 1
exit 0
