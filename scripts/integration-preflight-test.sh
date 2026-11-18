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
#   env     an env probe resolves valueFrom (ConfigMap/Secret, base64) and
#           envFrom (in order, later wins), and a literal env entry still wins:
#           substrates.gcp.sparkImage must be populated when the image comes from
#           an envFrom ConfigMap instead of a literal.
#
#   store   database probes are tri-state: true/false from psql on a ready
#           bundled store; null (unknown, not missing) for an external store, a
#           workload that is not ready, or a failed exec — with
#           store.databasesProbe = exec|unavailable and store.probeReason saying
#           why. The bundled workload is resolved (STORE_WORKLOAD, then
#           deployment/postgres, then statefulset/postgres), so a StatefulSet
#           store is probed too. The role is read from the substrate DSN Secret
#           (STORE_PG_USER overrides; STORE_PG_DB picks the maintenance database)
#           and a local-socket refusal retries with the full DSN, so a
#           host-owned role/password is probed too. A null never produces a
#           "create the database" recommendation.
#
#   rbac    effective permissions come from `kubectl auth can-i --as=<the
#           Deployment's ServiceAccount>` (yes/no/unknown), and the isolation
#           policy must name that identity: no / unknown / a VAP mismatch are
#           distinct outcomes (warn and fail --strict only for a real gap or a
#           policy that enforces nothing for the running identity).
#
#   dns/ca  a CoreDNS rewrite is judged by the target it names: a gateway target
#           is stale with no ready mesh (or with its substrate parked/absent), a
#           service target only when the Service is missing or has no ready
#           endpoints, and an unknown target is reported but never called stale.
#           A template answering a pinned Service ClusterIP therefore passes
#           without a mesh. A CA ConfigMap is stale only when it is labelled
#           jaiscloud.io/issuer=mesh (what certs/gen.sh writes) and the issuer is
#           gone; an unlabelled copy is host-issued and never warned about. All
#           probes are read-only.
#
#   timeout every case runs with PREFLIGHT_TIMEOUT_IMPL=watchdog by default, so
#           the bash watchdog (the macOS path: no GNU `timeout`/`gtimeout`) is
#           exercised throughout; one case also covers the `auto` path.
#
# The fixture is one fake cluster written as `kind|namespace|name|key=value;...`
# lines; the fake kubectl answers the read-only probes the preflight makes
# (Deployments, StatefulSets, Services, Endpoints, ConfigMaps, Secrets, …). Extra
# fixture files: authz.txt (`verb resource ns sa result`, `*` wildcards; an
# unlisted query answers yes), corefile.txt (the kube-system/coredns Corefile),
# and psql-fail (a broken store pod).
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
  # The data is everything after the third `|` — reconstruct it, since a value may
  # itself contain `|` (the VAP expression's `||`).
  awk -F'|' -v k="$1" -v n="$2" -v m="$3" \
    '$1==k && $3==m && ($2==n || $2=="-" || n=="") {
       out=""
       for (i=4; i<=NF; i++) out = out (i>4 ? "|" : "") $i
       print out; exit
     }' "$resources"
}

list_of() { # list_of <kind> <namespace> — one "name<TAB>data" line per record
  awk -F'|' -v k="$1" -v n="$2" '$1==k && ($2==n || $2=="-") {
    out=""
    for (i=4; i<=NF; i++) out = out (i>4 ? "|" : "") $i
    print $3 "\t" out
  }' "$resources"
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

field_has() { # field_has <data> <key> — true when the key exists (even when empty)
  local data=$1 key=$2 entry
  local oldifs=$IFS
  IFS=';'
  for entry in $data; do
    if [[ "$entry" == "$key="* ]]; then IFS=$oldifs; return 0; fi
  done
  IFS=$oldifs
  return 1
}

# answer_template <template> <data> — the value the preflight's go-template or
# jsonpath would produce for this record. Only the forms the preflight uses.
answer_template() { # answer_template <template> <data> <kind> <namespace>
  local template=$1 data=$2 key from rest obj k refs oldifs
  case "$template" in
    *'.metadata.labels'*)
      key=$(printf '%s' "$template" | sed -n 's/.*index \.metadata\.labels "\([^"]*\)".*/\1/p')
      [[ -n "$key" ]] || key=$(printf '%s' "$template" | sed -n "s/.*labels\['\([^']*\)'\].*/\1/p")
      field_val "$data" "label.$key" ;;
    *'.metadata.annotations'*)
      key=$(printf '%s' "$template" | sed -n "s/.*annotations\['\([^']*\)'\].*/\1/p")
      field_val "$data" "annotation.$key" ;;
    # explicit env[] entry: configmap/secret valueFrom, else the literal value
    *'(index .spec.template.spec.containers 0).envFrom'*)
      refs=$(field_val "$data" "envFrom")
      oldifs=$IFS; IFS=','
      for key in $refs; do
        [[ -n "$key" ]] || continue
        printf '%s %s\n' "${key%%:*}" "${key#*:}"
      done
      IFS=$oldifs ;;
    *'(index .spec.template.spec.containers 0).env'*)
      key=$(printf '%s' "$template" | sed -n 's/.*eq \.name "\([^"]*\)".*/\1/p')
      from=$(field_val "$data" "env.$key.from")
      if [[ -n "$from" ]]; then
        rest=${from#*:}; obj=${rest%%:*}; k=${rest#*:}
        printf '%s %s %s\n' "${from%%:*}" "$obj" "$k"
      elif field_has "$data" "env.$key"; then
        printf 'literal %s\n' "$(field_val "$data" "env.$key")"
      fi ;;
    *'.spec.matchConditions'*) field_val "$data" "matchExpression" ;;
    *'.serviceAccountName'*)   field_val "$data" "serviceAccountName" ;;
    *'.data "installed"'*) field_val "$data" "data.installed" ;;
    *'.data "desired"'*)   field_val "$data" "data.desired" ;;
    *'.data "store"'*)     field_val "$data" "data.store" ;;
    *'.data "Corefile"'*)  cat "$fixture/corefile.txt" 2>/dev/null ;;
    *'index .data "'*)
      key=$(printf '%s' "$template" | sed -n 's/.*index \.data "\([^"]*\)".*/\1/p')
      field_val "$data" "data.$key" ;;
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
    *'.status.readyReplicas'*) field_val "$data" "ready" ;;
    *'.items'*)
      # A list query (only the preflight's Service-by-clusterIP lookup uses it).
      list_of "$3" "$4" | while IFS=$'\t' read -r _n _d; do
        [[ -n "$_n" ]] && printf '%s %s\n' "$_n" "$(field_val "$_d" "clusterIP")"
      done ;;
    *'.spec.clusterIP'*)   field_val "$data" "clusterIP" ;;
    *'.subsets'*)
      # Endpoints readiness: one address when the fixture says ready=1.
      [[ "$(field_val "$data" "ready")" == "1" ]] && printf '10.0.0.1\n' ;;
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
    # db_state: `exec <workload> -- psql ... datname='<db>'`; the fake store has a
    # record per database. $fixture/psql-fail makes every exec fail (a broken
    # store pod); $fixture/psql-user-fail fails only the role form
    # (`psql -U …`), so the preflight's DSN retry is exercised. Every argv is
    # recorded in $fixture/exec-args.txt for the assertions.
    printf '%s\n' "$*" >> "$fixture/exec-args.txt"
    [[ -f "$fixture/psql-fail" ]] && { echo "error: unable to upgrade connection: container not found" >&2; exit 1; }
    if [[ -f "$fixture/psql-user-fail" && "$*" == *" -U "* ]]; then
      echo "psql: error: password authentication failed for user" >&2
      exit 2
    fi
    all="$*"
    db=$(printf '%s' "$all" | sed -n "s/.*datname='\([^']*\)'.*/\1/p")
    if [[ -n "$db" ]]; then
      present db jaiscloud-data "$db" && printf '1\n'
    fi
    exit 0 ;;
  auth)
    # `auth can-i <verb> <resource> --as=system:serviceaccount:<ns>:<sa> [-n <ns>]`
    # answers from $fixture/authz.txt (`<verb> <resource> <ns> <sa> <result>`,
    # `*` wildcards, last match wins; result yes|no|error). Without the file the
    # fake cluster allows everything, so only the cases that assert RBAC add it.
    shift                                  # can-i
    v=${1:-}
    r=${2:-}
    shift 2 || true
    as=""; tns=""
    while (($#)); do
      case "$1" in
        --as=*) as=${1#*=} ;;
        -n|--namespace) tns=${2:-}; shift || true ;;
      esac
      shift || true
    done
    sa_ns=${as#system:serviceaccount:}; sa_ns=${sa_ns%%:*}
    sa=${as##*:}
    result=""
    if [[ -f "$fixture/authz.txt" ]]; then
      result=$(awk -v v="$v" -v r="$r" -v n="$sa_ns" -v s="$sa" \
        '($1==v||$1=="*") && ($2==r||$2=="*") && ($3==n||$3=="*") && ($4==s||$4=="*") {res=$5} END {print (res=="" ? "yes" : res)}' \
        "$fixture/authz.txt")
    else
      result=yes
    fi
    case "$result" in
      yes|no) printf '%s\n' "$result"; exit 0 ;;
      *) echo "Error from server (Forbidden): users \"fake\" cannot impersonate resource \"serviceaccounts\"" >&2; exit 1 ;;
    esac ;;
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
  sa|serviceaccount) kind=serviceaccount ;;
  svc|service|services) kind=service ;;
  pvc|persistentvolumeclaims) kind=persistentvolumeclaim ;;
  cm|configmaps) kind=configmap ;;
  ns|namespaces) kind=namespace ;;
  deploy|deployment|deployments) kind=deployment ;;
  sts|statefulset|statefulsets) kind=statefulset ;;
  endpoints|endpoint) kind=endpoints ;;
esac
if [[ "$template" == *'.items'* && -z "$name" ]]; then
  # A list query (the preflight's Service-by-clusterIP lookup).
  answer_template "$template" "" "$kind" "$ns"
  exit 0
fi
[[ -n "$name" ]] || exit 0
present "$kind" "$ns" "$name" || exit 1
data=$(data_of "$kind" "$ns" "$name")
printf '%s' "$(answer_template "$template" "$data" "$kind" "$ns")"
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
validatingadmissionpolicy|-|jaiscloud-namespace-isolation|matchExpression=request.userInfo.username == 'system:serviceaccount:jaiscloud:jaiscloud' || request.userInfo.username == 'system:serviceaccount:jaiscloud-gcp:jaiscloud-gcp'
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
  # The macOS path by default: PREFLIGHT_TIMEOUT_IMPL=watchdog exercises the bash
  # watchdog instead of GNU `timeout`, so every case runs without coreutils.
  RUN_OUT=$(PATH="$TMP/bin:$PATH" FIXTURE="$fixture" KUBE_CONTEXT= \
    PREFLIGHT_TIMEOUT_IMPL="${PREFLIGHT_TIMEOUT_IMPL:-watchdog}" \
    STORE_WORKLOAD="${STORE_WORKLOAD:-}" \
    STORE_PG_USER="${STORE_PG_USER:-}" \
    STORE_PG_DB="${STORE_PG_DB:-}" \
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

expect_exec() { # expect_exec <label> <extended regex over the recorded exec argv>
  if grep -Eq -- "$2" "${F:-/nonexistent}/exec-args.txt" 2>/dev/null; then
    pass "$1"
  else
    fail "$1 (no exec argv matching /$2/; recorded: $(tr '\n' '|' < "${F:-/dev/null}/exec-args.txt" 2>/dev/null))"
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

# ── 9. envFrom: the Spark image comes from a ConfigMap, not a literal ───────
head_ "9. envFrom.configMapRef provides JAISCLOUD_K8S_SPARK_IMAGE"
F=$(new_fixture "envfrom-configmap")
{
  base_fixture
  echo "namespace|jaiscloud-gcp|jaiscloud-gcp|label.jaiscloud.io/role=substrate;label.jaiscloud.io/substrate=gcp"
  echo "deployment|jaiscloud-gcp|jaiscloud-gcp|image=gcr.io/example/jaiscloud-gcp:1;imagePullPolicy=IfNotPresent;replicas=1;available=1;envFrom=configmap:jaiscloud-gcp-env"
  echo "configmap|jaiscloud-gcp|jaiscloud-gcp-env|data.JAISCLOUD_K8S_SPARK_IMAGE=gcr.io/example/spark-gcs:3.5.0;data.JAISCLOUD_KAFKA_BROKER_MODE=docker"
  echo "serviceaccount|jaiscloud-gcp|jaiscloud-gcp|"
  echo "db|jaiscloud-data|jaiscloud_gcp|"
  echo "clusterrolebinding|-|jaiscloud-namespace-admin-gcp|"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate both
expect_status "envfrom-configmap" 0
expect_json "envfrom-configmap" '.substrates.gcp.sparkImage' "gcr.io/example/spark-gcs:3.5.0"
expect_json "envfrom-configmap" '.substrates.gcp.sparkImageGCS' true
expect_json "envfrom-configmap" '.substrates.gcp.kafkaBrokerMode' "docker"
expect_json "envfrom-configmap" '.warnings | map(select(test("is unset"))) | length' 0
expect_no_out "envfrom-configmap" "JAISCLOUD_K8S_SPARK_IMAGE is unset"

# ── 9b. a missing envFrom ref is empty, never fatal ────────────────────────
head_ "9b. an envFrom reference to a missing ConfigMap resolves to empty"
F=$(new_fixture "envfrom-missing")
{
  base_fixture
  echo "namespace|jaiscloud-gcp|jaiscloud-gcp|label.jaiscloud.io/role=substrate"
  echo "deployment|jaiscloud-gcp|jaiscloud-gcp|image=gcr.io/example/jaiscloud-gcp:1;imagePullPolicy=IfNotPresent;replicas=1;available=1;envFrom=configmap:absent-env"
  echo "db|jaiscloud-data|jaiscloud_gcp|"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate both
expect_status "envfrom-missing" 0
expect_json "envfrom-missing" '.substrates.gcp.sparkImage' ""
expect_out "envfrom-missing" "JAISCLOUD_K8S_SPARK_IMAGE is unset"

# ── 10. valueFrom.secretKeyRef and the literal-wins precedence ──────────────
head_ "10. valueFrom.secretKeyRef (base64) and a literal env entry both resolve"
F=$(new_fixture "envfrom-secret")
spark_b64=$(printf '%s' 'gcr.io/example/spark-secret:3.5.0' | base64 | tr -d '\n')
{
  base_fixture
  echo "namespace|jaiscloud-gcp|jaiscloud-gcp|label.jaiscloud.io/role=substrate"
  echo "deployment|jaiscloud-gcp|jaiscloud-gcp|image=gcr.io/example/jaiscloud-gcp:1;imagePullPolicy=IfNotPresent;replicas=1;available=1;env.JAISCLOUD_K8S_SPARK_IMAGE.from=secret:jaiscloud-gcp-spark:JAISCLOUD_K8S_SPARK_IMAGE"
  echo "secret|jaiscloud-gcp|jaiscloud-gcp-spark|data.JAISCLOUD_K8S_SPARK_IMAGE=$spark_b64"
  echo "db|jaiscloud-data|jaiscloud_gcp|"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate both
expect_json "envfrom-secret" '.substrates.gcp.sparkImage' "gcr.io/example/spark-secret:3.5.0"
expect_json "envfrom-secret" '.substrates.gcp.sparkImageGCS' true
expect_no_out "envfrom-secret" "JAISCLOUD_K8S_SPARK_IMAGE is unset"

head_ "10b. a literal env entry wins over envFrom"
F=$(new_fixture "env-literal-wins")
{
  base_fixture
  echo "namespace|jaiscloud-gcp|jaiscloud-gcp|label.jaiscloud.io/role=substrate"
  echo "deployment|jaiscloud-gcp|jaiscloud-gcp|image=gcr.io/example/jaiscloud-gcp:1;imagePullPolicy=IfNotPresent;replicas=1;available=1;env.JAISCLOUD_K8S_SPARK_IMAGE=apache/spark:3.5.0;envFrom=configmap:jaiscloud-gcp-env"
  echo "configmap|jaiscloud-gcp|jaiscloud-gcp-env|data.JAISCLOUD_K8S_SPARK_IMAGE=gcr.io/example/spark-gcs:3.5.0"
  echo "db|jaiscloud-data|jaiscloud_gcp|"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate both
expect_json "env-literal-wins" '.substrates.gcp.sparkImage' "apache/spark:3.5.0"
expect_json "env-literal-wins" '.substrates.gcp.sparkImageGCS' false
expect_out "env-literal-wins" "has no GCS connector"

# ── 11. tri-state databases: external, bundled, psql failure ────────────────
head_ "11. an external store reports databases=null (unknown, not missing)"
F=$(new_fixture "store-external")
{
  cat <<'EOF'
namespace|jaiscloud|jaiscloud|label.jaiscloud.io/role=substrate;label.jaiscloud.io/substrate=aws
namespace|jaiscloud-data|jaiscloud-data|label.jaiscloud.io/role=data
deployment|jaiscloud|jaiscloud|image=jaisraj/jaiscloud-aws:1.0.0;imagePullPolicy=IfNotPresent;replicas=1;available=1
serviceaccount|jaiscloud|jaiscloud|
configmap|jaiscloud-data|jaiscloud-install|data.installed=aws;data.desired=aws=running;data.store=external
validatingadmissionpolicy|-|jaiscloud-namespace-isolation|matchExpression=request.userInfo.username == 'system:serviceaccount:jaiscloud:jaiscloud'
clusterrolebinding|-|jaiscloud-namespace-admin-aws|
EOF
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "store-external" 0
expect_json "store-external" '.store.mode' "external"
expect_json "store-external" '.store.databases.jaiscloud' null
expect_json "store-external" '.store.databases.jaiscloud_gcp' null
expect_json "store-external" '.store.databasesProbe' "unavailable"
expect_json "store-external" '[.recommended[] | select(.action=="create-gcp-db")] | length' 0
run_preflight "$F" --substrate aws
expect_out "store-external" 'databases: jaiscloud=unknown jaiscloud_gcp=unknown'

head_ "11b. a healthy bundled store answers true/false"
F=$(new_fixture "store-bundled")
base_fixture > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "store-bundled" 0
expect_json "store-bundled" '.store.databases.jaiscloud' true
expect_json "store-bundled" '.store.databases.jaiscloud_gcp' false
expect_json "store-bundled" '.store.databasesProbe' "exec"
run_preflight "$F" --substrate aws
expect_out "store-bundled" 'databases: jaiscloud=true jaiscloud_gcp=false'

head_ "11c. a failing psql exec reports databases=null (unknown)"
F=$(new_fixture "store-psql-fail")
base_fixture > "$F/resources.txt"
: > "$F/psql-fail"
run_preflight "$F" --json --strict --substrate aws
expect_status "store-psql-fail" 0
expect_json "store-psql-fail" '.store.databases.jaiscloud' null
expect_json "store-psql-fail" '.store.databasesProbe' "unavailable"
expect_json "store-psql-fail" '.store.databases.jaiscloud_gcp' null

# ── 12. effective RBAC: yes / no / unknown ─────────────────────────────────
head_ "12. effective RBAC answers yes/no/unknown from kubectl auth can-i"
F=$(new_fixture "rbac-yes")
base_fixture > "$F/resources.txt"
printf 'delete namespaces jaiscloud jaiscloud yes\ncreate rolebindings jaiscloud jaiscloud yes\n' > "$F/authz.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "rbac-yes" 0
expect_json "rbac-yes" '.rbac.source' "auth"
expect_json "rbac-yes" '.rbac.effective.aws.serviceAccount' "jaiscloud"
expect_json "rbac-yes" '.rbac.effective.aws.deleteNamespaces' "yes"
expect_json "rbac-yes" '.rbac.effective.aws.createRoleBindings' "yes"
expect_json "rbac-yes" '.rbac.namespaceAdmin' true
expect_no_out "rbac-yes" "cannot verify effective RBAC"

F=$(new_fixture "rbac-no")
base_fixture > "$F/resources.txt"
printf '* * * * yes\ndelete namespaces jaiscloud jaiscloud no\n' > "$F/authz.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "rbac-no" 1
expect_json "rbac-no" '.rbac.effective.aws.deleteNamespaces' "no"
expect_json "rbac-no" '.rbac.namespaceAdmin' false
expect_out "rbac-no" "cannot delete namespaces"
expect_json "rbac-no" '[.recommended[] | select(.action=="install-rbac-policy" and .optional==false)] | length' 1

F=$(new_fixture "rbac-unknown")
base_fixture > "$F/resources.txt"
printf '* * * * error\n' > "$F/authz.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "rbac-unknown" 0
expect_json "rbac-unknown" '.rbac.effective.aws.deleteNamespaces' "unknown"
expect_json "rbac-unknown" '.rbac.namespaceAdmin' false
expect_out "rbac-unknown" "cannot verify effective RBAC"
expect_no_out "rbac-unknown" "namespace isolation is not enforced"

# ── 13. the VAP must name the running ServiceAccount ───────────────────────
head_ "13. policyMatchesSA: the policy names the running identity / does not"
F=$(new_fixture "vap-match")
base_fixture > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "vap-match" 0
expect_json "vap-match" '.rbac.policyMatchesSA' true
expect_json "vap-match" '.rbac.policyMentions.aws' true
expect_no_out "vap-match" "namespace isolation is not enforced for the running identity"

F=$(new_fixture "vap-mismatch")
base_fixture | grep -v '^validatingadmissionpolicy|' > "$F/resources.txt"
echo "validatingadmissionpolicy|-|jaiscloud-namespace-isolation|matchExpression=request.userInfo.username == 'system:serviceaccount:jaiscloud-gcp:jaiscloud-gcp'" >> "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "vap-mismatch" 1
expect_json "vap-mismatch" '.rbac.policyMatchesSA' false
expect_json "vap-mismatch" '.rbac.policyMentions.aws' false
expect_out "vap-mismatch" "namespace isolation is not enforced for the running identity jaiscloud:jaiscloud"
expect_json "vap-mismatch" '[.recommended[] | select(.action=="install-rbac-policy" and .optional==false)] | length' 1

# ── 14. a CoreDNS rewrite for a parked substrate is stale ──────────────────
head_ "14. CoreDNS rewrites a suffix whose substrate is parked"
F=$(new_fixture "dns-stale")
{
  base_fixture | grep -v '^deployment|jaiscloud|jaiscloud|'
  echo "deployment|jaiscloud|jaiscloud|image=jaisraj/jaiscloud-aws:1.0.0;imagePullPolicy=IfNotPresent;replicas=0;available=0"
  echo "configmap|kube-system|coredns|"
  echo "deployment|istio-system|istiod|available=1"
  echo "deployment|istio-system|istio-ingressgateway|available=1"
  echo "gateway|istio-system|jaiscloud|"
  echo "secret|istio-system|jaiscloud-wildcard|"
  echo "crd|-|gateways.networking.istio.io|"
} > "$F/resources.txt"
cat > "$F/corefile.txt" <<'EOF'
.:53 {
    errors
    rewrite stop name regex (.*\.)?amazonaws\.com\.?$ istio-ingressgateway.istio-system.svc.cluster.local.
    kubernetes cluster.local in-addr.arpa ip6.arpa
}
EOF
run_preflight "$F" --json --strict --substrate aws
expect_status "dns-stale" 0
expect_json "dns-stale" '.substrates.aws.state' "parked"
expect_json "dns-stale" '.dns.rewrites' true
expect_json "dns-stale" '(.dns.suffixes | join(","))' 'amazonaws.com'
expect_json "dns-stale" '.dns.target' "gateway"
expect_json "dns-stale" '.dns.targets["amazonaws.com"]' "gateway"
expect_json "dns-stale" '.dns.stale' true
expect_out "dns-stale" "CoreDNS rewrites amazonaws.com"
expect_out "dns-stale" "parked/absent backend"
expect_out "dns-stale" "re-add it when you resume aws"

head_ "14b. a CoreDNS rewrite with no gateway is stale"
F=$(new_fixture "dns-no-gateway")
{
  base_fixture
  echo "configmap|kube-system|coredns|"
} > "$F/resources.txt"
cat > "$F/corefile.txt" <<'EOF'
.:53 {
    rewrite stop name regex (.*\.)?googleapis\.com\.?$ istio-ingressgateway.istio-system.svc.cluster.local.
    kubernetes cluster.local in-addr.arpa ip6.arpa
}
EOF
run_preflight "$F" --json --strict --substrate aws
expect_json "dns-no-gateway" '.dns.rewrites' true
expect_json "dns-no-gateway" '(.dns.suffixes | join(","))' 'googleapis.com'
expect_json "dns-no-gateway" '.dns.target' "gateway"
expect_json "dns-no-gateway" '.dns.stale' true
expect_out "dns-no-gateway" "no ready gateway serves it"

# ── 14c. a template pinned to a ready Service needs no mesh ────────────────
head_ "14c. a CoreDNS template answering a ready Service's ClusterIP is not stale"
F=$(new_fixture "dns-template-ready")
{
  base_fixture
  echo "configmap|kube-system|coredns|"
  echo "service|jaiscloud|jaiscloud|clusterIP=10.43.0.10"
  echo "endpoints|jaiscloud|jaiscloud|ready=1"
} > "$F/resources.txt"
cat > "$F/corefile.txt" <<'EOF'
.:53 {
    errors
    template IN A (.*\.)?googleapis\.com\.?$ {
        answer "{{ .Name }}" 10.43.0.10
    }
    kubernetes cluster.local in-addr.arpa ip6.arpa
}
EOF
run_preflight "$F" --json --strict --substrate aws
expect_status "dns-template-ready" 0
expect_json "dns-template-ready" '.dns.rewrites' true
expect_json "dns-template-ready" '.dns.target' "service:jaiscloud/jaiscloud"
expect_json "dns-template-ready" '.dns.targets["googleapis.com"]' "service:jaiscloud/jaiscloud"
expect_json "dns-template-ready" '.dns.stale' false
expect_no_out "dns-template-ready" "no ready gateway"
expect_no_out "dns-template-ready" "CoreDNS rewrites googleapis.com"

# ── 14d. the same template with no ready endpoints is stale ────────────────
head_ "14d. a template whose Service has no ready endpoints is stale"
F=$(new_fixture "dns-template-parked")
{
  base_fixture
  echo "configmap|kube-system|coredns|"
  echo "service|jaiscloud|jaiscloud|clusterIP=10.43.0.10"
  echo "endpoints|jaiscloud|jaiscloud|ready=0"
} > "$F/resources.txt"
cat > "$F/corefile.txt" <<'EOF'
.:53 {
    errors
    template IN A (.*\.)?googleapis\.com\.?$ {
        answer "{{ .Name }}" 10.43.0.10
    }
    kubernetes cluster.local in-addr.arpa ip6.arpa
}
EOF
run_preflight "$F" --json --strict --substrate aws
expect_json "dns-template-parked" '.dns.target' "service:jaiscloud/jaiscloud"
expect_json "dns-template-parked" '.dns.stale' true
expect_out "dns-template-parked" "service jaiscloud/jaiscloud, which has no ready endpoints"
expect_out "dns-template-parked" "remove the googleapis.com rewrite"

# ── 14e. a rewrite to a Service that no longer exists is stale ────────────
head_ "14e. a rewrite naming a deleted Service is stale"
F=$(new_fixture "dns-service-missing")
{
  base_fixture
  echo "configmap|kube-system|coredns|"
} > "$F/resources.txt"
cat > "$F/corefile.txt" <<'EOF'
.:53 {
    rewrite stop name regex (.*\.)?googleapis\.com\.?$ jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local.
    kubernetes cluster.local in-addr.arpa ip6.arpa
}
EOF
run_preflight "$F" --json --strict --substrate aws
expect_json "dns-service-missing" '.dns.target' "service:jaiscloud-gcp/jaiscloud-gcp"
expect_json "dns-service-missing" '.dns.stale' true
expect_out "dns-service-missing" "which does not exist"

# ── 14f. an address that matches no known Service is unknown, not stale ────
head_ "14f. a template targeting an unknown address is reported, not warned"
F=$(new_fixture "dns-unknown-target")
{
  base_fixture
  echo "configmap|kube-system|coredns|"
} > "$F/resources.txt"
cat > "$F/corefile.txt" <<'EOF'
.:53 {
    template IN A (.*\.)?googleapis\.com\.?$ {
        answer "{{ .Name }}" 203.0.113.10
    }
    kubernetes cluster.local in-addr.arpa ip6.arpa
}
EOF
run_preflight "$F" --json --strict --substrate aws
expect_json "dns-unknown-target" '.dns.rewrites' true
expect_json "dns-unknown-target" '.dns.target' "unknown"
expect_json "dns-unknown-target" '.dns.stale' false
expect_no_out "dns-unknown-target" "CoreDNS rewrites googleapis.com"
run_preflight "$F" --substrate aws
expect_out "dns-unknown-target" "target=unknown"

# ── 15. only a mesh-labelled CA copy is judged stale ───────────────────────
head_ "15. an unlabelled CA copy is host-issued: issuer=external, never stale"
F=$(new_fixture "ca-external")
{ base_fixture; echo "configmap|jaiscloud|jaiscloud-ca-cert|data.ca.crt=dev-ca"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "ca-external" 0
expect_json "ca-external" '.ca.aws' true
expect_json "ca-external" '.ca.gcp' false
expect_json "ca-external" '.ca.issuerPresent' false
expect_json "ca-external" '.ca.issuers.aws' "external"
expect_json "ca-external" '.ca.issuers.gcp' "absent"
expect_no_out "ca-external" "stale CA"
expect_no_out "ca-external" "has no issuing mesh"

head_ "15a. a mesh-labelled CA copy without its issuer is stale"
F=$(new_fixture "ca-stale")
{
  base_fixture
  echo "configmap|jaiscloud|jaiscloud-ca-cert|data.ca.crt=dev-ca;label.jaiscloud.io/issuer=mesh"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "ca-stale" 0
expect_json "ca-stale" '.ca.aws' true
expect_json "ca-stale" '.ca.issuerPresent' false
expect_json "ca-stale" '.ca.issuers.aws' "mesh"
expect_out "ca-stale" "labelled jaiscloud.io/issuer=mesh"
expect_out "ca-stale" "certs/gen.sh"

head_ "15b. a CA copy with the issuer present is not stale"
F=$(new_fixture "ca-ok")
{
  base_fixture
  echo "configmap|jaiscloud|jaiscloud-ca-cert|data.ca.crt=dev-ca;label.jaiscloud.io/issuer=mesh"
  echo "secret|istio-system|jaiscloud-wildcard|"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_json "ca-ok" '.ca.issuerPresent' true
expect_json "ca-ok" '.ca.issuers.aws' "mesh"
expect_no_out "ca-ok" "stale CA"

# ── 16. no CoreDNS ConfigMap: rewrite state is unknown, not "none" ─────────
head_ "16. an absent kube-system/coredns makes the DNS state unknown"
F=$(new_fixture "dns-unknown")
base_fixture > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_json "dns-unknown" '.dns.rewrites' null
expect_json "dns-unknown" '(.dns.suffixes | length)' 0
expect_json "dns-unknown" '.dns.stale' false
run_preflight "$F" --substrate aws
expect_out "dns-unknown" "rewrites: unknown"

# ── 17. the store workload is resolved, not assumed ────────────────────────
head_ "17. a StatefulSet store (the common managed shape) is probed"
F=$(new_fixture "store-sts")
{
  base_fixture | grep -v '^deployment|jaiscloud-data|postgres|' | grep -v '^persistentvolumeclaim|jaiscloud-data|postgres-pvc|'
  echo "statefulset|jaiscloud-data|postgres|replicas=1;ready=1;label.app=postgres;label.jaiscloud.io/managed-by=jaiscloud"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "store-sts" 0
expect_json "store-sts" '.store.workload' "statefulset/postgres"
expect_json "store-sts" '.store.present' true
expect_json "store-sts" '.store.ready' true
expect_json "store-sts" '.store.databases.jaiscloud' true
expect_json "store-sts" '.store.databases.jaiscloud_gcp' false
expect_json "store-sts" '.store.databasesProbe' "exec"
expect_json "store-sts" '.store.probeReason' "ok"
run_preflight "$F" --substrate aws
expect_out "store-sts" "mode=bundled statefulset/postgres"

head_ "17b. STORE_WORKLOAD names the workload explicitly"
F=$(new_fixture "store-workload")
{
  base_fixture | grep -v '^deployment|jaiscloud-data|postgres|'
  echo "statefulset|jaiscloud-data|pg-main|replicas=1;ready=1;label.jaiscloud.io/managed-by=jaiscloud"
} > "$F/resources.txt"
STORE_WORKLOAD=statefulset/pg-main run_preflight "$F" --json --strict --substrate aws
expect_json "store-workload" '.store.workload' "statefulset/pg-main"
expect_json "store-workload" '.store.databases.jaiscloud' true
expect_json "store-workload" '.store.databasesProbe' "exec"

head_ "17c. a STORE_WORKLOAD that does not exist stays bundled and unprobeable"
F=$(new_fixture "store-workload-missing")
base_fixture > "$F/resources.txt"
STORE_WORKLOAD=statefulset/absent run_preflight "$F" --json --strict --substrate aws
expect_json "store-workload-missing" '.store.mode' "bundled"
expect_json "store-workload-missing" '.store.workload' "statefulset/absent"
expect_json "store-workload-missing" '.store.present' false
expect_json "store-workload-missing" '.store.databases.jaiscloud' null
expect_json "store-workload-missing" '.store.databasesProbe' "unavailable"
expect_json "store-workload-missing" '.store.probeReason' "no-workload"
expect_out "store-workload-missing" "STORE_WORKLOAD=statefulset/absent does not exist in jaiscloud-data"

head_ "17d. an unready store workload reports probeReason=not-ready"
F=$(new_fixture "store-not-ready")
{
  base_fixture | grep -v '^deployment|jaiscloud-data|postgres|'
  echo "deployment|jaiscloud-data|postgres|replicas=1;available=0;label.jaiscloud.io/managed-by=jaiscloud"
} > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_json "store-not-ready" '.store.present' true
expect_json "store-not-ready" '.store.ready' false
expect_json "store-not-ready" '.store.databases.jaiscloud' null
expect_json "store-not-ready" '.store.probeReason' "not-ready"

# ── 18. the GNU-timeout path answers too (auto) ────────────────────────────
head_ "18. PREFLIGHT_TIMEOUT_IMPL=auto (timeout/gtimeout) still probes"
F=$(new_fixture "store-bundled-auto")
base_fixture > "$F/resources.txt"
PREFLIGHT_TIMEOUT_IMPL=auto run_preflight "$F" --json --strict --substrate aws
expect_status "store-bundled-auto" 0
expect_json "store-bundled-auto" '.store.databases.jaiscloud' true
expect_json "store-bundled-auto" '.store.databasesProbe' "exec"

# ── 19. the probe role comes from the substrate DSN, not a constant ────────
dsn_b64=$(printf '%s' 'postgres://storeowner:s3cret@postgres.jaiscloud-data.svc.cluster.local:5432/jaiscloud' | base64 | tr -d '\n')
gcp_dsn_b64=$(printf '%s' 'postgres://storeowner:s3cret@postgres.jaiscloud-data.svc.cluster.local:5432/jaiscloud_gcp' | base64 | tr -d '\n')

head_ "19. the database probe uses the role in the substrate DSN Secret"
F=$(new_fixture "store-probe-user")
{ base_fixture; echo "secret|jaiscloud|jaiscloud-dsn|data.dsn=$dsn_b64"; } > "$F/resources.txt"
run_preflight "$F" --json --strict --substrate aws
expect_status "store-probe-user" 0
expect_json "store-probe-user" '.store.databases.jaiscloud' true
expect_exec "store-probe-user" 'psql -U storeowner -d postgres'

head_ "19b. STORE_PG_USER overrides the DSN role and STORE_PG_DB the database"
F=$(new_fixture "store-probe-user-override")
{ base_fixture; echo "secret|jaiscloud|jaiscloud-dsn|data.dsn=$dsn_b64"; } > "$F/resources.txt"
STORE_PG_USER=probe_role STORE_PG_DB=template1 run_preflight "$F" --json --strict --substrate aws
expect_status "store-probe-user-override" 0
expect_json "store-probe-user-override" '.store.databases.jaiscloud' true
expect_exec "store-probe-user-override" 'psql -U probe_role -d template1'

head_ "19c. a local-socket refusal retries with the full DSN (password auth)"
F=$(new_fixture "store-probe-dsn-retry")
{
  base_fixture
  echo "secret|jaiscloud|jaiscloud-dsn|data.dsn=$dsn_b64"
  echo "secret|jaiscloud-gcp|jaiscloud-gcp-dsn|data.dsn=$gcp_dsn_b64"
} > "$F/resources.txt"
: > "$F/psql-user-fail"
run_preflight "$F" --json --strict --substrate aws
expect_status "store-probe-dsn-retry" 0
expect_json "store-probe-dsn-retry" '.store.databases.jaiscloud' true
expect_json "store-probe-dsn-retry" '.store.probeReason' "ok"
expect_exec "store-probe-dsn-retry" 'psql postgres://storeowner:s3cret@'

# ── summary ─────────────────────────────────────────────────────────────────
head_ "summary"
printf '  passed: %s   failed: %s\n' "$PASSED" "$FAILED"
[[ "$FAILED" -eq 0 ]] || exit 1
exit 0
