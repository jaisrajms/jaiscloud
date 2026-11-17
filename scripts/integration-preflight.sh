#!/usr/bin/env bash
#
# integration-preflight.sh — READ-ONLY inspection of a cluster that may (or may
# not) run JaisCloud. It never mutates anything: no apply, scale, delete, exec
# writes or port-forwards. Agents use it to choose a scenario from
# docs/gcp/integration/contract.yaml instead of guessing.
#
# Usage:
#   scripts/integration-preflight.sh            # human-readable
#   scripts/integration-preflight.sh --json     # machine-readable (stable schema)
#   scripts/integration-preflight.sh --strict   # exit 1 unless the intended shape holds
#   scripts/integration-preflight.sh --substrate aws|gcp|both
#                                               # override the recorded intent
#   scripts/integration-preflight.sh --desired aws=running,gcp=parked
#                                               # override the recorded desired states
#
# Intent:
#   deploy/deploy.sh records what was installed and what the operator wants
#   running in ConfigMap jaiscloud-install in NS_DATA (jaiscloud-data):
#     schema=1, installed=aws,gcp, desired=aws=running,gcp=parked
#   States: running | parked (scaled to 0) | absent (no Deployment) — the probe
#   also reports degraded (present, not ready, replicas > 0).
#   Resolution precedence: --substrate/--desired flags > the cluster record >
#   inference from the substrate namespaces/Deployments. Recommendations are
#   relative to the resolved intent: a cloud that is not installed is never
#   recommended, and a non-installed cloud with objects in the cluster is
#   reported as a leak warning.
#
# Also reachable as: deploy/deploy.sh --check [--json] [--strict]
#                    [--substrate ...] [--desired ...]
#
# Env:
#   KUBE_CONTEXT   kubectl context (default: the current context)
#   NS_AWS/NS_GCP/NS_DATA   override the namespace names (default from the manifests)
#
# Exit codes:
#   0  probe completed (read `recommended` / `warnings`)
#   1  a kubectl/context probe failed, or --strict and a non-optional
#      recommendation (or a leak warning) is present
#   2  usage error
set -euo pipefail

PROG="$(basename "$0")"
NS_AWS="${NS_AWS:-jaiscloud}"
NS_GCP="${NS_GCP:-jaiscloud-gcp}"
NS_DATA="${NS_DATA:-jaiscloud-data}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
JSON=0
STRICT=0
SUBSTRATE_FLAG=""
DESIRED_FLAG=""

usage_error() { echo "$PROG: $* (try --help)" >&2; exit 2; }

while (($#)); do
  arg=$1
  case "$arg" in
    --json) JSON=1 ;;
    --strict) STRICT=1 ;;
    --substrate|--desired)
      (($# >= 2)) || usage_error "$arg needs a value"
      if [[ "$arg" == "--substrate" ]]; then SUBSTRATE_FLAG=$2; else DESIRED_FLAG=$2; fi
      shift ;;
    --substrate=*) SUBSTRATE_FLAG=${arg#*=} ;;
    --desired=*)   DESIRED_FLAG=${arg#*=} ;;
    --help|-h)
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0 ;;
    --*) usage_error "unknown argument: $arg" ;;
    *) usage_error "unexpected argument: $arg" ;;
  esac
  shift
done

case "$SUBSTRATE_FLAG" in
  ""|aws|gcp|both) ;;
  *) usage_error "--substrate must be aws, gcp or both (got '$SUBSTRATE_FLAG')" ;;
esac

IFS=',' read -r -a _desired_entries <<<"$DESIRED_FLAG"
for entry in ${_desired_entries+"${_desired_entries[@]}"}; do
  [[ -n "$entry" ]] || usage_error "--desired: empty entry in '$DESIRED_FLAG'"
  [[ "$entry" == *=* ]] || usage_error "--desired entry '$entry' must be <cloud>=<state>"
  case "${entry%%=*}" in
    aws|gcp) ;;
    *) usage_error "--desired cloud must be aws or gcp (got '${entry%%=*}')" ;;
  esac
  case "${entry#*=}" in
    running|parked|absent) ;;
    *) usage_error "--desired state must be running, parked or absent (got '${entry#*=}')" ;;
  esac
done

KUBECTL=(kubectl)
if [[ -n "$KUBE_CONTEXT" ]]; then
  KUBECTL=(kubectl --context "$KUBE_CONTEXT")
fi

kc() { "${KUBECTL[@]}" --request-timeout=10s "$@"; }

say()  { printf '%s\n' "$*"; }
head_() { printf '\n== %s\n' "$*"; }
ok_()  { printf '  ok       %s\n' "$*"; }
miss_() { printf '  MISSING  %s\n' "$*"; }
warn_() { printf '  WARN     %s\n' "$*"; }

json_escape() { printf '%s' "$1" | tr '\n\t' '  ' | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }
b() { if [[ "$1" == "1" ]]; then printf 'true'; else printf 'false'; fi; }

# ── comma lists (installed / desired intent) ────────────────────────────────
# list_has <csv> <item>
list_has() {
  local item
  IFS=',' read -r -a _items <<<"$1"
  for item in ${_items+"${_items[@]}"}; do
    [[ "$item" == "$2" ]] && return 0
  done
  return 1
}

# list_add <csv> <item> — prints the csv with item appended once.
list_add() {
  if list_has "$1" "$2"; then printf '%s' "$1"
  elif [[ -z "$1" ]]; then printf '%s' "$2"
  else printf '%s,%s' "$1" "$2"
  fi
}

# csv_json_array <csv> — ["a", "b"]
csv_json_array() {
  local item out=""
  IFS=',' read -r -a _items <<<"$1"
  for item in ${_items+"${_items[@]}"}; do
    [[ -z "$item" ]] && continue
    out+="${out:+, }\"$(json_escape "$item")\""
  done
  printf '[%s]' "$out"
}

# desired_json <csv "cloud=state,..."> — {"aws": "running", "gcp": "parked"}
desired_json() {
  local item out=""
  IFS=',' read -r -a _items <<<"$1"
  for item in ${_items+"${_items[@]}"}; do
    [[ -z "$item" ]] && continue
    out+="${out:+, }\"$(json_escape "${item%%=*}")\": \"$(json_escape "${item#*=}")\""
  done
  printf '{%s}' "$out"
}

# probe_ok <command...> — 1 when the command succeeds, 0 otherwise (never fatal).
probe_ok() { "$@" >/dev/null 2>&1 && echo 1 || echo 0; }

# field <namespace> <resource> <name> <go-template>
field() {
  kc -n "$1" get "$2" "$3" -o go-template="$4" 2>/dev/null || true
}

# ns_present <name> — prints 1/0 (for JSON capture).
ns_present() { probe_ok kc get namespace "$1"; }
# ns_exists <name> — exit status only (for human output; never prints).
ns_exists() { kc get namespace "$1" >/dev/null 2>&1; }
# yn <1|0>
yn() { [[ "$1" == "1" ]] && echo yes || echo no; }

# deployment_present <ns> <name>
deployment_present() { probe_ok kc -n "$1" get deployment "$2"; }

# deployment_ready <ns> <name> — availableReplicas >= 1
deployment_ready() {
  local n
  n=$(field "$1" deployment "$2" '{{if .status.availableReplicas}}{{.status.availableReplicas}}{{else}}0{{end}}')
  [[ "${n:-0}" -ge 1 ]] && echo 1 || echo 0
}

# deployment_image <ns> <name>
deployment_image() {
  field "$1" deployment "$2" '{{range .spec.template.spec.containers}}{{.image}}{{"\n"}}{{end}}' | head -1
}
deployment_pull_policy() {
  field "$1" deployment "$2" '{{range .spec.template.spec.containers}}{{.imagePullPolicy}}{{"\n"}}{{end}}' | head -1
}

# deployment_replicas <ns> <name> <present 1|0> — desired scale. `spec.replicas`
# is unset on a hand-written Deployment, where Kubernetes means 1.
deployment_replicas() {
  local n
  if [[ "$3" != "1" ]]; then echo 0; return; fi
  # jsonpath, not a Go template: `{{if .spec.replicas}}` treats a parked
  # Deployment's 0 as false and would report 1. An unset pointer is empty here.
  n=$(kc -n "$1" get deployment "$2" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
  echo "${n:-1}"
}

# substrate_state <ns> <name> <present 1|0> — absent|parked|running|degraded.
#   no Deployment          -> absent
#   spec.replicas == 0     -> parked  (deliberately scaled down, data kept)
#   availableReplicas >= 1 -> running
#   otherwise              -> degraded (present but not serving)
substrate_state() {
  if [[ "$3" != "1" ]]; then echo absent; return; fi
  if [[ "$(deployment_replicas "$1" "$2" "$3")" -eq 0 ]]; then echo parked; return; fi
  if [[ "$(deployment_ready "$1" "$2")" == "1" ]]; then echo running; else echo degraded; fi
}

# pod_image_id <ns> <app-label> — the first running container's imageID, which
# pins the digest the pod actually runs (not just the tag the Deployment names).
pod_image_id() {
  kc -n "$1" get pods -l "app=$2" -o go-template='{{range .items}}{{range .status.containerStatuses}}{{if .imageID}}{{.imageID}}{{"\n"}}{{end}}{{end}}{{end}}' 2>/dev/null \
    | head -1 || true
}

# private_registry <image> — 1 for a private/local registry, 0 for a public name.
private_registry() {
  case "$1" in
    10.*|192.168.*|172.1[6-9].*|172.2[0-9].*|172.3[01].*|localhost[:/]*|*".local[:/]"*) echo 1 ;;
    *) echo 0 ;;
  esac
}

# pvc_status <ns> <name> — phase, or "" when absent
pvc_status() { field "$1" pvc "$2" '{{.status.phase}}'; }
pvc_capacity() { field "$1" pvc "$2" '{{index .status.capacity "storage"}}'; }

# db_exists <name> — 1/0; only when the store deployment is ready.
db_exists() {
  local out
  if [[ "$(deployment_ready "$NS_DATA" postgres)" != "1" ]]; then echo 0; return; fi
  out=$(timeout 15 "${KUBECTL[@]}" --request-timeout=10s -n "$NS_DATA" exec deploy/postgres -- \
        psql -U jaiscloud -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$1'" 2>/dev/null | tr -d '[:space:]' || true)
  [[ "$out" == "1" ]] && echo 1 || echo 0
}

# migration_skew_warning <ns> <app> — prints the terminated message when the pod
# is crash-looping on a store migrated by a newer binary.
migration_skew_warning() {
  kc -n "$1" get pods -l "app=$2" -o go-template='{{range .items}}{{range .status.containerStatuses}}{{if .lastState.terminated}}{{.lastState.terminated.message}}{{"\n"}}{{end}}{{end}}{{end}}' 2>/dev/null \
    | grep -i "migration count" | head -1 || true
}

# ── probes ──────────────────────────────────────────────────────────────────
CTX_NAME=$(kc config current-context 2>/dev/null || true)
if [[ -z "$CTX_NAME" ]]; then
  echo "$PROG: no current kubectl context" >&2
  exit 1
fi
CTX_SERVER=$(kc config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)
if ! kc cluster-info >/dev/null 2>&1; then
  echo "$PROG: cluster '$CTX_NAME' ($CTX_SERVER) is not reachable" >&2
  exit 1
fi

NS_AWS_P=$(ns_present "$NS_AWS"); NS_GCP_P=$(ns_present "$NS_GCP"); NS_DATA_P=$(ns_present "$NS_DATA")
ROLE_AWS=$(field "" namespace "$NS_AWS" '{{index .metadata.labels "jaiscloud.io/role"}}' 2>/dev/null || true)
[[ -z "$ROLE_AWS" ]] && ROLE_AWS=""
SUB_AWS=$(field "" namespace "$NS_AWS" '{{index .metadata.labels "jaiscloud.io/substrate"}}' 2>/dev/null || true)
ROLE_GCP=$(field "" namespace "$NS_GCP" '{{index .metadata.labels "jaiscloud.io/role"}}' 2>/dev/null || true)
ROLE_DATA=$(field "" namespace "$NS_DATA" '{{index .metadata.labels "jaiscloud.io/role"}}' 2>/dev/null || true)

LEGACY_PG=$(deployment_present "$NS_AWS" postgres)
LEGACY_GCP=$(deployment_present "$NS_AWS" jaiscloud-gcp)
STORE_DEPLOY=$(deployment_present "$NS_DATA" postgres)
STORE_PVC=$(pvc_status "$NS_DATA" postgres-pvc)
STORE_READY=$(deployment_ready "$NS_DATA" postgres)
DB_AWS=$(db_exists jaiscloud)
DB_GCP=$(db_exists jaiscloud_gcp)

AWS_PRESENT=$(deployment_present "$NS_AWS" jaiscloud)
AWS_READY=$(deployment_ready "$NS_AWS" jaiscloud)
AWS_IMAGE=$(deployment_image "$NS_AWS" jaiscloud)
AWS_PULL=$(deployment_pull_policy "$NS_AWS" jaiscloud)
AWS_PRIVATE=$(private_registry "$AWS_IMAGE")
AWS_STATE=$(substrate_state "$NS_AWS" jaiscloud "$AWS_PRESENT")
AWS_REPLICAS=$(deployment_replicas "$NS_AWS" jaiscloud "$AWS_PRESENT")
AWS_IMAGE_ID=$(pod_image_id "$NS_AWS" jaiscloud)
GCP_PRESENT=$(deployment_present "$NS_GCP" jaiscloud-gcp)
GCP_READY=$(deployment_ready "$NS_GCP" jaiscloud-gcp)
GCP_IMAGE=$(deployment_image "$NS_GCP" jaiscloud-gcp)
GCP_PULL=$(deployment_pull_policy "$NS_GCP" jaiscloud-gcp)
GCP_PRIVATE=$(private_registry "$GCP_IMAGE")
GCP_STATE=$(substrate_state "$NS_GCP" jaiscloud-gcp "$GCP_PRESENT")
GCP_REPLICAS=$(deployment_replicas "$NS_GCP" jaiscloud-gcp "$GCP_PRESENT")
GCP_IMAGE_ID=$(pod_image_id "$NS_GCP" jaiscloud-gcp)

# ── intent: flag > install record (jaiscloud-data/jaiscloud-install) > inferred ─
RECORD_PRESENT=$(probe_ok kc -n "$NS_DATA" get configmap jaiscloud-install)
RECORD_INSTALLED=""
RECORD_DESIRED=""
if [[ "$RECORD_PRESENT" == "1" ]]; then
  RECORD_INSTALLED=$(field "$NS_DATA" configmap jaiscloud-install '{{index .data "installed"}}')
  RECORD_DESIRED=$(field "$NS_DATA" configmap jaiscloud-install '{{index .data "desired"}}')
fi

INTENT_SOURCE="inferred"
if [[ -n "$SUBSTRATE_FLAG" ]]; then
  case "$SUBSTRATE_FLAG" in
    aws) INSTALLED="aws" ;;
    gcp) INSTALLED="gcp" ;;
    both) INSTALLED="aws,gcp" ;;
  esac
  INTENT_SOURCE="flag"
elif [[ -n "$RECORD_INSTALLED" ]]; then
  INSTALLED="$RECORD_INSTALLED"
  INTENT_SOURCE="cluster"
else
  # No record: a substrate namespace or Deployment is the only evidence.
  INSTALLED=""
  [[ "$NS_AWS_P" == "1" || "$AWS_PRESENT" == "1" ]] && INSTALLED=$(list_add "$INSTALLED" aws)
  [[ "$NS_GCP_P" == "1" || "$GCP_PRESENT" == "1" ]] && INSTALLED=$(list_add "$INSTALLED" gcp)
fi

# Desired states: an explicit --desired wins, then the record, then a default
# (a flag-declared cloud means "running"; inferred intent mirrors what is there).
DESIRED=""
if [[ -n "$DESIRED_FLAG" ]]; then
  DESIRED="$DESIRED_FLAG"
  INTENT_SOURCE="flag"
elif [[ -n "$RECORD_DESIRED" ]]; then
  DESIRED="$RECORD_DESIRED"
elif [[ -n "$SUBSTRATE_FLAG" ]]; then
  for cloud in aws gcp; do
    list_has "$INSTALLED" "$cloud" && DESIRED=$(list_add "$DESIRED" "$cloud=running")
  done
else
  for cloud in aws gcp; do
    if list_has "$INSTALLED" "$cloud"; then
      if [[ "$cloud" == "aws" ]]; then state=$AWS_STATE; else state=$GCP_STATE; fi
      DESIRED=$(list_add "$DESIRED" "$cloud=$state")
    fi
  done
fi

# Unexpected footprint: a substrate that intent excludes but the cluster still
# has objects for. Tracked separately so --strict can fail on a leak.
LEAKS=()
intent_label="${INSTALLED:-none}"
for cloud in aws gcp; do
  list_has "$INSTALLED" "$cloud" && continue
  if [[ "$cloud" == "aws" ]]; then
    ns=$NS_AWS ns_p=$NS_AWS_P dep=$AWS_PRESENT db=$DB_AWS
  else
    ns=$NS_GCP ns_p=$NS_GCP_P dep=$GCP_PRESENT db=$DB_GCP
  fi
  reasons=""
  [[ "$ns_p" == "1" ]] && reasons+="${reasons:+, }namespace $ns exists"
  [[ "$dep" == "1" ]] && reasons+="${reasons:+, }Deployment in $ns exists"
  [[ "$db" == "1" ]] && reasons+="${reasons:+, }its database exists"
  [[ -n "$reasons" ]] && LEAKS+=("unexpected $cloud footprint (intent=$intent_label): $reasons")
done

MESH_NS=$(ns_present istio-system)
MESH_ISTIOD=$(deployment_ready istio-system istiod)
MESH_GATEWAY=$(deployment_ready istio-system istio-ingressgateway)
MESH_SECRET=$(probe_ok kc -n istio-system get secret jaiscloud-wildcard)
MESH_CRD=$(probe_ok kc get crd gateways.networking.istio.io)
MESH_GW=$(probe_ok kc -n istio-system get gateway jaiscloud)
MESH_VS=$(kc -n istio-system get virtualservices --no-headers 2>/dev/null | wc -l | tr -d ' ')
MESH_DR=$(kc -n istio-system get destinationrules --no-headers 2>/dev/null | wc -l | tr -d ' ')
MESH_ADDR=$(field istio-system svc istio-ingressgateway '{{with .status.loadBalancer.ingress}}{{index . 0 | printf "%v"}}{{end}}' 2>/dev/null || true)

WARNINGS=()
RECOMMENDED=() # "action|command|why|optional"
warn_add() { WARNINGS+=("$1"); }
rec_add() { RECOMMENDED+=("$1|$2|$3|$4"); }

[[ "$LEGACY_PG" == "1" || "$LEGACY_GCP" == "1" ]] && \
  warn_add "legacy pre-split layout detected in $NS_AWS; the store and GCP bytes must be migrated"
[[ "$AWS_PRESENT" == "1" && "$AWS_PRIVATE" == "1" ]] && \
  warn_add "the AWS substrate image '$AWS_IMAGE' points at a private/local registry"
[[ "$GCP_PRESENT" == "1" && "$GCP_PRIVATE" == "1" ]] && \
  warn_add "the GCP substrate image '$GCP_IMAGE' points at a private/local registry"
SKEW_AWS=$(migration_skew_warning "$NS_AWS" jaiscloud)
SKEW_GCP=$(migration_skew_warning "$NS_GCP" jaiscloud-gcp)
[[ -n "$SKEW_AWS" ]] && warn_add "AWS substrate crash-loops on store version skew: $SKEW_AWS"
[[ -n "$SKEW_GCP" ]] && warn_add "GCP substrate crash-loops on store version skew: $SKEW_GCP"
[[ "$MESH_NS" == "1" && "$MESH_GW" == "0" ]] && \
  warn_add "istio-system exists but Gateway/jaiscloud is missing; run the mesh scenario"
[[ "$STORE_DEPLOY" == "0" && -n "$STORE_PVC" ]] && \
  warn_add "pvc postgres-pvc exists in $NS_DATA but deployment/postgres is missing"
for leak in ${LEAKS+"${LEAKS[@]}"}; do warn_add "$leak"; done

# Recommendations are relative to intent: only an installed cloud is ever told
# to deploy, and it is non-optional when it is absent. Clouds outside the
# intent are leaks (warnings above), not deployment targets.
first_intended() {
  local cloud
  for cloud in aws gcp; do list_has "$INSTALLED" "$cloud" && { printf '%s' "$cloud"; return 0; }; done
  return 0
}

# desired_state <cloud> — the resolved desired state ("" when not listed).
desired_state() {
  local item
  IFS=',' read -r -a _items <<<"$DESIRED"
  for item in ${_items+"${_items[@]}"}; do
    [[ "${item%%=*}" == "$1" ]] && { printf '%s' "${item#*=}"; return 0; }
  done
  return 0
}

if [[ "$LEGACY_PG" == "1" || "$LEGACY_GCP" == "1" ]]; then
  rec_add "migrate-store" "SKIP_IMAGE_BUILD=1 deploy/deploy.sh --migrate-store --yes" \
    "pre-split layout: move the store and the legacy GCP bytes (destructive; confirm first)" "false"
elif [[ "$STORE_DEPLOY" == "0" && -z "$INSTALLED" ]]; then
  # No recorded or inferred install: keep the original guidance.
  if [[ "$GCP_PRESENT" == "1" && "$AWS_PRESENT" == "0" ]]; then
    rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "store missing; install the GCP substrate (and the store)" "false"
  else
    rec_add "deploy-aws" "deploy/deploy.sh --yes" "store missing; install the AWS substrate, the store and Postgres" "false"
    [[ "$GCP_PRESENT" == "0" ]] && \
      rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "install the GCP substrate as well (optional)" "true"
  fi
elif [[ "$STORE_DEPLOY" == "0" ]]; then
  # Installing either substrate brings the store; start with the first intended
  # cloud that the operator actually wants running (desired=absent is a
  # deliberate teardown, not a missing install).
  first=""
  for cloud in aws gcp; do
    list_has "$INSTALLED" "$cloud" || continue
    [[ "$(desired_state "$cloud")" == "absent" ]] && continue
    first=$cloud
    break
  done
  if [[ "$first" == "gcp" ]]; then
    rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "store missing; install the GCP substrate (and the store)" "false"
  elif [[ "$first" == "aws" ]]; then
    rec_add "deploy-aws" "deploy/deploy.sh --yes" "store missing; install the AWS substrate, the store and Postgres" "false"
  fi
  for cloud in aws gcp; do
    [[ "$cloud" == "$first" ]] && continue
    list_has "$INSTALLED" "$cloud" || continue
    [[ "$(desired_state "$cloud")" == "absent" ]] && continue
    if [[ "$cloud" == "aws" && "$AWS_STATE" == "absent" ]]; then
      rec_add "deploy-aws" "deploy/deploy.sh --yes" "$NS_AWS has no jaiscloud Deployment" "false"
    elif [[ "$cloud" == "gcp" && "$GCP_STATE" == "absent" ]]; then
      rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "$NS_GCP has no jaiscloud-gcp Deployment" "false"
    fi
  done
else
  if list_has "$INSTALLED" aws && [[ "$AWS_STATE" == "absent" ]] && [[ "$(desired_state aws)" != "absent" ]]; then
    rec_add "deploy-aws" "deploy/deploy.sh --yes" "$NS_AWS has no jaiscloud Deployment" "false"
  fi
  if list_has "$INSTALLED" gcp && [[ "$GCP_STATE" == "absent" ]] && [[ "$(desired_state gcp)" != "absent" ]]; then
    rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "$NS_GCP has no jaiscloud-gcp Deployment" "false"
  fi
  [[ "$STORE_READY" == "1" && "$DB_GCP" == "0" && "$GCP_PRESENT" == "1" ]] && list_has "$INSTALLED" gcp \
    && [[ "$(desired_state gcp)" != "absent" ]] && \
    rec_add "create-gcp-db" "make postgres-gcp-db" "jaiscloud_gcp is missing from the store" "false"
fi
if [[ "$MESH_NS" == "0" && "$((${AWS_PRESENT:-0} + ${GCP_PRESENT:-0}))" -gt 0 ]]; then
  rec_add "mesh" "see docs/gcp/integration/playbook.yaml#scenarios.mesh" \
    "optional: TLS + real cloud hostnames for external HTTP(S) clients" "true"
fi

# ── output ──────────────────────────────────────────────────────────────────
if [[ "$JSON" == "1" ]]; then
  printf '{\n'
  printf '  "apiVersion": "jaiscloud.dev/integration/v1",\n  "kind": "IntegrationPreflight",\n'
  printf '  "context": {"name": "%s", "server": "%s"},\n' "$(json_escape "$CTX_NAME")" "$(json_escape "$CTX_SERVER")"
  printf '  "intent": {"source": "%s", "installed": %s, "desired": %s},\n' \
    "$(json_escape "$INTENT_SOURCE")" "$(csv_json_array "$INSTALLED")" "$(desired_json "$DESIRED")"
  printf '  "namespaces": {\n'
  printf '    "%s": {"present": %s, "role": "%s", "substrate": "%s"},\n' "$NS_AWS" "$(b "$NS_AWS_P")" "$(json_escape "$ROLE_AWS")" "$(json_escape "$SUB_AWS")"
  printf '    "%s": {"present": %s, "role": "%s"},\n' "$NS_GCP" "$(b "$NS_GCP_P")" "$(json_escape "$ROLE_GCP")"
  printf '    "%s": {"present": %s, "role": "%s"}\n' "$NS_DATA" "$(b "$NS_DATA_P")" "$(json_escape "$ROLE_DATA")"
  printf '  },\n'
  printf '  "legacy": {"postgresInAws": %s, "gcpInAws": %s},\n' "$(b "$LEGACY_PG")" "$(b "$LEGACY_GCP")"
  printf '  "store": {\n'
  printf '    "present": %s, "ready": %s,\n' "$(b "$STORE_DEPLOY")" "$(b "$STORE_READY")"
  printf '    "pvc": {"name": "postgres-pvc", "namespace": "%s", "status": "%s", "capacity": "%s"},\n' \
    "$NS_DATA" "$(json_escape "$STORE_PVC")" "$(json_escape "$(pvc_capacity "$NS_DATA" postgres-pvc)")"
  printf '    "databases": {"jaiscloud": %s, "jaiscloud_gcp": %s}\n' "$(b "$DB_AWS")" "$(b "$DB_GCP")"
  printf '  },\n'
  printf '  "substrates": {\n'
  printf '    "aws": {"present": %s, "ready": %s, "state": "%s", "replicas": %s, "image": "%s", "imageID": "%s", "imagePullPolicy": "%s", "privateRegistry": %s},\n' \
    "$(b "$AWS_PRESENT")" "$(b "$AWS_READY")" "$AWS_STATE" "$AWS_REPLICAS" "$(json_escape "$AWS_IMAGE")" \
    "$(json_escape "$AWS_IMAGE_ID")" "$(json_escape "$AWS_PULL")" "$(b "$AWS_PRIVATE")"
  printf '    "gcp": {"present": %s, "ready": %s, "state": "%s", "replicas": %s, "image": "%s", "imageID": "%s", "imagePullPolicy": "%s", "privateRegistry": %s}\n' \
    "$(b "$GCP_PRESENT")" "$(b "$GCP_READY")" "$GCP_STATE" "$GCP_REPLICAS" "$(json_escape "$GCP_IMAGE")" \
    "$(json_escape "$GCP_IMAGE_ID")" "$(json_escape "$GCP_PULL")" "$(b "$GCP_PRIVATE")"
  printf '  },\n'
  printf '  "pvcs": [\n'
  first=1
  for spec in "$NS_AWS/jaiscloud-blobs-pvc" "$NS_AWS/jaiscloud-state-pvc" \
              "$NS_GCP/jaiscloud-gcp-blobs-pvc" "$NS_GCP/jaiscloud-gcp-state-pvc" \
              "$NS_DATA/postgres-pvc"; do
    ns=${spec%%/*}; pvc=${spec#*/}
    st=$(pvc_status "$ns" "$pvc"); cap=$(pvc_capacity "$ns" "$pvc")
    [[ -z "$st" ]] && continue
    [[ "$first" -eq 1 ]] || printf ',\n'
    first=0
    printf '    {"namespace": "%s", "name": "%s", "status": "%s", "capacity": "%s"}' \
      "$ns" "$(json_escape "$pvc")" "$(json_escape "$st")" "$(json_escape "$cap")"
  done
  [[ "$first" -eq 0 ]] && printf '\n'
  printf '  ],\n'
  printf '  "mesh": {"namespace": %s, "istiod": %s, "ingressGateway": %s, "tlsSecret": %s, "crds": %s, "gateway": %s, "virtualServices": %s, "destinationRules": %s, "address": "%s"},\n' \
    "$(b "$MESH_NS")" "$(b "$MESH_ISTIOD")" "$(b "$MESH_GATEWAY")" "$(b "$MESH_SECRET")" "$(b "$MESH_CRD")" \
    "$(b "$MESH_GW")" "${MESH_VS:-0}" "${MESH_DR:-0}" "$(json_escape "$MESH_ADDR")"
  printf '  "warnings": ['
  first=1
  for w in ${WARNINGS+"${WARNINGS[@]}"}; do
    [[ "$first" -eq 1 ]] || printf ', '
    first=0
    printf '"%s"' "$(json_escape "$w")"
  done
  printf '],\n'
  printf '  "recommended": ['
  first=1
  for r in ${RECOMMENDED+"${RECOMMENDED[@]}"}; do
    IFS='|' read -r action command why optional <<<"$r"
    [[ "$first" -eq 1 ]] || printf ', '
    first=0
    printf '{"action": "%s", "command": "%s", "why": "%s", "optional": %s}' \
      "$(json_escape "$action")" "$(json_escape "$command")" "$(json_escape "$why")" "$(b "$([[ "$optional" == "true" ]] && echo 1 || echo 0)")"
  done
  printf ']\n}\n'
else
  say "JaisCloud integration preflight"
  say "  context : $CTX_NAME  ($CTX_SERVER)"
  say "  intent  : installed=${INSTALLED:-(none)} source=$INTENT_SOURCE${DESIRED:+ desired=$DESIRED}"
  if [[ -z "$INSTALLED" && -z "$SUBSTRATE_FLAG" ]]; then
    say "  note    : no JaisCloud install detected; pass --substrate to check an intended shape"
  fi
  head_ "namespaces"
  for spec in "$NS_AWS:$ROLE_AWS:$SUB_AWS" "$NS_GCP:$ROLE_GCP:" "$NS_DATA:$ROLE_DATA:"; do
    IFS=: read -r ns role sub <<<"$spec"
    if ns_exists "$ns"; then ok_ "$ns  role=${role:-?} substrate=${sub:-–}"; else miss_ "$ns"; fi
  done
  head_ "layout"
  [[ "$LEGACY_PG" == "1" ]] && warn_ "legacy postgres Deployment in $NS_AWS" || ok_ "no legacy store in $NS_AWS"
  [[ "$LEGACY_GCP" == "1" ]] && warn_ "legacy GCP Deployment in $NS_AWS" || ok_ "no legacy GCP workload in $NS_AWS"
  if [[ "$STORE_DEPLOY" == "1" ]]; then
    ok_ "store: deployment/postgres ready=$([[ "$STORE_READY" == "1" ]] && echo yes || echo no), pvc=${STORE_PVC:-?}"
    ok_ "databases: jaiscloud=$([[ "$DB_AWS" == "1" ]] && echo yes || echo no) jaiscloud_gcp=$([[ "$DB_GCP" == "1" ]] && echo yes || echo no)"
  else
    miss_ "store: $NS_DATA/deployment/postgres"
  fi
  head_ "substrates"
  if [[ "$AWS_PRESENT" == "1" ]]; then
    ok_ "aws: state=$AWS_STATE replicas=$AWS_REPLICAS $AWS_IMAGE ready=$([[ "$AWS_READY" == "1" ]] && echo yes || echo no) pull=$AWS_PULL${AWS_IMAGE_ID:+ imageID=$AWS_IMAGE_ID}"
  else
    miss_ "aws: state=$AWS_STATE $NS_AWS/deployment/jaiscloud"
  fi
  if [[ "$GCP_PRESENT" == "1" ]]; then
    ok_ "gcp: state=$GCP_STATE replicas=$GCP_REPLICAS $GCP_IMAGE ready=$([[ "$GCP_READY" == "1" ]] && echo yes || echo no) pull=$GCP_PULL${GCP_IMAGE_ID:+ imageID=$GCP_IMAGE_ID}"
  else
    miss_ "gcp: state=$GCP_STATE $NS_GCP/deployment/jaiscloud-gcp"
  fi
  head_ "mesh (optional)"
  if [[ "$MESH_NS" == "1" ]]; then
    ok_ "istio-system: istiod=$(yn "$MESH_ISTIOD") gateway=$(yn "$MESH_GATEWAY") route-resource=$(yn "$MESH_GW") tls-secret=$(yn "$MESH_SECRET") virtualServices=${MESH_VS} destinationRules=${MESH_DR}${MESH_ADDR:+ address=$MESH_ADDR}"
  else
    say "  (not installed)"
  fi
  if [[ ${#WARNINGS[@]} -gt 0 ]]; then head_ "warnings"; for w in "${WARNINGS[@]}"; do warn_ "$w"; done; fi
  head_ "recommended"
  if [[ ${#RECOMMENDED[@]} -eq 0 ]]; then
    ok_ "nothing missing"
  else
    for r in "${RECOMMENDED[@]}"; do
      IFS='|' read -r action command why optional <<<"$r"
      printf '  %-16s %s\n' "$action" "$command"
      printf '  %-16s %s%s\n' "" "$why" "$([[ "$optional" == "true" ]] && echo ' (optional)')"
    done
  fi
  say ""
fi

if [[ "$STRICT" == "1" ]]; then
  nonoptional=0
  for r in ${RECOMMENDED+"${RECOMMENDED[@]}"}; do
    IFS='|' read -r action command why optional <<<"$r"
    [[ "$optional" == "true" ]] || nonoptional=1
  done
  if [[ "$nonoptional" == "1" || ${#LEAKS[@]} -gt 0 ]]; then
    exit 1
  fi
fi
exit 0
