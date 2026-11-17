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
#   scripts/integration-preflight.sh --strict   # exit 1 unless recommended=[] (implies --json? no)
#
# Also reachable as: deploy/deploy.sh --check [--json] [--strict]
#
# Env:
#   KUBE_CONTEXT   kubectl context (default: the current context)
#   NS_AWS/NS_GCP/NS_DATA   override the namespace names (default from the manifests)
#
# Exit codes:
#   0  probe completed (read `recommended` / `warnings`)
#   1  a kubectl/context probe failed, or --strict and something is missing
#   2  usage error
set -euo pipefail

PROG="$(basename "$0")"
NS_AWS="${NS_AWS:-jaiscloud}"
NS_GCP="${NS_GCP:-jaiscloud-gcp}"
NS_DATA="${NS_DATA:-jaiscloud-data}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
JSON=0
STRICT=0

for arg in "$@"; do
  case "$arg" in
    --json) JSON=1 ;;
    --strict) STRICT=1 ;;
    --help|-h)
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0 ;;
    *) echo "$PROG: unknown argument: $arg (try --help)" >&2; exit 2 ;;
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
GCP_PRESENT=$(deployment_present "$NS_GCP" jaiscloud-gcp)
GCP_READY=$(deployment_ready "$NS_GCP" jaiscloud-gcp)
GCP_IMAGE=$(deployment_image "$NS_GCP" jaiscloud-gcp)
GCP_PULL=$(deployment_pull_policy "$NS_GCP" jaiscloud-gcp)
GCP_PRIVATE=$(private_registry "$GCP_IMAGE")

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

if [[ "$LEGACY_PG" == "1" || "$LEGACY_GCP" == "1" ]]; then
  rec_add "migrate-store" "SKIP_IMAGE_BUILD=1 deploy/deploy.sh --migrate-store --yes" \
    "pre-split layout: move the store and the legacy GCP bytes (destructive; confirm first)" "false"
elif [[ "$STORE_DEPLOY" == "0" ]]; then
  if [[ "$GCP_PRESENT" == "1" && "$AWS_PRESENT" == "0" ]]; then
    rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "store missing; install the GCP substrate (and the store)" "false"
  else
    rec_add "deploy-aws" "deploy/deploy.sh --yes" "store missing; install the AWS substrate, the store and Postgres" "false"
    [[ "$GCP_PRESENT" == "0" ]] && \
      rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "install the GCP substrate as well (optional)" "true"
  fi
else
  [[ "$AWS_PRESENT" == "0" ]] && rec_add "deploy-aws" "deploy/deploy.sh --yes" "$NS_AWS has no jaiscloud Deployment" "true"
  [[ "$GCP_PRESENT" == "0" ]] && rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "$NS_GCP has no jaiscloud-gcp Deployment" "true"
  [[ "$STORE_READY" == "1" && "$DB_GCP" == "0" && "$GCP_PRESENT" == "1" ]] && \
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
  printf '    "aws": {"present": %s, "ready": %s, "image": "%s", "imagePullPolicy": "%s", "privateRegistry": %s},\n' \
    "$(b "$AWS_PRESENT")" "$(b "$AWS_READY")" "$(json_escape "$AWS_IMAGE")" "$(json_escape "$AWS_PULL")" "$(b "$AWS_PRIVATE")"
  printf '    "gcp": {"present": %s, "ready": %s, "image": "%s", "imagePullPolicy": "%s", "privateRegistry": %s}\n' \
    "$(b "$GCP_PRESENT")" "$(b "$GCP_READY")" "$(json_escape "$GCP_IMAGE")" "$(json_escape "$GCP_PULL")" "$(b "$GCP_PRIVATE")"
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
  if [[ "$AWS_PRESENT" == "1" ]]; then ok_ "aws: $AWS_IMAGE ready=$([[ "$AWS_READY" == "1" ]] && echo yes || echo no) pull=$AWS_PULL"; else miss_ "aws: $NS_AWS/deployment/jaiscloud"; fi
  if [[ "$GCP_PRESENT" == "1" ]]; then ok_ "gcp: $GCP_IMAGE ready=$([[ "$GCP_READY" == "1" ]] && echo yes || echo no) pull=$GCP_PULL"; else miss_ "gcp: $NS_GCP/deployment/jaiscloud-gcp"; fi
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

if [[ "$STRICT" == "1" && ${#RECOMMENDED[@]} -gt 0 ]]; then
  exit 1
fi
exit 0
