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
#   recommended, and a non-installed cloud with real objects in the cluster
#   (its Deployment, ServiceAccount, database or PVCs, or a namespace labelled
#   for it) is reported as a leak warning. A bare namespace is not evidence:
#   namespaces are shared and long-lived, so their existence alone is not a leak.
#
# Legacy (pre-split) layout:
#   legacy.* probes deployment/statefulset `postgres` and deployment
#   `jaiscloud-gcp` in NS_AWS. Presence alone is not a pending migration: the
#   object must carry a JaisCloud marker (jaiscloud.io/managed-by=jaiscloud, the
#   conventional app.kubernetes.io/managed-by=jaiscloud, or
#   jaiscloud.io/substrate). Only a marked object produces legacy.pending and the
#   non-optional migrate-store recommendation; an unmarked one is
#   legacy.candidate=true plus a warning (it may be an unrelated Postgres that
#   merely shares the name), and deploy.sh --migrate-store refuses it without
#   LEGACY_CONFIRM=1. legacy.postgresRetained / legacy.gcpRetained mark a
#   marked, scaled-to-0 rollback copy of an already-migrated store.
#
# RBAC/namespace isolation:
#   Both substrate ServiceAccounts hold cluster-wide namespace delete; the
#   namespace boundary between them is the ValidatingAdmissionPolicy
#   jaiscloud-namespace-isolation (K8s >= 1.30). Namespace admin is bound per
#   substrate (jaiscloud-namespace-admin-aws / …-gcp). The JSON reports
#   rbac.namespaceAdmin, rbac.admissionPolicy, rbac.serverVersion and the
#   *effective* permissions of each substrate's running ServiceAccount
#   (rbac.effective.<cloud>: deleteNamespaces / createNamespaces /
#   updateNamespaces cluster-scoped, createRoleBindings in its own namespace,
#   each yes|no|unknown, plus source="auth").
#   The effective check impersonates the ServiceAccount the Deployment actually
#   runs (spec.template.spec.serviceAccountName, falling back to jaiscloud /
#   jaiscloud-gcp) with `kubectl auth can-i --as=…`. When impersonation is not
#   allowed every answer is unknown: the preflight then warns "cannot verify
#   effective RBAC" and never claims that isolation is unenforced. A `no` for an
#   intended substrate is a warning plus a non-optional install-rbac-policy
#   recommendation. rbac.namespaceAdmin stays a boolean for compatibility: true
#   only when an intended substrate's effective deleteNamespaces is `yes`
#   (unknown never counts as true).
#   rbac.policyMatchesSA (true|false|unknown) compares the policy's
#   matchConditions[].expression with the running identities: a present substrate
#   whose system:serviceaccount:<ns>:<sa> the expression does not name is a
#   warning ("namespace isolation is not enforced for the running identity") and
#   a non-optional install-rbac-policy recommendation. deploy/deploy.sh renders
#   the policy from NS_AWS/SA_AWS/NS_GCP/SA_GCP (the isolation overlay), so the
#   fix is to re-apply that rendered object, never to hand-edit the default.
#
# The store:
#   store.mode is bundled (a PostgreSQL workload in NS_DATA) or external (a
#   server the operator brought: deploy.sh --store-dsn / JAISCLOUD_STORE_DSN, no
#   postgres.yaml). The mode comes from the install record's `store` key, then
#   falls back to bundled when the record or the workload exists, and to
#   external when a substrate exists without a bundled store workload.
#   The bundled workload is resolved the way --migrate-store resolves a legacy
#   store: STORE_WORKLOAD=<deployment|statefulset>/<name> when set, otherwise
#   deployment/postgres, then statefulset/postgres. store.workload names what was
#   probed ("" when none/external); a statefulset's claim is derived from its
#   volumeClaimTemplate, so store.pvc stays the Deployment's postgres-pvc.
#   store.databases is TRI-STATE: true / false / null. true|false only when the
#   resolved workload is ready and psql answered; null when the store is
#   external, the workload is absent or not ready, or the exec/psql failed —
#   "unknown", never "missing". store.databasesProbe is "exec" when both psql
#   probes answered, "unavailable" otherwise, and store.probeReason says why it
#   was unavailable (external|no-workload|not-ready|exec-failed). The probe never
#   creates a pod (that would mutate the cluster), so an external server is
#   reported as unprobeable (DSL9); verify it with your own psql.
#   The probe role is not hardcoded: STORE_PG_USER when set, otherwise the user
#   in the database's own substrate DSN Secret (jaiscloud-dsn / jaiscloud-gcp-dsn,
#   key dsn), then the other substrate's, then the shipped `jaiscloud`. It
#   connects over the pod's local socket (no password in argv) with
#   STORE_PG_DB (default postgres); a non-timeout failure is retried once with
#   the full DSN the emulator uses, for a host store that requires a password
#   even locally.
#   Recommendations only treat `false` as a missing database.
#
# Images / env:
#   substrates.<cloud>.image is the Deployment's image, imageID the digest the
#   pod actually runs, and privateRegistry a heuristic for a 10./192.168./172.16-
#   31./localhost/*.local registry. substrates.gcp.sparkImage is the deployment's
#   *effective* JAISCLOUD_K8S_SPARK_IMAGE (the Dataproc driver/executor image) and
#   sparkImageGCS a heuristic: false for an empty value or a plain apache/spark
#   tag, true for any other image name. Neither image policy fails --strict: they
#   are warnings only (a private Spark image -> ImagePullBackOff; an image without
#   the GCS connector -> gs:// jobs fail; a :latest substrate tag -> the digest is
#   whatever the node cached).
#   Env resolution order (envFrom included): an explicit containers[0].env entry
#   wins — its literal `value`, or its valueFrom.configMapKeyRef /
#   secretKeyRef (Secret data base64-decoded); otherwise envFrom sources are
#   scanned in order and the last source that provides the key wins. A missing
#   object, a missing key or a Forbidden read resolves to "" (the probe never
#   aborts because one ref is unreadable).
#
# DNS and CA:
#   dns.reads the kube-system/coredns ConfigMap Corefile (read-only; Forbidden or
#   absent -> unknown) and reports dns.rewrites (true|false|unknown),
#   dns.suffixes (the cloud suffixes with a rewrite/template), dns.target(s) and
#   dns.stale. A rewrite is judged by the target it names, never by assuming the
#   mesh: `rewrite … <name>.<ns>.svc.cluster.local` resolves to a Service, and a
#   `template` block's `answer <ip>` resolves to the Service holding that
#   spec.clusterIP (searched in the JaisCloud namespaces only). dns.target is
#   "gateway" (istio-system/istio-ingressgateway), "service:<ns>/<name>",
#   "unknown", or "mixed" when suffixes point at different targets; dns.targets
#   maps each suffix to its own target.
#   A gateway target is stale when no ready ingress serves it (that also needs
#   Gateway/jaiscloud) or its owning substrate is parked/absent. A service target
#   is stale only when the Service is missing or has no ready endpoints (a park
#   empties them). An unknown target is reported, never called stale: the probe
#   only diagnoses what it can see first-hand.
#   ca.aws / ca.gcp report the jaiscloud-ca-cert ConfigMap in each substrate
#   namespace, ca.issuerPresent the Istio gateway TLS secret
#   istio-system/jaiscloud-wildcard (or the Gateway resource), and
#   ca.issuers.<cloud> what the copy claims to be: "mesh" (labelled
#   jaiscloud.io/issuer=mesh by certs/gen.sh), "external" (unlabelled: a
#   host-issued CA copy JaisCloud did not create) or "absent". Only a
#   mesh-labelled copy with no issuing mesh is a stale CA: re-run certs/gen.sh
#   (which labels what it writes) or remove it. Park keeps the CA copy (a resumed
#   substrate and its managed workloads still trust the front door), so it is not
#   stale while the secret exists.
#
# Also reachable as: deploy/deploy.sh --check [--json] [--strict]
#                    [--substrate ...] [--desired ...]
#
# Env:
#   KUBE_CONTEXT   kubectl context (default: the current context)
#   NS_AWS/NS_GCP/NS_DATA   namespace names (defaults: the shipped layout —
#                 jaiscloud / jaiscloud-gcp / jaiscloud-data). deploy/deploy.sh
#                 exports the names it installs, so `deploy.sh --check` inspects
#                 the same layout a (custom-names) install used.
#   SA_AWS/SA_GCP  unused here (the ServiceAccount is read from each Deployment);
#                 accepted so a caller can pass deploy.sh's environment through.
#   STORE_WORKLOAD=<deployment|statefulset>/<name>
#                 the bundled store workload to probe (default: resolve
#                 deployment/postgres, then statefulset/postgres)
#   STORE_PG_USER the role the database probe connects as (default: the user in
#                 the substrate DSN Secret, else jaiscloud)
#   STORE_PG_DB   the maintenance database the probe connects to (default postgres)
#   PREFLIGHT_TIMEOUT_IMPL  auto (default: timeout -> gtimeout -> bash watchdog),
#                 timeout|gtimeout|watchdog|none. macOS has no GNU `timeout`, so
#                 no bare `timeout` call exists in this script; the offline
#                 harness pins `watchdog` to exercise the macOS path everywhere.
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
# Role and maintenance database for the store probe. A host-owned Postgres may
# use `postgres` instead of the shipped `jaiscloud` role; when STORE_PG_USER is
# unset the role is read from the substrate DSN Secret (see store_pg_user).
STORE_PG_USER_ENV="${STORE_PG_USER:-}"
STORE_PG_DB="${STORE_PG_DB:-postgres}"
[[ -n "$STORE_PG_DB" ]] || { echo "$PROG: STORE_PG_DB must not be empty (try --help)" >&2; exit 2; }

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

# ── portable wall-clock bound ───────────────────────────────────────────────
# macOS ships no GNU `timeout`, so this script must never call it unguarded.
# PREFLIGHT_TIMEOUT_IMPL resolves once: `auto` prefers timeout -> gtimeout ->
# a bash watchdog (the macOS path). The offline harness pins `watchdog` so every
# case exercises that path on Linux.
PREFLIGHT_TIMEOUT_IMPL="${PREFLIGHT_TIMEOUT_IMPL:-auto}"
case "$PREFLIGHT_TIMEOUT_IMPL" in
  auto)
    if command -v timeout >/dev/null 2>&1; then PREFLIGHT_TIMEOUT_IMPL=timeout
    elif command -v gtimeout >/dev/null 2>&1; then PREFLIGHT_TIMEOUT_IMPL=gtimeout
    else PREFLIGHT_TIMEOUT_IMPL=watchdog
    fi ;;
  timeout|gtimeout)
    command -v "$PREFLIGHT_TIMEOUT_IMPL" >/dev/null 2>&1 \
      || usage_error "PREFLIGHT_TIMEOUT_IMPL=$PREFLIGHT_TIMEOUT_IMPL, but '$PREFLIGHT_TIMEOUT_IMPL' is not on PATH" ;;
  watchdog|none) ;;
  *) usage_error "PREFLIGHT_TIMEOUT_IMPL must be auto, timeout, gtimeout, watchdog or none (got '$PREFLIGHT_TIMEOUT_IMPL')" ;;
esac

# bounded <seconds> <command...> — run a command with a wall-clock limit (the
# helper every exec uses instead of a bare `timeout`). Pass an executable, not a
# shell function: timeout/gtimeout exec their argument. `none` runs it unbounded;
# kubectl still carries --request-timeout.
bounded() {
  local secs=$1 pid wd rc=0
  shift
  case "$PREFLIGHT_TIMEOUT_IMPL" in
    none) "$@" ;;
    timeout|gtimeout) "$PREFLIGHT_TIMEOUT_IMPL" "$secs" "$@" ;;
    watchdog)
      # Portable watchdog (macOS bash 3.2 + no GNU coreutils). Output goes to
      # files, not pipes: a killed command's children can outlive it, and a
      # lingering child holding a pipe would make `$(bounded …)` wait out the
      # full timeout even though the command already returned.
      local out_f err_f
      out_f=$(mktemp "${TMPDIR:-/tmp}/jaiscloud-preflight-out.XXXXXX" 2>/dev/null || true)
      err_f=$(mktemp "${TMPDIR:-/tmp}/jaiscloud-preflight-err.XXXXXX" 2>/dev/null || true)
      if [[ -z "$out_f" || -z "$err_f" ]]; then
        [[ -n "$out_f" ]] && rm -f "$out_f"
        [[ -n "$err_f" ]] && rm -f "$err_f"
        "$@"
        return $?
      fi
      "$@" >"$out_f" 2>"$err_f" &
      pid=$!
      ( sleep "$secs"; pkill -TERM -P "$pid" 2>/dev/null || true; kill -TERM "$pid" 2>/dev/null && sleep 1 && kill -KILL "$pid" 2>/dev/null ) >/dev/null 2>&1 &
      wd=$!
      wait "$pid" || rc=$?
      kill -TERM "$wd" 2>/dev/null || true
      cat "$out_f"
      cat "$err_f" >&2
      rm -f "$out_f" "$err_f"
      return "$rc" ;;
  esac
}

say()  { printf '%s\n' "$*"; }
head_() { printf '\n== %s\n' "$*"; }
ok_()  { printf '  ok       %s\n' "$*"; }
miss_() { printf '  MISSING  %s\n' "$*"; }
warn_() { printf '  WARN     %s\n' "$*"; }

json_escape() { printf '%s' "$1" | tr '\n\t' '  ' | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }
b() { if [[ "$1" == "1" ]]; then printf 'true'; else printf 'false'; fi; }
# tri <true|false|null> — the tri-state as a JSON literal (anything else is null).
tri() { case "$1" in true) printf true ;; false) printf false ;; *) printf null ;; esac; }
# tri_word <true|false|null> — the human form (null reads "unknown").
tri_word() { case "$1" in true) printf true ;; false) printf false ;; *) printf unknown ;; esac; }

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

# auth_can_i <verb> <resource> <sa-namespace> <sa> — yes|no|unknown.
# Impersonates the substrate ServiceAccount with `kubectl auth can-i --as=…`, so
# it reports what that identity may actually do, not which binding exists. A
# namespaced resource (rolebindings) is evaluated in the SA's own namespace; a
# cluster-scoped one (namespaces) is evaluated cluster-wide.
# Only a clean yes/no is an answer: `no` exits 1, so the exit status is not the
# signal. An error (Forbidden impersonation, unknown verb/resource, no
# `auth` subcommand) is `unknown` — never a claim about the cluster.
auth_can_i() {
  local verb=$1 resource=$2 ns=$3 sa=$4 out
  local -a args=(auth can-i "$verb" "$resource" --as="system:serviceaccount:$ns:$sa")
  case "$resource" in
    rolebindings) args+=(-n "$ns") ;;
  esac
  out=$(kc "${args[@]}" 2>/dev/null) || true
  out=$(printf '%s' "$out" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
  case "$out" in
    yes) echo yes ;;
    no) echo no ;;
    *) echo unknown ;;
  esac
}

# ns_present <name> — prints 1/0 (for JSON capture).
ns_present() { probe_ok kc get namespace "$1"; }
# ns_exists <name> — exit status only (for human output; never prints).
ns_exists() { kc get namespace "$1" >/dev/null 2>&1; }
# yn <1|0>
yn() { [[ "$1" == "1" ]] && echo yes || echo no; }

# deployment_present <ns> <name>
deployment_present() { probe_ok kc -n "$1" get deployment "$2"; }
# sts_present <ns> <name>
sts_present() { probe_ok kc -n "$1" get statefulset "$2"; }
# sa_present <ns> <name>
sa_present() { probe_ok kc -n "$1" get serviceaccount "$2"; }
# pvc_present <ns> <name>
pvc_present() { probe_ok kc -n "$1" get pvc "$2"; }
# svc_present <ns> <name>
svc_present() { probe_ok kc -n "$1" get service "$2"; }
# svc_ready <ns> <name> — 1 when the Service has at least one ready address
# (Endpoints, the object the kubelet-side datapath actually programs). A Service
# with no Endpoints object or only notReadyAddresses is 0.
svc_ready() {
  local ip
  ip=$(kc -n "$1" get endpoints "$2" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)
  [[ -n "$ip" ]] && echo 1 || echo 0
}
# svc_by_cluster_ip <ip> — "ns/name" of the first Service whose spec.clusterIP is
# that address, searched in the namespaces this pack owns (never a cluster-wide
# list, which the probe may not be allowed to make).
svc_by_cluster_ip() {
  local ip=$1 ns all first
  all=$(
    for ns in "$NS_AWS" "$NS_GCP" "$NS_DATA" istio-system; do
      kc -n "$ns" get services \
        -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.clusterIP}{"\n"}{end}' 2>/dev/null \
        | awk -v ns="$ns" -v ip="$ip" '$2==ip {print ns "/" $1}'
    done
  ) || true
  first=${all%%$'\n'*}
  [[ -n "$first" ]] && printf '%s' "$first"
  return 0
}

# object_label <namespace> <kind> <name> <key> — one metadata label ("" when
# absent). Bracket notation: the label keys contain dots and a slash.
object_label() {
  kc -n "$1" get "$2" "$3" -o jsonpath="{.metadata.labels['$4']}" 2>/dev/null || true
}

# legacy_marked <namespace> <kind> <name> — 0 when the object carries a JaisCloud
# marker, 1 otherwise. Accepted markers:
#   jaiscloud.io/managed-by=jaiscloud       the store's own marker (postgres.yaml)
#   app.kubernetes.io/managed-by=jaiscloud  the conventional equivalent
#   jaiscloud.io/substrate=<any>            what the pre-split GCP emulator carried
# `app=postgres` is NOT evidence: any operator-supplied Postgres has it.
legacy_marked() {
  [[ "$(object_label "$1" "$2" "$3" jaiscloud.io/managed-by)" == "jaiscloud" ]] && return 0
  [[ "$(object_label "$1" "$2" "$3" app.kubernetes.io/managed-by)" == "jaiscloud" ]] && return 0
  [[ -n "$(object_label "$1" "$2" "$3" jaiscloud.io/substrate)" ]] && return 0
  return 1
}

# deployment_ready <ns> <name> — availableReplicas >= 1
deployment_ready() {
  local n
  n=$(field "$1" deployment "$2" '{{if .status.availableReplicas}}{{.status.availableReplicas}}{{else}}0{{end}}')
  [[ "${n:-0}" -ge 1 ]] && echo 1 || echo 0
}

# sts_ready <ns> <name> — readyReplicas >= 1 (a StatefulSet's readiness field)
sts_ready() {
  local n
  n=$(field "$1" statefulset "$2" '{{if .status.readyReplicas}}{{.status.readyReplicas}}{{else}}0{{end}}')
  [[ "${n:-0}" -ge 1 ]] && echo 1 || echo 0
}

# deployment_image <ns> <name>
deployment_image() {
  field "$1" deployment "$2" '{{range .spec.template.spec.containers}}{{.image}}{{"\n"}}{{end}}' | head -1
}
deployment_pull_policy() {
  field "$1" deployment "$2" '{{range .spec.template.spec.containers}}{{.imagePullPolicy}}{{"\n"}}{{end}}' | head -1
}

# deployment_env <ns> <deployment> <name> — the literal env value of one
# variable in the first container ("" when unset). Kept for callers that only
# want the literal form; use deployment_env_effective when envFrom matters.
deployment_env() {
  kc -n "$1" get deployment "$2" \
    -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name==\"$3\")].value}" 2>/dev/null || true
}

# configmap_key <ns> <name> <key> — data.<key> ("" when the object, the key or
# the read is unavailable).
configmap_key() {
  kc -n "$1" get configmap "$2" -o go-template="{{index .data \"$3\"}}" 2>/dev/null || true
}
# b64decode — stdin base64 -> stdout. GNU/BSD spell the decode flag differently
# (`base64 -d` vs `base64 -D`), and macOS ships the BSD form; openssl is the last
# resort (every JaisCloud host has it for the dev CA).
b64decode() {
  base64 -d 2>/dev/null || base64 -D 2>/dev/null || openssl base64 -d -A 2>/dev/null || true
}

# secret_key <ns> <name> <key> — data.<key>, base64-decoded (Secret data is
# base64); "" when anything is missing.
secret_key() {
  local v
  v=$(kc -n "$1" get secret "$2" -o go-template="{{index .data \"$3\"}}" 2>/dev/null) || return 0
  [[ -n "$v" ]] || return 0
  printf '%s' "$v" | b64decode
}

# deployment_env_entry <ns> <deployment> <name> — the first container's explicit
# env entry for one variable, as one line:
#   literal <value>          env[].value
#   configmap <name> <key>   env[].valueFrom.configMapKeyRef
#   secret <name> <key>      env[].valueFrom.secretKeyRef
# Nothing when the container does not declare the variable.
deployment_env_entry() {
  kc -n "$1" get deployment "$2" -o go-template="{{range (index .spec.template.spec.containers 0).env}}{{if eq .name \"$3\"}}{{if .valueFrom}}{{if .valueFrom.configMapKeyRef}}configmap {{.valueFrom.configMapKeyRef.name}} {{.valueFrom.configMapKeyRef.key}}{{end}}{{if .valueFrom.secretKeyRef}}secret {{.valueFrom.secretKeyRef.name}} {{.valueFrom.secretKeyRef.key}}{{end}}{{else}}literal {{.value}}{{end}}{{\"\\n\"}}{{end}}{{end}}" 2>/dev/null || true
}

# deployment_env_from <ns> <deployment> — the first container's envFrom sources
# in order, one "configmap <name>" / "secret <name>" line each.
deployment_env_from() {
  kc -n "$1" get deployment "$2" -o go-template="{{range (index .spec.template.spec.containers 0).envFrom}}{{if .configMapRef}}configmap {{.configMapRef.name}}{{\"\\n\"}}{{end}}{{if .secretRef}}secret {{.secretRef.name}}{{\"\\n\"}}{{end}}{{end}}" 2>/dev/null || true
}

# deployment_env_effective <ns> <deployment> <name> — the value Kubernetes
# would give the container, resolving envFrom:
#   1. an explicit env[] entry wins (literal value or valueFrom ref), even when
#      its value is empty;
#   2. otherwise envFrom is scanned in order (later sources win) for a source
#      that provides the key. A source without the key (or with an empty value)
#      does not override an earlier one.
# A missing object/key or a Forbidden read is "" — never an error.
deployment_env_effective() {
  local ns=$1 dep=$2 name=$3 entry kind obj key v=""
  entry=$(deployment_env_entry "$ns" "$dep" "$name")
  if [[ -n "$entry" ]]; then
    case "$entry" in
      literal\ *)   printf '%s' "${entry#literal }" ;;
      configmap\ *) read -r _ obj key <<<"$entry"; configmap_key "$ns" "$obj" "$key" ;;
      secret\ *)    read -r _ obj key <<<"$entry"; secret_key "$ns" "$obj" "$key" ;;
    esac
    return 0
  fi
  while read -r kind obj; do
    [[ -n "${obj:-}" ]] || continue
    case "$kind" in
      configmap) v=$(configmap_key "$ns" "$obj" "$name") ;;
      secret)    v=$(secret_key "$ns" "$obj" "$name") ;;
    esac
  done <<<"$(deployment_env_from "$ns" "$dep")"
  printf '%s' "$v"
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
# sts_replicas <ns> <name> <present 1|0> — desired replicas for a StatefulSet
# (the same semantics as deployment_replicas: unset means 1).
sts_replicas() {
  local n
  if [[ "$3" != "1" ]]; then echo 0; return; fi
  n=$(kc -n "$1" get statefulset "$2" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
  echo "${n:-1}"
}

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

# dsn_user <postgres://user[:pass]@host/db> — the role component ("" when the
# string is not a conninfo URL or has no user). Percent-encoding is left as-is:
# roles with characters that need it are not a shape the probe can name anyway.
dsn_user() {
  local rest authority user
  rest=${1#*://}
  [[ "$rest" != "$1" ]] || return 0
  authority=${rest%%/*}
  [[ "$authority" == *@* ]] || return 0
  user=${authority%@*}
  user=${user%%:*}
  printf '%s' "$user"
}

# store_probe_dsn <aws|gcp> — the DSN the substrate uses, from its own Secret
# (jaiscloud-dsn in NS_AWS / jaiscloud-gcp-dsn in NS_GCP, key dsn).
store_probe_dsn() {
  case "$1" in
    aws) secret_key "$NS_AWS" jaiscloud-dsn dsn ;;
    gcp) secret_key "$NS_GCP" jaiscloud-gcp-dsn dsn ;;
  esac
}

# store_probe_owner <database> — the substrate whose Secret owns that database
# (the bundled store is one server, so the other substrate's role is a fallback
# for the localized name only).
store_probe_owner() {
  case "$1" in
    jaiscloud)     printf aws ;;
    jaiscloud_gcp) printf gcp ;;
  esac
}

# store_pg_user <database> — the role the probe connects as:
#   1. STORE_PG_USER when set (an explicit escape hatch);
#   2. the user component of that database's substrate DSN Secret, then the
#      other substrate's (a bundled store is one server, so both are the same
#      role; a per-substrate override always wins for its own database);
#   3. the shipped role, jaiscloud.
# The role is used over the pod's local socket, so the password never enters the
# exec argv; db_state retries with the full DSN only when that socket refuses the
# role (a host store that requires a password even locally).
store_pg_user() {
  local db=$1 own other user=""
  [[ -n "$STORE_PG_USER_ENV" ]] && { printf '%s' "$STORE_PG_USER_ENV"; return; }
  own=$(store_probe_owner "$db")
  [[ "$own" == "aws" ]] && other=gcp || other=aws
  [[ -n "$own" ]] && user=$(dsn_user "$(store_probe_dsn "$own")")
  [[ -z "$user" ]] && user=$(dsn_user "$(store_probe_dsn "$other")")
  [[ -n "$user" ]] || user=jaiscloud
  printf '%s' "$user"
}

# db_state <name> — TRI-STATE: true|false|null.
#   true|false  the resolved bundled store workload is ready and psql answered
#               (psql exits 0; an empty row set means the database is absent)
#   null        the store is external, the workload is absent/not ready, or the
#               psql exec failed/timed out — "unknown", never "missing"
# The exec is wall-clock bounded through `bounded` (never a bare GNU `timeout`).
# It connects as store_pg_user (never a hardcoded role) over the pod's local
# socket; a non-timeout failure is retried once with the full DSN, which is what
# the emulator itself uses, for a store whose local auth requires a password.
db_state() {
  local db=$1 out rc=0 user dsn
  [[ "$STORE_MODE" == "external" ]] && { echo null; return; }
  [[ "$STORE_PRESENT" == "1" && -n "$STORE_WORKLOAD" ]] || { echo null; return; }
  [[ "$STORE_READY" == "1" ]] || { echo null; return; }
  user=$(store_pg_user "$db")
  out=$(bounded 15 "${KUBECTL[@]}" --request-timeout=10s -n "$NS_DATA" exec "$STORE_WORKLOAD" -- \
        psql -U "$user" -d "$STORE_PG_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" 2>/dev/null) || rc=$?
  out=$(printf '%s' "$out" | tr -d '[:space:]')
  if [[ "$rc" -ne 0 && "$rc" -ne 124 && "$rc" -ne 143 ]]; then
    dsn=$(store_probe_dsn "$(store_probe_owner "$db")")
    if [[ -n "$dsn" ]]; then
      rc=0
      out=$(bounded 15 "${KUBECTL[@]}" --request-timeout=10s -n "$NS_DATA" exec "$STORE_WORKLOAD" -- \
            psql "$dsn" -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" 2>/dev/null) || rc=$?
      out=$(printf '%s' "$out" | tr -d '[:space:]')
    fi
  fi
  [[ "$rc" -eq 0 ]] || { echo null; return; }
  case "$out" in
    1) echo true ;;
    "") echo false ;;
    *) echo null ;;
  esac
}

# db_exists <name> — 1 only when the database is known to exist (tri-state true).
db_exists() { [[ "$(db_state "$1")" == "true" ]] && echo 1 || echo 0; }

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

# ── platform: Kubernetes version + the namespace-isolation objects ──────────
# Both substrate ServiceAccounts hold cluster-wide namespace delete; the
# ValidatingAdmissionPolicy (K8s >= 1.30) is the boundary between them. This is a
# read-only probe: `kubectl get`/`kubectl version` only.
SERVER_INFO=$(kc version -o json 2>/dev/null || true)
SERVER_INFO=$(printf '%s' "$SERVER_INFO" | tr -d '[:space:]')
SERVER_VERSION=$(printf '%s' "$SERVER_INFO" | sed -n 's/.*"serverVersion":{[^}]*"gitVersion":"\([^"]*\)".*/\1/p')
SERVER_MINOR=$(printf '%s' "$SERVER_INFO" | sed -n 's/.*"serverVersion":{[^}]*"minor":"\([^"]*\)".*/\1/p')
if [[ -z "$SERVER_VERSION" ]]; then
  # Older/newer kubectl may not print serverVersion in `version -o json`.
  SERVER_VERSION=$(kc get --raw /version 2>/dev/null \
    | tr -d '[:space:]' | sed -n 's/.*"gitVersion":"\([^"]*\)".*/\1/p' || true)
fi
if [[ -z "$SERVER_MINOR" ]]; then
  SERVER_MINOR=$(kc get --raw /version 2>/dev/null \
    | tr -d '[:space:]' | sed -n 's/.*"minor":"\([^"]*\)".*/\1/p' || true)
fi
SERVER_MINOR=${SERVER_MINOR//[!0-9]/}
SERVER_OLD=0
if [[ -n "$SERVER_MINOR" ]] && (( SERVER_MINOR < 30 )); then SERVER_OLD=1; fi
VAP_PRESENT=$(probe_ok kc get validatingadmissionpolicy jaiscloud-namespace-isolation)
# Namespace admin is bound per substrate (…-aws / …-gcp), so a single-substrate
# install binds only its own; `namespaceAdmin` is derived from the intent below.
NAMESPACE_ADMIN_AWS=$(probe_ok kc get clusterrolebinding jaiscloud-namespace-admin-aws)
NAMESPACE_ADMIN_GCP=$(probe_ok kc get clusterrolebinding jaiscloud-namespace-admin-gcp)
# Pre-split installs bound one combined ClusterRoleBinding to both ServiceAccounts;
# the split manifests replace it with the two per-substrate bindings above.
LEGACY_NAMESPACE_ADMIN=$(probe_ok kc get clusterrolebinding jaiscloud-namespace-admin)

NS_AWS_P=$(ns_present "$NS_AWS"); NS_GCP_P=$(ns_present "$NS_GCP"); NS_DATA_P=$(ns_present "$NS_DATA")
ROLE_AWS=$(field "" namespace "$NS_AWS" '{{index .metadata.labels "jaiscloud.io/role"}}' 2>/dev/null || true)
[[ -z "$ROLE_AWS" ]] && ROLE_AWS=""
SUB_AWS=$(field "" namespace "$NS_AWS" '{{index .metadata.labels "jaiscloud.io/substrate"}}' 2>/dev/null || true)
ROLE_GCP=$(field "" namespace "$NS_GCP" '{{index .metadata.labels "jaiscloud.io/role"}}' 2>/dev/null || true)
ROLE_DATA=$(field "" namespace "$NS_DATA" '{{index .metadata.labels "jaiscloud.io/role"}}' 2>/dev/null || true)

# The pre-split store was a Deployment; --migrate-store also accepts a
# StatefulSet, so both count as the legacy store here. Presence alone is not a
# pending migration: the object must carry a JaisCloud marker, otherwise it is a
# candidate (someone's Postgres that happens to be named postgres).
LEGACY_PG_DEPLOY=$(deployment_present "$NS_AWS" postgres)
LEGACY_PG_STS=$(probe_ok kc -n "$NS_AWS" get statefulset postgres)
LEGACY_PG_OBJECT=""
if [[ "$LEGACY_PG_STS" == "1" && "$LEGACY_PG_DEPLOY" != "1" ]]; then
  LEGACY_PG_OBJECT="statefulset/postgres"
elif [[ "$LEGACY_PG_DEPLOY" == "1" ]]; then
  LEGACY_PG_OBJECT="deployment/postgres"
fi
LEGACY_PG=0
LEGACY_PG_MARKED=0
if [[ -n "$LEGACY_PG_OBJECT" ]]; then
  LEGACY_PG=1
  legacy_marked "$NS_AWS" "${LEGACY_PG_OBJECT%%/*}" "${LEGACY_PG_OBJECT#*/}" && LEGACY_PG_MARKED=1
fi
LEGACY_GCP=$(deployment_present "$NS_AWS" jaiscloud-gcp)
LEGACY_GCP_MARKED=0
[[ "$LEGACY_GCP" == "1" ]] && legacy_marked "$NS_AWS" deployment jaiscloud-gcp && LEGACY_GCP_MARKED=1

# ── bundled store workload ──────────────────────────────────────────────────
# postgres.yaml is a Deployment, but a host may run its store as a StatefulSet
# (the common managed shape) or under another name. Resolve it the same way
# --migrate-store resolves a legacy store, so the database probe follows the
# object that actually serves: STORE_WORKLOAD=<kind>/<name> when set, else
# deployment/postgres, then statefulset/postgres.
STORE_WORKLOAD_ENV="${STORE_WORKLOAD:-}"
STORE_KIND=""
STORE_NAME=""
STORE_WORKLOAD=""
if [[ -n "$STORE_WORKLOAD_ENV" ]]; then
  case "$STORE_WORKLOAD_ENV" in
    deployment/?*|statefulset/?*) ;;
    *) usage_error "STORE_WORKLOAD must be '<deployment|statefulset>/<name>' (got '$STORE_WORKLOAD_ENV')" ;;
  esac
  STORE_KIND="${STORE_WORKLOAD_ENV%%/*}"
  STORE_NAME="${STORE_WORKLOAD_ENV#*/}"
  case "$STORE_NAME" in
    */*) usage_error "STORE_WORKLOAD must be '<deployment|statefulset>/<name>' (got '$STORE_WORKLOAD_ENV')" ;;
  esac
  STORE_WORKLOAD="$STORE_WORKLOAD_ENV"
elif [[ "$(deployment_present "$NS_DATA" postgres)" == "1" ]]; then
  STORE_KIND=deployment; STORE_NAME=postgres; STORE_WORKLOAD=deployment/postgres
elif [[ "$(sts_present "$NS_DATA" postgres)" == "1" ]]; then
  STORE_KIND=statefulset; STORE_NAME=postgres; STORE_WORKLOAD=statefulset/postgres
fi
STORE_WL_PRESENT=0
STORE_READY=0
case "$STORE_KIND" in
  deployment)
    STORE_WL_PRESENT=$(deployment_present "$NS_DATA" "$STORE_NAME")
    [[ "$STORE_WL_PRESENT" == "1" ]] && STORE_READY=$(deployment_ready "$NS_DATA" "$STORE_NAME") ;;
  statefulset)
    STORE_WL_PRESENT=$(sts_present "$NS_DATA" "$STORE_NAME")
    [[ "$STORE_WL_PRESENT" == "1" ]] && STORE_READY=$(sts_ready "$NS_DATA" "$STORE_NAME") ;;
esac
# postgres-pvc is the Deployment's claim. A StatefulSet derives its claim from a
# volumeClaimTemplate (postgres-data-postgres-0), so it is not reported here.
STORE_PVC=""
[[ "$STORE_KIND" == "deployment" ]] && STORE_PVC=$(pvc_status "$NS_DATA" postgres-pvc)
# Database probes are tri-state and depend on store.mode, so they run after the
# mode is resolved (below), not here.

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
# The Dataproc driver/executor image. It is a deployment env var, not the
# substrate's own image, so it is probed separately — through envFrom too, since
# a Deployment may inject it from a ConfigMap/Secret.
GCP_SPARK_IMAGE=$(deployment_env_effective "$NS_GCP" jaiscloud-gcp JAISCLOUD_K8S_SPARK_IMAGE)
GCP_SPARK_PRIVATE=0
[[ -n "$GCP_SPARK_IMAGE" ]] && GCP_SPARK_PRIVATE=$(private_registry "$GCP_SPARK_IMAGE")
# Managed Kafka broker mode. A real broker (k8s/docker/native) has no PVC, so a
# park loses its topics (the cluster/topic records and namespace survive). The
# /park response reports topicDataLost; this warns before anyone parks.
GCP_KAFKA_BROKER_MODE=$(deployment_env_effective "$NS_GCP" jaiscloud-gcp JAISCLOUD_KAFKA_BROKER_MODE)
GCP_KAFKA_REAL=0
case "$GCP_KAFKA_BROKER_MODE" in
  k8s|docker|native) GCP_KAFKA_REAL=1 ;;
esac
# sparkImageGCS — a deliberately simple heuristic, not a registry query: an unset
# value or a plain apache/spark image has no GCS connector; any other name is
# assumed to be a connector build (the repo's spark-gcs, a GCS-enabled image, ...).
GCP_SPARK_GCS=0
if [[ -n "$GCP_SPARK_IMAGE" ]]; then
  case "$GCP_SPARK_IMAGE" in
    apache/spark|apache/spark:*) GCP_SPARK_GCS=0 ;;
    *) GCP_SPARK_GCS=1 ;;
  esac
fi

# Per-cloud ownership evidence for the leak probe. A namespace alone is shared,
# long-lived state and proves nothing; its ServiceAccount, Deployment, database or
# PVCs do.
SA_AWS=$(sa_present "$NS_AWS" jaiscloud)
SA_GCP=$(sa_present "$NS_GCP" jaiscloud-gcp)
PVC_AWS_BLOBS=$(pvc_present "$NS_AWS" jaiscloud-blobs-pvc)
PVC_AWS_STATE=$(pvc_present "$NS_AWS" jaiscloud-state-pvc)
PVC_GCP_BLOBS=$(pvc_present "$NS_GCP" jaiscloud-gcp-blobs-pvc)
PVC_GCP_STATE=$(pvc_present "$NS_GCP" jaiscloud-gcp-state-pvc)
SUB_GCP=$(field "" namespace "$NS_GCP" '{{index .metadata.labels "jaiscloud.io/substrate"}}')

# A substrate is present when either substrate namespace or Deployment exists:
# the namespace-admin binding is cluster-wide, so isolation matters even for a
# parked or half-removed install.
SUBSTRATE_PRESENT=0
[[ "$AWS_PRESENT" == "1" || "$GCP_PRESENT" == "1" || "$NS_AWS_P" == "1" || "$NS_GCP_P" == "1" ]] && SUBSTRATE_PRESENT=1

# ── intent: flag > install record (jaiscloud-data/jaiscloud-install) > inferred ─
RECORD_PRESENT=$(probe_ok kc -n "$NS_DATA" get configmap jaiscloud-install)
RECORD_INSTALLED=""
RECORD_DESIRED=""
RECORD_STORE=""
if [[ "$RECORD_PRESENT" == "1" ]]; then
  RECORD_INSTALLED=$(field "$NS_DATA" configmap jaiscloud-install '{{index .data "installed"}}')
  RECORD_DESIRED=$(field "$NS_DATA" configmap jaiscloud-install '{{index .data "desired"}}')
  RECORD_STORE=$(field "$NS_DATA" configmap jaiscloud-install '{{index .data "store"}}')
fi

# The store is either bundled (a PostgreSQL workload in NS_DATA) or external (a
# server the operator brought; deploy.sh --store-dsn). The record is
# authoritative; otherwise a workload (resolved above) means bundled, and a
# substrate without one means external. An explicit STORE_WORKLOAD is the
# operator's declaration of a bundled store even while the object is missing, so
# it never flips the mode to external. For an external store there is no pod to
# exec, so the database probe is null = "unknown", never "missing".
STORE_MODE="bundled"
if [[ -n "$RECORD_STORE" ]]; then
  case "$RECORD_STORE" in
    bundled|external) STORE_MODE="$RECORD_STORE" ;;
  esac
elif [[ -n "$STORE_WORKLOAD_ENV" ]]; then
  STORE_MODE="bundled"
elif [[ "$STORE_WL_PRESENT" != "1" && "$RECORD_PRESENT" != "1" && "$SUBSTRATE_PRESENT" == "1" ]]; then
  STORE_MODE="external"
fi
if [[ "$STORE_MODE" == "external" ]]; then
  STORE_PRESENT=1
  STORE_READY=0
else
  STORE_PRESENT="$STORE_WL_PRESENT"
fi
# Tri-state database probes + the probe disposition. "exec" means the bundled
# store workload was ready and both psql probes returned an answer (true or
# false); anything else is "unavailable" and store.probeReason says why
# (external store, no bundled workload, a workload that is not ready, or a
# failed exec) and the databases stay null.
DB_AWS=$(db_state jaiscloud)
DB_GCP=$(db_state jaiscloud_gcp)
STORE_PROBE_REASON="ok"
if [[ "$STORE_MODE" == "external" ]]; then
  STORE_PROBE_REASON="external"
elif [[ "$STORE_PRESENT" != "1" ]]; then
  STORE_PROBE_REASON="no-workload"
elif [[ "$STORE_READY" != "1" ]]; then
  STORE_PROBE_REASON="not-ready"
elif [[ "$DB_AWS" == "null" || "$DB_GCP" == "null" ]]; then
  STORE_PROBE_REASON="exec-failed"
fi
DATABASES_PROBE="unavailable"
[[ "$STORE_PROBE_REASON" == "ok" ]] && DATABASES_PROBE="exec"

# A pre-split object that is scaled to 0 while the new store serves is a
# retained rollback copy of a *marked* (migrated) store — deploy.sh
# --migrate-store keeps it by default — not a pending migration:
# re-recommending migrate-store forever would be noise. With an external store
# the new store has no Deployment, so the record mode is the evidence that the
# migration already happened.
LEGACY_PG_REPLICAS=$(deployment_replicas "$NS_AWS" postgres "$LEGACY_PG_DEPLOY")
[[ "$LEGACY_PG_DEPLOY" == "1" ]] || LEGACY_PG_REPLICAS=$(sts_replicas "$NS_AWS" postgres "$LEGACY_PG_STS")
LEGACY_PG_RETAINED=0
if [[ "$LEGACY_PG_MARKED" == "1" && "$LEGACY_PG_REPLICAS" == "0" ]]; then
  if [[ "$STORE_PRESENT" == "1" || "$STORE_MODE" == "external" ]]; then LEGACY_PG_RETAINED=1; fi
fi
LEGACY_GCP_REPLICAS=$(deployment_replicas "$NS_AWS" jaiscloud-gcp "$LEGACY_GCP")
LEGACY_GCP_RETAINED=0
if [[ "$LEGACY_GCP_MARKED" == "1" && "$LEGACY_GCP_REPLICAS" == "0" ]]; then
  if [[ "$GCP_PRESENT" == "1" || "$STORE_PRESENT" == "1" || "$STORE_MODE" == "external" ]]; then
    LEGACY_GCP_RETAINED=1
  fi
fi
# Only a marked object is a pending migration; an unmarked one is a candidate the
# operator must identify (and confirm) before any tooling touches it.
LEGACY_PENDING=0
[[ "$LEGACY_PG_MARKED" == "1" && "$LEGACY_PG_RETAINED" == "0" ]] && LEGACY_PENDING=1
[[ "$LEGACY_GCP_MARKED" == "1" && "$LEGACY_GCP_RETAINED" == "0" ]] && LEGACY_PENDING=1
LEGACY_CANDIDATE=0
[[ "$LEGACY_PG" == "1" && "$LEGACY_PG_MARKED" == "0" ]] && LEGACY_CANDIDATE=1
[[ "$LEGACY_GCP" == "1" && "$LEGACY_GCP_MARKED" == "0" ]] && LEGACY_CANDIDATE=1

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

# The namespace-admin ClusterRoleBinding is per substrate (…-aws / …-gcp), so a
# clean AWS-only install binds only jaiscloud-namespace-admin-aws. Report the
# binding that matches the intent; with no intent, accept either (or both).
NAMESPACE_ADMIN_PRESENT=0
if [[ -z "$INSTALLED" ]]; then
  if [[ "$NAMESPACE_ADMIN_AWS" == "1" || "$NAMESPACE_ADMIN_GCP" == "1" ]]; then
    NAMESPACE_ADMIN_PRESENT=1
  fi
else
  if list_has "$INSTALLED" aws && [[ "$NAMESPACE_ADMIN_AWS" == "1" ]]; then
    NAMESPACE_ADMIN_PRESENT=1
  fi
  if list_has "$INSTALLED" gcp && [[ "$NAMESPACE_ADMIN_GCP" == "1" ]]; then
    NAMESPACE_ADMIN_PRESENT=1
  fi
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
# has real objects for. A namespace existing is NOT a reason: namespaces are
# shared and long-lived, so the evidence is the substrate's own objects — its
# Deployment, its ServiceAccount, its database, its blobs/state PVCs — or a
# namespace labelled as belonging to that substrate. Tracked separately so
# --strict can fail on a leak.
LEAKS=()
intent_label="${INSTALLED:-none}"
for cloud in aws gcp; do
  list_has "$INSTALLED" "$cloud" && continue
  if [[ "$cloud" == "aws" ]]; then
    ns=$NS_AWS dep=$AWS_PRESENT db=$DB_AWS sa=$SA_AWS sub=$SUB_AWS
    workload=jaiscloud sa_name=jaiscloud
    blobs_name=jaiscloud-blobs-pvc state_name=jaiscloud-state-pvc
    blobs_p=$PVC_AWS_BLOBS state_p=$PVC_AWS_STATE
  else
    ns=$NS_GCP dep=$GCP_PRESENT db=$DB_GCP sa=$SA_GCP sub=$SUB_GCP
    workload=jaiscloud-gcp sa_name=jaiscloud-gcp
    blobs_name=jaiscloud-gcp-blobs-pvc state_name=jaiscloud-gcp-state-pvc
    blobs_p=$PVC_GCP_BLOBS state_p=$PVC_GCP_STATE
  fi
  reasons=""
  [[ "$dep" == "1" ]] && reasons+="${reasons:+, }deployment/$workload in $ns exists"
  [[ "$sa" == "1" ]] && reasons+="${reasons:+, }serviceaccount/$sa_name in $ns exists"
  [[ "$db" == "true" ]] && reasons+="${reasons:+, }its database exists"
  [[ "$blobs_p" == "1" ]] && reasons+="${reasons:+, }pvc/$blobs_name in $ns exists"
  [[ "$state_p" == "1" ]] && reasons+="${reasons:+, }pvc/$state_name in $ns exists"
  [[ "$sub" == "$cloud" ]] && reasons+="${reasons:+, }namespace $ns is labelled jaiscloud.io/substrate=$cloud"
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

# ── effective RBAC: what the running ServiceAccounts may actually do ────────
# The Deployment's spec.template.spec.serviceAccountName is the identity that
# matters — a binding on the default name does nothing for a renamed SA. Fall
# back to the conventional name when the field is unset (Kubernetes defaults to
# "default", but the manifests always name their SA).
AWS_SA=$(field "$NS_AWS" deployment jaiscloud '{{.spec.template.spec.serviceAccountName}}')
AWS_SA=${AWS_SA:-jaiscloud}
GCP_SA=$(field "$NS_GCP" deployment jaiscloud-gcp '{{.spec.template.spec.serviceAccountName}}')
GCP_SA=${GCP_SA:-jaiscloud-gcp}

AWS_DELETE_NS=$(auth_can_i delete namespaces "$NS_AWS" "$AWS_SA")
AWS_CREATE_NS=$(auth_can_i create namespaces "$NS_AWS" "$AWS_SA")
AWS_UPDATE_NS=$(auth_can_i update namespaces "$NS_AWS" "$AWS_SA")
AWS_CREATE_RB=$(auth_can_i create rolebindings "$NS_AWS" "$AWS_SA")
GCP_DELETE_NS=$(auth_can_i delete namespaces "$NS_GCP" "$GCP_SA")
GCP_CREATE_NS=$(auth_can_i create namespaces "$NS_GCP" "$GCP_SA")
GCP_UPDATE_NS=$(auth_can_i update namespaces "$NS_GCP" "$GCP_SA")
GCP_CREATE_RB=$(auth_can_i create rolebindings "$NS_GCP" "$GCP_SA")

# The intended substrate's effective namespace delete is what rbac.namespaceAdmin
# means now (kept boolean for compatibility). Unknown never counts as true, so a
# caller who may not impersonate gets false plus the "cannot verify" warning
# rather than a silent yes.
NAMESPACE_ADMIN_EFFECTIVE=0
if [[ -z "$INSTALLED" ]]; then
  if [[ "$AWS_DELETE_NS" == "yes" || "$GCP_DELETE_NS" == "yes" ]]; then NAMESPACE_ADMIN_EFFECTIVE=1; fi
else
  if list_has "$INSTALLED" aws && [[ "$AWS_DELETE_NS" == "yes" ]]; then NAMESPACE_ADMIN_EFFECTIVE=1; fi
  if list_has "$INSTALLED" gcp && [[ "$GCP_DELETE_NS" == "yes" ]]; then NAMESPACE_ADMIN_EFFECTIVE=1; fi
fi

# Does the isolation policy's matchConditions actually name the identity that is
# running? Until the policy is rendered from NS_*/SA_* (F3), a renamed namespace
# or ServiceAccount silently leaves the boundary open: the policy matches
# usernames that no request carries.
VAP_EXPR=""
if [[ "$VAP_PRESENT" == "1" ]]; then
  VAP_EXPR=$(kc get validatingadmissionpolicy jaiscloud-namespace-isolation \
    -o go-template='{{range .spec.matchConditions}}{{.expression}}{{"\n"}}{{end}}' 2>/dev/null || true)
fi
# policy_mentions <namespace> <sa> — true|false|null (null: no policy, no read).
policy_mentions() {
  if [[ "$VAP_PRESENT" != "1" || -z "$VAP_EXPR" ]]; then echo null; return; fi
  if printf '%s' "$VAP_EXPR" | grep -Fq "system:serviceaccount:$1:$2"; then echo true; else echo false; fi
}
POLICY_AWS=$(policy_mentions "$NS_AWS" "$AWS_SA")
POLICY_GCP=$(policy_mentions "$NS_GCP" "$GCP_SA")
POLICY_MATCHES_SA=unknown
if [[ "$POLICY_AWS" != "null" ]]; then
  POLICY_MATCHES_SA=true
  [[ "$AWS_PRESENT" == "1" && "$POLICY_AWS" == "false" ]] && POLICY_MATCHES_SA=false
  [[ "$GCP_PRESENT" == "1" && "$POLICY_GCP" == "false" ]] && POLICY_MATCHES_SA=false
fi

# ── DNS: are cloud suffixes rewritten to the gateway, and is that still valid? ─
# Read-only: kube-system/coredns is the cluster's resolver; an absent ConfigMap or
# a Forbidden read means the rewrite state is unknown, never "absent".
COREDNS_PRESENT=$(probe_ok kc -n kube-system get configmap coredns)
COREFILE=""
[[ "$COREDNS_PRESENT" == "1" ]] && COREFILE=$(configmap_key kube-system coredns Corefile)
DNS_REWRITES=unknown
DNS_SUFFIXES=""
DNS_STALE=0
DNS_TARGET=""
DNS_TARGETS_JSON=""
DNS_TARGETS_SEEN=""
DNS_TARGET_LINES=""
# A "gateway" means both the Istio Gateway route resource and a ready ingress
# Deployment: a rewrite pointing at a gateway Service with no route accepts the
# connection and answers 404/503.
GATEWAY_READY=0
[[ "$MESH_GW" == "1" && "$MESH_GATEWAY" == "1" ]] && GATEWAY_READY=1

# dns_rewrite_target <suffix-pattern> — the target of the first rewrite or
# template for one cloud suffix, as "name|<dns-name>" or "ip|<address>", or
# "unknown|" when the construct exists but its target is not parseable. Empty
# when the Corefile has no rewrite/template line for the suffix.
# Only the two documented shapes are parsed (deploy/k8s/istio/dns.yaml):
#   rewrite [stop] name regex (.*\.)?googleapis\.com\.?$ <target>
#   template IN A (.*\.)?googleapis\.com\.?$ { ... answer "{{ .Name }}" <ip> ... }
dns_rewrite_target() {
  local pat=$1 line last target="" seen=0 in_tpl=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line=${line%%#*}
    [[ -n "${line//[[:space:]]/}" ]] || continue
    if (( in_tpl )); then
      if [[ "$line" =~ (^|[^[:alnum:]_])answer([^[:alnum:]_]|$) ]]; then
        last=${line##*[[:space:]]}
        last=${last%\"}
        [[ -n "$target" ]] || target="ip|$last"
      fi
      [[ "$line" == *"}"* ]] && in_tpl=0
      continue
    fi
    if [[ "$line" =~ ^[[:space:]]*rewrite([[:space:]]|$) ]] && [[ "$line" =~ $pat ]]; then
      last=${line##*[[:space:]]}
      last=${last%.}
      case "$last" in
        ""|rewrite|stop|name|suffix|regex|answer) printf 'unknown|\n' ;;
        *) printf 'name|%s\n' "$last" ;;
      esac
      return 0
    fi
    if [[ "$line" =~ ^[[:space:]]*template([[:space:]]|$) ]] && [[ "$line" =~ $pat ]]; then
      seen=1
      if [[ "$line" == *"}"* ]]; then in_tpl=0; else in_tpl=1; fi
      # The documented form puts `answer` on its own line; accept the one-line
      # form too by stripping the closing brace before taking the last token.
      if [[ "$line" =~ (^|[^[:alnum:]_])answer([^[:alnum:]_]|$) ]]; then
        last=${line%\}*}
        last=${last##*[[:space:]]}
        last=${last%\"}
        [[ -n "$target" ]] || target="ip|$last"
      fi
      continue
    fi
  done <<<"$COREFILE"
  if (( seen )); then
    if [[ -n "$target" ]]; then printf '%s\n' "$target"; else printf 'unknown|\n'; fi
  fi
  return 0
}

if [[ "$COREDNS_PRESENT" == "1" && -n "$COREFILE" ]]; then
  DNS_REWRITES=false
  for suffix in amazonaws.com googleapis.com run.app cloudfunctions.net; do
    case "$suffix" in
      amazonaws.com)      pat='amazonaws\\?[.]com';      owner=aws ;;
      googleapis.com)     pat='googleapis\\?[.]com';     owner=gcp ;;
      run.app)            pat='run\\?[.]app';            owner=gcp ;;
      cloudfunctions.net) pat='cloudfunctions\\?[.]net'; owner=gcp ;;
    esac
    raw=$(dns_rewrite_target "$pat")
    [[ -n "$raw" ]] || continue
    DNS_REWRITES=true
    DNS_SUFFIXES=$(list_add "$DNS_SUFFIXES" "$suffix")
    kind=${raw%%|*}
    value=${raw#*|}
    target=unknown
    case "$kind" in
      name)
        case "$value" in
          *.svc.cluster.local)
            rest=${value%.svc.cluster.local}
            tname=${rest%%.*}
            tns=${rest#*.}
            if [[ "$tns" == "istio-system" && "$tname" == "istio-ingressgateway" ]]; then
              target=gateway
            else
              target="service:$tns/$tname"
            fi ;;
        esac ;;
      ip)
        hit=$(svc_by_cluster_ip "$value")
        if [[ -n "$hit" ]]; then
          target="service:$hit"
          [[ "$hit" == "istio-system/istio-ingressgateway" ]] && target=gateway
        fi ;;
    esac

    case "$target" in
      gateway)
        if [[ "$GATEWAY_READY" != "1" ]]; then
          warn_add "CoreDNS rewrites $suffix to the JaisCloud gateway, but no ready gateway serves it (istio-system/istio-ingressgateway + Gateway/jaiscloud): clients that resolve $suffix hit a dead route; remove the $suffix rewrite or (re)install the mesh — see deploy/k8s/istio/dns.yaml"
          DNS_STALE=1
        else
          case "$owner" in
            aws) owner_state=$AWS_STATE ;;
            gcp) owner_state=$GCP_STATE ;;
          esac
          case "$owner_state" in
            parked|absent)
              warn_add "CoreDNS rewrites $suffix to the JaisCloud gateway, but the $owner substrate is $owner_state: clients that resolve $suffix reach a parked/absent backend and time out (or get a fast 503); remove the $suffix rewrite for now (deploy/k8s/istio/dns.yaml) and re-add it when you resume $owner"
              DNS_STALE=1 ;;
          esac
        fi ;;
      service:*)
        rest=${target#service:}
        tns=${rest%%/*}
        tname=${rest#*/}
        if [[ "$(svc_present "$tns" "$tname")" != "1" ]]; then
          warn_add "CoreDNS rewrites $suffix to service $tns/$tname, which does not exist: clients that resolve $suffix reach nothing; remove the $suffix rewrite or recreate $tns/$tname — see deploy/k8s/istio/dns.yaml"
          DNS_STALE=1
        elif [[ "$(svc_ready "$tns" "$tname")" != "1" ]]; then
          warn_add "CoreDNS rewrites $suffix to service $tns/$tname, which has no ready endpoints (parked or scaled to 0): clients that resolve $suffix reach a dead backend; remove the $suffix rewrite until $tns/$tname is ready again — see deploy/k8s/istio/dns.yaml"
          DNS_STALE=1
        fi ;;
    esac

    DNS_TARGETS_JSON+="${DNS_TARGETS_JSON:+, }\"$suffix\": \"$(json_escape "$target")\""
    DNS_TARGET_LINES+="$suffix -> $target"$'\n'
    case " $DNS_TARGETS_SEEN " in
      *" $target "*) ;;
      *) DNS_TARGETS_SEEN="${DNS_TARGETS_SEEN:+$DNS_TARGETS_SEEN }$target" ;;
    esac
  done
  case "$DNS_TARGETS_SEEN" in
    "") ;;
    *" "*) DNS_TARGET="mixed" ;;
    *) DNS_TARGET="$DNS_TARGETS_SEEN" ;;
  esac
fi

# ── CA copies: only a mesh-labelled copy can be a stale JaisCloud CA ────────
# Park keeps the CA (a resumed substrate and its managed workloads still trust
# the front door), so a copy is only stale when the issuer it names is gone.
# certs/gen.sh labels what it creates (jaiscloud.io/issuer=mesh) and deploy.sh
# labels a copy it restores; anything else in the namespace is a host-issued CA
# the probe must not judge.
ca_issuer_kind() { # ca_issuer_kind <namespace> <present 1|0> — mesh|external|absent
  local label
  [[ "$2" == "1" ]] || { echo absent; return; }
  label=$(field "$1" configmap jaiscloud-ca-cert '{{index .metadata.labels "jaiscloud.io/issuer"}}')
  [[ "$label" == "mesh" ]] && echo mesh || echo external
}
CA_AWS=$(probe_ok kc -n "$NS_AWS" get configmap jaiscloud-ca-cert)
CA_GCP=$(probe_ok kc -n "$NS_GCP" get configmap jaiscloud-ca-cert)
CA_ISSUER=0
[[ "$MESH_SECRET" == "1" || "$MESH_GW" == "1" ]] && CA_ISSUER=1
CA_ISSUER_AWS=$(ca_issuer_kind "$NS_AWS" "$CA_AWS")
CA_ISSUER_GCP=$(ca_issuer_kind "$NS_GCP" "$CA_GCP")
if [[ "$CA_ISSUER" != "1" ]]; then
  [[ "$CA_ISSUER_AWS" == "mesh" ]] && \
    warn_add "the CA ConfigMap jaiscloud-ca-cert in $NS_AWS is labelled jaiscloud.io/issuer=mesh, but no issuing mesh exists (istio-system/jaiscloud-wildcard is missing): stale CA — re-run deploy/k8s/istio/certs/gen.sh or remove it"
  [[ "$CA_ISSUER_GCP" == "mesh" ]] && \
    warn_add "the CA ConfigMap jaiscloud-ca-cert in $NS_GCP is labelled jaiscloud.io/issuer=mesh, but no issuing mesh exists (istio-system/jaiscloud-wildcard is missing): stale CA — re-run deploy/k8s/istio/certs/gen.sh or remove it"
fi

[[ "$LEGACY_PENDING" == "1" ]] && \
  warn_add "legacy pre-split layout detected in $NS_AWS; the store and GCP bytes must be migrated"
if [[ "$LEGACY_PG" == "1" && "$LEGACY_PG_MARKED" == "0" ]]; then
  warn_add "a workload named postgres exists in $NS_AWS but is not marked as a JaisCloud store; if it is the pre-split store, run deploy.sh --migrate-store with LEGACY_CONFIRM=1, otherwise ignore it"
fi
if [[ "$LEGACY_GCP" == "1" && "$LEGACY_GCP_MARKED" == "0" ]]; then
  warn_add "a workload named jaiscloud-gcp exists in $NS_AWS but is not marked as a JaisCloud emulator; if it is the pre-split GCP workload, run deploy.sh --migrate-store with LEGACY_CONFIRM=1, otherwise ignore it"
fi
[[ "$LEGACY_PG_RETAINED" == "1" ]] && \
  warn_add "legacy postgres retained in $NS_AWS (scaled to 0, not serving) as a rollback copy; remove it once verified: kubectl -n $NS_AWS delete $LEGACY_PG_OBJECT service postgres postgres-external pvc postgres-pvc"
[[ "$LEGACY_GCP_RETAINED" == "1" ]] && \
  warn_add "legacy GCP workload retained in $NS_AWS (scaled to 0, not serving); remove it once verified: kubectl -n $NS_AWS delete deployment jaiscloud-gcp service jaiscloud-gcp pvc jaiscloud-gcp-blobs-pvc"
[[ "$AWS_PRESENT" == "1" && "$AWS_PRIVATE" == "1" ]] && \
  warn_add "the AWS substrate image '$AWS_IMAGE' points at a private/local registry"
[[ "$GCP_PRESENT" == "1" && "$GCP_PRIVATE" == "1" ]] && \
  warn_add "the GCP substrate image '$GCP_IMAGE' points at a private/local registry"
# Spark image policy: warnings only, never a --strict failure.
if [[ "$GCP_PRESENT" == "1" && "$GCP_SPARK_PRIVATE" == "1" ]]; then
  warn_add "JAISCLOUD_K8S_SPARK_IMAGE='$GCP_SPARK_IMAGE' points at a private/local registry; Dataproc driver Pods ImagePullBackOff unless every node can pull it"
fi
if [[ "$GCP_PRESENT" == "1" && "$GCP_SPARK_GCS" == "0" ]]; then
  if [[ -z "$GCP_SPARK_IMAGE" ]]; then
    warn_add "JAISCLOUD_K8S_SPARK_IMAGE is unset: the default Spark image has no GCS connector; Dataproc jobs that read gs:// need JAISCLOUD_K8S_SPARK_IMAGE set to a GCS-enabled image"
  else
    warn_add "JAISCLOUD_K8S_SPARK_IMAGE='$GCP_SPARK_IMAGE' has no GCS connector; Dataproc jobs that read gs:// need JAISCLOUD_K8S_SPARK_IMAGE set to a GCS-enabled image"
  fi
fi
# Managed Kafka broker data is not persistent (no broker PVC): the /park
# response reports topicDataLost and deploy.sh --park warns; say it up front so a
# park is never a surprise data loss.
if [[ "$GCP_PRESENT" == "1" && "$GCP_KAFKA_REAL" == "1" ]]; then
  warn_add "JAISCLOUD_KAFKA_BROKER_MODE='$GCP_KAFKA_BROKER_MODE': Managed Kafka broker topic data is not persistent (the broker has no PVC), so parking the GCP substrate loses topics; cluster/topic metadata and the namespace survive"
fi
# A moving :latest tag means the digest is whatever the node cached, so a parked
# substrate resumed after an upgrade can change binary (or fail a store
# migration). Pinning is DSL7; warn, do not fail.
if [[ "$AWS_PRESENT" == "1" && "$AWS_IMAGE" == *:latest ]]; then
  warn_add "the AWS substrate image '$AWS_IMAGE' is a moving :latest tag: the running digest depends on the node cache, so a parked substrate resumed after an upgrade can change binary or fail a store migration; pin a release tag when one is available (DSL7 tracks the pinning work)"
fi
if [[ "$GCP_PRESENT" == "1" && "$GCP_IMAGE" == *:latest ]]; then
  warn_add "the GCP substrate image '$GCP_IMAGE' is a moving :latest tag: the running digest depends on the node cache, so a parked substrate resumed after an upgrade can change binary or fail a store migration; pin a release tag when one is available (DSL7 tracks the pinning work)"
fi
SKEW_AWS=$(migration_skew_warning "$NS_AWS" jaiscloud)
SKEW_GCP=$(migration_skew_warning "$NS_GCP" jaiscloud-gcp)
[[ -n "$SKEW_AWS" ]] && warn_add "AWS substrate crash-loops on store version skew: $SKEW_AWS"
[[ -n "$SKEW_GCP" ]] && warn_add "GCP substrate crash-loops on store version skew: $SKEW_GCP"
[[ "$MESH_NS" == "1" && "$MESH_GW" == "0" ]] && \
  warn_add "istio-system exists but Gateway/jaiscloud is missing; run the mesh scenario"
[[ "$MESH_ISTIOD" == "1" ]] && \
  warn_add "istiod is already installed; do not run \`istioctl install --set profile=minimal\` (it can replace the existing IOP); use the gateway-only overlay in deploy/k8s/istio/istiooperator-gateway-only.yaml"
[[ "$STORE_PRESENT" == "0" && -n "$STORE_PVC" ]] && \
  warn_add "pvc postgres-pvc exists in $NS_DATA but no bundled store workload (deployment/postgres or statefulset/postgres) is present"
if [[ -n "$STORE_WORKLOAD_ENV" && "$STORE_WL_PRESENT" != "1" ]]; then
  warn_add "STORE_WORKLOAD=$STORE_WORKLOAD_ENV does not exist in $NS_DATA: the database probe cannot run (store.workload=$STORE_WORKLOAD, store.databases=null)"
fi
for leak in ${LEAKS+"${LEAKS[@]}"}; do warn_add "$leak"; done
if [[ "$LEGACY_NAMESPACE_ADMIN" == "1" && "$NAMESPACE_ADMIN_PRESENT" == "0" ]]; then
  warn_add "legacy combined clusterrolebinding jaiscloud-namespace-admin is bound to both substrates; re-run an install path (deploy/deploy.sh) to replace it with jaiscloud-namespace-admin-aws / jaiscloud-namespace-admin-gcp"
fi
if [[ "$SUBSTRATE_PRESENT" == "1" && "$VAP_PRESENT" == "0" ]]; then
  if [[ "$SERVER_OLD" == "1" ]]; then
    warn_add "namespace isolation is not enforced: Kubernetes 1.$SERVER_MINOR predates ValidatingAdmissionPolicy (>= 1.30), so this cluster cannot enforce deploy/k8s/rbac-admission-policy.yaml and one substrate can delete the other's namespaces"
    rec_add "install-rbac-policy" "kubectl apply -f deploy/k8s/rbac-admission-policy.yaml" \
      "namespace isolation is not enforced; upgrade to Kubernetes >= 1.30 (server is 1.$SERVER_MINOR) and apply the policy" "false"
  else
    warn_add "namespace isolation is not enforced: ValidatingAdmissionPolicy jaiscloud-namespace-isolation is missing; one substrate can delete the other's namespaces"
    rec_add "install-rbac-policy" "deploy/deploy.sh --render-only /tmp/jaiscloud-render && kubectl apply -k /tmp/jaiscloud-render/overlays/isolation" \
      "namespace isolation is not enforced (K8s >= 1.30 required); apply the rendered isolation policy" "false"
  fi
fi

# The policy exists but does not name the identity that is running (a custom
# namespace/ServiceAccount, or a hand-edited policy): the boundary matches
# nobody. Re-apply the policy deploy.sh renders for these names.
if [[ "$POLICY_MATCHES_SA" == "false" ]]; then
  for cloud in aws gcp; do
    case "$cloud" in
      aws) present=$AWS_PRESENT ns=$NS_AWS sa=$AWS_SA mentions=$POLICY_AWS ;;
      gcp) present=$GCP_PRESENT ns=$NS_GCP sa=$GCP_SA mentions=$POLICY_GCP ;;
    esac
    [[ "$present" == "1" && "$mentions" == "false" ]] || continue
    warn_add "namespace isolation is not enforced for the running identity $ns:$sa: ValidatingAdmissionPolicy jaiscloud-namespace-isolation matches system:serviceaccount:<namespace>:<serviceaccount> usernames that do not include this one; re-apply the policy deploy.sh renders for these namespaces/ServiceAccounts and re-run the preflight"
    rec_add "install-rbac-policy" \
      "NS_AWS=$NS_AWS SA_AWS=$AWS_SA NS_GCP=$NS_GCP SA_GCP=$GCP_SA deploy/deploy.sh --render-only /tmp/jaiscloud-render && kubectl apply -k /tmp/jaiscloud-render/overlays/isolation" \
      "the isolation policy does not match the $cloud substrate's running ServiceAccount ($ns:$sa), so it enforces nothing for it" "false"
  done
fi

# Effective RBAC: a `no` is a real permission gap for an intended substrate; an
# `unknown` (impersonation not allowed, or kubectl too old) is reported as such —
# never as "isolation is unenforced".
for cloud in aws gcp; do
  list_has "$INSTALLED" "$cloud" || continue
  case "$cloud" in
    aws) present=$AWS_PRESENT ns=$NS_AWS sa=$AWS_SA del=$AWS_DELETE_NS ;;
    gcp) present=$GCP_PRESENT ns=$NS_GCP sa=$GCP_SA del=$GCP_DELETE_NS ;;
  esac
  [[ "$present" == "1" ]] || continue
  case "$del" in
    no)
      warn_add "the $cloud substrate's ServiceAccount $ns:$sa cannot delete namespaces (effective RBAC, kubectl auth can-i --as): Dataproc cluster namespaces and Managed Kafka brokers cannot be created/torn down, and the run falls back to mock; apply deploy/k8s/rbac-shared.yaml and deploy/k8s/rbac-$cloud.yaml"
      rec_add "install-rbac-policy" "kubectl apply -f deploy/k8s/rbac-shared.yaml -f deploy/k8s/rbac-$cloud.yaml -f deploy/k8s/rbac-admission-policy.yaml" \
        "$ns:$sa has no cluster-wide namespace delete (Dataproc/Managed Kafka need it); the admission policy keeps that grant to owned namespaces" "false" ;;
    unknown)
      warn_add "cannot verify effective RBAC for $cloud: kubectl auth can-i --as=system:serviceaccount:$ns:$sa did not answer yes/no (impersonation may be forbidden for this kubeconfig); the permission state is unknown, not unenforced — re-run with a cluster-admin kubeconfig or check the bindings by hand" ;;
  esac
done

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

if [[ "$LEGACY_PENDING" == "1" ]]; then
  rec_add "migrate-store" "SKIP_IMAGE_BUILD=1 deploy/deploy.sh --migrate-store --yes" \
    "pre-split layout: move the store and the legacy GCP bytes (the legacy workload is kept scaled to 0)" "false"
elif [[ "$STORE_PRESENT" == "0" && "$STORE_MODE" == "bundled" && -z "$INSTALLED" ]]; then
  # No recorded or inferred install: keep the original guidance.
  if [[ "$GCP_PRESENT" == "1" && "$AWS_PRESENT" == "0" ]]; then
    rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "store missing; install the GCP substrate (and the store)" "false"
  else
    rec_add "deploy-aws" "deploy/deploy.sh --yes" "store missing; install the AWS substrate, the store and Postgres" "false"
    [[ "$GCP_PRESENT" == "0" ]] && \
      rec_add "deploy-gcp" "deploy/deploy.sh --gcp --yes" "install the GCP substrate as well (optional)" "true"
  fi
elif [[ "$STORE_PRESENT" == "0" && "$STORE_MODE" == "bundled" ]]; then
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
  [[ "$STORE_READY" == "1" && "$DB_GCP" == "false" && "$GCP_PRESENT" == "1" ]] && list_has "$INSTALLED" gcp \
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
  printf '  "legacy": {"namespace": "%s", "postgresInAws": %s, "postgresStatefulSetInAws": %s, "gcpInAws": %s, "marked": %s, "gcpMarked": %s, "candidate": %s, "pending": %s, "postgresRetained": %s, "gcpRetained": %s},\n' \
    "$(json_escape "$NS_AWS")" "$(b "$LEGACY_PG")" "$(b "$LEGACY_PG_STS")" "$(b "$LEGACY_GCP")" \
    "$(b "$LEGACY_PG_MARKED")" "$(b "$LEGACY_GCP_MARKED")" "$(b "$LEGACY_CANDIDATE")" \
    "$(b "$LEGACY_PENDING")" "$(b "$LEGACY_PG_RETAINED")" "$(b "$LEGACY_GCP_RETAINED")"
  printf '  "store": {\n'
  printf '    "mode": "%s", "present": %s, "ready": %s, "workload": "%s",\n' \
    "$STORE_MODE" "$(b "$STORE_PRESENT")" "$(b "$STORE_READY")" "$(json_escape "$STORE_WORKLOAD")"
  printf '    "pvc": {"name": "postgres-pvc", "namespace": "%s", "status": "%s", "capacity": "%s"},\n' \
    "$NS_DATA" "$(json_escape "$STORE_PVC")" "$(json_escape "$(pvc_capacity "$NS_DATA" postgres-pvc)")"
  printf '    "databases": {"jaiscloud": %s, "jaiscloud_gcp": %s}, "databasesProbe": "%s", "probeReason": "%s"\n' \
    "$(tri "$DB_AWS")" "$(tri "$DB_GCP")" "$DATABASES_PROBE" "$STORE_PROBE_REASON"
  printf '  },\n'
  printf '  "substrates": {\n'
  printf '    "aws": {"present": %s, "ready": %s, "state": "%s", "replicas": %s, "image": "%s", "imageID": "%s", "imagePullPolicy": "%s", "privateRegistry": %s},\n' \
    "$(b "$AWS_PRESENT")" "$(b "$AWS_READY")" "$AWS_STATE" "$AWS_REPLICAS" "$(json_escape "$AWS_IMAGE")" \
    "$(json_escape "$AWS_IMAGE_ID")" "$(json_escape "$AWS_PULL")" "$(b "$AWS_PRIVATE")"
  printf '    "gcp": {"present": %s, "ready": %s, "state": "%s", "replicas": %s, "image": "%s", "imageID": "%s", "imagePullPolicy": "%s", "privateRegistry": %s, "sparkImage": "%s", "sparkImageGCS": %s, "kafkaBrokerMode": "%s"}\n' \
    "$(b "$GCP_PRESENT")" "$(b "$GCP_READY")" "$GCP_STATE" "$GCP_REPLICAS" "$(json_escape "$GCP_IMAGE")" \
    "$(json_escape "$GCP_IMAGE_ID")" "$(json_escape "$GCP_PULL")" "$(b "$GCP_PRIVATE")" \
    "$(json_escape "$GCP_SPARK_IMAGE")" "$(b "$GCP_SPARK_GCS")" "$(json_escape "$GCP_KAFKA_BROKER_MODE")"
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
  printf '  "mesh": {"namespace": %s, "istiod": %s, "existingInstall": %s, "ingressGateway": %s, "tlsSecret": %s, "crds": %s, "gateway": %s, "virtualServices": %s, "destinationRules": %s, "address": "%s"},\n' \
    "$(b "$MESH_NS")" "$(b "$MESH_ISTIOD")" "$(b "$MESH_ISTIOD")" "$(b "$MESH_GATEWAY")" "$(b "$MESH_SECRET")" "$(b "$MESH_CRD")" \
    "$(b "$MESH_GW")" "${MESH_VS:-0}" "${MESH_DR:-0}" "$(json_escape "$MESH_ADDR")"
  printf '  "dns": {"rewrites": %s, "suffixes": %s, "stale": %s, "target": "%s", "targets": {%s}},\n' \
    "$(tri "$DNS_REWRITES")" "$(csv_json_array "$DNS_SUFFIXES")" "$(b "$DNS_STALE")" \
    "$(json_escape "$DNS_TARGET")" "$DNS_TARGETS_JSON"
  printf '  "ca": {"aws": %s, "gcp": %s, "issuerPresent": %s, "issuers": {"aws": "%s", "gcp": "%s"}},\n' \
    "$(b "$CA_AWS")" "$(b "$CA_GCP")" "$(b "$CA_ISSUER")" "$CA_ISSUER_AWS" "$CA_ISSUER_GCP"
  printf '  "rbac": {\n'
  printf '    "namespaceAdmin": %s, "admissionPolicy": %s, "serverVersion": "%s", "source": "auth",\n' \
    "$(b "$NAMESPACE_ADMIN_EFFECTIVE")" "$(b "$VAP_PRESENT")" "$(json_escape "$SERVER_VERSION")"
  printf '    "policyMatchesSA": %s,\n' "$(tri "$POLICY_MATCHES_SA")"
  printf '    "policyMentions": {"aws": %s, "gcp": %s},\n' "$(tri "$POLICY_AWS")" "$(tri "$POLICY_GCP")"
  printf '    "namespaceAdminBindings": {"aws": %s, "gcp": %s},\n' "$(b "$NAMESPACE_ADMIN_AWS")" "$(b "$NAMESPACE_ADMIN_GCP")"
  printf '    "effective": {\n'
  printf '      "aws": {"serviceAccount": "%s", "namespace": "%s", "source": "auth", "deleteNamespaces": "%s", "createNamespaces": "%s", "updateNamespaces": "%s", "createRoleBindings": "%s"},\n' \
    "$(json_escape "$AWS_SA")" "$(json_escape "$NS_AWS")" "$AWS_DELETE_NS" "$AWS_CREATE_NS" "$AWS_UPDATE_NS" "$AWS_CREATE_RB"
  printf '      "gcp": {"serviceAccount": "%s", "namespace": "%s", "source": "auth", "deleteNamespaces": "%s", "createNamespaces": "%s", "updateNamespaces": "%s", "createRoleBindings": "%s"}\n' \
    "$(json_escape "$GCP_SA")" "$(json_escape "$NS_GCP")" "$GCP_DELETE_NS" "$GCP_CREATE_NS" "$GCP_UPDATE_NS" "$GCP_CREATE_RB"
  printf '    }\n'
  printf '  },\n'
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
  if [[ "$LEGACY_PG_RETAINED" == "1" ]]; then
    ok_ "legacy $LEGACY_PG_OBJECT in $NS_AWS is retained (scaled to 0) as a rollback copy"
  elif [[ "$LEGACY_PG_MARKED" == "1" ]]; then
    warn_ "legacy $LEGACY_PG_OBJECT in $NS_AWS (marked; pending migration)"
  elif [[ "$LEGACY_PG" == "1" ]]; then
    warn_ "a workload named postgres exists in $NS_AWS but is not marked as a JaisCloud store (candidate, not a pending migration)"
  else
    ok_ "no legacy store in $NS_AWS"
  fi
  if [[ "$LEGACY_GCP_RETAINED" == "1" ]]; then
    ok_ "legacy GCP workload in $NS_AWS is retained (scaled to 0); its bytes were copied"
  elif [[ "$LEGACY_GCP_MARKED" == "1" ]]; then
    warn_ "legacy GCP workload in $NS_AWS (marked; pending migration)"
  elif [[ "$LEGACY_GCP" == "1" ]]; then
    warn_ "a workload named jaiscloud-gcp exists in $NS_AWS but is not marked as a JaisCloud emulator (candidate, not a pending migration)"
  else
    ok_ "no legacy GCP workload in $NS_AWS"
  fi
  if [[ "$STORE_MODE" == "external" ]]; then
    ok_ "store: mode=external (your server; no bundled workload, database probes skipped — probeReason=external)"
    ok_ "databases: jaiscloud=$(tri_word "$DB_AWS") jaiscloud_gcp=$(tri_word "$DB_GCP") databases-probe=$DATABASES_PROBE"
    say "  note    : store.ready=false and databases=unknown mean 'not probeable', not 'missing'"
  elif [[ "$STORE_PRESENT" == "1" ]]; then
    ok_ "store: mode=bundled ${STORE_WORKLOAD:-?} ready=$([[ "$STORE_READY" == "1" ]] && echo yes || echo no), pvc=${STORE_PVC:-–} databases-probe=$DATABASES_PROBE${STORE_PROBE_REASON:+ ($STORE_PROBE_REASON)}"
    ok_ "databases: jaiscloud=$(tri_word "$DB_AWS") jaiscloud_gcp=$(tri_word "$DB_GCP")"
  else
    miss_ "store: mode=bundled $NS_DATA/${STORE_WORKLOAD:-deployment/postgres or statefulset/postgres} (probeReason=$STORE_PROBE_REASON)"
    [[ -n "$STORE_WORKLOAD_ENV" ]] && say "  note    : STORE_WORKLOAD=$STORE_WORKLOAD_ENV does not exist in $NS_DATA"
  fi
  head_ "substrates"
  if [[ "$AWS_PRESENT" == "1" ]]; then
    ok_ "aws: state=$AWS_STATE replicas=$AWS_REPLICAS $AWS_IMAGE ready=$([[ "$AWS_READY" == "1" ]] && echo yes || echo no) pull=$AWS_PULL${AWS_IMAGE_ID:+ imageID=$AWS_IMAGE_ID}"
  else
    miss_ "aws: state=$AWS_STATE $NS_AWS/deployment/jaiscloud"
  fi
  if [[ "$GCP_PRESENT" == "1" ]]; then
    ok_ "gcp: state=$GCP_STATE replicas=$GCP_REPLICAS $GCP_IMAGE ready=$([[ "$GCP_READY" == "1" ]] && echo yes || echo no) pull=$GCP_PULL${GCP_IMAGE_ID:+ imageID=$GCP_IMAGE_ID}"
    spark_gcs=no; [[ "$GCP_SPARK_GCS" == "1" ]] && spark_gcs=yes
    spark_extra=""
    [[ "$GCP_SPARK_PRIVATE" == "1" ]] && spark_extra=" privateRegistry=yes"
    ok_ "gcp spark: ${GCP_SPARK_IMAGE:-<unset>} gcs-connector=$spark_gcs$spark_extra"
    if [[ -n "$GCP_KAFKA_BROKER_MODE" ]]; then
      kafka_note=""
      [[ "$GCP_KAFKA_REAL" == "1" ]] && kafka_note=" (topics are not persistent: lost on park)"
      say "  gcp kafka: broker-mode=$GCP_KAFKA_BROKER_MODE$kafka_note"
    fi
  else
    miss_ "gcp: state=$GCP_STATE $NS_GCP/deployment/jaiscloud-gcp"
  fi
  head_ "rbac"
  say "  server     ${SERVER_VERSION:-unknown}"
  say "  namespace admin bindings: aws=$(yn "$NAMESPACE_ADMIN_AWS") gcp=$(yn "$NAMESPACE_ADMIN_GCP")  (intended: ${INSTALLED:-none})"
  say "  effective  aws $NS_AWS:$AWS_SA delete-namespaces=$AWS_DELETE_NS create-namespaces=$AWS_CREATE_NS update-namespaces=$AWS_UPDATE_NS create-rolebindings=$AWS_CREATE_RB"
  say "  effective  gcp $NS_GCP:$GCP_SA delete-namespaces=$GCP_DELETE_NS create-namespaces=$GCP_CREATE_NS update-namespaces=$GCP_UPDATE_NS create-rolebindings=$GCP_CREATE_RB"
  say "             (kubectl auth can-i --as; unknown = impersonation not allowed, not unenforced)"
  if [[ "$NAMESPACE_ADMIN_EFFECTIVE" == "1" ]]; then
    ok_ "namespace create/delete allowed for the intended substrate (${INSTALLED:-any})"
  else
    miss_ "namespace create/delete not verified/allowed for the intended substrate (${INSTALLED:-none})"
  fi
  if [[ "$VAP_PRESENT" == "1" ]]; then
    case "$POLICY_MATCHES_SA" in
      true)  ok_ "ValidatingAdmissionPolicy jaiscloud-namespace-isolation enforces the namespace boundary and matches the running identity" ;;
      false) warn_ "ValidatingAdmissionPolicy jaiscloud-namespace-isolation does not match the running identity — it enforces nothing for it (re-apply the policy deploy.sh renders for these names)" ;;
      *)     warn_ "ValidatingAdmissionPolicy jaiscloud-namespace-isolation exists but its matchConditions could not be read" ;;
    esac
  elif [[ "$SERVER_OLD" == "1" ]]; then
    miss_ "ValidatingAdmissionPolicy jaiscloud-namespace-isolation (Kubernetes 1.$SERVER_MINOR cannot enforce it; >= 1.30 required)"
  else
    miss_ "ValidatingAdmissionPolicy jaiscloud-namespace-isolation (namespace isolation not enforced)"
  fi
  head_ "dns (CoreDNS rewrites, read-only)"
  if [[ "$DNS_REWRITES" == "unknown" ]]; then
    say "  rewrites: unknown (kube-system/coredns is absent or not readable)"
  elif [[ "$DNS_REWRITES" == "false" ]]; then
    say "  rewrites: none of amazonaws.com/googleapis.com/run.app/cloudfunctions.net (clients need --resolve or hostAliases)"
  else
    say "  rewrites: target=${DNS_TARGET:-none}  stale=$([[ "$DNS_STALE" == "1" ]] && echo yes || echo no)"
    while IFS= read -r line; do
      [[ -n "$line" ]] && say "            $line"
    done <<<"$DNS_TARGET_LINES"
    say "            (target: gateway = istio-ingressgateway; service:<ns>/<name> = a Service's ready endpoints; unknown = not diagnosed)"
  fi
  head_ "ca (TLS trust copies)"
  say "  configmaps/jaiscloud-ca-cert: aws=$(yn "$CA_AWS") issuer=$CA_ISSUER_AWS   gcp=$(yn "$CA_GCP") issuer=$CA_ISSUER_GCP"
  say "  mesh issuer istio-system/jaiscloud-wildcard: $(yn "$CA_ISSUER")   (only issuer=mesh copies are judged stale; unlabelled = host-issued)"
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
