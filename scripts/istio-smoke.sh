#!/usr/bin/env bash
#
# istio-smoke.sh — prove the Istio front door routes both substrates correctly.
#
# Four assertions, all through the gateway's HTTPS listener, all against the
# REAL cloud hostnames (never localhost / service names):
#
#   1. GKE, not Managed Kafka. container.googleapis.com's address is
#      /v1/projects/{p}/locations/{l}/clusters, which Managed Kafka also serves
#      (internal/gcp/adapter/router.go). If anything in the mesh rewrote
#      :authority, the request would land on Kafka and come back as Kafka's
#      empty list / its own create error. This is the assertion that guards the
#      one rule the whole front door is built around.
#   2. GCS REST: storage.googleapis.com/storage/v1/b?project=p -> 200.
#   3. gRPC: the content-type split routes Firestore's gRPC to port 8081 (h2c).
#   4. STS: sts.amazonaws.com GetCallerIdentity -> 200 XML, i.e. *.amazonaws.com
#      reaches the AWS substrate on 4566.
#
# The script needs no cluster DNS changes and no /etc/hosts edit: it resolves
# the gateway once and uses curl --connect-to, which keeps the URL host (and the
# Host/:authority the emulator routes on) exactly as a real client would send
# it. The DNS path — CoreDNS rewrite for every pod in the cluster — is separate
# and documented in deploy/k8s/istio/dns.yaml / README.md.
#
# Usage:
#   scripts/istio-smoke.sh
#   scripts/istio-smoke.sh --keep     # leave the GKE probe cluster behind
#
# Env:
#   KUBE_CONTEXT        kubectl context (default: the current context)
#   ISTIO_NS            namespace of the gateway Service (default istio-system)
#   GW_SVC              gateway Service name (default istio-ingressgateway)
#   GW_ADDR             host:port of the gateway, wins over all discovery
#   GW_IP / GW_PORT     address / TLS port (default: discovered; port 443)
#   CA_FILE             CA PEM to trust (default: extracted from the
#                       jaiscloud-wildcard secret's ca.crt)
#   SECRET_NAME         TLS secret to read the CA from (default jaiscloud-wildcard)
#   GKE_PROJECT         project for the GKE probe (default p)
#   GKE_LOCATION        location for the GKE probe (default l)
#   PROBE_NAME          GKE probe cluster name (default dpl4-smoke)
#   SMOKE_TIMEOUT       per-request timeout in seconds (default 30)
#   SMOKE_SKIP_GRPC=1   skip assertion 3 when grpcurl is not installed
#                       (default: a missing grpcurl FAILS the run)
#   SMOKE_PORT_FORWARD=1 force the kubectl port-forward fallback
#
# Exit: 0 only when every assertion passed.
set -euo pipefail

PROG="$(basename "$0")"
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1 ;;
    --help|-h)
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0
      ;;
    *) echo "$PROG: unknown argument: $arg (try --help)" >&2; exit 2 ;;
  esac
done

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
ISTIO_NS="${ISTIO_NS:-istio-system}"
GW_SVC="${GW_SVC:-istio-ingressgateway}"
GW_PORT="${GW_PORT:-443}"
SECRET_NAME="${SECRET_NAME:-jaiscloud-wildcard}"
GKE_PROJECT="${GKE_PROJECT:-p}"
GKE_LOCATION="${GKE_LOCATION:-l}"
PROBE_NAME="${PROBE_NAME:-dpl4-smoke}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-30}"
SMOKE_SKIP_GRPC="${SMOKE_SKIP_GRPC:-0}"
SMOKE_PORT_FORWARD="${SMOKE_PORT_FORWARD:-0}"

TMPDIR_SMOKE="$(mktemp -d "${TMPDIR:-/tmp}/jaiscloud-istio-smoke.XXXXXX")"
BODY_FILE="$TMPDIR_SMOKE/body"
PF_PID=""
GW_IP=""
PF_PORT=18443
FAILED=0
PASSED=0

cleanup() {
  if [ -n "$PF_PID" ]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMPDIR_SMOKE"
}
trap cleanup EXIT

say()  { printf '%s\n' "$*"; }
head_() { printf '\n== %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); printf '  PASS  %s\n' "$*"; }
fail() { FAILED=$((FAILED + 1)); printf '  FAIL  %s\n' "$*" >&2; }
# bad() reports a failed sub-step; the enclosing assertion counts once.
bad()  { printf '  FAIL  %s\n' "$*" >&2; }
skip() { printf '  SKIP  %s\n' "$*"; }

kc() {
  if [[ -n "$KUBE_CONTEXT" ]]; then
    kubectl --context "$KUBE_CONTEXT" "$@"
  else
    kubectl "$@"
  fi
}

# ── prerequisites ───────────────────────────────────────────────────────────
command -v kubectl >/dev/null 2>&1 || { echo "$PROG: kubectl is required" >&2; exit 2; }
command -v curl    >/dev/null 2>&1 || { echo "$PROG: curl is required" >&2;    exit 2; }

kc get namespace "$ISTIO_NS" >/dev/null 2>&1 \
  || { echo "$PROG: namespace $ISTIO_NS not found in context $KUBE_CONTEXT" >&2; exit 2; }
kc -n "$ISTIO_NS" get svc "$GW_SVC" >/dev/null 2>&1 \
  || { echo "$PROG: Service $ISTIO_NS/$GW_SVC not found — install Istio with the ingress gateway component (see deploy/k8s/istio/README.md; profile=minimal alone has no gateway)" >&2; exit 2; }

# ── resolve the gateway's address ───────────────────────────────────────────
start_port_forward() {
  say "  (falling back to kubectl port-forward $PF_PORT -> svc/$GW_SVC:443)"
  kc -n "$ISTIO_NS" port-forward "svc/$GW_SVC" "${PF_PORT}:443" >"$TMPDIR_SMOKE/pf.log" 2>&1 &
  PF_PID=$!
  local i
  for i in $(seq 1 40); do
    # The fd and its stderr silence live in the subshell only: a bare `exec`
    # here would redirect this script's stderr for the rest of the run and
    # swallow every later FAIL message.
    if (exec 3<>"/dev/tcp/127.0.0.1/$PF_PORT") 2>/dev/null; then
      GW_IP=127.0.0.1
      GW_PORT=$PF_PORT
      return 0
    fi
    kill -0 "$PF_PID" 2>/dev/null || break
    sleep 0.5
  done
  echo "$PROG: port-forward to $ISTIO_NS/$GW_SVC did not come up:" >&2
  sed 's/^/    /' "$TMPDIR_SMOKE/pf.log" >&2 || true
  exit 2
}

# tcp_reachable HOST PORT — probe without leaving a file descriptor or stderr
# redirect behind in this shell.
tcp_reachable() {
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null
}

resolve_gateway() {
  if [ -n "${GW_ADDR:-}" ]; then
    GW_IP="${GW_ADDR%:*}"
    GW_PORT="${GW_ADDR##*:}"
    return
  fi
  if [ -n "${GW_IP:-}" ]; then
    return
  fi
  if [ "$SMOKE_PORT_FORWARD" = "1" ]; then
    start_port_forward
    return
  fi

  # LoadBalancer ingress (kind publishes 127.0.0.1, cloud LBs a real address).
  local lb=""
  lb="$(kc -n "$ISTIO_NS" get svc "$GW_SVC" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
  if [ -z "$lb" ]; then
    lb="$(kc -n "$ISTIO_NS" get svc "$GW_SVC" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
    if [ -n "$lb" ]; then
      lb="$(getent hosts "$lb" 2>/dev/null | awk 'NR==1{print $1}')"
    fi
  fi
  if [ -n "$lb" ] && tcp_reachable "$lb" "$GW_PORT"; then
    GW_IP="$lb"
    return
  fi

  # NodePort: k3d/k3s ServiceLB without an external address, and kind without
  # extraPortMappings, land here.
  local np="" node_ip=""
  np="$(kc -n "$ISTIO_NS" get svc "$GW_SVC" -o jsonpath='{.spec.ports[?(@.port==443)].nodePort}' 2>/dev/null || true)"
  if [ -n "$np" ]; then
    node_ip="$(kc get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)"
  fi
  if [ -n "$np" ] && [ -n "$node_ip" ] && tcp_reachable "$node_ip" "$np"; then
    GW_IP="$node_ip"
    GW_PORT="$np"
    return
  fi

  # Nothing routable from here (a k3d node address the host cannot reach, or a
  # LoadBalancer still pending): the port-forward always works.
  start_port_forward
}

# ── CA to trust ─────────────────────────────────────────────────────────────
resolve_ca() {
  if [ -n "${CA_FILE:-}" ]; then
    [ -r "$CA_FILE" ] || { echo "$PROG: CA_FILE=$CA_FILE is not readable" >&2; exit 2; }
    return
  fi
  local encoded
  encoded="$(kc -n "$ISTIO_NS" get secret "$SECRET_NAME" -o jsonpath='{.data.ca\.crt}' 2>/dev/null || true)"
  if [ -z "$encoded" ]; then
    cat >&2 <<EOF
$PROG: no CA available. The TLS secret $ISTIO_NS/$SECRET_NAME has no ca.crt key.
       Run deploy/k8s/istio/certs/gen.sh (it stores the CA in that secret), or
       point CA_FILE at the dev CA PEM.
EOF
    exit 2
  fi
  CA_FILE="$TMPDIR_SMOKE/ca.crt"
  printf '%s' "$encoded" | base64 -d > "$CA_FILE"
  if command -v openssl >/dev/null 2>&1; then
    openssl x509 -in "$CA_FILE" -noout >/dev/null 2>&1 \
      || { echo "$PROG: extracted ca.crt is not a PEM certificate" >&2; exit 2; }
  fi
}

# ── request helpers ─────────────────────────────────────────────────────────
# http HOST PATH [curl args...] -> prints the status code, body goes to BODY_FILE.
#
# --connect-to points the TCP connection at the gateway while the URL keeps the
# real host and port 443, so the TLS SNI and the Host/:authority header are
# exactly what a real SDK sends. That is deliberate: the emulator routes on
# :authority, so the smoke must not quietly change it.
http() {
  local host="$1" path="$2"
  shift 2
  curl -sS \
    --cacert "$CA_FILE" \
    --connect-to "$host:443:$GW_IP:$GW_PORT" \
    --max-time "$SMOKE_TIMEOUT" \
    -o "$BODY_FILE" -w '%{http_code}' \
    "https://$host$path" "$@"
}

body() { cat "$BODY_FILE" 2>/dev/null || true; }
show_body() { sed -n '1,20p' "$BODY_FILE" 2>/dev/null | cut -c1-400 | sed 's/^/          /' >&2 || true; }

# ───────────────────────────────────────────────────────────────────────────
resolve_gateway
resolve_ca

say "$PROG: context=$KUBE_CONTEXT gateway=$GW_IP:$GW_PORT ca=$CA_FILE"
say "      (hostnames are the real cloud ones; no DNS or /etc/hosts change needed)"

GLB="/v1/projects/$GKE_PROJECT/locations/$GKE_LOCATION/clusters"

# ── 1. GKE, not Managed Kafka ──────────────────────────────────────────────
head_ "1. container.googleapis.com — GKE routing wins on /v1/.../clusters"
gke_fail=0
# Leftover probe from an interrupted run would make the create return 409.
http container.googleapis.com "$GLB/$PROBE_NAME" -X DELETE >/dev/null 2>&1 || true

code=000
if ! code="$(http container.googleapis.com "$GLB" -X POST \
      -H 'Content-Type: application/json' \
      -d "{\"cluster\":{\"name\":\"$PROBE_NAME\",\"initialNodeCount\":1,\"location\":\"$GKE_LOCATION\"}}")"; then
  code=000
fi
if [ "$code" = "200" ] || [ "$code" = "201" ]; then
  if grep -q '"operationType":"CREATE_CLUSTER"' "$BODY_FILE" 2>/dev/null; then
    say "        create returned a GKE operation (CREATE_CLUSTER)"
  else
    bad "create answered like Managed Kafka, not GKE (a lost :authority looks exactly like this)"
    show_body
    gke_fail=1
  fi
else
  bad "POST $GLB returned HTTP $code"
  show_body
  gke_fail=1
fi

if [ "$gke_fail" -eq 0 ]; then
  code=000
  if ! code="$(http container.googleapis.com "$GLB")"; then code=000; fi
  if [ "$code" != "200" ]; then
    bad "GET $GLB returned HTTP $code"
    show_body
    gke_fail=1
  elif ! grep -q "\"name\":\"$PROBE_NAME\"" "$BODY_FILE" 2>/dev/null; then
    bad "GET $GLB did not list the GKE cluster $PROBE_NAME (empty list = Kafka's answer)"
    show_body
    gke_fail=1
  elif grep -q 'capacityConfig' "$BODY_FILE" 2>/dev/null; then
    bad "GET $GLB returned Managed Kafka cluster objects"
    gke_fail=1
  else
    say "        GET $GLB listed the GKE cluster (not Kafka's empty list)"
  fi
fi

# Managed Kafka serves no /v1beta1 surface, so a 200 here is structural proof
# that the authority reached GKE's router instead of the shared /v1 resolver.
if [ "$gke_fail" -eq 0 ]; then
  code=000
  if ! code="$(http container.googleapis.com "/v1beta1/projects/$GKE_PROJECT/locations/$GKE_LOCATION/clusters")"; then code=000; fi
  if [ "$code" != "200" ]; then
    bad "GET the GKE-only /v1beta1 clusters surface returned HTTP $code"
    show_body
    gke_fail=1
  else
    say "        /v1beta1 clusters answered 200 (GKE-only surface)"
  fi
fi

if [ "$KEEP" -eq 0 ]; then
  http container.googleapis.com "$GLB/$PROBE_NAME" -X DELETE >/dev/null 2>&1 || true
  say "        (removed probe cluster $PROBE_NAME)"
else
  say "        (--keep: probe cluster $PROBE_NAME left in place)"
fi

if [ "$gke_fail" -eq 0 ]; then
  pass "container.googleapis.com routes to GKE, authority preserved"
else
  FAILED=$((FAILED + 1))
fi

# ── 2. GCS REST ────────────────────────────────────────────────────────────
head_ "2. storage.googleapis.com — REST to :8080"
code=000
if ! code="$(http storage.googleapis.com "/storage/v1/b?project=$GKE_PROJECT")"; then code=000; fi
if [ "$code" = "200" ] && grep -q '"kind":"storage#buckets"' "$BODY_FILE" 2>/dev/null; then
  pass "storage.googleapis.com/storage/v1/b returned a storage#buckets list"
else
  fail "storage.googleapis.com/storage/v1/b returned HTTP $code without a storage#buckets list"
  show_body
fi

# ── 3. gRPC on :8081 ───────────────────────────────────────────────────────
head_ "3. firestore.googleapis.com:443 — gRPC split to :8081 (h2c)"
if ! command -v grpcurl >/dev/null 2>&1; then
  if [ "$SMOKE_SKIP_GRPC" = "1" ]; then
    skip "grpcurl not installed (SMOKE_SKIP_GRPC=1)"
  else
    fail "grpcurl not installed — install it or set SMOKE_SKIP_GRPC=1 to waive this assertion"
  fi
else
  grpc_fail=0
  out=""
  if ! out="$(grpcurl -cacert "$CA_FILE" -authority firestore.googleapis.com \
        -max-time "$SMOKE_TIMEOUT" "$GW_IP:$GW_PORT" list 2>&1)"; then
    bad "grpcurl reflection against $GW_IP:$GW_PORT failed"
    printf '%s\n' "$out" | sed -n '1,10p' | sed 's/^/          /' >&2
    grpc_fail=1
  elif ! grep -qx 'google.firestore.v1.Firestore' <<<"$out"; then
    bad "reflection did not list google.firestore.v1.Firestore (routed to the REST port?)"
    printf '%s\n' "$out" | sed -n '1,10p' | sed 's/^/          /' >&2
    grpc_fail=1
  else
    say "        reflection over TLS listed google.firestore.v1.Firestore"
  fi

  if [ "$grpc_fail" -eq 0 ]; then
    out=""
    if ! out="$(grpcurl -cacert "$CA_FILE" -authority firestore.googleapis.com \
          -max-time "$SMOKE_TIMEOUT" -d '{"service":""}' \
          "$GW_IP:$GW_PORT" grpc.health.v1.Health/Check 2>&1)"; then
      bad "grpc.health.v1.Health/Check failed"
      printf '%s\n' "$out" | sed -n '1,10p' | sed 's/^/          /' >&2
      grpc_fail=1
    elif ! grep -q 'SERVING' <<<"$out"; then
      bad "grpc.health.v1.Health/Check did not report SERVING"
      printf '%s\n' "$out" | sed -n '1,10p' | sed 's/^/          /' >&2
      grpc_fail=1
    else
      say "        grpc.health.v1.Health/Check -> SERVING (h2c upstream reached)"
    fi
  fi

  if [ "$grpc_fail" -eq 0 ]; then
    pass "gRPC content-type routed to jaiscloud-gcp:8081"
  else
    FAILED=$((FAILED + 1))
  fi
fi

# ── 4. STS over the AWS substrate ──────────────────────────────────────────
head_ "4. sts.amazonaws.com — GetCallerIdentity to :4566"
code=000
if ! code="$(http sts.amazonaws.com "/" -X POST \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data 'Action=GetCallerIdentity&Version=2011-06-15')"; then
  code=000
fi
if [ "$code" = "200" ] && grep -q 'GetCallerIdentityResponse' "$BODY_FILE" 2>/dev/null; then
  pass "sts.amazonaws.com GetCallerIdentity returned 200 XML"
else
  fail "sts.amazonaws.com GetCallerIdentity returned HTTP $code without a GetCallerIdentityResponse"
  show_body
fi

# ── summary ────────────────────────────────────────────────────────────────
head_ "summary"
say "  passed: $PASSED   failed: $FAILED"
if [ "$FAILED" -ne 0 ]; then
  say ""
  say "  Cluster DNS is NOT part of this check: the smoke dials the gateway"
  say "  directly (curl --connect-to / grpcurl -authority). See"
  say "  deploy/k8s/istio/dns.yaml for the in-cluster CoreDNS rewrite."
  exit 1
fi
say "  front door is green: GKE/GCS/gRPC -> jaiscloud-gcp, STS -> jaiscloud"
exit 0
