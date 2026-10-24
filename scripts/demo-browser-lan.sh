#!/usr/bin/env bash
#
# SPK7 — serve the emulator's Cloud Run data plane on the host's LAN address.
#
# A browser cannot set the Host header a Cloud Run data-plane request routes on,
# and the default synthesized suffix (*.run.app) does not resolve. SPK4 solved
# this for a browser *on the emulator host* with a *.localhost authority; SPK7
# solves it for a recording browser *elsewhere* by synthesizing the authority
# under a nip.io wildcard name — any name under `run.<lan-ip>.nip.io` resolves to
# the emulator host's LAN IP, so the remote browser sends the routing Host with
# no /etc/hosts edit and no reverse proxy.
#
#   scripts/demo-browser-lan.sh [options]
#
#   --port N        host-forwarded authority port (default 8080)
#   --ip IP         emulator host LAN IP (default: first `hostname -I` address)
#   --service ID    print the exact URL for this Cloud Run service id
#   --print-only    print the URL recipe and exit (no cluster changes)
#   --check         after configuring, assert the control plane answers over the
#                   LAN address (a control-plane GET; the data-plane path is the
#                   SPK7 gate `make test-e2e-cloudrun-browser-lan`)
#
# The script leaves the 0.0.0.0 port-forward running until interrupted, so it can
# stay up while the demo is recorded:
#   scripts/demo-browser-lan.sh                 # terminal 1 (keep running)
#   scripts/demo-record.sh --url <printed URL>  # terminal 2
#
# Requires a k3d emulator (deploy/k8s/jaiscloud-gcp.yaml) and nip.io DNS.
set -euo pipefail

NS="${K8S_NAMESPACE:-jaiscloud}"
PORT="${DEMO_LAN_PORT:-8080}"
IP="${DEMO_LAN_IP:-}"
SERVICE_ID=""
PRINT_ONLY=0
CHECK=0

while [ $# -gt 0 ]; do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    --ip) IP="$2"; shift 2 ;;
    --service) SERVICE_ID="$2"; shift 2 ;;
    --print-only) PRINT_ONLY=1; shift ;;
    --check) CHECK=1; shift ;;
    --help|-h) sed -n '2,34p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$IP" ]; then
  IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi
[ -n "$IP" ] || { echo "ERROR: could not determine the host LAN IP; pass --ip" >&2; exit 1; }

SUFFIX="run.${IP}.nip.io"
TOKEN="<token>"   # 12-hex-char SHA-256 prefix of the project (real Cloud Run shape)
LOC="<location>"
URL_HOST="${SERVICE_ID:-<service>}-${TOKEN}.${LOC}.${SUFFIX}"

cat <<EOF
LAN authority recipe (SPK7):
  suffix:  $SUFFIX
  port:    $PORT
  URL:     http://${URL_HOST}:${PORT}/

Configure the deployed emulator and forward the REST port on every interface:
  kubectl -n $NS set env deployment/jaiscloud-gcp \\
    JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s \\
    JAISCLOUD_CLOUDRUN_URL_SUFFIX=$SUFFIX \\
    JAISCLOUD_CLOUDRUN_URL_PORT=$PORT
  kubectl -n $NS port-forward --address 0.0.0.0 svc/jaiscloud-gcp $PORT:8080
EOF

if [ "$PRINT_ONLY" = 1 ]; then
  exit 0
fi

command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found" >&2; exit 1; }

kubectl -n "$NS" set env deployment/jaiscloud-gcp \
  JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s \
  JAISCLOUD_CLOUDRUN_URL_SUFFIX="$SUFFIX" \
  JAISCLOUD_CLOUDRUN_URL_PORT="$PORT"
kubectl -n "$NS" rollout status deployment/jaiscloud-gcp --timeout=180s

echo "==> forwarding svc/jaiscloud-gcp ${PORT}:8080 on 0.0.0.0 (Ctrl-C to stop)"
kubectl -n "$NS" port-forward --address 0.0.0.0 svc/jaiscloud-gcp "${PORT}:8080" &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT

# Wait for the forward to answer on loopback before anything else.
for _ in $(seq 1 50); do
  curl -fsS --noproxy '*' "http://127.0.0.1:${PORT}/_jaiscloud/health" >/dev/null 2>&1 && break
  sleep 0.2
done

if [ "$CHECK" = 1 ]; then
  echo "==> checking the control plane over the LAN address ${IP}:${PORT}"
  # A control-plane request: this proves the forward is bound to the LAN
  # interface and reachable off-loopback. The Host-routed data-plane path is the
  # SPK7 gate `make test-e2e-cloudrun-browser-lan`.
  curl -fsS --noproxy '*' "http://${IP}:${PORT}/_jaiscloud/health" >/dev/null \
    || { echo "ERROR: $IP:$PORT is not reachable" >&2; exit 1; }
  echo "==> control plane reachable on ${IP}:${PORT}"
fi

wait "$PF_PID"
