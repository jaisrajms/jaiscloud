#!/usr/bin/env bash
#
# SPK6 — whole-spine verification on the demo's exact combined config.
#
# Every link of the crypto-medallion demo spine is gated *individually*, but no
# single run uses the demo's combined config on one emulator instance, and the
# per-gate Make targets reconfigure the deployment ($CLOUDRUN_URL_SUFFIX/PORT,
# the scheduler hop forcing JAISCLOUD_SPARK_EXECUTOR_MODE=mock). This script
# configures the deployment once with the demo's combined config and runs the
# composed gates against that single instance, recording a green acceptance
# baseline (jobs, objects, rows, incidents) for the recording to build on.
#
#   make test-e2e-demo-spine-k3d
#
# Config applied (one instance):
#   JAISCLOUD_SPARK_EXECUTOR_MODE=k8s        real Spark client-mode pods
#   JAISCLOUD_KAFKA_BROKER_MODE=k8s          real Redpanda broker
#   JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s     revisions as Pods + ClusterIP
#   JAISCLOUD_CLOUDRUN_URL_SUFFIX=run.localhost   browser-reachable authority (SPK4)
#   JAISCLOUD_CLOUDRUN_URL_PORT=<port>       host-forwarded data-plane port (SPK4)
#   JAISCLOUD_GCP_THROTTLE unset             throttle off (design §9)
#   JAISCLOUD_DATAPROC_HMS_ENDPOINT          pod-reachable Hive Metastore (SPK1)
#   LRO mode = the emulator default (synchronous). The demo's async-LRO
#   pending→running→done pacing beat (design §9) is a "use on cue" control, not a
#   steady setting. Since SPK9 the Run/Eventarc k3s gate harnesses poll
#   operations.get, so the cue can be folded into the config without failing
#   them: set SPINE_LRO_ASYNC=1 to include it. It is separately gated by
#   `make test-lro-async-gcp`.
#
# Suites run on that instance, in order:
#   1. Iceberg batch on HMS                   (SPK1)
#   2. Iceberg Structured Streaming sink      (SPK2)
#   3. Lakehouse medallion (real sparkSqlJob) (existing)
#   4. Cloud Run browser reachability         (SPK4)
#   5. Eventarc delivery                      (existing)
#   6. Scheduler -> Dataproc submit hop       (SPK3; mock Spark — see note)
#   7. Monitoring metric -> alert incident    (SPK5; ephemeral, local build)
#
# Note on (6): the Scheduler hop gate deliberately runs the emulator's Spark
# executor in mock mode — it freezes the emulator clock and asserts the hop, not
# a Spark run (see the test header). It is the one control-plane step whose gate
# needs mock; it runs last so the combined real-Spark config holds for the whole
# data-plane spine. Real `sparkSqlJob` submission is proven by (3).
#
# The monitoring step (7) runs the raw-HTTP integration test against a local
# ephemeral emulator built from the same tree, because `TestMonitoringAlertPolicyFiresIncident`
# needs `/_jaiscloud/reset` between cases and does not need a cluster.
#
# Env:
#   K8S_NAMESPACE            (default: jaiscloud)
#   CLOUDRUN_BROWSER_SUFFIX  (default: run.localhost)
#   CLOUDRUN_BROWSER_PORT    (default: 18080)
#   SPINE_HEALTH_PORT        (default: 18099)  local port for the health forward
#   SPINE_LOG_DIR            (default: /tmp/opencode/demo-spine-<utc timestamp>)
#   SPINE_CONTINUE=1         keep going after a failing suite (default: stop)
#   SPINE_SKIP_SCHEDULER=1   skip the Scheduler hop suite
#   SPINE_SKIP_MONITORING=1  skip the Monitoring suite
#   SPINE_LRO_ASYNC=1        also apply the demo's async-LRO pacing cue
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# go is not on the default PATH in OpenCode sessions (see AGENTS.md).
if ! command -v go >/dev/null 2>&1 && [ -x /tmp/opencode/go/bin/go ]; then
  export PATH=/tmp/opencode/go/bin:$PATH
fi

NS="${K8S_NAMESPACE:-jaiscloud}"
SUFFIX="${CLOUDRUN_BROWSER_SUFFIX:-run.localhost}"
BROWSER_PORT="${CLOUDRUN_BROWSER_PORT:-18080}"
# Health-check forward only. It must NOT use BROWSER_PORT: the browser gate binds
# that fixed port itself (the synthesized *.localhost authority dials it).
HEALTH_PORT="${SPINE_HEALTH_PORT:-18099}"
LOG_DIR="${SPINE_LOG_DIR:-/tmp/opencode/demo-spine-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$LOG_DIR"

RESULTS_FILE="$LOG_DIR/RESULTS.txt"
: > "$RESULTS_FILE"
declare -a FAILED=()

log()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
pass() { printf '  \033[32mPASS\033[0m  %s\n' "$*" | tee -a "$RESULTS_FILE"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$*" | tee -a "$RESULTS_FILE"; }

# settle waits for the previous suite's Spark/Kafka driver+executor+broker pods
# to drain before the next suite needs the same ~6 GiB k3d node. The gates reap
# their own resources, but the reap is asynchronous, and running a heavy suite
# into a still-draining one leaves the next Spark driver unschedulable.
settle() {
  for _ in $(seq 1 60); do
    local leftover
    leftover=$(kubectl get pods -A --no-headers 2>/dev/null \
      | grep -Eic 'spark|driver|exec|kafka|redpanda|broker' || true)
    [ "$leftover" = "0" ] && break
    sleep 2
  done
  sleep 3
}

# run_suite <name> <logfile> <env-assignments...> -- <command...>
# Runs a suite, tees to a log, records PASS/FAIL; aborts unless SPINE_CONTINUE=1.
run_suite() {
  local name="$1" logfile="$2"; shift 2
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift # drop --
  log "$name"
  settle
  go clean -testcache
  set +e
  env "${envs[@]}" "$@" 2>&1 | tee "$logfile"
  local rc=${PIPESTATUS[0]}
  set -e
  if [ "$rc" -eq 0 ]; then
    pass "$name ($logfile)"
  else
    fail "$name (rc=$rc, $logfile)"
    FAILED+=("$name")
    [ "${SPINE_CONTINUE:-0}" = "1" ] || { note "stopping (set SPINE_CONTINUE=1 to continue)"; summary; exit 1; }
  fi
}

summary() {
  log "summary — logs in $LOG_DIR"
  if [ "${#FAILED[@]}" -eq 0 ]; then
    note "all suites passed"
  else
    note "failed: ${FAILED[*]}"
  fi
}

cleanup() {
  if [ -n "${PF_PID:-}" ]; then kill "$PF_PID" 2>/dev/null || true; fi
  if [ -n "${EPHEMERAL_PID:-}" ]; then kill "$EPHEMERAL_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM

# ── 0. preflight ────────────────────────────────────────────────────────────
log "preflight"
command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }
kubectl get namespace "$NS" >/dev/null 2>&1 || { echo "ERROR: namespace $NS not found"; exit 1; }
kubectl -n "$NS" get svc jaiscloud-gcp >/dev/null 2>&1 || { echo "ERROR: svc/jaiscloud-gcp missing — kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml"; exit 1; }
note "namespace=$NS suffix=$SUFFIX browser-port=$BROWSER_PORT"
note "logs=$LOG_DIR"

# ── 1. apply the manifest + the demo's combined config on ONE instance ───────
log "configure the combined demo config"
kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml
kubectl -n "$NS" set env deployment/jaiscloud-gcp \
  JAISCLOUD_SPARK_EXECUTOR_MODE=k8s \
  JAISCLOUD_KAFKA_BROKER_MODE=k8s \
  JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s \
  JAISCLOUD_CLOUDRUN_URL_SUFFIX="$SUFFIX" \
  JAISCLOUD_CLOUDRUN_URL_PORT="$BROWSER_PORT" \
  JAISCLOUD_GCP_THROTTLE=""
if [ "${SPINE_LRO_ASYNC:-0}" = "1" ]; then
  kubectl -n "$NS" set env deployment/jaiscloud-gcp JAISCLOUD_LRO_MODE=async JAISCLOUD_LRO_DELAY=2s
else
  # Remove any async-LRO cue left by a previous run so the standing config is
  # the emulator's synchronous default.
  kubectl -n "$NS" set env deployment/jaiscloud-gcp JAISCLOUD_LRO_MODE- JAISCLOUD_LRO_DELAY-
fi
kubectl -n "$NS" rollout status deployment/jaiscloud-gcp --timeout=240s

# Health-check the configured instance. The individual gates open their own
# port-forwards (the browser gate binds BROWSER_PORT itself), so this forward is
# on a separate local port.
kubectl -n "$NS" port-forward svc/jaiscloud-gcp "$HEALTH_PORT:8080" >"$LOG_DIR/port-forward.log" 2>&1 &
PF_PID=$!
for _ in $(seq 1 30); do
  curl -sf "http://localhost:$HEALTH_PORT/_jaiscloud/health" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "http://localhost:$HEALTH_PORT/_jaiscloud/health" >/dev/null || { echo "ERROR: emulator not healthy on :$HEALTH_PORT"; cat "$LOG_DIR/port-forward.log"; exit 1; }
note "emulator healthy on :$HEALTH_PORT (combined config applied)"

# ── 2. the data-plane spine, all on the one configured instance ──────────────
run_suite "Iceberg batch on HMS (SPK1)" "$LOG_DIR/01-iceberg-batch.log" \
  K8S_NAMESPACE="$NS" -- \
  go test -v -tags iceberg_k3d_e2e -run '^TestDataprocIcebergK3d$' -timeout 25m ./tests/persistent_mode/gcp/iceberg-k3d/

run_suite "Iceberg Structured Streaming sink (SPK2)" "$LOG_DIR/02-iceberg-streaming.log" \
  K8S_NAMESPACE="$NS" -- \
  go test -v -tags iceberg_k3d_e2e -run '^TestDataprocIcebergStreamingK3d$' -timeout 35m ./tests/persistent_mode/gcp/iceberg-k3d/

run_suite "Lakehouse medallion (real sparkSqlJob)" "$LOG_DIR/03-lakehouse.log" \
  K8S_NAMESPACE="$NS" -- \
  go test -v -tags lakehouse_e2e -timeout 20m ./tests/persistent_mode/gcp/lakehouse/

run_suite "Cloud Run browser reachability (SPK4)" "$LOG_DIR/04-cloudrun-browser.log" \
  K8S_NAMESPACE="$NS" CLOUDRUN_E2E_BROWSER=1 CLOUDRUN_BROWSER_SUFFIX="$SUFFIX" CLOUDRUN_BROWSER_PORT="$BROWSER_PORT" -- \
  go test -v -tags cloudrun_e2e -run TestCloudRunBrowserReachability -timeout 15m ./tests/persistent_mode/gcp/cloudrun/

run_suite "Eventarc delivery" "$LOG_DIR/05-eventarc.log" \
  K8S_NAMESPACE="$NS" EVENTARC_E2E_K8S=1 -- \
  go test -v -tags eventarc_e2e -timeout 20m ./tests/persistent_mode/gcp/eventarc/

# ── 3. control-plane hop (its gate runs mock Spark by design) ────────────────
if [ "${SPINE_SKIP_SCHEDULER:-0}" != "1" ]; then
  log "Scheduler -> Dataproc hop (SPK3, mock Spark executor)"
  kubectl -n "$NS" set env deployment/jaiscloud-gcp JAISCLOUD_SPARK_EXECUTOR_MODE=mock
  kubectl -n "$NS" rollout status deployment/jaiscloud-gcp --timeout=240s
  run_suite "Scheduler -> Dataproc submit hop (SPK3)" "$LOG_DIR/06-scheduler-hop.log" \
    K8S_NAMESPACE="$NS" SCHEDULER_E2E_K8S=1 -- \
    go test -v -tags scheduler_e2e -timeout 15m ./tests/persistent_mode/gcp/scheduler/
  kubectl -n "$NS" set env deployment/jaiscloud-gcp JAISCLOUD_SPARK_EXECUTOR_MODE=k8s
  kubectl -n "$NS" rollout status deployment/jaiscloud-gcp --timeout=240s
else
  note "SPINE_SKIP_SCHEDULER=1 — skipping the Scheduler hop suite"
fi

# ── 4. monitoring: ephemeral local build (needs /_jaiscloud/reset) ───────────
if [ "${SPINE_SKIP_MONITORING:-0}" != "1" ]; then
  log "Monitoring metric -> alert incident (SPK5, ephemeral)"
  go build -o "$LOG_DIR/jaiscloud-gcp" ./cmd/jaiscloud-gcp/
  "$LOG_DIR/jaiscloud-gcp" start --port 18081 --grpc-port 18082 --ephemeral >"$LOG_DIR/ephemeral.log" 2>&1 &
  EPHEMERAL_PID=$!
  for _ in $(seq 1 30); do
    curl -sf http://localhost:18081/_jaiscloud/health >/dev/null 2>&1 && break
    sleep 1
  done
  curl -sf http://localhost:18081/_jaiscloud/health >/dev/null || { echo "ERROR: ephemeral emulator not healthy"; cat "$LOG_DIR/ephemeral.log"; exit 1; }
  run_suite "Monitoring metric -> alert incident (SPK5)" "$LOG_DIR/07-monitoring.log" \
    JAISCLOUD_GCP_ENDPOINT=http://localhost:18081 -- \
    go test -v -run '^TestMonitoringAlertPolicyFiresIncident$' -timeout 5m ./tests/integration/gcp/
  kill "$EPHEMERAL_PID" 2>/dev/null || true
  EPHEMERAL_PID=""
else
  note "SPINE_SKIP_MONITORING=1 — skipping the Monitoring suite"
fi

summary
[ "${#FAILED[@]}" -eq 0 ]
