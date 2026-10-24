#!/usr/bin/env bash
#
# jaiscloud-gcp SDK tour — error / retry / idempotency driver.
#
# Same shape as run.sh (toolchains -> ephemeral emulator -> four official-client
# legs -> cross-language matrix) but runs the *failure* surfaces instead of the
# happy path, and starts the emulator with --metrics so each leg can read back
# the exact attempt count from the Prometheus counters.
#
# Two phases:
#   phase A (env)     — emulator started with JAISCLOUD_GCP_THROTTLE* in the
#                       environment; a raw probe proves that path injects a
#                       429 + Retry-After + google.rpc.RetryInfo.
#   phase B (runtime) — emulator restarted clean; each leg arms the injector at
#                       runtime (POST /_jaiscloud/throttle) per scenario, so one
#                       process covers every phase without further restarts.
#
# Everything downloaded/cached lives outside the repo (under /tmp/opencode) or
# in gitignored directories (.venv, node_modules, target, results-errors).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
TMP="/tmp/opencode/sdk-tour-errors"
mkdir -p "$TMP"

RUN_ID="${SDK_TOUR_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
RESULTS_DIR="$HERE/results-errors/$RUN_ID"
mkdir -p "$RESULTS_DIR"
export SDK_TOUR_RUN_ID="$RUN_ID"
export SDK_TOUR_MODE=errors
export PROJECT_ID="${PROJECT_ID:-jaiscloud-project}"

REST_PORT="${SDK_TOUR_REST_PORT:-8080}"
GRPC_PORT="${SDK_TOUR_GRPC_PORT:-8081}"
EMULATOR_MODE="${SDK_TOUR_EMULATOR:-ephemeral}"

EMULATOR_PID=""

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

stop_emulator() {
  if [ -n "$EMULATOR_PID" ] && kill -0 "$EMULATOR_PID" 2>/dev/null; then
    kill "$EMULATOR_PID" 2>/dev/null || true
    wait "$EMULATOR_PID" 2>/dev/null || true
  fi
  EMULATOR_PID=""
}

cleanup() { stop_emulator; }
trap cleanup EXIT INT TERM

# ─── toolchains ──────────────────────────────────────────────────────────────

if ! command -v go >/dev/null 2>&1; then
  export PATH="/tmp/opencode/go/bin:$PATH"
fi
if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: go not found (expected /tmp/opencode/go/bin/go)" >&2
  exit 2
fi
export GOFLAGS="${GOFLAGS:--mod=mod}"

JAVA_HOME="${JAVA_HOME:-/tmp/opencode/toolchain/jdk-21.0.12.1+1}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:/tmp/opencode/toolchain/maven/bin:$PATH"
export MAVEN_REPO="${MAVEN_REPO:-/tmp/opencode/m2}"

NODE_DIR="/tmp/opencode/node22"
if ! command -v node >/dev/null 2>&1; then
  export PATH="$NODE_DIR/bin:$PATH"
fi
if ! command -v node >/dev/null 2>&1; then
  log "node not found; downloading a current Node LTS into $NODE_DIR"
  mkdir -p "$NODE_DIR"
  node_ver="$(curl -fsSL https://nodejs.org/dist/index.json \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(next(v["version"] for v in d if v["lts"]))')"
  arch="$(uname -m)"; case "$arch" in x86_64) narch=x64;; aarch64|arm64) narch=arm64;; *) narch=x64;; esac
  curl -fsSL "https://nodejs.org/dist/${node_ver}/node-${node_ver}-linux-${narch}.tar.xz" \
    | tar -xJ --strip-components=1 -C "$NODE_DIR"
  export PATH="$NODE_DIR/bin:$PATH"
fi
if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: node unavailable after bootstrap" >&2
  exit 2
fi

PYTHON="${PYTHON:-python3}"

# ─── emulator ────────────────────────────────────────────────────────────────

BIN="$TMP/jaiscloud-gcp"

wait_health() {
  local n=0
  until curl -sf "http://localhost:$REST_PORT/_jaiscloud/health" >/dev/null 2>&1; do
    n=$((n + 1))
    if [ "$n" -ge 60 ]; then
      echo "ERROR: jaiscloud-gcp not healthy" >&2
      tail -20 "$TMP/emulator.log" >&2 || true
      exit 2
    fi
    sleep 0.5
  done
}

start_emulator() { # extra env pairs as arguments
  local log="$1"; shift
  log "starting ephemeral jaiscloud-gcp (REST :$REST_PORT, gRPC :$GRPC_PORT, --metrics)"
  env "$@" "$BIN" start --port "$REST_PORT" --grpc-port "$GRPC_PORT" --ephemeral --metrics \
    > "$TMP/$log" 2>&1 &
  EMULATOR_PID=$!
  wait_health
}

if [ "$EMULATOR_MODE" = "k3d" ]; then
  export EMULATOR_REST="${EMULATOR_REST:?SDK_TOUR_EMULATOR=k3d requires EMULATOR_REST}"
  export EMULATOR_GRPC="${EMULATOR_GRPC:?SDK_TOUR_EMULATOR=k3d requires EMULATOR_GRPC}"
  log "using existing emulator REST=$EMULATOR_REST gRPC=$EMULATOR_GRPC (must have been started with --metrics)"
else
  log "building jaiscloud-gcp -> $BIN"
  ( cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/jaiscloud-gcp/ ) || {
    echo "ERROR: emulator build failed" >&2; exit 2; }

  # ── phase A: the env-configured injector path ──────────────────────────────
  start_emulator emulator-env.log \
    JAISCLOUD_GCP_THROTTLE=fault \
    JAISCLOUD_GCP_THROTTLE_SERVICES=storage \
    JAISCLOUD_GCP_THROTTLE_FAIL_FIRST=1 \
    JAISCLOUD_GCP_THROTTLE_STATUS=429 \
    JAISCLOUD_GCP_THROTTLE_RETRY_DELAY=1s
  export EMULATOR_REST="http://localhost:$REST_PORT"
  export EMULATOR_GRPC="localhost:$GRPC_PORT"
  log "phase A: env-configured injector probe"
  probe_code="$(curl -s -o "$TMP/env-probe.json" -D "$TMP/env-probe.hdr" \
    -w '%{http_code}' "$EMULATOR_REST/storage/v1/b/env-probe")"
  retry_after="$(tr -d '\r' < "$TMP/env-probe.hdr" | awk 'tolower($1)=="retry-after:"{print $2}')"
  retry_info="$(python3 -c 'import json,sys
try:
    d=json.load(open(sys.argv[1]))
    details=d.get("error",{}).get("details",[]) or []
    print(next((x.get("retryDelay") for x in details if x.get("@type")=="type.googleapis.com/google.rpc.RetryInfo"),""))
except Exception:
    print("")' "$TMP/env-probe.json")"
  echo "ENV errors.retry_env 429=${probe_code} Retry-After=${retry_after} RetryInfo=${retry_info}"
  if [ "$probe_code" != "429" ] || [ "$retry_after" != "1" ] || [ "$retry_info" != "1s" ]; then
    echo "ERROR: env-configured throttle probe failed (want 429 / Retry-After=1 / RetryInfo=1s)" >&2
    exit 1
  fi
  stop_emulator

  # ── phase B: runtime-armed scenarios ───────────────────────────────────────
  start_emulator emulator.log
  export EMULATOR_REST="http://localhost:$REST_PORT"
  export EMULATOR_GRPC="localhost:$GRPC_PORT"
fi

export EMULATOR_HMS="${EMULATOR_HMS:-localhost:9083}"
export FIRESTORE_EMULATOR_HOST="$EMULATOR_GRPC"
export PUBSUB_EMULATOR_HOST="$EMULATOR_GRPC"
export STORAGE_EMULATOR_HOST="$EMULATOR_REST"
export BIGQUERY_EMULATOR_HOST="$EMULATOR_REST"

# ─── legs ────────────────────────────────────────────────────────────────────
# Each leg writes results-errors/<lang>.jsonl; a leg that cannot start is
# reported as MISSING for that language rather than aborting the run.

log "go leg"
( cd "$HERE/go" && go build -o "$TMP/sdk-tour-errors-go" . && \
  SDK_TOUR_RUN_ID="$RUN_ID-go" SDK_TOUR_RESULTS="$RESULTS_DIR/go.jsonl" "$TMP/sdk-tour-errors-go" ) \
  || echo "go leg exited non-zero"

log "python leg"
VENV="$HERE/python/.venv"
if [ ! -x "$VENV/bin/python" ]; then
  rm -rf "$VENV"
  if ! "$PYTHON" -m venv "$VENV" >/dev/null 2>&1; then
    virtualenv -q "$VENV" || { echo "ERROR: could not create python venv" >&2; }
  fi
fi
if [ -x "$VENV/bin/pip" ]; then
  "$VENV/bin/pip" install -q -r "$HERE/python/requirements.txt" \
    || echo "python deps install failed"
  ( cd "$HERE/python" && SDK_TOUR_RUN_ID="$RUN_ID-python" SDK_TOUR_RESULTS="$RESULTS_DIR/python.jsonl" \
      "$VENV/bin/python" sdk_tour.py ) || echo "python leg exited non-zero"
fi

log "node leg"
if ! command -v npm >/dev/null 2>&1; then
  echo "ERROR: npm unavailable" >&2
else
  ( cd "$HERE/node" && { [ -d node_modules ] || npm ci --no-audit --no-fund >/dev/null 2>&1; } && \
    SDK_TOUR_RUN_ID="$RUN_ID-node" SDK_TOUR_RESULTS="$RESULTS_DIR/node.jsonl" node sdk_tour.js ) \
    || echo "node leg exited non-zero"
fi

log "java leg"
( cd "$HERE/java" && SDK_TOUR_RUN_ID="$RUN_ID-java" SDK_TOUR_RESULTS="$RESULTS_DIR/java.jsonl" \
    mvn -q -Dmaven.repo.local="$MAVEN_REPO" -DskipTests compile exec:java \
    > "$TMP/java.log" 2>&1 ) || echo "java leg exited non-zero (see $TMP/java.log)"

# ─── aggregate ───────────────────────────────────────────────────────────────

log "aggregating error matrix (run id $RUN_ID)"
python3 "$HERE/aggregate_errors.py" "$RESULTS_DIR"
status=$?
exit "$status"
