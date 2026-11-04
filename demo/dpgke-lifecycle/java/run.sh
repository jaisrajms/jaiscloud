#!/usr/bin/env bash
#
# run.sh — the gate for the Dataproc-on-GKE Java SDK leg (DGD3).
#
# Builds the Maven project, runs it twice against an emulator, and asserts the
# lifecycle the way the official Google clients see it:
#
#   run 1  DGD3_TEARDOWN=false  create everything, submit a job, leave it in
#                               place (the "idempotent create" precondition)
#   run 2  DGD3_TEARDOWN=true   adopt the existing GKE cluster and virtual
#                               cluster (no duplicate create), submit a second
#                               job, assert it, then delete job -> virtual
#                               cluster -> GKE cluster -> buckets and prove
#                               nothing is left
#
# Both runs must be green. The first also proves the create path; the second
# proves the get-or-create path and the teardown order.
#
# Transport: the Dataproc and GKE clients use their default gRPC transport on
# --grpc-port (plaintext, no credentials); the Storage client uses its HTTP/JSON
# transport against --port. Nothing falls back to REST silently — see README.md.
#
# Honesty: the emulator's default Dataproc executor is mock, so a job reaches
# DONE with a driver-output URI without Spark running. The app asserts the job
# state and reads the (synthetic) driver object; it does not claim Spark wrote
# records. Real bytes are DGD6's k3d gate.
#
# It never installs anything. When java or mvn is absent (after trying the
# SDKMAN init script) it prints SKIP and exits 0, the `make test-gcp-terraform`
# precedent. An emulator it starts is stopped on exit; one that was already
# healthy at ENDPOINT is reused and left running.
#
# Usage:
#   demo/dpgke-lifecycle/java/run.sh [ENDPOINT]
#
#     ENDPOINT  base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#               The gRPC port is ENDPOINT's port + 1, matching the emulator's
#               documented 8080/8081 pair.
#
# Env: PROJECT, REGION, GKE_LOCATION, GKE_CLUSTER, DATAPROC_CLUSTER, NAMESPACE
#      override the module's defaults; DGD3_* variables are forwarded to the app.

set -euo pipefail

ENDPOINT="${1:-${DGD3_REST_ENDPOINT:-http://localhost:8080}}"
ENDPOINT="${ENDPOINT%/}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../../.." && pwd)"
LOG="${TMPDIR:-/tmp}/jaiscloud-gcp-dpgke-java.log"

# ── Toolchain ─────────────────────────────────────────────────────────────────
# The harness shell is non-interactive, and ~/.bashrc returns early for those,
# so SDKMAN is not on PATH by default. Source its init script when the tools are
# missing — never install anything.
if ! command -v java >/dev/null 2>&1 || ! command -v mvn >/dev/null 2>&1; then
  if [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]]; then
    # SDKMAN's init script reads variables that `set -u` considers unbound.
    set +u
    # shellcheck disable=SC1091
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    set -u
  fi
fi
if ! command -v java >/dev/null 2>&1; then
  echo "SKIP: 'java' not installed — the Java SDK leg needs a JDK 21 (see README.md)"
  exit 0
fi
if ! command -v mvn >/dev/null 2>&1; then
  echo "SKIP: 'mvn' not installed — the Java SDK leg needs Maven (see README.md)"
  exit 0
fi

# ── Build + unit test ────────────────────────────────────────────────────────
# Before any DGD3_* export: the defaults test only runs with a clean
# environment. A compile/test failure must not leave an emulator running.
echo "== java: build and unit test"
( cd "$DIR" && mvn -q -e -DskipTests package )
( cd "$DIR" && mvn -q -e test )

# ── Endpoint / port split ────────────────────────────────────────────────────
ENDPOINT_HOST="$(printf '%s' "$ENDPOINT" | sed -E 's#^https?://##; s#[:/].*##')"
PORT="$(printf '%s' "$ENDPOINT" | sed -nE 's#^https?://[^/]*:([0-9]+).*#\1#p')"
PORT="${PORT:-8080}"
GRPC_PORT="${DGD3_GRPC_PORT:-$((PORT + 1))}"

DESTROYED=0
EMULATOR_PID=""

stop_emulator() {
  if [[ -n "$EMULATOR_PID" ]]; then
    kill "$EMULATOR_PID" 2>/dev/null || true
    wait "$EMULATOR_PID" 2>/dev/null || true
  fi
}

on_exit() {
  local rc=$?
  # If the second run did not finish its own teardown and this script did not
  # start the emulator, try the app's cleanup-only path so a reused emulator is
  # not left dirty. (An emulator we started is --ephemeral: killing it is clean.)
  if [[ "$DESTROYED" != "1" && -z "$EMULATOR_PID" && -f "$DIR/target/dpgke-lifecycle-java.jar" ]]; then
    ( cd "$DIR" && \
        DGD3_REST_ENDPOINT="$ENDPOINT" \
        DGD3_GRPC_ENDPOINT="$ENDPOINT_HOST" \
        DGD3_GRPC_PORT="$GRPC_PORT" \
        DGD3_TEARDOWN=true DGD3_CLEANUP_ONLY=true DGD3_DELETE_BUCKETS=true \
        java -jar target/dpgke-lifecycle-java.jar ) >/dev/null 2>&1 || true
  fi
  stop_emulator
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ── Emulator ─────────────────────────────────────────────────────────────────
if curl -sf "$ENDPOINT/_jaiscloud/health" >/dev/null 2>&1; then
  echo "== emulator: reusing the one already healthy at $ENDPOINT (gRPC :$GRPC_PORT)"
else
  case "$ENDPOINT_HOST" in
    localhost | 127.0.0.1 | ::1) ;;
    *)
      echo "ERROR: no emulator is healthy at $ENDPOINT and $ENDPOINT_HOST is not local"
      echo "       start one there, or point ENDPOINT at a local emulator"
      exit 1
      ;;
  esac
  if ! command -v go >/dev/null 2>&1; then
    export PATH="/tmp/opencode/go/bin:$PATH"
  fi
  echo "== emulator: building ./jaiscloud-gcp from $ROOT"
  ( cd "$ROOT" && go build -o ./jaiscloud-gcp ./cmd/jaiscloud-gcp/ )

  echo "== emulator: starting on :$PORT (grpc :$GRPC_PORT, log $LOG)"
  "$ROOT/jaiscloud-gcp" start --port "$PORT" --grpc-port "$GRPC_PORT" --ephemeral >"$LOG" 2>&1 &
  EMULATOR_PID=$!

  READY=0
  for _ in $(seq 1 30); do
    if curl -sf "$ENDPOINT/_jaiscloud/health" >/dev/null 2>&1; then
      READY=1
      break
    fi
    if ! kill -0 "$EMULATOR_PID" 2>/dev/null; then
      echo "ERROR: jaiscloud-gcp exited while starting; last log lines:"
      tail -n 20 "$LOG"
      exit 1
    fi
    sleep 1
  done
  if [[ "$READY" != "1" ]]; then
    echo "ERROR: emulator did not become healthy within 30s; last log lines:"
    tail -n 20 "$LOG"
    exit 1
  fi
fi

# The REST health endpoint can be up a moment before the gRPC listener accepts
# connections; the clients connect once, so wait for the port.
GRPC_READY=0
for _ in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/$ENDPOINT_HOST/$GRPC_PORT") 2>/dev/null; then
    exec 3>&- 3<&-
    GRPC_READY=1
    break
  fi
  sleep 1
done
if [[ "$GRPC_READY" != "1" ]]; then
  echo "ERROR: no gRPC listener on $ENDPOINT_HOST:$GRPC_PORT after 30s"
  exit 1
fi

# ── Configuration handed to the app ──────────────────────────────────────────
export DGD3_PROJECT="${DGD3_PROJECT:-${PROJECT:-dpgke-demo}}"
export DGD3_REGION="${DGD3_REGION:-${REGION:-us-central1}}"
export DGD3_GKE_LOCATION="${DGD3_GKE_LOCATION:-${GKE_LOCATION:-us-central1-a}}"
export DGD3_GKE_CLUSTER="${DGD3_GKE_CLUSTER:-${GKE_CLUSTER:-dpgke-gke}}"
export DGD3_DATAPROC_CLUSTER="${DGD3_DATAPROC_CLUSTER:-${DATAPROC_CLUSTER:-dpgke-dataproc}}"
export DGD3_NAMESPACE="${DGD3_NAMESPACE:-${NAMESPACE:-dpgke-demo}}"
export DGD3_REST_ENDPOINT="$ENDPOINT"
export DGD3_GRPC_ENDPOINT="${DGD3_GRPC_ENDPOINT:-$ENDPOINT_HOST}"
export DGD3_GRPC_PORT="$GRPC_PORT"
export DGD3_SPARK_SCRIPT="${DGD3_SPARK_SCRIPT:-$DIR/../spark/wordcount.py}"
# Nothing else owns these buckets in the standalone gate, so remove them.
export DGD3_DELETE_BUCKETS="${DGD3_DELETE_BUCKETS:-true}"

run_app() {
  ( cd "$DIR" && DGD3_TEARDOWN="$1" java -jar target/dpgke-lifecycle-java.jar )
}

echo
echo "== java run 1/2: create and leave the clusters in place (idempotent-create precondition)"
run_app false

echo
echo "== java run 2/2: adopt the existing clusters, submit, tear down, assert clean"
run_app true

DESTROYED=1
echo
echo "== java gate: both runs green (run 2 adopted what run 1 created; nothing is left)"
