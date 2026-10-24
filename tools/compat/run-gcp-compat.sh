#!/usr/bin/env bash
#
# run-gcp-compat.sh — run the localgcp harness and the floci-gcp compatibility
# suites against a locally-built jaiscloud-gcp emulator.
#
# OPTIONAL INTEROP SMOKE, NOT A SPEC. floci-gcp/localgcp are sibling emulators;
# their assertions encode their own conventions. The authority for GCP wire
# behavior is the service Discovery document and the official client SDKs (see
# CLAUDE.md / AGENTS.md). When one of these suites fails on a shape that
# disagrees with Discovery, classify it TEST-NON-COMPLIANT rather than changing
# jaiscloud to match.
#
# This script is intentionally NOT committed (scratch tooling under tools/compat/).
#
# What it does:
#   1. builds and boots jaiscloud-gcp (REST :8080, gRPC :8081) ephemeral by default,
#   2. builds and runs the localgcp replay harness (tools/compat/localgcp-harness),
#   3. starts a tiny single-port h2c/REST multiplexer on :4588 (floci serves both
#      transports on one port; jaiscloud splits them),
#   4. runs the selected floci-gcp compatibility suites with the emulator-host env
#      wired to the local build, resetting state between suites,
#   5. stops everything and prints a per-suite pass/fail/skip summary.
#
# Usage:
#   tools/compat/run-gcp-compat.sh [--suites localgcp,python,go] [--setup] [options]
#
# Suites: localgcp java python node go rust gcloud terraform opentofu all
#   Default: localgcp + every floci suite whose toolchain is installed.
#
# Options:
#   --suites LIST     comma-separated suites (default: see above)
#   --test PATTERN    run only the matching test(s) within each suite:
#                     java `-Dtest=PATTERN` (e.g. CloudRunTest, or
#                     CloudRunTest,StorageTest), python `-k PATTERN`, go `-run
#                     PATTERN`, rust the test-name filter. With no --suites this
#                     defaults to `java` so `--test CloudRunTest` is enough.
#   --setup           install/refresh suite deps (venv+pip, npm install, mvn
#                     dependency:resolve, cargo fetch, terraform/tofu init)
#   --dsn DSN         run the emulator with PostgreSQL (default: ephemeral)
#   --blob-dir DIR    blob dir for S3/GCS bytes (default: a temp dir)
#   --rest-port N     REST port (default 8080)
#   --grpc-port N     gRPC port (default 8081)
#   --mux-port N      single-port mux port (default 4588)
#   --no-mux          do not start the single-port mux
#   --keep            leave the emulator running after the run
#   --list            list suites and exit
#   -h, --help        this help
#
# Env overrides:
#   GO                    go binary (default: `go`, else /tmp/opencode/go/bin/go)
#   GCP_PROJECT           project id (default test-project)
#   FLOCI_COMPAT_DIR      floci-gcp compatibility-tests dir
#                         (default ~/code/floci-gcp/compatibility-tests)
#   LOCALGCP_HARNESS_DIR  localgcp harness dir (default tools/compat/localgcp-harness)
#   JAISCLOUD_GCP_BIN     prebuilt emulator binary (skip the build)
#   EMU_EXTRA_ARGS        extra args for `jaiscloud-gcp start`

set -euo pipefail

# ─── paths ────────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

GO="${GO:-}"
if [[ -z "$GO" ]]; then
  if command -v go >/dev/null 2>&1; then GO="go"; elif [[ -x /tmp/opencode/go/bin/go ]]; then GO=/tmp/opencode/go/bin/go; fi
fi

GCP_PROJECT="${GCP_PROJECT:-test-project}"
FLOCI_COMPAT_DIR="${FLOCI_COMPAT_DIR:-$HOME/code/floci-gcp/compatibility-tests}"
LOCALGCP_HARNESS_DIR="${LOCALGCP_HARNESS_DIR:-$ROOT/tools/compat/localgcp-harness}"

REST_PORT=8080
GRPC_PORT=8081
MUX_PORT=4588
USE_MUX=1
DO_SETUP=0
KEEP=0
DSN="${DSN:-}"
BLOB_DIR=""
SUITE_TIMEOUT="${SUITE_TIMEOUT:-1800}"
EMU_EXTRA_ARGS="${EMU_EXTRA_ARGS:-}"
SUITES=""
TEST_FILTER="${TEST_FILTER:-}"

ALL_SUITES=(localgcp java python node go rust gcloud terraform opentofu)

# ─── args ─────────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --suites) SUITES="${2:?--suites needs a value}"; shift 2 ;;
    --test) TEST_FILTER="${2:?--test needs a value}"; shift 2 ;;
    --setup) DO_SETUP=1; shift ;;
    --dsn) DSN="${2:?--dsn needs a value}"; shift 2 ;;
    --blob-dir) BLOB_DIR="${2:?--blob-dir needs a value}"; shift 2 ;;
    --rest-port) REST_PORT="${2:?}"; shift 2 ;;
    --grpc-port) GRPC_PORT="${2:?}"; shift 2 ;;
    --mux-port) MUX_PORT="${2:?}"; shift 2 ;;
    --no-mux) USE_MUX=0; shift ;;
    --keep) KEEP=1; shift ;;
    --list) printf '%s\n' "${ALL_SUITES[@]}"; exit 0 ;;
    -h|--help) sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$GO" ]]; then
  echo "error: no go binary found (set GO=)" >&2; exit 1
fi

# ─── suites selection ─────────────────────────────────────────────────────────
selected=()
has_suite() { local s; for s in "${selected[@]}"; do [[ "$s" == "$1" ]] && return 0; done; return 1; }
tool() { command -v "$1" >/dev/null 2>&1; }

# A --test filter targets the floci Java suite by default, so
# `--test CloudRunTest` runs exactly one class instead of the whole matrix.
if [[ -z "$SUITES" && -n "$TEST_FILTER" ]]; then
  SUITES="java"
fi

if [[ -z "$SUITES" ]]; then
  selected=(localgcp)
  [[ -d "$FLOCI_COMPAT_DIR" ]] || { echo "note: floci compat dir not found ($FLOCI_COMPAT_DIR); running localgcp only"; }
  if [[ -d "$FLOCI_COMPAT_DIR" ]]; then
    tool mvn     && selected+=(java)
    tool python3 && selected+=(python)
    tool npm     && selected+=(node)
    tool go      && selected+=(go)
    tool cargo   && selected+=(rust)
    tool bats && tool gcloud && selected+=(gcloud)
    tool bats && tool terraform && selected+=(terraform)
    tool bats && tool tofu && selected+=(opentofu)
  fi
elif [[ "$SUITES" == "all" ]]; then
  selected=("${ALL_SUITES[@]}")
else
  IFS=',' read -r -a selected <<<"$SUITES"
fi
for s in "${selected[@]}"; do
  case " ${ALL_SUITES[*]} " in *" $s "*) ;; *) echo "unknown suite: $s" >&2; exit 2 ;; esac
done

# ─── temp workspace + cleanup ─────────────────────────────────────────────────
WORK="$(mktemp -d "${TMPDIR:-/tmp}/jc-compat.XXXXXX")"
EMU_LOG="$WORK/jaiscloud-gcp.log"
MUX_LOG="$WORK/portmux.log"
EMU_BIN="${JAISCLOUD_GCP_BIN:-$WORK/jaiscloud-gcp}"
MUX_BIN="$WORK/portmux-bin"
EMU_PID=""; MUX_PID=""
declare -a SUMMARY=()

cleanup() {
  set +e
  if [[ "$KEEP" == "1" && -n "$EMU_PID" ]]; then
    echo "(--keep: leaving emulator pid $EMU_PID running on :$REST_PORT / :$GRPC_PORT)"
    if [[ -n "$MUX_PID" ]]; then kill "$MUX_PID" 2>/dev/null; fi
    return
  fi
  if [[ -n "$MUX_PID" ]]; then kill "$MUX_PID" 2>/dev/null; fi
  if [[ -n "$EMU_PID" ]]; then kill "$EMU_PID" 2>/dev/null; fi
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# ─── helpers ──────────────────────────────────────────────────────────────────
log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

wait_health() {
  local i
  for i in $(seq 1 60); do
    if curl -sf "http://localhost:$REST_PORT/_jaiscloud/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "error: jaiscloud-gcp not healthy on :$REST_PORT"; tail -n 40 "$EMU_LOG" >&2 || true; return 1
}

reset_state() {
  curl -sf -X POST "http://localhost:$REST_PORT/_jaiscloud/reset" >/dev/null 2>&1 || true
}

# check_suite NAME COMMAND... — run, record PASS/FAIL, reset afterwards.
check_suite() {
  local name="$1"; shift
  reset_state
  log "suite: $name"
  if "$@"; then SUMMARY+=("$name|PASS"); else SUMMARY+=("$name|FAIL"); fi
  reset_state
}

port_busy() {
  command -v ss >/dev/null 2>&1 || return 1
  ss -ltn "sport = :$1" 2>/dev/null | grep -q LISTEN
}

# ─── 1. build + boot the emulator ─────────────────────────────────────────────
if [[ ! -x "$EMU_BIN" ]]; then
  log "building jaiscloud-gcp"
  (cd "$ROOT" && "$GO" build -o "$EMU_BIN" ./cmd/jaiscloud-gcp/)
fi

for p in "$REST_PORT" "$GRPC_PORT"; do
  if port_busy "$p"; then echo "error: port $p already in use" >&2; exit 1; fi
done

start_args=(start --port "$REST_PORT" --grpc-port "$GRPC_PORT")
if [[ -n "$DSN" ]]; then
  start_args+=(--dsn "$DSN")
else
  start_args+=(--ephemeral)
fi
if [[ -z "$BLOB_DIR" ]]; then BLOB_DIR="$WORK/blobs"; fi
start_args+=(--blob-dir "$BLOB_DIR")
# shellcheck disable=SC2206
if [[ -n "$EMU_EXTRA_ARGS" ]]; then start_args+=($EMU_EXTRA_ARGS); fi

log "starting jaiscloud-gcp (${start_args[*]})"
"$EMU_BIN" "${start_args[@]}" >"$EMU_LOG" 2>&1 &
EMU_PID=$!
wait_health
note "healthy (REST :$REST_PORT, gRPC :$GRPC_PORT)"

# ─── 2. single-port mux (h2c vs REST) for floci suites ────────────────────────
if [[ "$USE_MUX" == "1" ]]; then
  if port_busy "$MUX_PORT"; then echo "error: mux port $MUX_PORT already in use" >&2; exit 1; fi
  log "building single-port mux on :$MUX_PORT (gRPC preface -> :$GRPC_PORT, else REST -> :$REST_PORT)"
  mkdir -p "$WORK/portmux"
  cat >"$WORK/portmux/main.go" <<'GOEOF'
package main

import (
	"bufio"
	"io"
	"net"
	"os"
)

// portmux serves REST and cleartext HTTP/2 (gRPC) on one port, mirroring
// floci-gcp's single-port design. It sniffs the HTTP/2 client preface
// ("PRI * HTTP/2.0") and forwards to the gRPC backend, otherwise to the REST
// backend, then splices the connection both ways.
func main() {
	if len(os.Args) != 4 {
		panic("usage: portmux <listen> <rest-backend> <grpc-backend>")
	}
	listen, rest, grpcAddr := os.Args[1], os.Args[2], os.Args[3]
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		panic(err)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c, rest, grpcAddr)
	}
}

func handle(c net.Conn, rest, grpcAddr string) {
	defer c.Close()
	br := bufio.NewReader(c)
	const preface = "PRI * HTTP/2.0"
	peek, err := br.Peek(len(preface))
	backend := rest
	if err == nil && string(peek) == preface {
		backend = grpcAddr
	}
	s, err := net.Dial("tcp", backend)
	if err != nil {
		return
	}
	defer s.Close()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(s, br); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, s); done <- struct{}{} }()
	<-done
}
GOEOF
  (cd "$WORK/portmux" && "$GO" mod init portmux >/dev/null 2>&1 && "$GO" build -o "$MUX_BIN" .)
  "$MUX_BIN" ":$MUX_PORT" "127.0.0.1:$REST_PORT" "127.0.0.1:$GRPC_PORT" >"$MUX_LOG" 2>&1 &
  MUX_PID=$!
  sleep 0.3
fi

# floci env: per-transport vars point straight at jaiscloud's split ports; the
# single-endpoint vars (FLOCI_GCP_ENDPOINT) go through the mux.
export FLOCI_GCP_ENDPOINT="http://localhost:$MUX_PORT"
export FLOCI_GCP_HOST="localhost:$MUX_PORT"
export FLOCI_GCP_PROJECT="$GCP_PROJECT"
export GOOGLE_CLOUD_PROJECT="$GCP_PROJECT"
export GOOGLE_OAUTH_ACCESS_TOKEN="fake-token-floci-gcp"
export PUBSUB_EMULATOR_HOST="localhost:$GRPC_PORT"
export FIRESTORE_EMULATOR_HOST="localhost:$GRPC_PORT"
export DATASTORE_EMULATOR_HOST="localhost:$GRPC_PORT"
export SECRET_MANAGER_EMULATOR_HOST="localhost:$GRPC_PORT"
export STORAGE_EMULATOR_HOST="http://localhost:$REST_PORT"
export STORAGE_EMULATOR_HOST_GRPC="localhost:$GRPC_PORT"

# ─── 3. localgcp harness ──────────────────────────────────────────────────────
run_localgcp() {
  [[ -d "$LOCALGCP_HARNESS_DIR" ]] || { echo "localgcp harness dir not found: $LOCALGCP_HARNESS_DIR" >&2; return 1; }
  local bin="$WORK/localgcp-harness-bin" out="$WORK/localgcp.out"
  (cd "$LOCALGCP_HARNESS_DIR" && "$GO" build -o "$bin" .)
  (cd "$LOCALGCP_HARNESS_DIR" && \
    GCP_REST_ENDPOINT="http://localhost:$REST_PORT" \
    GCP_GRPC_ENDPOINT="localhost:$GRPC_PORT" \
    HARNESS_REPORT="$WORK/localgcp-results.json" \
    timeout "$SUITE_TIMEOUT" "$bin") | tee "$out"
  # The harness reports per-case FAILs but always exits 0; surface them here.
  grep -q '\[FAIL\]' "$out" && return 1
  return 0
}

# ─── 4. floci suites (mirror compatibility-tests/justfile) ────────────────────
require_dir() { [[ -d "$1" ]] || { echo "missing dir: $1" >&2; return 1; }; }

run_java()      { require_dir "$FLOCI_COMPAT_DIR/sdk-test-java";       (cd "$FLOCI_COMPAT_DIR/sdk-test-java" && timeout "$SUITE_TIMEOUT" mvn test -q ${TEST_FILTER:+-Dtest="$TEST_FILTER"}); }
run_python()    { require_dir "$FLOCI_COMPAT_DIR/sdk-test-python";     (cd "$FLOCI_COMPAT_DIR/sdk-test-python" && timeout "$SUITE_TIMEOUT" .venv/bin/pytest tests/ -v ${TEST_FILTER:+-k "$TEST_FILTER"}); }
run_node()      { require_dir "$FLOCI_COMPAT_DIR/sdk-test-node";       (cd "$FLOCI_COMPAT_DIR/sdk-test-node" && timeout "$SUITE_TIMEOUT" npm test); }
run_go()        { require_dir "$FLOCI_COMPAT_DIR/sdk-test-go";         (cd "$FLOCI_COMPAT_DIR/sdk-test-go" && timeout "$SUITE_TIMEOUT" "$GO" test ./tests/... -v -timeout 120s ${TEST_FILTER:+-run "$TEST_FILTER"}); }
run_rust()      { require_dir "$FLOCI_COMPAT_DIR/sdk-test-rust";       (cd "$FLOCI_COMPAT_DIR/sdk-test-rust" && timeout "$SUITE_TIMEOUT" cargo test --locked ${TEST_FILTER:+$TEST_FILTER}); }
run_gcloud()    { require_dir "$FLOCI_COMPAT_DIR/sdk-test-gcloud";     (cd "$FLOCI_COMPAT_DIR/sdk-test-gcloud" && timeout "$SUITE_TIMEOUT" bats test/); }
run_terraform() { require_dir "$FLOCI_COMPAT_DIR/compat-terraform";    (cd "$FLOCI_COMPAT_DIR/compat-terraform" && timeout "$SUITE_TIMEOUT" bats test/terraform.bats); }
run_opentofu()  { require_dir "$FLOCI_COMPAT_DIR/compat-opentofu";    (cd "$FLOCI_COMPAT_DIR/compat-opentofu" && timeout "$SUITE_TIMEOUT" bats test/opentofu.bats); }

setup_suite() {
  local s="$1"
  case "$s" in
    java)      tool mvn   && (cd "$FLOCI_COMPAT_DIR/sdk-test-java" && mvn dependency:resolve -q) ;;
    python)    tool python3 && (cd "$FLOCI_COMPAT_DIR/sdk-test-python" && { [[ -x .venv/bin/pip ]] || python3 -m venv .venv; } && .venv/bin/pip install -q -r requirements.txt) ;;
    node)      tool npm   && (cd "$FLOCI_COMPAT_DIR/sdk-test-node" && npm install) ;;
    go)        (cd "$FLOCI_COMPAT_DIR/sdk-test-go" && "$GO" mod tidy) ;;
    rust)      tool cargo && (cd "$FLOCI_COMPAT_DIR/sdk-test-rust" && cargo fetch) ;;
    gcloud)    : ;;
    terraform) tool terraform && (cd "$FLOCI_COMPAT_DIR/compat-terraform" && terraform init -input=false) ;;
    opentofu)  tool tofu && (cd "$FLOCI_COMPAT_DIR/compat-opentofu" && tofu init -input=false) ;;
  esac
}

run_floci() {
  local s="$1"
  case "$s" in
    java)      check_suite floci:java      run_java ;;
    python)    check_suite floci:python    run_python ;;
    node)      check_suite floci:node      run_node ;;
    go)        check_suite floci:go        run_go ;;
    rust)      check_suite floci:rust      run_rust ;;
    gcloud)    check_suite floci:gcloud    run_gcloud ;;
    terraform) check_suite floci:terraform run_terraform ;;
    opentofu)  check_suite floci:opentofu  run_opentofu ;;
  esac
}

# ─── run ──────────────────────────────────────────────────────────────────────
reset_state

if has_suite localgcp; then
  check_suite localgcp run_localgcp
fi

for s in "${selected[@]}"; do
  [[ "$s" == "localgcp" ]] && continue
  if [[ "$DO_SETUP" == "1" ]]; then log "setup: $s"; setup_suite "$s"; fi
  # Skip (don't hard-fail) a floci suite whose required tool is missing.
  case "$s" in
    java)      tool mvn   || { note "skip floci:java (mvn not installed)"; SUMMARY+=("floci:java|SKIP"); continue; } ;;
    python)    [[ -x "$FLOCI_COMPAT_DIR/sdk-test-python/.venv/bin/pytest" ]] || { note "skip floci:python (no venv; run with --setup)"; SUMMARY+=("floci:python|SKIP"); continue; } ;;
    node)      tool npm   || { note "skip floci:node (npm not installed)"; SUMMARY+=("floci:node|SKIP"); continue; } ;;
    rust)      tool cargo || { note "skip floci:rust (cargo not installed)"; SUMMARY+=("floci:rust|SKIP"); continue; } ;;
    gcloud)    { tool bats && tool gcloud; } || { note "skip floci:gcloud (bats/gcloud missing)"; SUMMARY+=("floci:gcloud|SKIP"); continue; } ;;
    terraform) { tool bats && tool terraform; } || { note "skip floci:terraform (bats/terraform missing)"; SUMMARY+=("floci:terraform|SKIP"); continue; } ;;
    opentofu)  { tool bats && tool tofu; } || { note "skip floci:opentofu (bats/tofu missing)"; SUMMARY+=("floci:opentofu|SKIP"); continue; } ;;
    go)        : ;;
  esac
  [[ -d "$FLOCI_COMPAT_DIR" ]] || { note "skip floci:$s (compat dir missing)"; SUMMARY+=("floci:$s|SKIP"); continue; }
  run_floci "$s" || true
done

# ─── summary ──────────────────────────────────────────────────────────────────
log "summary"
if [[ ${#SUMMARY[@]} -gt 0 ]]; then
  for row in "${SUMMARY[@]}"; do
    printf '   %-22s %s\n' "${row%%|*}" "${row##*|}"
  done
else
  note "(no suites ran)"
fi
echo
note "emulator log: $EMU_LOG"
[[ "$USE_MUX" == "1" ]] && note "mux log: $MUX_LOG"

# Fail the script if any suite FAILed.
if [[ ${#SUMMARY[@]} -gt 0 ]] && printf '%s\n' "${SUMMARY[@]}" | grep -q '|FAIL$'; then exit 1; fi
exit 0
