#!/usr/bin/env bash
# Per-item gate for the TF confidence series (tf_series).
#
#   tests/integration/gcp/terraform/series-gate.sh kms
#   tests/integration/gcp/terraform/series-gate.sh scheduler tasks workflows
#
# Runs, for every touched service:
#
#   1. build + vet + the GCP unit tests (fast; CI runs the race suite)
#   2. the Terraform fixture gate (the job CI runs on a terraform-visible PR)
#   3. every pinned consumer-module gate that covers the service
#   4. the web corpus restricted to those services (plan + curated apply)
#
# A SKIP is a setup failure, never green (family rule): a missing binary or
# module checkout fails the run. The corpus half also needs
# TF_PROVIDER_DOCS/TF_SAMPLES_DIR; pass SKIP_CORPUS=1 only for an item that
# cannot change provider-observable behaviour.
#
# Parallel worktrees: pick a distinct listener pair per worktree and use the
# same pair for every gate in that worktree:
#
#   GCP_HTTP_PORT=18080 GCP_GRPC_PORT=18081 make tf-series-gate SERVICES=kms
#
# Environment: TF_BIN (terraform|tofu), GCP_HTTP_PORT, GCP_GRPC_PORT,
# TF_PROVIDER_DOCS, TF_SAMPLES_DIR, SKIP_FIXTURE=1, SKIP_CORPUS=1.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <service> [<service>...]" >&2
  exit 2
fi

TF_BIN="${TF_BIN:-terraform}"
GCP_HTTP_PORT="${GCP_HTTP_PORT:-8080}"
GCP_GRPC_PORT="${GCP_GRPC_PORT:-8081}"
LOG_DIR="${TMPDIR:-/tmp}/jc-tf-series-${GCP_HTTP_PORT}"
mkdir -p "$LOG_DIR"

# module cases a service's provider surface is pinned against (see the README's
# module table). Services without one are covered by the fixture or the corpus.
module_cases() {
  case "$1" in
    bigquery) echo "bigquery" ;;
    clouddns) echo "clouddns" ;;
    cloudsql) echo "sql-db" ;;
    container) echo "gke dataproc" ;;
    dataproc) echo "dataproc" ;;
    datastore) echo "datastore" ;;
    firestore) echo "firestore" ;;
    functions) echo "functions" ;;
    iam) echo "iam service-accounts" ;;
    kms) echo "kms" ;;
    memorystore) echo "memorystore" ;;
    monitoring) echo "cloud-operations" ;;
    pubsub) echo "pubsub pubsub-topic" ;;
    run) echo "cloudrun" ;;
    scheduler) echo "scheduler" ;;
    secretmanager) echo "secret-manager" ;;
    storage) echo "storage" ;;
    tasks) echo "tasks" ;;
    workflows) echo "workflows" ;;
    *) echo "" ;;
  esac
}

FAILED=()
RAN=()

run() {
  local label="$1"; shift
  local log="$LOG_DIR/${label}.log"
  echo "=== $label"
  if ! "$@" >"$log" 2>&1; then
    echo "FAIL: $label (see $log)"
    tail -25 "$log" | sed 's/^/    /'
    FAILED+=("$label")
    return 1
  fi
  if grep -q 'SKIP:' "$log"; then
    echo "FAIL: $label skipped (a skip is a setup failure)"
    grep 'SKIP:' "$log" | sed 's/^/    /'
    FAILED+=("$label (skip)")
    return 1
  fi
  RAN+=("$label")
  return 0
}

mk() {
  make --no-print-directory \
    TF_BIN="$TF_BIN" \
    GCP_HTTP_PORT="$GCP_HTTP_PORT" GCP_GRPC_PORT="$GCP_GRPC_PORT" \
    TF_PROVIDER_DOCS="${TF_PROVIDER_DOCS:-}" TF_SAMPLES_DIR="${TF_SAMPLES_DIR:-}" \
    "$@"
}

echo "TF series gate: services=$* terraform=$TF_BIN rest=:$GCP_HTTP_PORT grpc=:$GCP_GRPC_PORT"
echo "logs: $LOG_DIR"

run build go build ./...
run vet go vet ./internal/gcp/... ./cmd/jaiscloud-gcp/
run unit go test ./internal/gcp/... ./tools/tfcorpus/ ./tools/tfcoverage/

if [ "${SKIP_FIXTURE:-0}" != "1" ]; then
  run fixture make --no-print-directory TF_BIN="$TF_BIN" \
    GCP_HTTP_PORT="$GCP_HTTP_PORT" GCP_GRPC_PORT="$GCP_GRPC_PORT" STRICT=1 \
    test-gcp-terraform
fi

for svc in "$@"; do
  for case_name in $(module_cases "$svc"); do
    run "module-$case_name" mk STRICT=1 "test-gcp-terraform-$case_name"
  done
done

if [ "${SKIP_CORPUS:-0}" != "1" ]; then
  if [ -z "${TF_PROVIDER_DOCS:-}" ] || [ -z "${TF_SAMPLES_DIR:-}" ]; then
    echo "FAIL: TF_PROVIDER_DOCS/TF_SAMPLES_DIR not set (the corpus half is required; use SKIP_CORPUS=1 only if this item cannot affect it)"
    FAILED+=("corpus (missing checkouts)")
  else
    services="$(IFS=,; echo "$*")"
    if run corpus mk STRICT=1 CORPUS_SERVICES="$services" test-gcp-terraform-corpus; then
      if ! grep -q 'tfcorpus: planned' "$LOG_DIR/corpus.log"; then
        echo "FAIL: corpus matched no plan case for SERVICES=$services (check the service names)"
        FAILED+=("corpus (no cases matched)")
      fi
    fi
  fi
fi

echo
if [ "${#FAILED[@]}" -gt 0 ]; then
  echo "TF series gate: FAILED (${#FAILED[@]})"
  printf '  - %s\n' "${FAILED[@]}"
  exit 1
fi
echo "TF series gate: PASS (${#RAN[@]} gates)"
printf '  - %s\n' "${RAN[@]}"
