#!/usr/bin/env bash
#
# run-all.sh — run the Terraform fixture and every pinned module case against
# one already-running jaiscloud-gcp, with a per-case log and one summary.
#
# This is the aggregate used by `make test-gcp-terraform-all` and the nightly.
# The per-case Makefile targets remain the developer entry point; this script
# does not start/stop the emulator.
#
# Usage:
#   tests/integration/gcp/terraform/run-all.sh [ENDPOINT] [PROJECT]
#
#     ENDPOINT  base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#     PROJECT   project id (default: test-project)
#
# Env:
#   TF_BIN        terraform (default) or tofu
#   STRICT=1      a SKIP is a failure (CI); without it a SKIP is reported and
#                 the script still exits 0, matching the per-case targets
#   LOG_DIR       per-case logs + summary.md/summary.tsv
#                 (default: /tmp/jaiscloud-gcp-terraform-all)
#   CASES         whitespace-separated subset of module cases (default: all)
#   SKIP_FIXTURE=1  skip the fixture, run only the module cases
#
# Exit: 0 all ran and passed (or SKIP without STRICT); 1 a failure, a missing
# tool/module under STRICT, or no case ran at all.

set -uo pipefail

ENDPOINT="${1:-http://localhost:8080}"
PROJECT="${2:-test-project}"
TF="${TF_BIN:-terraform}"
STRICT="${STRICT:-0}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-/tmp/jaiscloud-gcp-terraform-all}"

mkdir -p "$LOG_DIR"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  [ "$STRICT" = "1" ] && exit 1
  exit 0
fi

export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

all_cases() {
  local f
  for f in "$DIR"/module-cases/*.sh; do
    [[ -e "$f" ]] || continue
    basename "$f" .sh
  done | sort
}

# Explicit CASES wins; otherwise every case file.
if [[ -n "${CASES:-}" ]]; then
  # shellcheck disable=SC2206
  CASE_LIST=(${CASES})
else
  # shellcheck disable=SC2207
  CASE_LIST=($(all_cases))
fi

PASS=0
FAIL=0
SKIP=0
ROWS=()

# module_commit reports the checkout the case will actually use, so the
# scorecard records what was tested rather than what the pin says.
module_commit() {
  local case="$1" repo dir
  repo="$(sed -n 's/^MODULE_REPO="\([^"]*\)".*/\1/p' "$DIR/module-cases/$case.sh" | head -1)"
  [[ -n "$repo" ]] || { printf '%s' "-"; return; }
  dir="${TF_MODULE_DIR:-$HOME/.cache/terraform-modules/$repo}"
  if [[ -d "$dir/.git" ]]; then
    git -C "$dir" rev-parse --short HEAD 2>/dev/null || printf '%s' "-"
  else
    printf '%s' "-"
  fi
}

run_one() {
  local name="$1" commit="$2"
  shift 2
  local log="$LOG_DIR/$name.log" start end rc verdict secs
  start="$(date +%s)"
  "$@" >"$log" 2>&1
  rc=$?
  end="$(date +%s)"
  secs=$((end - start))
  if [[ "$rc" -eq 0 ]] && grep -q '^SKIP' "$log"; then
    verdict="SKIP"
    SKIP=$((SKIP + 1))
  elif [[ "$rc" -eq 0 ]]; then
    verdict="PASS"
    PASS=$((PASS + 1))
  else
    verdict="FAIL"
    FAIL=$((FAIL + 1))
  fi
  printf '  [%-4s] %-18s %4ss  %s\n' "$verdict" "$name" "$secs" "$log"
  ROWS+=("$name|$verdict|$secs|$commit|$log")
}

echo "== terraform all-cases (endpoint=$ENDPOINT project=$PROJECT tf=$TF strict=$STRICT)"
echo "   logs: $LOG_DIR"
[[ -n "${TF_MODULE_DIR:-}" ]] && echo "   TF_MODULE_DIR override: $TF_MODULE_DIR"

if [[ "${SKIP_FIXTURE:-0}" != "1" ]]; then
  run_one "fixture" "-" "$DIR/run.sh" "$ENDPOINT" "$PROJECT"
fi

for case in "${CASE_LIST[@]}"; do
  run_one "$case" "$(module_commit "$case")" "$DIR/run-module.sh" "$case" "$ENDPOINT" "$PROJECT"
done

{
  printf '# Terraform all-cases summary\n\n'
  printf -- '- date: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf -- '- endpoint: %s\n' "$ENDPOINT"
  printf -- '- project: %s\n' "$PROJECT"
  printf -- '- binary: %s\n' "$("$TF" version 2>/dev/null | head -1)"
  printf -- '- strict: %s\n\n' "$STRICT"
  printf '| case | result | seconds | module HEAD | log |\n'
  printf '|---|---|---|---|---|\n'
  local_row=""
  for local_row in "${ROWS[@]}"; do
    IFS='|' read -r n v s c l <<<"$local_row"
    printf '| %s | %s | %s | %s | `%s` |\n' "$n" "$v" "$s" "$c" "$l"
  done
  printf '\n**%d passed, %d failed, %d skipped**\n' "$PASS" "$FAIL" "$SKIP"
} >"$LOG_DIR/summary.md"

{
  printf 'case\tresult\tseconds\tmodule_head\tlog\n'
  local_row=""
  for local_row in "${ROWS[@]}"; do
    printf '%s\n' "${local_row//|/$'\t'}"
  done
} >"$LOG_DIR/summary.tsv"

echo
echo "== summary ($PASS passed, $FAIL failed, $SKIP skipped)"
cat "$LOG_DIR/summary.md"

if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
if [[ "$STRICT" = "1" && "$SKIP" -gt 0 ]]; then
  echo "STRICT=1: $SKIP case(s) skipped (a missing tool/module is a failure in CI)" >&2
  exit 1
fi
if [[ "${#ROWS[@]}" -eq 0 ]]; then
  echo "ERROR: no cases ran (empty CASES?)" >&2
  exit 1
fi
exit 0
