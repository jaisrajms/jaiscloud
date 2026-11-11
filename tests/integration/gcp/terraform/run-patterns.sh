#!/usr/bin/env bash
#
# run-patterns.sh — aggregate for the TF4 composition patterns.
#
# Each pattern is an independent root module + driver under patterns/<NN-name>/.
# This aggregate runs them in order against one already-running emulator (the
# Makefile target and the nightly start it), writes one log per pattern and a
# summary, and treats a SKIP as a failure under STRICT=1 (the nightly) so a
# missing binary cannot pass as green.
#
# Usage:
#   tests/integration/gcp/terraform/run-patterns.sh [ENDPOINT] [PROJECT]
#
# Env:
#   TF_BIN        terraform (default) or tofu
#   STRICT=1      a SKIP is a failure
#   PATTERNS      whitespace-separated subset (default: every directory with a
#                 run.sh, in name order)
#   LOG_DIR       per-pattern logs + summary.md/summary.tsv/failures.md
#                 (default: /tmp/jaiscloud-gcp-terraform-patterns)
#
# Exit: 0 all patterns passed (or SKIP without STRICT); 1 a failure, a missing
# tool under STRICT, or no pattern ran.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERNS_DIR="$SCRIPT_DIR/patterns"
ENDPOINT="${1:-${TF_ENDPOINT:-http://localhost:8080}}"
PROJECT="${2:-${TF_PROJECT:-test-project}}"
TF="${TF_BIN:-terraform}"
STRICT="${STRICT:-0}"
LOG_DIR="${LOG_DIR:-/tmp/jaiscloud-gcp-terraform-patterns}"
export LOG_DIR

mkdir -p "$LOG_DIR"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  [ "$STRICT" = "1" ] && exit 1
  exit 0
fi

all_patterns() {
  local d
  for d in "$PATTERNS_DIR"/*/; do
    [[ -f "$d/run.sh" ]] || continue
    basename "$d"
  done | sort
}

if [[ -n "${PATTERNS:-}" ]]; then
  # shellcheck disable=SC2206
  PATTERN_LIST=(${PATTERNS})
else
  # shellcheck disable=SC2207
  PATTERN_LIST=($(all_patterns))
fi

if [[ "${#PATTERN_LIST[@]}" -eq 0 ]]; then
  echo "ERROR: no composition patterns found under $PATTERNS_DIR" >&2
  exit 1
fi

PASS=0
FAIL=0
SKIP=0
ROWS=()
# failures.md collects the tail of every failing pattern log, so the GitHub step
# log and the auto-filed nightly issue carry the cause without an artifact
# download (a pattern that dies on `set -e` leaves no [FAIL] line of its own).
FAILURES="$LOG_DIR/failures.md"
: >"$FAILURES"

run_one() {
  local name="$1"
  local log="$LOG_DIR/$name.log" start end rc verdict secs
  start="$(date +%s)"
  TF_BIN="$TF" STRICT="$STRICT" "$PATTERNS_DIR/$name/run.sh" "$ENDPOINT" "$PROJECT" >"$log" 2>&1
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
    {
      printf '## %s (exit %s)\n\n```\n' "$name" "$rc"
      tail -n 40 "$log" 2>/dev/null || true
      printf '```\n\n'
    } >>"$FAILURES"
  fi
  printf '  [%-4s] %-16s %4ss  %s\n' "$verdict" "$name" "$secs" "$log"
  ROWS+=("$name|$verdict|$secs|$log")
}

echo "== terraform composition patterns (endpoint=$ENDPOINT project=$PROJECT tf=$TF strict=$STRICT)"
echo "   logs: $LOG_DIR"

for pattern in "${PATTERN_LIST[@]}"; do
  run_one "$pattern"
done

{
  printf '# Terraform composition patterns\n\n'
  printf -- '- date: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf -- '- endpoint: %s\n' "$ENDPOINT"
  printf -- '- project: %s\n' "$PROJECT"
  printf -- '- binary: %s\n' "$("$TF" version 2>/dev/null | head -1)"
  printf -- '- strict: %s\n\n' "$STRICT"
  printf '| pattern | result | seconds | log |\n'
  printf '|---|---|---|---|\n'
  local_row=""
  for local_row in "${ROWS[@]}"; do
    IFS='|' read -r n v s l <<<"$local_row"
    printf '| %s | %s | %s | `%s` |\n' "$n" "$v" "$s" "$l"
  done
  printf '\n**%d passed, %d failed, %d skipped**\n' "$PASS" "$FAIL" "$SKIP"
} >"$LOG_DIR/summary.md"

{
  printf 'pattern\tresult\tseconds\tlog\n'
  local_row=""
  for local_row in "${ROWS[@]}"; do
    printf '%s\n' "${local_row//|/$'\t'}"
  done
} >"$LOG_DIR/summary.tsv"

echo
echo "== summary ($PASS passed, $FAIL failed, $SKIP skipped)"
cat "$LOG_DIR/summary.md"

if [[ "$FAIL" -gt 0 ]]; then
  echo
  echo "== failed pattern logs (tail)"
  cat "$FAILURES"
  exit 1
fi
if [[ "$STRICT" = "1" && "$SKIP" -gt 0 ]]; then
  echo "STRICT=1: $SKIP pattern(s) skipped (a missing tool is a failure in CI)" >&2
  exit 1
fi
exit 0
