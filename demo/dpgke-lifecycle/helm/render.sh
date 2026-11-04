#!/usr/bin/env bash
#
# render.sh — the chart's gate (DGD4). No cluster, no credentials, no network:
# `helm lint` plus three renders, two of which must agree byte for byte.
#
# Why a golden at all: this chart's job is to turn Terraform's outputs into the
# app's DGD3_* environment without a human editing YAML. That mapping is the
# thing worth reviewing, and it is exactly what a text diff shows. Keeping the
# goldens in git means CI can assert the chart without a cluster, and a change
# to the env contract (DemoConfig.java) shows up as a chart diff instead of as a
# pod that starts and then fails on a missing variable.
#
# The chart must render deterministically, so anything that could make two runs
# differ — randAlphaNum, now, lookup, a generated Secret value — is banned.
# Running the render twice is what catches it; a diff against the golden alone
# would only catch it if the value happened to differ from the committed one.
#
# Usage:
#   ./render.sh              lint + determinism + golden check (what CI runs)
#   UPDATE=1 ./render.sh     rewrite golden/*.yaml from the current chart
#
# Prints SKIP and exits 0 when helm is absent, the `make test-gcp-terraform`
# precedent: a missing tool must not turn into a silent pass, and must not fail
# a contributor who is not touching the chart. It never installs anything.
#
# The goldens were produced with Helm v4.3.1; the chart is apiVersion v2, so
# Helm 3 accepts it too. If a Helm release ever changes how it renders, this
# script reports drift and the goldens are regenerated deliberately.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$DIR"
GOLDEN_DIR="$DIR/golden"
NAMESPACE="dpgke-demo"
UPDATE="${UPDATE:-0}"

if ! command -v helm >/dev/null 2>&1; then
  echo "SKIP: 'helm' not installed — the chart gate needs Helm 3+ (see README.md)"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/dpgke-helm.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

echo "== helm: lint ($(helm version --short))"
helm lint "$CHART" --namespace "$NAMESPACE"

render() { # render <output-file> [helm template args...]
  local out="$1"
  shift
  helm template dpgke-lifecycle "$CHART" --namespace "$NAMESPACE" "$@" > "$out"
}

check() { # check <label> <golden-file> [helm template args...]
  local label="$1"
  local golden="$2"
  shift 2

  local first="$TMP/$label.yaml"
  local second="$TMP/$label.second.yaml"

  render "$first" "$@"
  render "$second" "$@"

  # Determinism first: same input, same bytes. Only then compare to the golden,
  # so a non-deterministic chart fails with the actual symptom.
  if ! cmp -s "$first" "$second"; then
    echo "FAIL: $label rendered differently on two runs — the chart is not deterministic"
    diff -u "$first" "$second" || true
    exit 1
  fi
  echo "ok: $label is deterministic ($(wc -l < "$first" | tr -d ' ') lines, sha256 $(sha256sum "$first" | cut -d' ' -f1))"

  if [[ "$UPDATE" == "1" ]]; then
    cp "$first" "$golden"
    echo "updated: ${golden#"$DIR"/}"
    return 0
  fi
  if [[ ! -f "$golden" ]]; then
    echo "FAIL: $label has no golden at ${golden#"$DIR"/} (run UPDATE=1 $0 once to create it)"
    exit 1
  fi
  if ! diff -u "$golden" "$first" > "$TMP/$label.diff"; then
    echo "FAIL: $label drifted from ${golden#"$DIR"/}:"
    cat "$TMP/$label.diff"
    echo "If the change is intended, regenerate with UPDATE=1 $0 and commit the golden."
    exit 1
  fi
  echo "ok: $label matches ${golden#"$DIR"/}"
}

# The defaults: what a reader gets from `terraform apply` plus `helm install`.
check defaults "$GOLDEN_DIR/defaults.yaml"

# A second render with different values, so the templates really are templated:
# the Secret conditional, extraArgs/extraEnv, serviceAccountName and the
# cross-project bucket shape all have to render correctly here.
check overrides "$GOLDEN_DIR/overrides.yaml" --values "$GOLDEN_DIR/override-values.yaml"

echo "== chart gate passed"
