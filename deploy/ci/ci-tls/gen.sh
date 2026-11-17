#!/usr/bin/env bash
#
# ci-tls/gen.sh — a dev CA + wildcard certificate for CI tests that insist on
# the real https://<cloud host> names.
#
# It reuses the front door's generator (deploy/k8s/istio/certs/gen.sh) so the CI
# proxy and the in-cluster gateway share one SAN policy: the four wildcards plus
# the deeper names X.509 wildcards cannot reach (regional/virtual-hosted AWS
# endpoints, *.{location}.run.app, *.execute-api.{region}.amazonaws.com).
#
# Usage:
#   deploy/ci/ci-tls/gen.sh                 # writes ./certs if absent
#   OUT_DIR=/tmp/ci-certs .../gen.sh --force
#
# Env (passed through to the generator):
#   OUT_DIR             output directory (default: deploy/ci/ci-tls/certs)
#   AWS_REGIONS         regions for the AWS depth SANs (default us-east-1)
#   AWS_SERVICE_PREFIXES two-label AWS hosts (default api.ecr streams.dynamodb …)
#   RUN_APP_LOCATIONS   locations for *. {location}.run.app (default us-central1)
#   EXTRA_SANS          extra DNS SANs, space separated
#
# Files written: ca.crt (what clients trust), tls.crt + tls.key (nginx serves).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DIR/../../.." && pwd)"
GEN="$REPO_ROOT/deploy/k8s/istio/certs/gen.sh"

[[ -x "$GEN" ]] || { echo "gen.sh: missing $GEN" >&2; exit 2; }

OUT_DIR="${OUT_DIR:-$DIR/certs}"
# `localhost` is added by default so a client that only insists on the https
# scheme (rather than a real cloud host) validates too; pass EXTRA_SANS to add
# more. (IP literals are not covered — use a hostname, or skip verification.)
EXTRA_SANS="localhost ${EXTRA_SANS:-}"
export OUT_DIR EXTRA_SANS

exec "$GEN" --generate-only "$@"
