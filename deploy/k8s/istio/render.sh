#!/usr/bin/env bash
#
# render.sh — render (or apply) the JaisCloud mesh manifests for a namespace
# layout that is not the shipped default.
#
# virtualservices.yaml and destinationrule.yaml address each substrate with a
# fully-qualified Service name, because a VirtualService in istio-system cannot
# resolve a short name against a substrate namespace:
#
#   jaiscloud.<NS_AWS>.svc.cluster.local:4566          (*.amazonaws.com)
#   jaiscloud-gcp.<NS_GCP>.svc.cluster.local:8080/8081 (*.googleapis.com,
#                                                       *.run.app,
#                                                       *.cloudfunctions.net)
#
# A custom install (NS_AWS / NS_GCP, the same variables deploy/deploy.sh reads)
# therefore needs those FQDNs rewritten. This script substitutes them and either
# prints the result or applies it; with no variables set it renders the shipped
# names, so the checked-in files and this script agree on the default layout.
#
# Usage:
#   deploy/k8s/istio/render.sh --plan     # print the rendered manifests (default)
#   deploy/k8s/istio/render.sh --apply    # kubectl apply -f - the rendered manifests
#   deploy/k8s/istio/render.sh --help
#
# Env:
#   NS_AWS          AWS substrate namespace (default jaiscloud)
#   NS_GCP          GCP substrate namespace (default jaiscloud-gcp)
#   KUBE_CONTEXT    kubectl --context value for --apply (default: current context)
#
# The gateway and its certificate are namespace-independent (the gateway lives in
# istio-system and routes by host), so they are not rendered here. The CA
# ConfigMap copies are placed by deploy/k8s/istio/certs/gen.sh, which follows the
# same NS_AWS/NS_GCP variables.
set -euo pipefail

PROG="$(basename "$0")"
HERE="$(cd "$(dirname "$0")" && pwd)"

NS_AWS="${NS_AWS:-jaiscloud}"
NS_GCP="${NS_GCP:-jaiscloud-gcp}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

MODE=plan
for arg in "$@"; do
  case "$arg" in
    --plan)  MODE=plan ;;
    --apply) MODE=apply ;;
    --help|-h)
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0 ;;
    *) echo "$PROG: unknown argument: $arg (try --help)" >&2; exit 2 ;;
  esac
done

die() { printf '%s: error: %s\n' "$PROG" "$*" >&2; exit 1; }

validate_label() { # validate_label <name> <value>
  [[ "$2" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
    || die "$1 must be a lowercase DNS-1123 label; got '$2'"
}
validate_label NS_AWS "$NS_AWS"
validate_label NS_GCP "$NS_GCP"

[ -r "$HERE/virtualservices.yaml" ] || die "missing $HERE/virtualservices.yaml"
[ -r "$HERE/destinationrule.yaml" ] || die "missing $HERE/destinationrule.yaml"

render() {
  sed -e "s/jaiscloud-gcp\.jaiscloud-gcp\.svc\.cluster\.local/jaiscloud-gcp.$NS_GCP.svc.cluster.local/g" \
      -e "s/jaiscloud\.jaiscloud\.svc\.cluster\.local/jaiscloud.$NS_AWS.svc.cluster.local/g" \
      "$HERE/virtualservices.yaml" "$HERE/destinationrule.yaml"
}

if [[ "$MODE" == "plan" ]]; then
  echo "# rendered for NS_AWS=$NS_AWS NS_GCP=$NS_GCP (defaults: jaiscloud / jaiscloud-gcp)" >&2
  echo "# apply with: NS_AWS=$NS_AWS NS_GCP=$NS_GCP $0 --apply" >&2
  render
  exit 0
fi

command -v kubectl >/dev/null 2>&1 || die "kubectl is required for --apply"
if [[ -n "$KUBE_CONTEXT" ]]; then
  render | kubectl --context "$KUBE_CONTEXT" apply -f -
else
  render | kubectl apply -f -
fi
