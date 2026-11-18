#!/usr/bin/env bash
#
# custom-names-render-test.sh — offline proof that deploy.sh installs a custom
# namespace/ServiceAccount layout without hand-editing a manifest, and that the
# defaults still render to exactly the shipped names.
#
# What it does (no cluster, no kubectl server call, no apply):
#   1. `deploy/deploy.sh --render-only DIR` with NS_AWS/NS_GCP/NS_DATA/SA_AWS/SA_GCP
#      set to custom names;
#   2. `kubectl kustomize` each rendered overlay (data, data-ns, aws, gcp,
#      isolation) and assert the resolved objects: Namespace names, every
#      Deployment's metadata.namespace and serviceAccountName, ServiceAccount
#      names, RoleBinding/ClusterRoleBinding subjects, the DSN hostnames, the
#      jaiscoud K8S identity env values (AWS ConfigMap, GCP Deployment env, the
#      GCP Service FQDNs) and both ServiceAccount usernames in the
#      ValidatingAdmissionPolicy (matchConditions and the substrate variable);
#   3. the same render with the NS_*/SA_* variables unset, asserting the shipped
#      defaults (jaiscloud / jaiscloud-gcp / jaiscloud-data, SA jaiscloud /
#      jaiscloud-gcp), so a change that "fixes" custom names by editing the base
#      fails here.
#
# Usage:
#   scripts/custom-names-render-test.sh      # exit 0 when every assertion holds
#
# Env:
#   STRICT=1   treat a missing kubectl/kustomize as a failure instead of a SKIP
#
# The render step itself needs no cluster; KUBECONFIG is pointed at /dev/null so
# an accidental `kubectl apply` would fail rather than touch a real cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEPLOY="$REPO_ROOT/deploy/deploy.sh"
STRICT="${STRICT:-0}"

CUSTOM_NS_AWS=team-aws
CUSTOM_NS_GCP=team-gcp
CUSTOM_NS_DATA=team-data
CUSTOM_SA_AWS=team-sa-aws
CUSTOM_SA_GCP=team-sa-gcp

PASSED=0
FAILED=0
pass() { PASSED=$((PASSED + 1)); printf '  PASS  %s\n' "$*"; }
fail() { FAILED=$((FAILED + 1)); printf '  FAIL  %s\n' "$*" >&2; }
head_() { printf '\n== %s\n' "$*"; }

# ── prerequisites ───────────────────────────────────────────────────────────
command -v bash >/dev/null 2>&1 || { echo "bash is required" >&2; exit 2; }
[[ -x "$DEPLOY" ]] || { echo "ERROR: $DEPLOY is not executable" >&2; exit 2; }

if command -v kubectl >/dev/null 2>&1; then
  KUSTOMIZE=(kubectl kustomize)
elif command -v kustomize >/dev/null 2>&1; then
  KUSTOMIZE=(kustomize build)
else
  if [[ "$STRICT" == "1" ]]; then
    echo "ERROR: neither kubectl (with kustomize) nor kustomize is on PATH" >&2
    exit 1
  fi
  echo "SKIP: neither kubectl (with kustomize) nor kustomize is on PATH — the render test needs one to expand the overlays (STRICT=1 fails instead)"
  exit 0
fi

# Proves the render path never needs (or reaches) a cluster: any real kubectl
# call that needs a server fails against this kubeconfig.
export KUBECONFIG=/dev/null

TMP="$(mktemp -d "${TMPDIR:-/tmp}/custom-names-render.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# doc <file> <kind> <name> — print one kustomize document (kustomize separates
# documents by starting a new apiVersion line, not by `---`).
doc() {
  awk -v want_kind="$2" -v want_name="$3" '
    function flush() {
      if (buf != "" && kind == want_kind && name == want_name) printf "%s", buf
      buf = ""; kind = ""; name = ""
    }
    /^apiVersion:/ { flush() }
    { buf = buf $0 "\n" }
    /^kind: / { kind = $2 }
    /^  name: / && name == "" { name = $2 }
    END { flush() }
  ' "$1"
}

# assert_doc <description> <file> <kind> <name> <literal> — the document exists
# and contains the literal.
assert_doc() {
  local desc=$1 file=$2 kind=$3 name=$4 want=$5 body
  body=$(doc "$file" "$kind" "$name")
  if [[ -z "$body" ]]; then
    fail "$desc — no $kind/$name in $(basename "$file")"
  elif ! grep -qF -- "$want" <<<"$body"; then
    fail "$desc — $kind/$name has no '$want'"
  else
    pass "$desc"
  fi
}

# assert_absent <description> <file> <literal> — the literal appears nowhere.
assert_absent() {
  local desc=$1 file=$2 want=$3
  if grep -qF -- "$want" "$file"; then
    fail "$desc — unexpected '$want' in $(basename "$file")"
  else
    pass "$desc"
  fi
}

# assert_doc_line_absent <description> <file> <kind> <name> <line> — the document
# has no line equal to <line> (an exact match, so `name: jaiscloud` does not
# match `name: jaiscloud-namespace-admin-aws`).
assert_doc_line_absent() {
  local desc=$1 file=$2 kind=$3 name=$4 line=$5 body
  body=$(doc "$file" "$kind" "$name")
  if [[ -z "$body" ]]; then
    fail "$desc — no $kind/$name in $(basename "$file")"
  elif grep -qxF -- "$line" <<<"$body"; then
    fail "$desc — $kind/$name still carries '$line'"
  else
    pass "$desc"
  fi
}

# build <dir> <overlay> — expand one rendered overlay into <dir>/<overlay>.yaml.
build() {
  local dir=$1 overlay=$2
  "${KUSTOMIZE[@]}" "$dir/overlays/$overlay" > "$dir/$overlay.yaml"
}

# ── custom names ────────────────────────────────────────────────────────────
head_ "render: custom names"
CUSTOM="$TMP/custom"
NS_AWS=$CUSTOM_NS_AWS NS_GCP=$CUSTOM_NS_GCP NS_DATA=$CUSTOM_NS_DATA \
  SA_AWS=$CUSTOM_SA_AWS SA_GCP=$CUSTOM_SA_GCP \
  "$DEPLOY" --render-only "$CUSTOM" > "$TMP/custom-render.log" 2>&1 \
  || { cat "$TMP/custom-render.log" >&2; echo "ERROR: --render-only failed" >&2; exit 1; }
pass "--render-only wrote the overlay tree without a cluster"
for overlay in data data-ns aws gcp isolation; do
  build "$CUSTOM" "$overlay"
  pass "kustomize expands overlays/$overlay"
done
grep -q 'AWS   namespace team-aws   ServiceAccount team-sa-aws' "$TMP/custom-render.log" \
  && pass "--render-only reports the resolved AWS names" \
  || fail "--render-only did not report AWS=team-aws/team-sa-aws"
grep -q 'GCP   namespace team-gcp   ServiceAccount team-sa-gcp' "$TMP/custom-render.log" \
  && pass "--render-only reports the resolved GCP names" \
  || fail "--render-only did not report GCP=team-gcp/team-sa-gcp"

head_ "store overlay (data)"
assert_doc "store Namespace is team-data" "$CUSTOM/data.yaml" Namespace team-data "name: team-data"
assert_absent "store overlay has no default store namespace" "$CUSTOM/data.yaml" "namespace: jaiscloud-data"
assert_doc "postgres Secret DSN names postgres.team-data" "$CUSTOM/data.yaml" Secret postgres-secret \
  "postgres.team-data.svc.cluster.local:5432/jaiscloud"
assert_doc "postgres Deployment is in team-data" "$CUSTOM/data.yaml" Deployment postgres "namespace: team-data"
assert_doc "postgres Service is in team-data" "$CUSTOM/data.yaml" Service postgres "namespace: team-data"

head_ "AWS substrate overlay"
assert_doc "AWS Namespace is team-aws" "$CUSTOM/aws.yaml" Namespace team-aws "name: team-aws"
assert_doc "AWS ServiceAccount is team-sa-aws" "$CUSTOM/aws.yaml" ServiceAccount team-sa-aws \
  "namespace: team-aws"
assert_doc "AWS Deployment is in team-aws" "$CUSTOM/aws.yaml" Deployment jaiscloud "namespace: team-aws"
assert_doc "AWS Deployment runs as team-sa-aws" "$CUSTOM/aws.yaml" Deployment jaiscloud \
  "serviceAccountName: team-sa-aws"
assert_doc "AWS RoleBinding subject is the custom identity" "$CUSTOM/aws.yaml" RoleBinding jaiscloud-executor \
  "namespace: team-aws"
assert_doc "AWS ClusterRoleBinding subject is the custom identity" "$CUSTOM/aws.yaml" \
  ClusterRoleBinding jaiscloud-namespace-admin-aws "name: team-sa-aws"
assert_doc "jaiscloud-config names the custom namespace" "$CUSTOM/aws.yaml" ConfigMap jaiscloud-config \
  "JAISCLOUD_K8S_NAMESPACE: team-aws"
assert_doc "jaiscloud-config names the custom ServiceAccount" "$CUSTOM/aws.yaml" ConfigMap jaiscloud-config \
  "JAISCLOUD_K8S_SA: team-sa-aws"
assert_doc "AWS DSN Secret names postgres.team-data" "$CUSTOM/aws.yaml" Secret jaiscloud-dsn \
  "postgres.team-data.svc.cluster.local:5432/jaiscloud"
assert_absent "AWS overlay has no default namespace field" "$CUSTOM/aws.yaml" "namespace: jaiscloud"
assert_doc_line_absent "AWS ClusterRoleBinding subject is not the default ServiceAccount" \
  "$CUSTOM/aws.yaml" ClusterRoleBinding jaiscloud-namespace-admin-aws "  name: jaiscloud"
assert_absent "AWS overlay has no default DSN host" "$CUSTOM/aws.yaml" "postgres.jaiscloud-data.svc.cluster.local"

head_ "GCP substrate overlay"
assert_doc "GCP Namespace is team-gcp" "$CUSTOM/gcp.yaml" Namespace team-gcp "name: team-gcp"
assert_doc "GCP ServiceAccount is team-sa-gcp" "$CUSTOM/gcp.yaml" ServiceAccount team-sa-gcp \
  "namespace: team-gcp"
assert_doc "GCP Deployment is in team-gcp" "$CUSTOM/gcp.yaml" Deployment jaiscloud-gcp "namespace: team-gcp"
assert_doc "GCP Deployment runs as team-sa-gcp" "$CUSTOM/gcp.yaml" Deployment jaiscloud-gcp \
  "serviceAccountName: team-sa-gcp"
assert_doc "GCP Deployment names the custom namespace" "$CUSTOM/gcp.yaml" Deployment jaiscloud-gcp \
  "value: team-gcp"
assert_doc "GCP Deployment names the custom ServiceAccount" "$CUSTOM/gcp.yaml" Deployment jaiscloud-gcp \
  "value: team-sa-gcp"
assert_doc "GCP GCS endpoint names the custom namespace" "$CUSTOM/gcp.yaml" Deployment jaiscloud-gcp \
  "http://jaiscloud-gcp.team-gcp.svc.cluster.local:8080"
assert_doc "GCP HMS endpoint names the custom namespace" "$CUSTOM/gcp.yaml" Deployment jaiscloud-gcp \
  "jaiscloud-gcp.team-gcp.svc.cluster.local:9083"
assert_doc "GCP RoleBinding subject is the custom identity" "$CUSTOM/gcp.yaml" RoleBinding jaiscloud-executor \
  "namespace: team-gcp"
assert_doc "GCP ClusterRoleBinding subject is the custom identity" "$CUSTOM/gcp.yaml" \
  ClusterRoleBinding jaiscloud-namespace-admin-gcp "name: team-sa-gcp"
assert_doc "GCP DSN Secret names postgres.team-data" "$CUSTOM/gcp.yaml" Secret jaiscloud-gcp-dsn \
  "postgres.team-data.svc.cluster.local:5432/jaiscloud_gcp"
assert_absent "GCP overlay has no default namespace field" "$CUSTOM/gcp.yaml" "namespace: jaiscloud-gcp"
assert_absent "GCP overlay has no default service FQDN" "$CUSTOM/gcp.yaml" \
  "jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local"
assert_absent "GCP overlay has no default DSN host" "$CUSTOM/gcp.yaml" "postgres.jaiscloud-data.svc.cluster.local"

head_ "isolation overlay (ValidatingAdmissionPolicy)"
assert_doc "policy matches the custom AWS identity" "$CUSTOM/isolation.yaml" ValidatingAdmissionPolicy \
  jaiscloud-namespace-isolation "system:serviceaccount:team-aws:team-sa-aws"
assert_doc "policy matches the custom GCP identity" "$CUSTOM/isolation.yaml" ValidatingAdmissionPolicy \
  jaiscloud-namespace-isolation "system:serviceaccount:team-gcp:team-sa-gcp"
assert_doc "policy derives the substrate from the custom AWS identity" "$CUSTOM/isolation.yaml" \
  ValidatingAdmissionPolicy jaiscloud-namespace-isolation \
  "request.userInfo.username == 'system:serviceaccount:team-aws:team-sa-aws'"
assert_absent "policy no longer names the default identities" "$CUSTOM/isolation.yaml" \
  "system:serviceaccount:jaiscloud:jaiscloud"
assert_doc "the Deny binding is rendered" "$CUSTOM/isolation.yaml" ValidatingAdmissionPolicyBinding \
  jaiscloud-namespace-isolation "validationActions:"

# A custom render must not leak into the repository: the base stays kustomize-free.
if find "$REPO_ROOT/deploy" -name kustomization.yaml -print -quit | grep -q .; then
  fail "a kustomization.yaml was written under deploy/ (the base must stay plain manifests)"
else
  pass "no kustomization.yaml was written into the repository"
fi

# ── defaults ────────────────────────────────────────────────────────────────
head_ "render: default names (NS_*/SA_* unset)"
DEFAULT="$TMP/default"
env -u NS_AWS -u NS_GCP -u NS_DATA -u SA_AWS -u SA_GCP \
  "$DEPLOY" --render-only "$DEFAULT" > "$TMP/default-render.log" 2>&1 \
  || { cat "$TMP/default-render.log" >&2; echo "ERROR: --render-only failed" >&2; exit 1; }
for overlay in data data-ns aws gcp isolation; do
  build "$DEFAULT" "$overlay"
done

assert_doc "default store Namespace is jaiscloud-data" "$DEFAULT/data.yaml" Namespace jaiscloud-data \
  "name: jaiscloud-data"
assert_doc "default store DSN host is postgres.jaiscloud-data" "$DEFAULT/data.yaml" Secret postgres-secret \
  "postgres.jaiscloud-data.svc.cluster.local:5432/jaiscloud"
assert_doc "default AWS Namespace is jaiscloud" "$DEFAULT/aws.yaml" Namespace jaiscloud "name: jaiscloud"
assert_doc "default AWS ServiceAccount is jaiscloud" "$DEFAULT/aws.yaml" ServiceAccount jaiscloud \
  "namespace: jaiscloud"
assert_doc "default AWS Deployment runs as jaiscloud" "$DEFAULT/aws.yaml" Deployment jaiscloud \
  "serviceAccountName: jaiscloud"
assert_doc "default jaiscloud-config keeps jaiscloud" "$DEFAULT/aws.yaml" ConfigMap jaiscloud-config \
  "JAISCLOUD_K8S_NAMESPACE: jaiscloud"
assert_doc "default jaiscloud-config keeps the jaiscloud SA" "$DEFAULT/aws.yaml" ConfigMap jaiscloud-config \
  "JAISCLOUD_K8S_SA: jaiscloud"
assert_doc "default AWS DSN host is postgres.jaiscloud-data" "$DEFAULT/aws.yaml" Secret jaiscloud-dsn \
  "postgres.jaiscloud-data.svc.cluster.local:5432/jaiscloud"
assert_doc "default GCP Namespace is jaiscloud-gcp" "$DEFAULT/gcp.yaml" Namespace jaiscloud-gcp \
  "name: jaiscloud-gcp"
assert_doc "default GCP ServiceAccount is jaiscloud-gcp" "$DEFAULT/gcp.yaml" ServiceAccount jaiscloud-gcp \
  "namespace: jaiscloud-gcp"
assert_doc "default GCP Deployment runs as jaiscloud-gcp" "$DEFAULT/gcp.yaml" Deployment jaiscloud-gcp \
  "serviceAccountName: jaiscloud-gcp"
assert_doc "default GCP service FQDN is unchanged" "$DEFAULT/gcp.yaml" Deployment jaiscloud-gcp \
  "http://jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8080"
assert_doc "default GCP DSN host is postgres.jaiscloud-data" "$DEFAULT/gcp.yaml" Secret jaiscloud-gcp-dsn \
  "postgres.jaiscloud-data.svc.cluster.local:5432/jaiscloud_gcp"
assert_doc "default policy matches system:serviceaccount:jaiscloud:jaiscloud" "$DEFAULT/isolation.yaml" \
  ValidatingAdmissionPolicy jaiscloud-namespace-isolation \
  "system:serviceaccount:jaiscloud:jaiscloud"
assert_doc "default policy matches system:serviceaccount:jaiscloud-gcp:jaiscloud-gcp" \
  "$DEFAULT/isolation.yaml" ValidatingAdmissionPolicy jaiscloud-namespace-isolation \
  "system:serviceaccount:jaiscloud-gcp:jaiscloud-gcp"

# ── summary ─────────────────────────────────────────────────────────────────
head_ "summary"
printf '  passed: %d   failed: %d\n' "$PASSED" "$FAILED"
if ((FAILED != 0)); then
  exit 1
fi
printf '  the rendered overlays install the custom names, and the defaults still render to the shipped layout\n'
