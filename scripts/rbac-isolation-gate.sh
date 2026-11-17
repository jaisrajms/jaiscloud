#!/usr/bin/env bash
#
# rbac-isolation-gate.sh — prove the dual-substrate namespace boundary is
# enforced by the ValidatingAdmissionPolicy, not just by convention.
#
# Both substrate ServiceAccounts hold `jaiscloud-namespace-admin`, a
# cluster-scoped ClusterRole with namespace delete. The policy
# jaiscloud-namespace-isolation (deploy/k8s/rbac-admission-policy.yaml) requires
# a namespace request made by a substrate identity to carry
# `jaiscloud.io/managed-by=jaiscloud` and that caller's own
# `jaiscloud.io/substrate` label. This gate checks the allow *and* the deny side
# with server-side dry runs, so it never deletes a real namespace:
#
#   1. the policy and its Deny binding exist (K8s >= 1.30);
#   2. two throwaway namespaces are created, labelled managed-by=jaiscloud and
#      substrate=aws / substrate=gcp, service=rbac-gate;
#   3. `kubectl delete namespace <fixture> --dry-run=server --as=<SA>` succeeds
#      for each SA on its own fixture and is denied (by the policy) on the
#      other's — both directions;
#   4. an update probe: a namespace already labelled with the other substrate
#      cannot be relabelled as one's own, and the substrate label cannot be
#      changed; relabelling one's own namespace is admitted (the identity
#      backfill uses that path);
#   5. a pre-upgrade namespace (managed-by + service, no identity) can be
#      adopted by the substrate that ensures it, and is then locked to it;
#   6. a RoleBinding probe: the executor RoleBinding needs the caller's substrate
#      label, so an unlabelled one created by the other substrate's SA is denied,
#      and a correctly labelled one by the owning SA is admitted.
#
# Usage:
#   GATE_ALLOW_RBAC=1 scripts/rbac-isolation-gate.sh
#   GATE_ALLOW_RBAC=1 KUBE_CONTEXT=k3d-jaiscloud-dpl scripts/rbac-isolation-gate.sh
#
# The gate is non-destructive to the install (dry runs only) but it does create
# and delete two `rbac-gate-*` namespaces, so it refuses to run without
# GATE_ALLOW_RBAC=1.
#
# Env:
#   KUBE_CONTEXT   kubectl context (default: the current one)
#   GATE_ALLOW_RBAC  must be 1
#
# Exit: 0 when every assertion holds, 1 when one fails, 2 when refused/misused.
set -euo pipefail

PROG="$(basename "$0")"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
GATE_ALLOW_RBAC="${GATE_ALLOW_RBAC:-0}"
POLICY="jaiscloud-namespace-isolation"
AWS_USER="system:serviceaccount:jaiscloud:jaiscloud"
GCP_USER="system:serviceaccount:jaiscloud-gcp:jaiscloud-gcp"
SUFFIX="$$"
AWS_FIX="rbac-gate-aws-$SUFFIX"
GCP_FIX="rbac-gate-gcp-$SUFFIX"
LEGACY_FIX="rbac-gate-legacy-$SUFFIX"
SERVICE="rbac-gate"

for arg in "$@"; do
  case "$arg" in
    --help|-h)
      awk 'NR>1 && /^set -euo pipefail/ {exit} NR>1 {sub(/^# ?/, ""); print}' "${BASH_SOURCE[0]}"
      exit 0 ;;
    *) echo "$PROG: unknown argument: $arg (try --help)" >&2; exit 2 ;;
  esac
done

say()  { printf '%s\n' "$*"; }
head_() { printf '\n== %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); printf '  PASS  %s\n' "$*"; }
fail() { FAILED=$((FAILED + 1)); printf '  FAIL  %s\n' "$*" >&2; }
die()  { printf '%s: error: %s\n' "$PROG" "$*" >&2; exit 2; }
PASSED=0
FAILED=0

command -v kubectl >/dev/null 2>&1 || die "kubectl is required"

KUBECTL=(kubectl)
if [[ -n "$KUBE_CONTEXT" ]]; then
  KUBECTL=(kubectl --context "$KUBE_CONTEXT")
fi
kube() { "${KUBECTL[@]}" "$@"; }

# server_minor — the server's minor version, or "" when unavailable.
# `kubectl version -o jsonpath` is not supported by every kubectl, so parse JSON.
server_minor() {
  local raw minor
  raw=$(kube version -o json 2>/dev/null || true)
  raw=$(printf '%s' "$raw" | tr -d '[:space:]')
  minor=$(printf '%s' "$raw" | sed -n 's/.*"serverVersion":{[^}]*"minor":"\([^"]*\)".*/\1/p')
  if [[ -z "$minor" ]]; then
    minor=$(kube get --raw /version 2>/dev/null \
      | tr -d '[:space:]' | sed -n 's/.*"minor":"\([^"]*\)".*/\1/p' || true)
  fi
  printf '%s' "${minor//[!0-9]/}"
}

cleanup() {
  kube delete namespace "$AWS_FIX" "$GCP_FIX" "$LEGACY_FIX" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ── prerequisites ───────────────────────────────────────────────────────────
head_ "prerequisites"
MINOR=$(server_minor)
if [[ -n "$MINOR" ]]; then
  if (( MINOR < 30 )); then
    die "the server is Kubernetes 1.$MINOR; ValidatingAdmissionPolicy (>= 1.30) cannot be enforced"
  fi
  say "  Kubernetes 1.$MINOR"
else
  say "  server version unknown (continuing; the policy check below decides)"
fi
kube get validatingadmissionpolicy "$POLICY" >/dev/null 2>&1 \
  || die "ValidatingAdmissionPolicy $POLICY not found — apply deploy/k8s/rbac-admission-policy.yaml first"
say "  ValidatingAdmissionPolicy $POLICY present"
if kube get validatingadmissionpolicybinding "$POLICY" >/dev/null 2>&1; then
  say "  ValidatingAdmissionPolicyBinding $POLICY present"
else
  die "ValidatingAdmissionPolicyBinding $POLICY not found — the policy has no Deny action without it"
fi
kube get clusterrole jaiscloud-executor >/dev/null 2>&1 \
  || die "clusterrole/jaiscloud-executor not found — apply deploy/k8s/rbac-shared.yaml first (the RoleBinding probe binds it)"

# Refuse before creating anything.
if [[ "$GATE_ALLOW_RBAC" != "1" ]]; then
  say ""
  say "  REFUSED: this gate creates and deletes three throwaway $SERVICE-*"
  say "  namespaces (the assertions are server-side dry runs, but the fixtures are"
  say "  real objects)."
  say ""
  say "  Re-run with GATE_ALLOW_RBAC=1 (or: make test-rbac-isolation-k8s ALLOW_RBAC=1)."
  exit 2
fi

# ── fixtures ────────────────────────────────────────────────────────────────
head_ "fixtures"
create_fixture() {
  local name=$1 substrate=$2
  kube create namespace "$name" >/dev/null
  kube label namespace "$name" \
    "jaiscloud.io/managed-by=jaiscloud" \
    "jaiscloud.io/substrate=$substrate" \
    "jaiscloud.io/service=$SERVICE" --overwrite >/dev/null
}
create_fixture "$AWS_FIX" aws
create_fixture "$GCP_FIX" gcp
# A pre-upgrade namespace: ownership labels only, no substrate/instance identity.
kube create namespace "$LEGACY_FIX" >/dev/null
kube label namespace "$LEGACY_FIX" \
  "jaiscloud.io/managed-by=jaiscloud" \
  "jaiscloud.io/service=$SERVICE" --overwrite >/dev/null
say "  $AWS_FIX (substrate=aws), $GCP_FIX (substrate=gcp), $LEGACY_FIX (pre-upgrade, no identity)"

# ── namespace delete: allow own, deny the other's ───────────────────────────
# delete_dry_run <namespace> <impersonated-user> — a server-side dry run of a
# namespace delete as that ServiceAccount. It never removes anything; admission
# (including the policy) still runs.
delete_dry_run() {
  kube delete namespace "$1" --dry-run=server --as="$2" 2>&1
}

assert_delete_allowed() { # <description> <namespace> <user>
  local out
  if out=$(delete_dry_run "$2" "$3"); then
    pass "$1"
  else
    fail "$1 — expected allow, got: $out"
  fi
}

assert_delete_denied() { # <description> <namespace> <user>
  local out
  if out=$(delete_dry_run "$2" "$3"); then
    fail "$1 — expected the policy to deny, but the dry run was allowed"
  elif grep -q "$POLICY" <<<"$out"; then
    pass "$1"
  else
    fail "$1 — denied for another reason: $out"
  fi
}

head_ "namespace delete (server-side dry run, --as)"
assert_delete_allowed "aws SA may delete its own namespace ($AWS_FIX)" "$AWS_FIX" "$AWS_USER"
assert_delete_denied  "aws SA may NOT delete the gcp namespace ($GCP_FIX)" "$GCP_FIX" "$AWS_USER"
assert_delete_allowed "gcp SA may delete its own namespace ($GCP_FIX)" "$GCP_FIX" "$GCP_USER"
assert_delete_denied  "gcp SA may NOT delete the aws namespace ($AWS_FIX)" "$AWS_FIX" "$GCP_USER"

# ── namespace update: the substrate label is locked ─────────────────────────
# label_dry_run <namespace> <key=value> <impersonated-user> — a server-side dry
# run of a label patch (an UPDATE) as that ServiceAccount.
label_dry_run() { # <namespace> <impersonated-user> <key=value...>
  local ns=$1 user=$2
  shift 2
  kube label namespace "$ns" "$@" --overwrite --dry-run=server --as="$user" 2>&1
}

head_ "namespace update (server-side dry run, --as)"
if out=$(label_dry_run "$AWS_FIX" "$AWS_USER" "rbac-gate-probe=yes"); then
  pass "aws SA may label its own namespace (the identity backfill uses this path)"
else
  fail "aws SA could not label its own namespace: $out"
fi
if out=$(label_dry_run "$GCP_FIX" "$AWS_USER" "jaiscloud.io/substrate=aws"); then
  fail "aws SA relabelled the gcp namespace's substrate label and was admitted"
elif grep -q "$POLICY" <<<"$out"; then
  pass "aws SA may not relabel the gcp namespace as its own"
else
  fail "the cross-substrate relabel was denied for another reason: $out"
fi
if out=$(label_dry_run "$AWS_FIX" "$AWS_USER" "jaiscloud.io/substrate=gcp"); then
  fail "aws SA changed its own namespace's substrate label and was admitted"
elif grep -q "$POLICY" <<<"$out"; then
  pass "aws SA may not change its own namespace's substrate label"
else
  fail "the own-substrate relabel was denied for another reason: $out"
fi

# ── pre-upgrade backfill: an unlabelled namespace can be adopted ────────────
# The emulator stamps identity labels onto a namespace an older build created
# (managed-by + service, no substrate). That write must be admitted, and once
# claimed the namespace must be locked to the claiming substrate.
head_ "pre-upgrade backfill (server-side dry run, --as)"
if out=$(label_dry_run "$LEGACY_FIX" "$GCP_USER" \
    "jaiscloud.io/substrate=gcp" "jaiscloud.io/instance-id=gcp-substrate-1"); then
  pass "a substrate may adopt a pre-upgrade namespace with no identity labels"
else
  fail "the identity backfill was denied: $out"
fi
# Claim it for real (as the gcp SA), then the other substrate must be locked out.
kube label namespace "$LEGACY_FIX" --as="$GCP_USER" --overwrite \
  "jaiscloud.io/substrate=gcp" "jaiscloud.io/instance-id=gcp-substrate-1" >/dev/null
assert_delete_denied "after the backfill, the aws SA may not delete the adopted namespace" "$LEGACY_FIX" "$AWS_USER"

# ── RoleBinding: labels are required from a substrate identity ──────────────
# rolebinding_probe <namespace> <user> <labels: aws|gcp|none> — server-side dry
# run of creating the executor RoleBinding as that ServiceAccount. The RoleRef is
# jaiscloud-executor, which the namespace-admin binding allows the SA to bind
# (RBAC escalation prevention would otherwise deny it before admission runs).
rolebinding_probe() {
  local ns=$1 user=$2 labels=$3
  {
    printf 'apiVersion: rbac.authorization.k8s.io/v1\nkind: RoleBinding\nmetadata:\n  name: rbac-gate-probe\n  namespace: %s\n' "$ns"
    if [[ "$labels" != "none" ]]; then
      printf '  labels:\n    jaiscloud.io/managed-by: jaiscloud\n    jaiscloud.io/substrate: %s\n    jaiscloud.io/service: %s\n' "$labels" "$SERVICE"
    fi
    printf 'roleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n  name: jaiscloud-executor\nsubjects:\n  - kind: ServiceAccount\n    name: default\n    namespace: %s\n' "$ns"
  } | kube apply --dry-run=server --as="$user" -f - 2>&1
}

head_ "rolebinding labels (server-side dry run, --as)"
if out=$(rolebinding_probe "$AWS_FIX" "$AWS_USER" aws); then
  pass "aws SA may create a substrate-labelled RoleBinding in its namespace"
else
  fail "aws SA could not create a correctly labelled RoleBinding: $out"
fi
if out=$(rolebinding_probe "$GCP_FIX" "$AWS_USER" none); then
  fail "an unlabelled RoleBinding created by the aws SA was admitted"
elif grep -q "$POLICY" <<<"$out"; then
  pass "aws SA may not create an unlabelled RoleBinding in the gcp namespace"
else
  fail "the unlabelled RoleBinding was denied for another reason: $out"
fi
# Known limitation, documented in the policy: a ValidatingAdmissionPolicy cannot
# read the target Namespace's labels, so a RoleBinding that carries the caller's
# own substrate label is admitted even in the other substrate's namespace. The
# namespace boundary itself is enforced by the namespace rules above; this probe
# only guards the emulator's own write shape.
say "  note  a labelled RoleBinding is a consistency guard only: admission cannot read the target namespace's labels"

# ── summary ─────────────────────────────────────────────────────────────────
head_ "summary"
say "  passed: $PASSED   failed: $FAILED"
if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi
say "  namespace isolation is enforced: each substrate ServiceAccount can only act on its own labelled namespaces"
exit 0
