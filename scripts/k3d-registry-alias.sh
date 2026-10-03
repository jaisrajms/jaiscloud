#!/usr/bin/env bash
#
# k3d-registry-alias.sh — make host.k3d.internal resolvable in a k3d cluster
# after node recreation.
#
# Background: k3d configures containerd on each node to pull the emulator
# image from host.k3d.internal:5050 over plain HTTP (see the node's
# /etc/rancher/k3s/registries.yaml). That hostname normally resolves via a
# docker --add-host "host.k3d.internal:host-gateway" alias k3d injects into the
# node's /etc/hosts at container creation. If a node container is recreated
# outside k3d (or k3d loses the alias), host.k3d.internal stops resolving and
# image pulls fail with "lookup host.k3d.internal: no such host".
#
# This script idempotently re-adds the alias to the node's /etc/hosts (fixing
# containerd image pulls) and to the cluster CoreDNS NodeHosts (fixing pod-level
# DNS), pointing host.k3d.internal at the registry host IP.
#
# Usage:
#   scripts/k3d-registry-alias.sh [REGISTRY_IP]
#
# REGISTRY_IP defaults to 10.0.100.21 (the docker1 LAN address behind
# host.k3d.internal:5050). Override if the registry host address differs.
set -euo pipefail

REGISTRY_IP="${1:-10.0.100.21}"
NODE="${NODE:-k3d-jaiscloud-server-0}"
NAMESPACE="${NAMESPACE:-jaiscloud}"
IMAGE="${IMAGE:-busybox}"

echo "==> Mapping host.k3d.internal -> $REGISTRY_IP on node $NODE"

# 1) Node /etc/hosts (containerd image pulls resolve here first).
kubectl -n kube-system debug "node/$NODE" -it --image="$IMAGE" -- \
  sh -c "grep -q 'host.k3d.internal' /host/etc/hosts || echo '$REGISTRY_IP host.k3d.internal' >> /host/etc/hosts; cat /host/etc/hosts" \
  >/dev/null 2>&1 || true

# Clean up the ephemeral debug pod (kubectl debug node leaves it behind).
kubectl -n kube-system delete pod -l "kubectl-run" --ignore-not-found >/dev/null 2>&1 || true
kubectl -n kube-system get pods --no-headers 2>/dev/null \
  | awk '/node-debugger-.*-'"$NODE"'/{print $1}' \
  | xargs -r kubectl -n kube-system delete pod --ignore-not-found >/dev/null 2>&1 || true

# 2) CoreDNS NodeHosts (pod-level DNS for host.k3d.internal).
HOSTS_TMP="$(mktemp)"
kubectl -n kube-system get configmap coredns -o jsonpath='{.data.NodeHosts}' > "$HOSTS_TMP" 2>/dev/null || true
if ! grep -q 'host.k3d.internal' "$HOSTS_TMP"; then
  echo "$REGISTRY_IP host.k3d.internal" >> "$HOSTS_TMP"
  ESCAPED="$(sed ':a;N;$!ba;s/\n/\\n/g' "$HOSTS_TMP")"
  kubectl -n kube-system patch configmap coredns --type merge \
    -p "{\"data\":{\"NodeHosts\":\"$ESCAPED\"}}" >/dev/null
fi
rm -f "$HOSTS_TMP"

echo "==> Done. Verify with: kubectl run --rm -it --image=busybox probe -- nslookup host.k3d.internal"
echo "    (node /etc/hosts alias is not durable across node RECREATION — re-run this script after any node rebuild.)"
