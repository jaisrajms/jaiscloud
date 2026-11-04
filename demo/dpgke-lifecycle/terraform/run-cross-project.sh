#!/usr/bin/env bash
#
# run-cross-project.sh — the DGD8 gate: the same module, two projects.
#
# Runs run.sh in the cross-project shape: the GKE cluster, the Dataproc virtual
# cluster and the job live in `project`, while the input/output/staging buckets
# belong to `storage_project`. That is the shape customers run when a platform
# team owns the storage account and a workload team owns the cluster, and until
# this gate nothing in the repo pinned it beyond the isolation suite.
#
# Deliberately thin: run.sh is the gate. This wrapper only picks the two project
# ids and a separate Terraform workspace, then delegates, so both shapes assert
# the same GKE/Dataproc/destroy-order facts and only the cross-project block that
# run.sh turns on when the projects differ is extra. The bucket-owning project is
# created by the gate through Cloud Resource Manager (see ensure_project in
# run.sh), because real GCP refuses a bucket in a project that does not exist and
# this module cannot create one.
#
# The workspace keeps this shape's state apart from a single-project run: each
# shape applies, re-plans and destroys independently, and an emptied workspace is
# deleted on the way out.
#
# It never installs tooling; with no terraform on PATH run.sh prints SKIP and
# this wrapper exits 0.
#
# Usage:
#   demo/dpgke-lifecycle/terraform/run-cross-project.sh [ENDPOINT]
#
#     ENDPOINT  base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#
# Env:
#   PROJECT          runner project (cluster + job); default dpgke-demo
#   STORAGE_PROJECT  bucket owner; default ${PROJECT}-storage
#   DPGKE_WORKSPACE  Terraform workspace; default cross-project
#
# Manual equivalent (what the README shows a reader):
#   terraform apply -var project=dpgke-demo -var storage_project=dpgke-demo-storage

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENDPOINT="${1:-http://localhost:8080}"

RUNNER="${PROJECT:-dpgke-demo}"
OWNER="${STORAGE_PROJECT:-${RUNNER}-storage}"
if [[ "$RUNNER" == "$OWNER" ]]; then
  echo "ERROR: the runner and bucket-owner projects must differ (both are $RUNNER);"
  echo "       set STORAGE_PROJECT to the project that should own the buckets."
  exit 2
fi

echo "== cross-project gate: cluster and job in $RUNNER, buckets owned by $OWNER"
echo "   (the same gate as run.sh, run with -var project=$RUNNER -var storage_project=$OWNER)"

PROJECT="$RUNNER" \
STORAGE_PROJECT="$OWNER" \
DPGKE_WORKSPACE="${DPGKE_WORKSPACE:-cross-project}" \
  "$DIR/run.sh" "$ENDPOINT"
