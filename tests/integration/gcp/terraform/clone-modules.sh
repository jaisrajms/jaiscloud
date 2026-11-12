#!/usr/bin/env bash
#
# clone-modules.sh — the single source of the pinned consumer-module checkouts
# the Terraform gates need.
#
# Both scheduled workflows call this script — `.github/workflows/terraform-nightly.yml`
# (the Terraform and OpenTofu jobs) and `.github/workflows/gcp-provider-drift.yml` —
# so the pins and the fetch strategy cannot drift apart again. The module driver
# (`run-module.sh`) never clones: it answers SKIP (a failure under STRICT) when
# `$HOME/.cache/terraform-modules/<repo>` is missing, and the workflows pre-clone
# into that directory. Each workflow keys its module cache on `hashFiles()` of
# this file, so a pin bump invalidates it.
#
# Why checkout and not `git fetch <sha>`: the pins are short SHAs, and git treats
# an argument to fetch as a ref name — `git fetch --depth 1 origin c7effc0be0f4`
# fails with "fatal: couldn't find remote ref". A full clone carries the history,
# so the abbreviation resolves locally; a checkout restored from a cache that
# holds an older pin may need one fetch first (#732).
#
# Usage: tests/integration/gcp/terraform/clone-modules.sh [cache-dir]
#   cache-dir defaults to $TF_MODULE_CACHE_DIR, then $HOME/.cache/terraform-modules
#   (the directory run-module.sh reads unless TF_MODULE_DIR points elsewhere).
set -euo pipefail

CACHE_DIR="${1:-${TF_MODULE_CACHE_DIR:-$HOME/.cache/terraform-modules}}"
mkdir -p "$CACHE_DIR"

clone() {
  repo="$1"; commit="$2"; url="${3:-https://github.com/terraform-google-modules/$repo}"
  dir="$CACHE_DIR/$repo"
  if [ ! -d "$dir/.git" ]; then
    git clone --quiet "$url" "$dir"
  fi
  # `commit` is a short SHA (the pin run-module.sh documents): resolve it
  # locally against the clone, fetching first when a cached checkout predates it.
  if ! git -C "$dir" checkout --quiet "$commit" 2>/dev/null; then
    git -C "$dir" fetch --quiet --tags origin
    git -C "$dir" checkout --quiet "$commit"
  fi
}

clone terraform-google-cloud-storage c7effc0be0f4
clone terraform-google-pubsub 6cf1860
clone terraform-google-kms ef6e0eda7032
clone terraform-google-firestore 50d170b9ebfc https://github.com/GoogleCloudPlatform/terraform-google-firestore
clone terraform-google-cloud-dns 35cd6de8e647
clone terraform-google-memorystore 5415d5ccca28
clone terraform-google-event-function 9f2e26ae3b73
clone terraform-google-cloud-operations 8c3e37951bb4
clone terraform-google-cloud-run 4ce640a2a519 https://github.com/GoogleCloudPlatform/terraform-google-cloud-run
clone terraform-google-bigquery e11db0dcd597
clone terraform-google-secret-manager 77fca80ccd6a https://github.com/GoogleCloudPlatform/terraform-google-secret-manager
clone terraform-google-cloud-workflows 4c431381b5d8 https://github.com/GoogleCloudPlatform/terraform-google-cloud-workflows
clone terraform-google-cloud-scheduler ece5c3e87412 https://github.com/GoogleCloudPlatform/terraform-google-cloud-scheduler
clone terraform-google-cloud-tasks ba7dbc000bfb https://github.com/GoogleCloudPlatform/terraform-google-cloud-tasks
clone terraform-google-dataproc 56c7c7ed630f https://github.com/GoogleCloudPlatform/terraform-google-dataproc
clone terraform-google-service-accounts 570caafff32d
clone terraform-google-iam 9b9eacfa2b4f
clone terraform-google-sql-db 75b57dc6838e
clone terraform-google-cloud-datastore 1616a282bbf4
clone terraform-google-kubernetes-engine a19597ac18ad
