#!/usr/bin/env bash
#
# gcp-session-start.sh — prepare a clean GCP work session. Called by
# `make gcp-session-start`, and therefore by the `/gcp/new` command.
#
#   1. Refuse to start if the current checkout has uncommitted changes. A
#      previous or concurrent session's work must be committed or stashed first.
#      The guard runs before any fetch/checkout, so refs are never touched over
#      dirty work.
#   2. Fast-forward the local base branch from `upstream/<base>` (the canonical
#      remote; `origin` is a fork and its branches are stale). `--ff-only` means
#      a diverged base fails loudly instead of resetting local commits.
#      The base defaults to `main`; set GCP_SESSION_BASE=<branch> to work off an
#      integration branch instead (e.g. `gke` for the GKE series). When the base
#      does not exist yet it is created off `upstream/main` and pushed to origin
#      and upstream, so PRs can target it. The old `gcp` branch is frozen
#      (merged into main via #610) and is refused.
#   3. Leave the checkout on the base branch; the session branches the item's
#      Branch off it.
#
# Exits non-zero on a dirty checkout or a failed fetch/fast-forward so the caller
# can abort the session.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
  echo "gcp-session-start: refusing to start — the current checkout has uncommitted changes." >&2
  echo "                   Commit or stash them (a previous or concurrent session's work) first." >&2
  echo >&2
  git status --short >&2
  exit 1
fi

echo "gcp-session-start: fetching upstream..."
git fetch upstream --prune

BASE="${GCP_SESSION_BASE:-main}"

if [ "$BASE" = "gcp" ]; then
  echo "gcp-session-start: 'gcp' is frozen (merged into main via #610) — pick another base." >&2
  exit 1
fi

if git show-ref --verify --quiet "refs/heads/$BASE"; then
  git checkout "$BASE"
  if git show-ref --verify --quiet "refs/remotes/upstream/$BASE"; then
    git merge --ff-only "upstream/$BASE"
  else
    echo "gcp-session-start: '$BASE' exists only locally; using it as-is"
  fi
elif git show-ref --verify --quiet "refs/remotes/upstream/$BASE"; then
  echo "gcp-session-start: creating local '$BASE' tracking upstream/$BASE"
  git checkout -b "$BASE" --track "upstream/$BASE"
else
  # An integration branch for a series: start it at the trunk so the series'
  # PRs have a shared base. It must exist on the upstream repo too, or a PR
  # cannot target it, so push there as well (a fork-only branch is not enough).
  echo "gcp-session-start: creating integration branch '$BASE' off upstream/main..."
  git checkout -b "$BASE" upstream/main
  git push -u origin "$BASE"
  if ! git push upstream "$BASE"; then
    echo "gcp-session-start: WARNING: could not push '$BASE' to upstream." >&2
    echo "                   A PR cannot target '$BASE' until it exists there." >&2
  fi
fi

echo "gcp-session-start: base $BASE at $(git rev-parse --short HEAD) — $(git log --oneline -1 --format=%s)"
