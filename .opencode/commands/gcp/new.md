---
description: Start the next GCP backlog item from the status ledger
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; set -f; args='$ARGUMENTS'; base=main; q="$args"; case "$args" in base=*) rest="${args#base=}"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; --base\ *) rest="${args#--base }"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; esac; set +f; echo "session base: $base"; if [ -n "$q" ]; then echo "item query: $q"; fi; echo; GCP_SESSION_BASE="$base" make gcp-session-start 2>&1 && { echo; make gcp-status-next N=10 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2; }`

You are starting a new GCP work session in the jaiscloud repo. The ledger output
above is the source of truth for what to do next.

The command first ran `make gcp-session-start`: it aborts on a dirty checkout
(uncommitted files from a previous or concurrent session) and otherwise fetches
`upstream` and fast-forwards the session base. If it aborted, no ledger is
shown — stop and report the uncommitted files; do not start an item. When it
proceeds, branch the item's `Branch` off the refreshed base.

**Base branch.** The base is printed as `session base:` above and defaults to
`main`. To work off a series integration branch, say so explicitly and keep the
rest of the argument as the item query:

- `base=<branch>` — e.g. `/gcp/new base=gke datastore timestamps`
- `--base <branch>` — e.g. `/gcp/new --base gke bigquery load`

There is deliberately no bare-word form: `/gcp/new datastore` is an item query,
never a branch. A base that does not exist yet is created off `upstream/main`
by session-start and pushed to origin and upstream (a PR cannot target a branch
that only exists in the fork), so that series' PRs must then be opened against
it:

```
gh pr create --repo jaisrajms/jaiscloud --base <session base> --head <fork>:<item branch>
```

Say so when a branch was created, because the series' PRs target it instead of
`main`.

- If an item query was given (`$ARGUMENTS` minus a `base=` prefix), assess it
  with `make gcp-status-check Q="<the item query>"`. Otherwise take the first
  `NEXT` item above (ignore the `ATTENTION` list unless it directly blocks the
  item) and assess **that** item before branching:
  `make gcp-status-check SERVICE=<service> Q="<a few distinctive words from its gap>"`.
  Use the item's keywords, not just its ID: a differently-IDed row in another
  plan family can already have closed the same gap. Verdicts: exit 2 = already
  done / merged / no-fix → stop and say so; 3 = in flight → coordinate with the
  existing branch/PR; 0 = proceed.
- **Announce the item first, briefly.** Before creating the branch, print a
  concise header — a handful of lines, no preamble and no restating the ledger:
  `## <ID> — <service>: <feature>`, then one or two sentences of context (what
  the gap is and why it matters — real GCP vs the emulator). Then implement.
- Read the item's `plan doc` and its `source` section from the ledger. **If the item
  has no plan doc yet, create it first with `/gcp/plan <service>`** (or
  `make gcp-plan-new SERVICE=<service>`) and follow the `gcp-phase-workflow` skill §0 —
  do not implement an unscheduled item. Then **load the `gcp-phase-workflow` skill**
  (use the skill tool with id `gcp-phase-workflow`; if it is not registered, read
  `~/.config/opencode/skills/gcp-phase-workflow/SKILL.md` and
  `parity-fix-workflow.md` beside it) and follow it, together with `AGENTS.md` /
  `CLAUDE.md` (transport-neutral core owns logic, `gcperr` errors, REST + gRPC
  parity, serialized shared hotspots, never import `internal/aws`).
- Close out — the session is **not** finished when the PR is opened, only when its
  CI is green:
  1. `make gcp-status`, `make gcp-status-lint-plans` and
     `make gcp-status-coverage`; finalize the plan doc with
     `make gcp-status-finalize PLAN=plan_docs/<doc>.md ID=<ID>` (put the backlog
     ID in the commit/PR title). If the session changed operations, also run
     `make gcp-matrix-diff REF=upstream/main`.
  2. `make gcp-ci-preflight` before pushing: it mirrors the offline gates of CI's
     `test-gcp-unit` / `test-gcp-conformance` jobs (race unit tests, statement-
     coverage ratchet, behavioral + depth gates, `tools/` tests, `gcplint`, and
     the fidelity matrix). **A stale `docs/fidelity` is the most common red-CI
     cause in this repo** — any change to a conformance check or to a service's
     evidence makes the committed matrix stale, so run
     `make gen-gcp-fidelity-matrix`, commit `docs/fidelity`, and re-run.
  3. Push, open the PR against the session base (see above), then
     `make gcp-ci-wait PR=<number>`. It wraps `gh pr checks --watch --fail-fast`
     and, on a red check, prints the failing jobs plus their `--log-failed` tail
     (with the runner's cache-restore tar noise filtered out).
  4. On a failure: diagnose, fix, commit, push, and run `make gcp-ci-wait
     PR=<number>` again — repeat until every check passes. Only a failure proven
     to be runner infrastructure (cancelled run, cache-restore noise) may be
     dismissed, and then only once `gh run rerun <run-id>` comes back green.
     Never report the session as done with a red or still-pending check, and
     never leave the next session a knowingly red PR.
