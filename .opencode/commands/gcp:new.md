---
description: Start the next GCP backlog item from the status ledger
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; make gcp-session-start 2>&1 && { echo; make gcp-status-next N=10 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2; }`

You are starting a new GCP work session in the jaiscloud repo. The ledger output
above is the source of truth for what to do next.

The command first ran `make gcp-session-start`: it aborts on a dirty checkout
(uncommitted files from a previous or concurrent session) and otherwise fetches
`upstream` and fast-forwards the local `gcp` base. If it aborted, no ledger is
shown — stop and report the uncommitted files; do not start an item. When it
proceeds, branch the item's `Branch` off the refreshed `gcp`.

- If arguments were given (`$ARGUMENTS`), assess them with
  `make gcp-status-check Q="$ARGUMENTS"`. Otherwise take the first `NEXT` item
  above (ignore the `ATTENTION` list unless it directly blocks the item) and
  assess **that** item before branching:
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
  has no plan doc yet, create it first with `/gcp:plan <service>`** (or
  `make gcp-plan-new SERVICE=<service>`) and follow the `gcp-phase-workflow` skill §0 —
  do not implement an unscheduled item. Then **load the `gcp-phase-workflow` skill**
  (use the skill tool with id `gcp-phase-workflow`; if it is not registered, read
  `~/.config/opencode/skills/gcp-phase-workflow/SKILL.md` and
  `parity-fix-workflow.md` beside it) and follow it, together with `AGENTS.md` /
  `CLAUDE.md` (transport-neutral core owns logic, `gcperr` errors, REST + gRPC
  parity, serialized shared hotspots, never import `internal/aws`).
- Close out: run `make gcp-status`, `make gcp-status-lint-plans` and
  `make gcp-status-coverage`; finalize the plan doc with
  `make gcp-status-finalize PLAN=plan_docs/<doc>.md ID=<ID>` (put the backlog ID in the
  commit/PR title). If the session changed operations, also run
  `make gcp-matrix-diff REF=upstream/gcp`.
