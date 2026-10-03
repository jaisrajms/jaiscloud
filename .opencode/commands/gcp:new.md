---
description: Start the next GCP backlog item from the status ledger
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; make gcp-status-next N=10 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2`

You are starting a new GCP work session in the jaiscloud repo. The ledger output
above is the source of truth for what to do next.

- If arguments were given (`$ARGUMENTS`), assess them first with
  `make gcp-status-check Q="$ARGUMENTS"` (exit 2 = already done/merged → stop and
  say so; 3 = in flight → coordinate with the existing branch/PR; 0 = proceed).
- Otherwise take the first `NEXT` item above. Ignore the `ATTENTION` list unless
  it directly blocks the item.
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
