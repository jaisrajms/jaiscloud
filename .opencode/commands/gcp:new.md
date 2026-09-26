---
description: Start the next GCP backlog item from the status ledger
---

!`make gcp-status-next N=10 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2`

You are starting a new GCP work session in the jaiscloud repo. The ledger output
above is the source of truth for what to do next.

- If arguments were given (`$ARGUMENTS`), assess them first with
  `make gcp-status-check Q="$ARGUMENTS"` (exit 2 = already done/merged → stop and
  say so; 3 = in flight → coordinate with the existing branch/PR; 0 = proceed).
- Otherwise take the first `NEXT` item above. Ignore the `ATTENTION` list unless
  it directly blocks the item.
- Read the item's `plan doc` and its `source` section from the ledger. Then
  **load the `gcp-phase-workflow` skill** (use the skill tool with id
  `gcp-phase-workflow`; if it is not registered, read
  `~/.config/opencode/skills/gcp-phase-workflow/SKILL.md` and
  `parity-fix-workflow.md` beside it) and follow it, together with `AGENTS.md` /
  `CLAUDE.md` (transport-neutral core owns logic, `gcperr` errors, REST + gRPC
  parity, serialized shared hotspots, never import `internal/aws`).
- Close out: run `make gcp-status`, `make gcp-status-lint-plans` and
  `make gcp-status-coverage`; move the plan doc to
  `plan_docs/final/<ID>-<slug>.md`; put the backlog ID in the commit/PR title.
