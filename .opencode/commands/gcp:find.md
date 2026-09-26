---
description: Assess a GCP change against the ledger (already done / in flight / new)
---

Assess this proposed change against the known GCP state:

$ARGUMENTS

Run `make gcp-status-check Q="$ARGUMENTS"` (add `SERVICE=<svc>` or `-by pri` if
useful) and report the verdict:

- **exit 2** — already done/merged: show the PR/merge and its plan doc; do not re-implement.
- **exit 3** — in flight: show the branch/PR and say to coordinate.
- **exit 0** — new work (or already scheduled): show the matching IDs, their
  wave/series/priority and `plan doc`; if nothing matched, say it is untracked
  and suggest where a plan row should go.

Do not edit `plan_docs/STATUS.md` / `status.json`.
