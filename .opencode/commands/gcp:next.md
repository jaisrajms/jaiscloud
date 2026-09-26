---
description: Show the next GCP items from the status ledger
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; make gcp-status-next N=10 2>&1`

Present the next GCP items above concisely — one line each: ID, series/wave,
branch, `P`-rank and impact — then recommend the single top item to work on and
why. List the `ATTENTION` items separately with what each needs. To start work on
the top item, use `/gcp:new`.

The ledger is derived — do not edit `plan_docs/STATUS.md` or `status.json`.
$ARGUMENTS
