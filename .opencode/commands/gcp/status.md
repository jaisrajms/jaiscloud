---
description: Show the GCP parity ledger summary and audit
---

!`make gcp-status 2>&1 | tail -3; echo; make gcp-status-audit 2>&1`

Summarize the GCP parity ledger above:

1. Totals (backlog vs PR history) and the audit class counts.
2. The `ATTENTION` items (`oversight?` / `unowned` / `stale-doc` / `claimed-done`
   / `abandoned`) and what each needs (verify code, fix the record, or open an item).
3. The next few items from `make gcp-status-next` you would pick, and why.

The ledger is derived — do not edit `plan_docs/STATUS.md` or `status.json`.
$ARGUMENTS
