---
description: List the next actionable GCP items from the ledger
---

!`make gcp-status-next N=25 2>&1`

List the actionable GCP items above in priority order. Group them by series/wave,
show the branch, the `P`-rank, the impact and the `plan doc`, and call out the
`ATTENTION` list with what each item needs. Note anything marked in-flight.

The ledger is derived — do not edit `plan_docs/STATUS.md` or `status.json`.
$ARGUMENTS
