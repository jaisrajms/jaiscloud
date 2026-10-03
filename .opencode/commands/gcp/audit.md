---
description: Triage the GCP ledger audit (oversight/unowned/stale-doc/...)
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; make gcp-status-audit 2>&1`

Triage the audit output above. For every `ATTENTION` item — `oversight?`,
`unowned`, `stale-doc`, `claimed-done`, `abandoned` — verify it against the code
and the conformance evidence (or `make gcp-status-check Q="…"`), then recommend
one of: **fix the record** (doc lag), **open a tracked item** (real gap), or
**leave it** (intentional). Do not change `intentional` items. Summarize counts
and give a short action list.
$ARGUMENTS
