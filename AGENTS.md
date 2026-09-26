# Repository agent guide (jaiscloud)

> Auto-loaded by OpenCode. Project conventions live in [CLAUDE.md](CLAUDE.md) —
> read it before coding. This file carries the cross-session workflow rules.

## GCP parity work — check the status ledger before you plan

The GCP backlog spans many plan docs and PRs. Do not rely on memory or chat
history to decide what is implemented.

1. **Read the ledger.** `make gcp-status` rebuilds the canonical ledger
   (`plan_docs/STATUS.md` human view + `plan_docs/status.json`) by parsing every
   `plan_docs/**/*.md` table and joining it with git branch and GitHub PR state.
   `merged` / `branch` / `pr` come from git/GitHub and are authoritative;
   `todo` / `done?` are doc-derived (`done?` = a doc claims done but no merged
   PR/branch was found — verify before trusting it).
2. **Assess the proposed change before implementing.**
   `make gcp-status-check Q="<service> <keywords>"` (add `SERVICE=<svc>`).
   Verdict/exit: **already done/merged** → do not re-implement (reuse or
   verify); **in flight** → coordinate with the existing branch/PR;
   **no match** → new work.
3. **Record the outcome when you finish.** Move the plan doc into
   `plan_docs/final/` with an ID-prefixed name (e.g.
   `final/J1-datastore-rest-protobuf.md`) and put the backlog ID in the
   commit/PR title (e.g. `fix(gcp/datastore): … (J1)`) so the next run links it.

`plan_docs/` is gitignored local scratch; `tools/gcpstatus` and this file are
committed.
