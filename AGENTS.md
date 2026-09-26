# Repository agent guide (jaiscloud)

> Auto-loaded by OpenCode. Project conventions live in [CLAUDE.md](CLAUDE.md) —
> read it before coding. This file carries the cross-session workflow rules.

## GCP parity work — check the status ledger before you plan

**Toolchain:** OpenCode sessions do not have `go` on `PATH`. Before any `go`,
`gofmt`, or `make gcp-status*` command, run
`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH` (`go` is
`/tmp/opencode/go/bin/go`). For the Java/SDK compat gate also export
`JAVA_HOME=/tmp/opencode/toolchain/jdk-21.0.12.1+1` and
`/tmp/opencode/toolchain/maven/bin`.

The GCP backlog spans many plan docs and PRs. Do not rely on memory or chat
history to decide what is implemented.

**Any GCP plan you write must follow
[`docs/gcp-wave-plan-template.md`](docs/gcp-wave-plan-template.md):** a wave index
with `Session` / `IDs` / `Branch` columns, a detail table, and one session = one
branch = one PR. The ledger parses it automatically; a plan without those
headers is invisible to `gcp-status-next` / `gcp-status-audit` / `gcp-status-coverage`.
Plans are independent *families* (default: the filename) and order via
`SERIES` (e.g. `make gcp-status-next SERIES="java-compat,bigquery-ga"`); the
Makefile default keeps `java-compat` first. Scaffold a new plan with
`make gcp-plan-new SERVICE=<svc>` (pre-fills that service's non-`ga` fidelity
cells), and `make gcp-status-lint-plans` fails any plan-shaped doc that lacks a
parseable index/detail.

1. **Read the ledger.** `make gcp-status` rebuilds the canonical ledger
   (`plan_docs/STATUS.md` human view + `plan_docs/status.json`) by parsing every
   `plan_docs/**/*.md` table (backlog IDs, the dual-protocol phase tracker, the
   Java wave plan, the deferred-debt status tables, and the authoritative
   "what's actually left" list; docs without tables fall back to a prose scan of
   their status bullets, and the wave plan emits one row per W-session), joining
   it with git branch + GitHub PR state,
   and adding a **PR-history row for every base-`gcp` PR** not otherwise
   represented — so merged work is never invisible.
   `merged` / `branch` / `pr` come from git/GitHub and are authoritative; `todo`
   / `done?` are doc-derived (`done?` = a doc claims done but no merged
   PR/branch was found). Each row also carries a **disposition**
   (`fix`/`no-fix`/`optional`) distinct from its state. Non-`ga` fidelity cells
   are also ingested automatically as `kind=matrix` in a separate **Fidelity
   gaps** section (disable with `MATRIX=`); they are records, not scheduled work.
2. **Assess the proposed change before implementing.**
   `make gcp-status-check Q="<service> <keywords>"` (add `SERVICE=<svc>`).
   Verdict/exit: **already done/merged** → do not re-implement (reuse or
   verify); **in flight** → coordinate with the existing branch/PR;
   **no match** → new work.
3. **Pick what's next.** `make gcp-status-next` prints the next actionable items
   in priority order — the wave-plan execution order (Wave 1 client-impact
   first), with an explicit `Pri` (P1–P20) as the fallback when an item is not in
   a wave and `impact` breaking ties — plus a separate `ATTENTION` list. Take
   the top item unless told otherwise. Slash aliases: `/gcp:next` (show next),
   `/gcp:new` (next item + start), `/gcp:status`, `/gcp:list`, `/gcp:audit`,
   `/gcp:find` (nested `/gcp/next` etc. resolve too).
4. **Know what is *not* done and why.** `make gcp-status-audit` classifies the
   not-done items: `oversight?` (declared in a finished wave, not merged),
   `unowned` (fix verdict, no branch, only a historical table), `stale-doc` (a
   merged fix the docs still call open), `abandoned` (branch/PR closed unmerged),
   `claimed-done` (doc says done, no evidence) vs the healthy `scheduled` /
   `unscheduled` / `intentional`. Use it to decide oversight vs planned future
   work. `make gcp-status-coverage` fails if any `plan_docs` file still has
   status markers but produced no rows (an audit blind spot).
5. **Record the outcome when you finish.** Move the plan doc into
   `plan_docs/final/` with an ID-prefixed name (e.g.
   `final/J1-datastore-rest-protobuf.md`) and put the backlog ID in the
   commit/PR title (e.g. `fix(gcp/datastore): … (J1)`) so the next run links it.
   **Record deferrals:** for every item you deferred or left unimplemented (a
   REST/gRPC-only limitation, an unsupported parameter, a code `TODO`), add a row
   with an ID and a `verdict` — `fix` + a `prompt` branch to schedule it, or
   `no fix` / `out of scope` to close it (operation-level gaps are auto-covered by
   the matrix). Then run `make gcp-status` plus `make gcp-status-lint-plans` and
   `make gcp-status-coverage`. If the session changed operations, also run
   `make gcp-matrix-diff REF=upstream/gcp` (fails on a `ga`→worse regression).

`plan_docs/` is gitignored local scratch; `tools/gcpstatus` and this file are
committed.
