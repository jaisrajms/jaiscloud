# Repository agent guide (jaiscloud)

> Auto-loaded by OpenCode. Project conventions live in [CLAUDE.md](CLAUDE.md) —
> read it before coding. This file carries the cross-session workflow rules.

## GCP parity work — check the status ledger before you plan

**Toolchain:** `go` is on `PATH`; the `make gcp-status*` targets (which shell
out to `go build`) and any `go`/`gofmt` work need it. The `/gcp/*` slash
commands prepend their own toolchain dir when they run. The **optional** floci
Java interop suite also needs `JAVA_HOME` and `mvn` on `PATH`.

**GCP source of truth — real GCP, not a sibling emulator.** The authority order
for GCP wire behavior is: the service's Discovery document / proto and official
Google docs → real client-SDK wire behavior → the AWS reference implementation
(repo conventions only) → jaiscloud's own code → jaiscloud's own tests.
**floci-gcp and localgcp are optional smoke/interop harnesses, never the spec.**
When a floci/localgcp expectation disagrees with the Discovery document (an
emulator-only response field, a token prefix, a response shape), real GCP wins:
do not add emulator-only fields/values to make a floci assertion pass — classify
it TEST-NON-COMPLIANT and record it. The compliance gate is the offline
conformance harness (`tests/gcpconformance`: vendored Discovery + committed
transcripts, the official gRPC clients, and the `gcloud`/Python client suites).

**Review protocol:** two lenses (GCP architect for wire parity, senior developer
for conventions/quality) with a Discovery-doc brief, cross-audit, and a
calibrated triage — `docs/gcp/review-checklist.md`, with worked briefs under
`docs/gcp/review-briefs/`.

**e2e environment:** the provided dev environment has a working k3d cluster and
a Docker daemon, so cluster-/image-based gates (`*-k3d`, k8s executor modes) and
docker-executor gates (`test-e2e-*-docker`, `test-e2e-cloudrun-docker`) are
runnable — the exact commands are the `test-e2e-*` / `*-k3d` targets in the
`Makefile`. The `docker` CLI's active context is `remote` (SSH to the build
host) for image builds; the emulator and the docker-executor gates use the
**local** daemon at `/var/run/docker.sock` (`docker --context default`), so the
invoking shell must be in the `docker` group (or `sg docker -c '…'`).

**Starting a session (`/gcp/new`):** the command first runs
`make gcp-session-start`, which aborts if the current checkout has uncommitted
changes (commit or stash a previous or concurrent session's work first),
otherwise `git fetch upstream --prune` and fast-forwards the session base from
`upstream` (`--ff-only`, so a diverged base fails loudly rather than resetting),
then rebuilds the ledger. The base defaults to `main`; for a series, name it
explicitly with `base=<branch>` (`/gcp/new base=gke datastore timestamps`) or
`--base <branch>`, which sets `GCP_SESSION_BASE`. There is no bare-word form —
an argument without `base=` is always an item query, never a branch. A base that
does not exist yet is created off `upstream/main` and pushed to origin **and**
upstream — upstream matters because a PR cannot target a branch that exists only
in the fork — and that series' PRs then use it as their base instead of `main`.
Branch the item's `Branch` off the refreshed base. Always refresh from `upstream` — `origin` is a fork and its
branches are stale. The old `gcp` branch is frozen: it was merged into `main`
(#610, released as `v0.1.1`), so nothing new targets it.

The GCP backlog spans many plan docs and PRs. Do not rely on memory or chat
history to decide what is implemented.

**Any GCP plan you write must follow
[`docs/gcp/wave-plan-template.md`](docs/gcp/wave-plan-template.md):** a wave index
with `Session` / `IDs` / `Branch` columns, a detail table, and one session = one
branch = one PR. The ledger parses it automatically; a plan without those
headers is invisible to `gcp-status-next` / `gcp-status-audit` / `gcp-status-coverage`.
Plans are independent *families* (default: the filename) and order via
`SERIES` (e.g. `make gcp-status-next SERIES="java-compat,bigquery-ga"`); the
Makefile default keeps `dataproc-gke` first (the current priority), then
`ga-fidelity`, `functions-docker-executor` and `console-ui` (the full order is
`SERIES` at the top of the `Makefile`). Scaffold a new plan with
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
   and adding a **PR-history row for every PR into the tracked base branch** not otherwise
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
   the top item unless told otherwise. Slash commands live in
   `.opencode/commands/gcp/` and are invoked nested: `/gcp/next` (show next),
   `/gcp/new` (next item + start), `/gcp/plan <service>` (scaffold a GA wave
   plan), `/gcp/status`, `/gcp/list`, `/gcp/audit`, `/gcp/find`.
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
   One step: `make gcp-status-finalize PLAN=plan_docs/<doc>.md ID=<J-id>`
   (`SLUG=…` to override the slug, `DRY=1` to preview, `OPS=1` to also run
   `make gcp-matrix-diff REF=upstream/main`); it moves the doc and runs
   `gcp-status` + `gcp-status-lint-plans` + `gcp-status-coverage` for you.
   **Record deferrals:** for every item you deferred or left unimplemented (a
   REST/gRPC-only limitation, an unsupported parameter, a code `TODO`), add a row
   with an ID and a `verdict` — `fix` + a `prompt` branch to schedule it, or
   `no fix` / `out of scope` to close it (operation-level gaps are auto-covered by
   the matrix). Then run `make gcp-status` plus `make gcp-status-lint-plans` and
   `make gcp-status-coverage`. If the session changed operations, also run
   `make gcp-matrix-diff REF=upstream/main` (fails on a `ga`→worse regression).
6. **Do not finish on a red PR.** Run `make gcp-ci-preflight` before pushing (it
   mirrors CI's offline GCP gates; a stale `docs/fidelity` — any new/changed
   conformance check or evidence string — is the most common CI failure, fixed by
   `make gen-gcp-fidelity-matrix` + committing `docs/fidelity`). After opening the
   PR, `make gcp-ci-wait PR=<n>` blocks until the checks settle and prints the
   failing jobs' logs; fix, push, and repeat until every check is green. A check
   may only be dismissed as runner infrastructure, and then only after
   `gh run rerun <run-id>` comes back green.

`plan_docs/` is gitignored local scratch; `tools/gcpstatus` and this file are
committed.
