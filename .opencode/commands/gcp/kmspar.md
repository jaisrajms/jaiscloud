---
description: Implement the whole kms-parity series (CKP1, CKP2, CKG1, CKG2) as one PR
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; set -f; args='$ARGUMENTS'; base=main; q="$args"; case "$args" in base=*) rest="${args#base=}"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; --base\ *) rest="${args#--base }"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; esac; set +f; echo "session base: $base"; if [ -n "$q" ]; then echo "workstream filter: $q"; fi; echo; test -f plan_docs/gcp-kms-parity-wave-plan.md || echo "MISSING plan_docs/gcp-kms-parity-wave-plan.md (plan_docs is gitignored — copy it from the primary checkout before proceeding)"; echo; GCP_SESSION_BASE="$base" make gcp-session-start 2>&1 && { echo; echo "--- kms-parity ledger (family-scoped, whole series) ---"; make gcp-status-next SERIES="kms-parity" N=10 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2; }`

You are running the **entire** `kms-parity` family — W1.1 `CKP1`, W1.2 `CKP2`,
W2.1 `CKG1`, W2.2 `CKG2` from `plan_docs/gcp-kms-parity-wave-plan.md` — in one
session, delivered as **one PR**. The ledger above is family-scoped; those four
`NEXT` rows are in scope. `CKP3` and `CKG3` are `no fix` and `CKG4` is
`optional`: do not implement them, and do not "improve" them on the way past.
An argument after an optional `base=` prefix is a workstream filter (`CKP1`,
`CKG2`, …) for resuming a partly-done run; otherwise all four are in scope.

Stop and report instead of improvising if: the plan doc is missing, the ledger
does not show the four rows (someone merged or re-scoped them), or a row's gap
no longer matches the code.

## Delivery shape

- **One integration branch**, `feat/gcp-kms-parity`, cut off the refreshed base
  printed above (`main` unless `base=`/`--base` says otherwise), and **one PR**
  into that base. The four `Branch` values in the plan index are the per-session
  branches; they become stacked child branches only where they must (see
  "Serialization"), are merged into the integration branch, and carry no PR of
  their own.
- Do **not** create a series base branch: all four rows target `main`, and the
  ledger would then split `main`'s history for no benefit.
- Do **not** add `kms-parity` to `SERIES` in the `Makefile`. While the
  integration branch is open, a concurrent `/gcp/new` would otherwise be handed
  these rows.
- If one PR genuinely cannot work (a reviewer requires the CMEK change split
  out, or a hotspot forces a real stack), fall back to stacked PRs
  `child → feat/gcp-kms-parity` merged in order, with a single integration PR
  into the base. Say why in the PR description; do not silently split.

## Read first

1. The plan's §2.1 reproduction matrix — it is the evidence baseline. Re-verify
   the CKP1 symptom before touching anything (create a key with purpose
   `ASYMMETRIC_SIGN`, `POST …/cryptoKeys/<k>:updatePrimaryVersion
   {"cryptoKeyVersionId":"1"}` → currently `200` with no `primary`), so the PR
   can state "before" and "after".
2. The four §4 session blocks, verbatim.
3. `make gcp-status-check Q="kms updatePrimaryCryptoKeyVersion restore" SERVICE=kms`
   (exit 0 expected) and `docs/gcp/review-checklist.md`.
4. Load the `gcp-phase-workflow` skill and follow `AGENTS.md` / `CLAUDE.md`.

## Workstreams and file ownership

The streams are cut so that they never edit the same file. The **parent is the
only writer of shared artifacts** — that is what makes concurrent subagents safe
in a single checkout.

| WS | ID | Owns (edit) | Must not touch |
|---|---|---|---|
| A | CKP1 | `internal/gcp/service/kms/`, `internal/gcp/transport/rest/kms/`, `internal/gcp/transport/grpc/kms/`, the KMS tests in `tests/integration/gcp/sdk-rest/` | shared artifacts; storage; UI |
| B | CKP2 | `internal/gcp/ui/kms/`, `internal/gcp/ui/routed.go`, `ui/src/gcp/kms/` | shared artifacts; core `service/kms` |
| C | CKG1 | `internal/gcp/reference/`, `internal/gcp/crypto/`, `internal/gcp/provider/storage/`, `internal/gcp/adapter/gcs.go` | shared artifacts; kms core |
| P | CKG2 | `tests/gcpdifferential/` | — (own scope) |
| P | all | `tests/gcpconformance/scenarios.go`, `tests/gcpconformance/testdata/transcripts.json`, `docs/fidelity/*`, `docs/gcp/wire-contract.md`, `README-GCP.md`, `docs/gcpstatus-resolved.yaml`, `cmd/jaiscloud-gcp/main.go` | — |

CKG2 runs **last**, because its goldens pin the error reasons CKG1 introduces.

## Subagents

- Dispatch A, B and C with the `subagent` tool (`agent: general`,
  `background: true`) in a **single message** so they run concurrently. Each
  prompt must carry: the ID and its §4 block verbatim, its "Owns" row and the
  shared-artifact ban, "edit files in this checkout; do not run git, commit,
  push or `git checkout`", the package-scoped tests to run
  (`go test ./internal/gcp/<pkg>/...`, `gofmt -l` on its tree), and "return:
  files changed, tests run + result, any discovered gap that needs a ledger row".
- Review each returned diff yourself, run its package tests, then commit that
  workstream alone: `git add <its paths> && git commit -m "fix(gcp/kms): … (CKP1)"`.
  Do not stage shared artifacts into these commits.
- If a workstream turns out to need a file another owns, stop it and re-cut it as
  a stacked child: `git worktree add /tmp/opencode/kms-<id> -b <plan Branch>
  feat/gcp-kms-parity`, work it serially there, then `git merge --no-ff <branch>`
  into the integration branch and remove the worktree. Report that you stacked.
- Implement **CKG2 yourself** (a subagent may gather the scenario list, but the
  parent writes `tests/gcpdifferential/error_scenarios.go` and owns the record
  and replay runs).

## Shared artifacts (parent only, once, after A–C are committed)

1. `tests/gcpconformance/scenarios.go`: the CKP1 negative pair and the CKG1
   upload-rejection scenario, then regenerate the committed transcript through
   the repo's normal record path (`make test-gcp-wire-conformance` rewrites it;
   never hand-edit `testdata/transcripts.json`).
2. `make gen-gcp-fidelity-matrix` and commit `docs/fidelity` — a stale matrix is
   the number-one red-CI cause in this repo.
3. `docs/gcp/wire-contract.md` (§KMS, §storage) and `README-GCP.md` (console KMS
   line, CMEK limitations) for all four items: console restore, the
   primary-version requirement and its documented reasons, that
   `:updatePrimaryCryptoKeyVersion` is a method id and not a wire path, and that
   the CMEK key-state check is deliberate.
4. `gofmt -l internal/gcp/ cmd/jaiscloud-gcp/` empty,
   `go build ./... && go vet ./internal/gcp/... ./cmd/jaiscloud-gcp/`.

## Gates (all must pass before the PR is called done)

```
go test ./internal/gcp/...
pnpm --dir ui run test && pnpm --dir ui run build        # CKP2 (tsc -b catches types)
go build -o /tmp/opencode/jaiscloud-gcp ./cmd/jaiscloud-gcp
/tmp/opencode/jaiscloud-gcp start --port 8080 --ephemeral &   # kill by listener PID when done
go test -race -count=1 ./tests/integration/gcp/
( cd tests/integration/gcp/sdk-rest && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -race -count=1 ./... )
make test-gcp-differential        # offline replay of the CMEK goldens
make gcp-ci-preflight             # mirrors CI's offline GCP gates
make ga-check
make gcp-matrix-diff REF=upstream/main    # no ga -> worse regression
```

Nested SDK modules run from their own directory. Kill the emulator by listener
PID (`ss -tlnp | grep ':8080'`), never `pkill -f`. Real GCP wins every
disagreement: never add an emulator-only field, verb spelling or leniency to
make a harness pass — record it as TEST-NON-COMPLIANT and say so in the PR.

## Review (two lenses, per `docs/gcp/review-checklist.md`)

Dispatch two **read-only** review subagents in parallel — one as GCP architect
(wire parity: documented path and verb, error code/reason/status, REST and gRPC
answering identically, Discovery evidence for each claim) and one as senior
developer (conventions: `clock.Now()`, `nr.ResourceID` for response names,
`store.GlobalRegion`, `errors.Is`, `model.NewProviderError` via `gcperr`, no
`internal/aws` import, test style, docs accuracy). Cross-audit their findings,
triage against the checklist's calibration list, fix everything blocking, and
re-run only the gates the fixes touch.

## PR

- Open it early as a draft so the number exists, then keep pushing:
  `gh pr create --repo jaisrajms/jaiscloud --base <base> --head aamerik:feat/gcp-kms-parity --title "feat(gcp/kms): kms-parity series — updatePrimary purpose, console restore, CMEK error contract (CKP1 CKP2 CKG1 CKG2)"`.
- In the same PR, add four entries to `docs/gcpstatus-resolved.yaml` (the ledger
  anchors these rows on per-session branches that carry no PR, so the overlay is
  what closes them): `source: "kms-parity"`, a `match` phrase unique to each row
  (`CKP1..CKG2` alone would hit mentions in other rows — use the wave-row
  summary or a distinctive gap phrase), `pr: <n>`, a `note`. The overlay is
  evidence-gated, so a closed or reverted PR leaves the rows open.
- PR description sections: Description / What's in this PR / Out of scope / How
  to test / Test report (point at test files, not transcripts). Name the four
  IDs in the title, the single-PR decision, and anything you deliberately left
  as `no fix`.
- `make gcp-ci-wait PR=<n>` until every check is green. Fix → commit → push →
  wait again. A red check may only be dismissed as runner infrastructure, and
  then only after `gh run rerun <run-id>` comes back green. Never hand over a
  red or pending PR.

## Close-out

- After merge: `make gcp-status` (the four rows should read `merged` through the
  overlay), then finalize the family once —
  `make gcp-status-finalize PLAN=plan_docs/gcp-kms-parity-wave-plan.md ID=CKP1 SLUG=kms-parity`.
  Do not finalize mid-run: the doc holds four sessions and moves as one file.
- Every gap discovered during the run gets a ledger row with an ID and a
  `verdict` (`fix` + branch, or `no fix`/`out of scope` with the reason) before
  finalizing. Do not expand scope silently.
- Report: PR number and state, the per-workstream commits, which items were
  stacked (if any), the gate results, and the final ledger rows.
