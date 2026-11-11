---
description: Start the next Firestore Enterprise (FE) series session
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; set -f; args='$ARGUMENTS'; base=main; q="$args"; case "$args" in base=*) rest="${args#base=}"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; --base\ *) rest="${args#--base }"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; esac; set +f; echo "session base: $base"; if [ -n "$q" ]; then echo "item query: $q"; fi; echo; GCP_SESSION_BASE="$base" make gcp-session-start 2>&1 && { echo; echo "--- firestore-enterprise ledger (family-scoped) ---"; make gcp-status-next SERIES="firestore-enterprise" N=5 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2; }`

You are starting the next session from
`plan_docs/gcp-firestore-enterprise-wave-plan.md`. The ledger above is
**family-scoped**: the first `NEXT` item is the session to start — not the
default `/gcp/new` top item, which follows the global `SERIES` order.

If an item query was given (`$ARGUMENTS` minus an optional `base=` prefix), use
it instead of the first `NEXT` item: assess it with
`make gcp-status-check Q="<the query or the ID>"` (exit 2 = already done /
merged / no-fix → stop and say so; 3 = in flight → coordinate; 0 = proceed).

Follow the same protocol as `/gcp/new`:

- Announce `## <ID> — <service>: <feature>` with one or two sentences of
  context, then read the session block in the plan doc.
- Branch the item's `Branch` off the refreshed base (the base is printed above;
  `base=<branch>` / `--base <branch>` selects a series base).
- Load the `gcp-phase-workflow` skill and follow `AGENTS.md` / `CLAUDE.md`
  (transport-neutral core, `gcperr` errors, REST + gRPC parity, serialized
  shared hotspots).
- Close out only when CI is green: `make gcp-ci-preflight`, then open the PR
  against the session base and run `make gcp-ci-wait PR=<n>`. Never report a
  session done on a red or pending check.

Family-specific rules:

- **One session = one branch = one PR.** Do not batch sessions, and do not pick
  up another family's item when this one is blocked — report the blocker.
- **Serialize the family.** Both remaining sessions touch
  `internal/gcp/provider/firestore` and each regenerates
  `tests/gcpconformance/testdata/transcripts.json` and `docs/fidelity`, so never
  run two FE sessions in parallel: each must start from the merged tip of the
  previous one (the regenerated files conflict otherwise).
- **Do not finalize mid-family.** The doc still carries open rows after a
  single session; finalize once when the family is complete (FE3 + FE6 merged,
  FE4/FE5/FE7 recorded `no fix`) with
  `make gcp-status-finalize PLAN=plan_docs/gcp-firestore-enterprise-wave-plan.md ID=FE1`
  (`PR=` optional; the ledger already links merged rows by PR title).
- **Index-surface items must pass the Terraform gate.** For a change to the
  index resource shape (FE3 and anything touching `density`/`apiScope`/fields):
  `make tf-series-gate SERVICES=firestore GCP_HTTP_PORT=18130 GCP_GRPC_PORT=18131
  TF_PROVIDER_DOCS=/tmp/opencode/tpg TF_SAMPLES_DIR=/tmp/opencode/tds`.
  A `SKIP` is a failure of the gate's setup, never green. A data-plane-only
  item may pass `SKIP_CORPUS=1` only when it cannot change
  provider-observable behaviour — say why in the PR.
- **Every discovered gap gets a ledger row** with an ID and a `verdict` before
  the session closes (`fix` + branch, or `no fix`/`out of scope` with the
  reason). Do not expand scope silently.
- An unverified real-GCP wire detail (an error code, an operation envelope) is
  recorded as an explicit evidence gap in the row and the README rather than
  silently inferred.
