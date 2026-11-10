---
description: Run FE1 (Firestore Enterprise index surface) end to end and open a PR to main
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; set -f; args='$ARGUMENTS'; base=main; q="$args"; case "$args" in base=*) rest="${args#base=}"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; --base\ *) rest="${args#--base }"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; esac; set +f; echo "session base: $base"; if [ -n "$q" ]; then echo "item query: $q"; fi; echo; GCP_SESSION_BASE="$base" make gcp-session-start 2>&1 && { echo; echo "--- firestore-enterprise ledger (family-scoped) ---"; make gcp-status-next SERIES="firestore-enterprise" N=5 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2; }`

You are running **FE1 — Firestore Enterprise edition index surface** end to end.
The session base is `main` unless a `base=` prefix was given, and the PR must
target that base. The plan is
`plan_docs/gcp-firestore-enterprise-wave-plan.md`; read §2 (the row) and §4 (the
session) before coding. If `$ARGUMENTS` names another item, treat it as an item
query and assess it with `make gcp-status-check Q="<query>"`; otherwise the item
is FE1.

## Protocol

- Assess first: `make gcp-status-check Q="FE1"`. Exit 2 = already done / merged /
  no-fix → stop and say so; 3 = in flight → coordinate; 0 = proceed.
- Announce `## FE1 — firestore: Enterprise edition index surface` with one or
  two sentences of context, then implement.
- Branch `fix/gcp-firestore-enterprise-indexes` off the refreshed base. One
  session = one branch = one PR; do not batch other items.
- Load the `gcp-phase-workflow` skill and follow `AGENTS.md` / `CLAUDE.md`
  (transport-neutral core, `gcperr` errors, REST + gRPC parity, serialized
  shared hotspots, never import `internal/aws`).

## What FE1 is — and what it is not

The docs check is already done and **inverted the original premise**: Firestore
**Enterprise edition indexes are optional for queries**; a missing index means a
full collection scan, not a query failure. Standard edition is the one that
requires indexes, with single-field indexes created automatically. Therefore:

- **Do not** change `internal/gcp/provider/firestore/query.go` to require a
  single-field index on Enterprise — that would invent a failure real GCP does
  not return. If you touch the planner at all, say why, with a citation.
- **Do** re-verify the two sources before coding and cite them in the commit/PR:
  `firebase.google.com/docs/firestore/editions`,
  `firebase.google.com/docs/firestore/compare-editions-for-native-mode`,
  `docs.cloud.google.com/firestore/native/docs/enterprise-index-overview`.

The real work is the **index surface**:

1. Model the database **edition** as a stored property (at least Enterprise vs
   Standard) so edition-dependent behaviour can be exercised.
2. The Admin v1 `Index` **`density`** (`DENSE` is the Enterprise default;
   `SPARSE_ALL`, `SPARSE_ANY`) and **`apiScope`** are currently dropped by
   `indexDef` (`internal/gcp/provider/firestore/indexes.go`) and never rendered —
   make them round-trip (create → get → list → update/delete).
3. Enterprise manages **single-field and composite indexes through
   `google.firestore.admin.v1.Index`**, while the emulator registers composite
   indexes only and treats single-field indexes as implicit (`google_firestore_field`
   is the Standard mechanism). Decide and implement: accept a single-field
   Enterprise `Index` and render it; leave the Standard `Field` path unchanged.
4. Unique and vector index types are Enterprise-only and unmodelled; either
   model them or record them as a ledger row with a verdict (`no fix` /
   `out of scope` with the reason) — do not leave them silent.
5. Confirm with provider 7.46.2 what `google_firestore_index` actually sends for
   `density`/`apiScope` by planning an Enterprise index before coding.

## Acceptance

- Raw REST (and gRPC if the Admin API serves it there): create/get/list an
  Enterprise index carrying `density`/`apiScope`; a single-field `Index` resource
  accepted and rendered the way Enterprise documents; Standard `google_firestore_field`
  behaviour unchanged.
- A Terraform case (pinned module or corpus) plans an Enterprise index →
  apply → asserts → plan **No changes** → destroy. Enable it in the `firestore`
  module case or add a corpus case; commit the evidence.
- `go build ./...`; `make tf-series-gate SERVICES=firestore GCP_HTTP_PORT=18130
  GCP_GRPC_PORT=18131 TF_PROVIDER_DOCS=/tmp/opencode/tpg
  TF_SAMPLES_DIR=/tmp/opencode/tds` (a SKIP is a failure); `make
  gcp-ci-preflight`.
- Every discovered gap gets a ledger row with an ID and a verdict before the
  session closes. Do not expand scope silently.

## Close out

- One commit, `fix(gcp/firestore): model the Enterprise index surface (FE1)`,
  body with the docs citations, the edition decision, the provider evidence and
  the tests.
- Push and open the PR **against `main`** (or the printed session base):
  `gh pr create --repo jaisrajms/jaiscloud --base main --head aamerik:fix/gcp-firestore-enterprise-indexes`
  then `make gcp-ci-wait PR=<n>` until every check passes. Never report the
  session done on a red or pending check; a runner-infrastructure failure is
  dismissed only after `gh run rerun <run-id>` comes back green.
- After the PR merges, finalize the single-session family once:
  `make gcp-status-finalize PLAN=plan_docs/gcp-firestore-enterprise-wave-plan.md ID=FE1`
  (skip it if the session added follow-up rows that still need scheduling — say
  so instead).
- Report: PR, files, docs citations, gate results, the edition/index decisions,
  and any follow-up rows.
