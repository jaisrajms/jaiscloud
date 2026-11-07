---
description: Start the next Terraform-confidence session (TF0–TF6)
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; set -f; args='$ARGUMENTS'; base=main; q="$args"; case "$args" in base=*) rest="${args#base=}"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; --base\ *) rest="${args#--base }"; base="${rest%% *}"; q="${rest#"$base"}"; q="${q# }";; esac; set +f; echo "session base: $base"; if [ -n "$q" ]; then echo "item query: $q"; fi; echo; GCP_SESSION_BASE="$base" make gcp-session-start 2>&1 && { echo; echo "--- terraform-confidence ledger (family-scoped) ---"; make gcp-status-next SERIES="terraform-confidence" N=10 2>&1; echo; echo '--- coverage ---'; make gcp-status-coverage 2>&1 | tail -2; }`

You are starting the next session from
`plan_docs/gcp-terraform-confidence-wave-plan.md`. The ledger above is
**family-scoped**: the first `NEXT` item is the session to start — not the
default `/gcp/new` top item, which follows the global `SERIES` order.

If an item query was given (`$ARGUMENTS` minus an optional `base=` prefix),
use it instead of the first `NEXT` item: assess it with
`make gcp-status-check Q="<the query or the ID>"` (exit 2 = already done /
merged / no-fix → stop and say so; 3 = in flight → coordinate; 0 = proceed).

Follow the same protocol as `/gcp/new`:

- Announce `## <ID> — <service>: <feature>` with one or two sentences of
  context, then read the session block in the plan doc.
- Branch the item's `Branch` off the refreshed base (the base is printed above;
  `base=<branch>` / `--base <branch>` selects a series base).
- Load the `gcp-phase-workflow` skill and follow `AGENTS.md` / `CLAUDE.md`
  (transport-neutral core, REST + gRPC parity, serialized shared hotspots).
- Close out only when CI is green: `make gcp-ci-preflight`, then open the PR
  against the session base and run `make gcp-ci-wait PR=<n>`. Never report a
  session done on a red or pending check.

Family-specific rules:

- **One session = one branch = one PR.** Do not batch sessions, and do not
  pick up another family's item when this one is blocked — report the blocker.
- **Do not run `make gcp-status-finalize` after the session.** The doc carries
  15 sessions; finalize moves the whole file to `plan_docs/final/`, and its ID
  regex rejects hyphenated sub-IDs (`TF3-1`). Keep
  `plan_docs/gcp-terraform-confidence-wave-plan.md` in place and finalize once
  when the family is complete, with `ID=TF0`.
- **`SKIP` is not a pass in CI.** A Terraform/OpenTofu gate that skips because a
  binary or module checkout is missing must be treated as a failure of the
  gate's setup, not as green.
- **Every discovered gap gets a ledger row** with an ID and a `verdict` before
  the session closes (`fix` + branch, or `no fix`/`out of scope` with the
  reason). Do not expand scope silently.
