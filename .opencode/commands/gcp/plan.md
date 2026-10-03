---
description: Scaffold a preview→GA wave plan for a GCP service
---

!`export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH; make gcp-status-lint-plans 2>&1 | tail -2`

You are creating a GCP plan document — **planning only, do not implement service code**.

Service/effort requested: $ARGUMENTS

Follow the `gcp-phase-workflow` skill §0 ("Plan documents — mandatory template") and
`docs/gcp-wave-plan-template.md` exactly; the status ledger parses plans mechanically.

1. Resolve the service from `$ARGUMENTS` (a service name, plus an optional `EFFORT`
   label, default `ga`). If empty, take the top `NEXT` item from
   `make gcp-status-next` and ask if ambiguous.
2. Assess before planning:
   - `make gcp-status-check Q="<service>" SERVICE=<service>` (exit 2 = already
     done/merged → stop and say so; 3 = in flight → coordinate with the existing
     branch/PR; 0 = new work).
3. Scaffold the plan:
   - `make gcp-plan-new SERVICE=<service> EFFORT=<effort>` — writes
     `plan_docs/gcp-<service>-<effort>-wave-plan.md`, pre-filled with that service's
     non-`ga` fidelity cells.
   - A service with **no matrix cells** (greenfield, e.g. Cloud Scheduler) makes
     `gcp-plan-new` fail; copy `docs/gcp-wave-plan-template.md` by hand instead.
4. Turn the scaffold into a real plan:
   - group the raw cells into sensible sessions (not one session per cell);
   - index headers **exactly** `Wave | Session | IDs | Service(s) | Branch | Depends on`
     (optional `Series`); sessions are `W<wave>.<seq>`; IDs unique `[A-Z]{1,3}[0-9]+`;
   - detail table **exactly** `ID | service | gap | impact | effort | verdict | prompt`;
   - **one session = one branch = one PR**; the backlog ID goes in the commit/PR title;
   - when a session's gate needs a cluster or an image build, plan it against the
     provided k3d cluster / remote Docker (see the `gcp-phase-workflow` skill's
     Environment section) — those are real, runnable gates;
   - fill §3 Serialization (shared hotspots: `cmd/jaiscloud-gcp/main.go`,
     `tests/gcpconformance/grpc_enumerate.go`, `docs/fidelity/*`, `docs/GA.md`,
     `go.mod`), §4 per-session blocks, §5 Out of scope / do not change, §6 Preview→GA
     exit checklist (every target cell derives `ga` in `make gen-gcp-fidelity-matrix`,
     no remaining `docs/fidelity-overrides.yaml` downgrade, `docs/GA.md`/README updated,
     conformance + `make ga-check` green);
   - classify every `limited`/`unsupported` cell REAL vs TEST-NON-COMPLIANT (real GCP
     wins) and record the verdict.
5. Validate the plan and fix anything that fails:
   - `make gcp-status-lint-plans`
   - `make gcp-status-coverage`
   - `make gcp-status` (the plan's rows now appear)
   - `make gcp-status-next SERIES="<family>"` (family = the filename stem, e.g. `functions-ga`)

Out of scope in this flow: no service code, no wire changes, no PR (the plan lives in
gitignored `plan_docs/`). Report: the plan path, the session/ID table you settled on,
the verdicts you chose with evidence, and the lint/coverage/next output.
