# GCP wave-plan template

Copy this into `plan_docs/gcp-<service>-<effort>-wave-plan.md` and fill it in.
`tools/gcpstatus` (`make gcp-status`) parses it automatically; the **index table
headers must be exactly** `Wave | Session | IDs | Service(s) | Branch | Depends on`
(`Session`, `IDs`, `Branch` are required — they are how the ledger discovers the
plan, aliases its IDs, and links merged PRs).

A plan that does not use this shape is invisible to `make gcp-status-next`,
`gcp-status-audit`, and `gcp-status-coverage`.

---

## 1. Index — drives priority

`make gcp-status-next` orders by `Session` (`W<wave>.<seq>`, wave-major); ties
break on the detail table's `Pri`/`impact`.

| Wave | Session | IDs | Service(s) | Branch | Depends on |
|---|---|---|---|---|---|
| 4 | W4.1 | SVC1 | <service> <op group> | `feat/gcp-<svc>-<topic>` | — |
| 4 | W4.2 | SVC2 | <service> <op group> | `fix/gcp-<svc>-<topic>` | W4.1 |
| 5 | W5.1 | SVC3 | <service> <op group> | `feat/gcp-<svc>-<topic>` | — |

Rules:
- **Continue the global `W` sequence** (the Java plan uses W1–W3; new efforts
  start at W4+), so ordering works with no tool change.
- **IDs**: unique `[A-Z]{1,3}[0-9]+` (e.g. `BQ1`, `ICE2`). One ID per deliverable.
  Reuse an existing ID only when aliasing (e.g. `J2/R2`).
- **Branch** is the branch the session will create; the ledger matches the merged
  PR by this name (or by the ID in the PR title).
- **Depends on**: `—`, another session, or `DONE #<pr>` once merged.

## 2. Detail — intent, impact, priority

| ID | service | gap | impact | effort | verdict | prompt |
|---|---|---|---|---|---|---|
| SVC1 | <service> | <what is missing/broken> | high | M | fix | `feat/gcp-<svc>-<topic>` |

`verdict` = `fix` / `fix (follow-up)` / `optional` / `no fix` / `out of scope`
(drives the audit's `intentional` vs future-work split). `prompt` = the branch.

## 3. Serialization — shared hotspots (one session at a time)

| file / area | sessions |
|---|---|
| `cmd/jaiscloud-gcp/main.go`, `tests/gcpconformance/grpc_enumerate.go`, `docs/fidelity/*`, `docs/GA.md`, `go.mod` | all |
| `internal/gcp/adapter/router.go`, `jsoncodec.go` | <sessions> |
| `<service> store schema` | <sessions> |

## 4. Sessions

One fenced block per session: `<BRANCH>`, `IDs`, evidence (fidelity cells + SDK
failure), task, tests, and the GA/fidelity contribution.

```
### W4.1 — <title>
<BRANCH> feat/gcp-<svc>-<topic>
IDs: SVC1

Evidence: <fidelity-matrix cells / SDK test names + current failure>
Task: <implement ... over the transport-neutral core; REST + gRPC parity>
Tests: <unit + conformance + SDK gate>
GA contribution: <service>.<op> <transport> <state> -> ga.
```

## 5. Out of scope / do not change

- Services/methods deliberately excluded (with `docs/GA.md §7` reference).
- TEST-NON-COMPLIANT rows (real GCP wins) — do **not** change the emulator.

## 6. Exit checklist (preview → GA plans)

- [ ] every target cell derives `ga` in `make gen-gcp-fidelity-matrix`
- [ ] no remaining downgrade for the service in `docs/fidelity-overrides.yaml`
- [ ] `docs/GA.md` §2 rollup + §7 and `README-GCP.md` limitations updated
- [ ] wire + gRPC conformance cover the new ops; `make ga-check` green
- [ ] official SDK compatibility gate green
- [ ] `make check-gcp-fidelity-matrix` green (matrix regenerated)

## 7. Ledger loop (per `AGENTS.md`)

```
make gcp-status            # rebuild; this doc's sessions appear as W rows
make gcp-status-next       # confirm the next session / priority
make gcp-status-check Q="<service> <keywords>"
# ... implement, review, PR with the ID in the title ...
make gcp-status            # the merged PR flips the session to `merged`
make gcp-status-coverage   # no blind spots
```
