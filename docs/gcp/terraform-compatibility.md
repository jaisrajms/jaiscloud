# Terraform compatibility: what local evidence proves

This page is the **trust boundary** for every Terraform/OpenTofu claim in this
repository (wave plan TF6). It answers one question: when a Terraform gate is
green against `jaiscloud-gcp`, what is actually proven, and what is not?

The short version: local evidence proves the **control-plane request/response
contract and Terraform's own convergence** on the served surface. It does not
prove authorization, quotas, organizational policy, or any data plane.

## What is exercised locally

| evidence | what it proves | gate |
|---|---|---|
| Fixture (`tests/integration/gcp/terraform/main.tf`) | every declared resource applies, the provider's read-back succeeds, a second plan is **No changes**, destroy removes it | `make test-gcp-terraform` |
| 21 pinned consumer modules (`module-cases/`) | a real third-party module converges: create, read-back, in-place update, second plan **No changes**, destroy | `make test-gcp-terraform-all` |
| Lifecycle depth (`lifecycle-cases/`) | import, refresh, replace, out-of-band drift and negative paths behave like the API contract | `make test-gcp-terraform-lifecycle` |
| Composition patterns (`patterns/01–04`) | cross-resource dependency graphs and multi-workspace isolation converge | `make test-gcp-terraform-patterns` |
| Provider capture (`tools/tfcapture`) | every attribute the provider sends is served or classified; no silent client-only workaround | `make check-gcp-terraform-coverage` / module gates |
| Static inventory (`tools/tfcoverage`) | every provider resource type is exercised, plan-only or exempted with a reason | `make check-gcp-terraform-coverage` (PR) |
| Differential goldens (`differential/`) | the emulator's normalized Terraform state, first plan and re-plan match a recording taken against **real GCP**, with every difference classified | `make test-gcp-terraform-differential` (nightly) |

"Converges" always means: `terraform apply` succeeds, the provider's read does
not permadiff, and `terraform plan -detailed-exitcode` reports **0** before the
stack is destroyed. An apply exit code alone is not evidence.

## What local evidence does not prove

These are outside the boundary. A green Terraform gate says nothing about them,
and no emulator-only field is added to fake them:

- **Authorization and IAM condition enforcement.** Project IAM policies,
  resource IAM and `condition` blocks are stored and round-trip; no request is
  rejected because the caller lacks a permission. There is no organization,
  folder or principal model.
- **Quotas, rate limits and throttling.** The throttle path injects failures on
  purpose; real quota values and per-region quotas are not modelled.
- **Organization / folder hierarchy and org policies.** Project-level only
  (`wire-contract.md` §8).
- **The Cloud Billing catalog.** Only the project↔billing-account association
  exists; no SKUs, pricing, sub-accounts or budget surface.
- **LRO timing.** Long-running operations settle immediately (or on the next
  read); real durations, polling intervals and `operation.error` semantics that
  depend on a backend are not reproduced.
- **Data planes.** Compute VMs/disks, the Cloud SQL engine, Redis, GKE's real
  control plane and Kubernetes API, Dataproc/Spark execution, Cloud Run image
  pulls and container networking, DNS serving, load balancers and BigQuery's
  query engine beyond the documented SQL subset are metadata-only or mocked.
  A `google_sql_database_instance` that reads `RUNNABLE` is a stored record, not
  a running PostgreSQL server.
- **Client-side provider quirks.** A value the provider strips or rewrites
  before sending (for example empty label values, TGM15) is absent from both
  real GCP and the emulator. That is recorded as a provider quirk, not an
  emulator fix, and it still permadiffs against real GCP.
- **Cross-project / Shared VPC identity.** The emulator is project-scoped; an
  aliased provider pointing at another project only sees that project's rows.
  A composition that needs a second real project is excluded from the
  differential corpus for that reason.

## The differential (real GCP → emulator)

`tools/tfdifferential` records a small curated corpus against a real project
(Application Default Credentials, no custom endpoints) and replays the same
modules against the emulator. A recording captures:

1. the first plan's **diff set** (addresses, actions and the paths of computed
   unknowns — never values),
2. the **post-apply state** with project ids/numbers, the run suffix, origins
   and server-generated values (timestamps, etags, generations, numeric ids)
   folded to placeholders,
3. the **re-plan**, which must be empty.

Each golden carries provenance (project, UTC timestamp, Terraform version,
provider version, platform, case directory) and a content-hash manifest. The
replay diffs the normalized documents; every difference must match a row in
`differential/classifications.yaml`:

| verdict | meaning |
|---|---|
| `emulator-gap` | the emulator should change; the row carries a ledger id and a branch |
| `provider-quirk` | the provider or real GCP behaves this way; not an emulator fix |
| `environment` | project id/region/organization difference, not behavior |
| `accepted-risk` | known and deliberate, with the reason recorded here |

An unclassified difference fails the run. Recording is a credentialed operator
step and is never part of CI; the replay is offline (emulator + committed
goldens) and runs in the nightly.

## Gating policy

| gate | when | why |
|---|---|---|
| `make check-gcp-terraform-coverage` + `tools/tfcoverage` tests | **every PR** (via `make gcp-ci-preflight`) | static: every provider resource type classified; no network |
| `tests/gcpconformance` (REST/gRPC), unit and race suites | **every PR** | the wire contract the provider depends on |
| Fixture + module cases (`make test-gcp-terraform-all`) | nightly, Terraform and OpenTofu | provider-registry drift and module regressions |
| Lifecycle depth + composition patterns | nightly | import/refresh/replace/drift and cross-resource graphs |
| Differential replay (`make test-gcp-terraform-differential`) | nightly | keeps the recorded real-GCP baseline honest |
| Differential record (`make record-gcp-terraform-differential`) | manual, credentialed | a real project, ADC, and cleanup |
| Full apply matrix | **not** a required PR check | a provider download and minutes of apply per PR; the nightly has the stable history |

The full suite (fixture, module cases, lifecycle, patterns) also runs on a
path-filtered pull request when the change touches
`tests/integration/gcp/terraform/**`, `internal/gcp/**`, the Terraform tooling or
the fidelity snapshots (`.github/workflows/gcp-terraform.yml`). A Terraform/OpenTofu
gate that prints `SKIP` because a binary or module checkout is missing is a
broken gate, not a pass: the nightly and drift workflows run with `STRICT=1`.

## Known differences (as of the last recording)

The committed report (`tests/integration/gcp/terraform/differential/report.md`)
is the authoritative, regenerated table. It currently classifies:

- `TF6-1` KMS `CryptoKey.get` omits `destroyScheduledDuration` (real GCP:
  `2592000s`) — emulator gap, scheduled.
- `TF6-2` BigQuery `datasets.get` omits `selfLink` — emulator gap, scheduled.
- `TF6-3` GCS object `mediaLink` omits the `generation` query parameter —
  emulator gap, scheduled.
- `TF6-4` a bucket that never configured versioning is returned as
  `versioning:{enabled:false}` where real GCP omits the block — accepted risk
  (both are valid API shapes; Terraform reports no diff).
- `TF6-5` the default BigQuery dataset ACL names a built-in emulator identity
  where real GCP names the authenticated user — accepted risk (identity is
  outside the boundary).

Nothing on this list is worked around in the emulator, and an `emulator-gap`
row is not "green because it is classified": it is a scheduled fix with a
ledger id.
