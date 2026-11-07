# Terraform compatibility scorecard

Baseline for the `terraform-confidence` plan (TF0). It records what the
Terraform/OpenTofu suite actually proves today, so the nightly has something to
compare against and every red row has an owner.

Regenerate with:

```bash
make test-gcp-terraform-all STRICT=1                 # Terraform
make test-gcp-terraform-all TF_BIN=tofu STRICT=1     # OpenTofu
```

The aggregate starts one emulator, runs the fixture and every pinned module
case against it, and writes `summary.md`, `summary.tsv` and one log per case to
`/tmp/jaiscloud-gcp-terraform-all/` (override with `LOG_DIR=`). `STRICT=1` makes
a `SKIP` — a missing binary or module checkout — a failure, which is what the
nightly and drift workflows use.

## Environment

| | |
|---|---|
| base | `main` at `35d62a1c` (runs include this commit's TF0 changes) |
| Terraform | 1.16.3 |
| OpenTofu | 1.12.6 |
| hashicorp/google | 7.46.2 (module cases pin `= 7.46.2`; the fixture's `~> 7.36` resolved to 7.46.2) |
| OS | linux/amd64 |
| module checkouts | `$HOME/.cache/terraform-modules/<repo>` at the pinned commits in the table |

## Baseline run (2026-11-07T11:19:51Z)

First aggregate run on this branch: **8 passed / 2 failed**. Both reds were
test/gate defects, not emulator gaps:

| case | result | cause | fix |
|---|---|---|---|
| `cloud-operations` | FAIL (8s) | FC6 capture gate rejected `google_monitoring_alert_policy.conditions.name` as `output-only`, but the provider sends it on update. The field catalog already classifies it `derived` (a client-supplied name identifies an existing condition and is preserved). | mapped it `served` in `docs/fidelity/provider-attributes.csv` |
| `firestore` | FAIL (17s) | The spot check asserted the literal substring `"cmekConfig":{"kmsKeyName":…}`. TGM11-1 added `activeKeyVersion` ahead of `kmsKeyName`, so the emulator's correct response no longer matched. A direct REST repro confirmed both members are echoed. | the check now requires `"cmekConfig"` and `"kmsKeyName":"<key>"` independently |

Both re-ran green with `CASES="cloud-operations firestore" SKIP_FIXTURE=1`, and
the full runs below are green.

## Final runs

### Terraform 1.16.3 — 10 passed, 0 failed (2026-11-07T11:24:02Z)

| case | result | seconds | module HEAD |
|---|---|---|---|
| fixture | PASS | 51 | — |
| clouddns | PASS | 20 | `35cd6de` |
| cloud-operations | PASS | 8 | `8c3e379` |
| firestore | PASS | 17 | `50d170b` |
| functions | PASS | 17 | `9f2e26a` |
| kms | PASS | 8 | `ef6e0ed` |
| memorystore | PASS | 41 | `5415d5c` |
| pubsub | PASS | 16 | `6cf1860` |
| pubsub-topic | PASS | 8 | `6cf1860` |
| storage | PASS | 23 | `c7effc0` |

### OpenTofu 1.12.6 — 10 passed, 0 failed (2026-11-07T11:27:44Z)

| case | result | seconds | module HEAD |
|---|---|---|---|
| fixture | PASS | 51 | — |
| clouddns | PASS | 20 | `35cd6de` |
| cloud-operations | PASS | 9 | `8c3e379` |
| firestore | PASS | 17 | `50d170b` |
| functions | PASS | 17 | `9f2e26a` |
| kms | PASS | 9 | `ef6e0ed` |
| memorystore | PASS | 42 | `5415d5c` |
| pubsub | PASS | 17 | `6cf1860` |
| pubsub-topic | PASS | 8 | `6cf1860` |
| storage | PASS | 24 | `c7effc0` |

## What a PASS means

- **Fixture**: `init` → `validate` → `plan` → `apply` → the REST spot checks →
  second `plan` (no changes) → in-place update phases (uptime check, BigQuery
  routine/row access policy, Cloud Run container env) → `destroy` → absence
  checks.
- **Module case**: `init` → `plan` → `apply` → read-back assertions → `plan`
  **No changes** → mutate → `plan` No changes → clear → `plan` No changes →
  `destroy`, plus the FC6 capture gate over the provider's outgoing request
  bodies joined to the field catalog.
- Every module checkout in the table is at the commit the case pins; a moved
  checkout is visible in `summary.tsv` via its `module_head` column.

## TF3-1 — Cloud Run v2 consumer case (2026-11-07)

The `cloudrun` case pins `GoogleCloudPlatform/terraform-google-cloud-run`
`4ce640a2a519` and drives its `modules/v2` submodule (the repo root is the v1
service, which is not served). Both runs below are green on the session commit:

| binary | result | case |
|---|---|---|
| Terraform 1.16.3 | PASS (20 passed, 0 failed) | `cloudrun` (`TF_MODULE_DIR=…/terraform-google-cloud-run`) |
| OpenTofu 1.12.6 | PASS (20 passed, 0 failed) | `cloudrun` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (68 passed, 0 failed) | `fixture` (includes the raw `google_cloud_run_v2_service` with a `readiness_probe`) |

The module's service account is created by the module itself
(`create_service_account = true`) because the default-account data source needs
the unserved `compute.projects.get` (TF2-1); `cloud_run_deletion_protection` is
disabled because it is a provider-side destroy guard. Jobs, WorkerPools, the v1
surface and traffic targets are outside the served surface.

## TF3-2 — BigQuery consumer case (2026-11-07)

The `bigquery` case pins `terraform-google-modules/terraform-google-bigquery`
`e11db0dcd597` and drives its root module (dataset + tables + `access` ACL).
Both runs below are green on the session commit:

| binary | result | case |
|---|---|---|
| Terraform 1.16.3 | PASS (20 passed, 0 failed) | `bigquery` (`TF_MODULE_DIR=…/terraform-google-bigquery`) |
| OpenTofu 1.12.6 | PASS (20 passed, 0 failed) | `bigquery` (`TF_BIN=tofu`) |

Dataset- and table-level IAM is a recorded gap (TF1-1): the
`getIamPolicy`/`setIamPolicy` operations are not in the registry, so the module
case drives the served dataset ACL (`access`) instead. Views, materialized and
external tables, routines, CMEK and resource tags stay at their defaults.

## What this does not prove

This is emulator-only evidence. It says nothing about authz/IAM conditions,
quotas/throttling, the org/folder hierarchy, the billing-account catalog, LRO
timing, or the data planes of the metadata-only services (Compute, GKE, Cloud
SQL, Memorystore). TF6 records the same corpus against real GCP and writes the
trust boundary in `docs/gcp/terraform-compatibility.md`; until then, a green row
means "converges against the emulator", not "identical to GCP".
