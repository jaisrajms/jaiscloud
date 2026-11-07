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
| hashicorp/google | 7.46.2 (module cases pin `= 7.46.2` except `workflows`, which pins `= 6.49.3` because its module declares `< 7.0`; the fixture's `~> 7.36` resolved to 7.46.2) |
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
  bodies joined to the field catalog. A **plan-only** case (GKE, Datastore, or
  any case run with `PLAN_ONLY=1`) stops after `init` → `validate` → `plan`:
  it asserts the planned resource addresses and the reads the module needs, and
  never applies, destroys or captures (a plan sends no write bodies).
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

## TF3-3 — Secret Manager and Workflows consumer cases (2026-11-07)

Two cases were added on one branch:

| case | module | pinned commit | what it drives |
|---|---|---|---|
| `secret-manager` | `GoogleCloudPlatform/terraform-google-secret-manager` | `77fca80ccd6a` | automatic + user-managed replication, CMEK (global and per-replica), rotation + topic, versions, authoritative IAM binding |
| `workflows` | `GoogleCloudPlatform/terraform-google-cloud-workflows` (`modules/simple_workflow`) | `4c431381b5d8` | the workflow resource with a root-module-owned service account and project `roles/workflows.invoker` grant |

Both runs below are green on the session commit (each run against a fresh
ephemeral emulator). The full aggregate — the fixture plus all thirteen module
cases — is also green under both binaries (`14 passed, 0 failed, 0 skipped`):

| binary | result | case |
|---|---|---|
| Terraform 1.16.3 | PASS (25 passed, 0 failed) | `secret-manager` |
| OpenTofu 1.12.6 | PASS (25 passed, 0 failed) | `secret-manager` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (20 passed, 0 failed) | `workflows` |
| OpenTofu 1.12.6 | PASS (20 passed, 0 failed) | `workflows` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (14 passed, 0 failed) | full aggregate (`make test-gcp-terraform-all STRICT=1`) |
| OpenTofu 1.12.6 | PASS (14 passed, 0 failed) | full aggregate (`TF_BIN=tofu`) |

Three boundaries are recorded, not worked around:

- The workflows module declares `hashicorp/google < 7.0`, so that case pins
  6.49.3. The FC6 capture is scoped to `google_workflows_workflow` and absent
  mapping rows (attributes postdating 6.x) are notes; the mapping carries the
  union of both provider versions.
- The module's `modules/simple_workflow` does not expose
  `deletion_protection`, whose provider default is `true`; the case removes the
  workflow from state and issues the DELETE the provider guards, then destroys
  the service account and IAM grant through Terraform.
- Secret Manager's `add_kms_permissions`/`add_pubsub_permissions` inputs drive
  `google_project_service_identity` (google-beta), which needs the unserved
  Service Usage `generateServiceIdentity`; the CMEK keys are passed directly.

Workflow CMEK and resource-level IAM are not implemented (README-GCP), and the
provider exposes no workflow IAM resource type; regional Secret Manager secrets
remain the recorded TF1-4 gap.

## TF3-4 — Cloud Scheduler and Cloud Tasks consumer cases (2026-11-07)

Two cases were added on one branch:

| case | module | pinned commit | what it drives |
|---|---|---|---|
| `scheduler` | `GoogleCloudPlatform/terraform-google-cloud-scheduler` | `ece5c3e87412` | a paused far-future job with a RetryConfig and a Pub/Sub target; schedule/description/retry/payload change; `:resume` and `:pause` transitions |
| `tasks` | `GoogleCloudPlatform/terraform-google-cloud-tasks` | `ba7dbc000bfb` | a queue with rate limits, retry config, Stackdriver logging config and one queue `iam_member`; rate/retry/IAM change; no task created |

Both runs below are green on the session commit (each run against a fresh
ephemeral emulator). The full aggregate — the fixture plus all fifteen module
cases — is also green under both binaries (`16 passed, 0 failed, 0 skipped`):

| binary | result | case |
|---|---|---|
| Terraform 1.16.3 | PASS (28 passed, 0 failed) | `scheduler` |
| OpenTofu 1.12.6 | PASS (28 passed, 0 failed) | `scheduler` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (21 passed, 0 failed) | `tasks` |
| OpenTofu 1.12.6 | PASS (21 passed, 0 failed) | `tasks` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (16 passed, 0 failed) | full aggregate (`make test-gcp-terraform-all STRICT=1`) |
| OpenTofu 1.12.6 | PASS (16 passed, 0 failed) | full aggregate (`TF_BIN=tofu STRICT=1`) |

Three boundaries are recorded, not worked around:

- Both modules declare `hashicorp/google < 7.0`, so the two cases pin 6.49.3
  (the scheduler module also requires `hashicorp/null`, pinned to 3.3.2). The
  FC6 capture is scoped to each case's own resources
  (`google_cloud_scheduler_job`; `google_cloud_tasks_queue` +
  `google_cloud_tasks_queue_iam_member`) and absent 7.x-only mapping rows are
  notes.
- The scheduler case drives the **Pub/Sub target**, not HTTP: provider 6.49.3
  panics in `flattenCloudSchedulerJobHttpTargetHeaders` when the response omits
  an empty `httpTarget.headers` map, while real Cloud Scheduler answers
  `headers: {}` (the provider's own 6.49.3 acceptance test uses a header-less
  HTTP target and 7.x added a nil guard). That is TF3-4-1
  (`fix/gcp-scheduler-http-target-headers`), so the HTTP-target path is
  unmeasured until it is fixed.
- The module's default `retry_config = { retry_count = 0 }` is dropped from the
  create body by the provider and the emulator does not synthesize the
  documented RetryConfig defaults, so that input re-plans
  `+ retry_config { retry_count = 0 }` forever; the case creates a non-default
  retry config and default synthesis is TF3-4-2
  (`fix/gcp-scheduler-retry-config-defaults`). The task queue's omitted
  `maxBurstSize` reads back 0 where real Cloud Tasks defaults 100 (TF3-4-3,
  `fix/gcp-tasks-queue-rate-limit-defaults`); it is computed-only in the
  provider, so it cannot permadiff, but a raw reader sees the wrong value.

The paused create also keeps the case off TF2-12 (the provider always issues
`:resume` after an enabled create, and the emulator's resume is not idempotent
on an ENABLED job); the case still exercises both custom methods by unpausing
and re-pausing an existing job.

## TF3-5 — Dataproc, service-accounts and IAM consumer cases (2026-11-07)

Three cases were added on one branch:

| case | module | pinned commit | what it drives |
|---|---|---|---|
| `dataproc` | `GoogleCloudPlatform/terraform-google-dataproc` | `56c7c7ed630f` | a GCE-shaped cluster (master/worker machine types and counts, software image) plus a root-module pyspark job; labels + worker-count update |
| `service-accounts` | `terraform-google-modules/terraform-google-service-accounts` | `570caafff32d` | three accounts with two project roles each; role-set change under `-parallelism=10` |
| `iam` | `terraform-google-modules/terraform-google-iam` (`modules/projects_iam`, `modules/storage_buckets_iam`, `modules/pubsub_topics_iam`) | `9b9eacfa2b4f` | project, bucket and Pub/Sub topic IAM in additive mode; role-set change under `-parallelism=10` (no lost updates) |

Both runs below are green on the session commit (each run against a fresh
ephemeral emulator). The full aggregate — the fixture plus all eighteen module
cases — is also green under both binaries (`19 passed, 0 failed, 0 skipped`,
`STRICT=1`, so a SKIP would have failed the run):

| binary | result | case |
|---|---|---|
| Terraform 1.16.3 | PASS (18 passed, 0 failed) | `dataproc` |
| OpenTofu 1.12.6 | PASS (17 passed, 0 failed) | `dataproc` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (21 passed, 0 failed) | `service-accounts` |
| OpenTofu 1.12.6 | PASS (20 passed, 0 failed) | `service-accounts` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (31 passed, 0 failed) | `iam` |
| OpenTofu 1.12.6 | PASS (30 passed, 0 failed) | `iam` (`TF_BIN=tofu`) |
| Terraform 1.16.3 | PASS (19 passed, 0 failed) | full aggregate (`make test-gcp-terraform-all STRICT=1`) |
| OpenTofu 1.12.6 | PASS (19 passed, 0 failed) | full aggregate (`TF_BIN=tofu STRICT=1`) |

The pass counts differ by one between the binaries only because the tofu runs
were made with `JC_TF_CAPTURE=0` for a faster local loop; the aggregate runs
above include the FC6 capture and pass under both.

Boundaries, recorded rather than worked around:

- The Dataproc module's `main.tf` unconditionally reads
  `data.google_compute_default_service_account` and grants it
  `roles/dataproc.worker` through `google_project_iam_member`. The data source
  calls Compute `projects.get` (`GET /compute/v1/projects/{project}`), which the
  emulator does not serve yet — TF2-1 (`fix/gcp-compute-projects-get`). The case
  prunes both resources with `-target` on plan, apply and destroy, so the module
  still renders and updates its cluster and the root module's job while the IAM
  prerequisite stays unmeasured. `virtual_cluster_config` is left null: the
  fixture already covers the GKE-shaped cluster and its NodePool reconciliation.
- `service-accounts` passes the module's documented `"project=>role"` format;
  a bare `"roles/viewer"` makes the module parse the role as both the project
  and the role. `display_name` is set explicitly because the provider's
  attribute is Optional (not Computed) while the emulator defaults an omitted
  `serviceAccount.displayName` to the account id — TF2-14
  (`fix/gcp-iam-serviceaccount-display-name`); the module's own default is a
  non-empty display name, so a real consumer does not hit it. Billing/org IAM
  and key generation are disabled (outside the project-scoped surface).
- The IAM repo's submodules reference `../helper`; a plain local-path module
  source makes Terraform treat the submodule directory as the package root and
  rejects the relative helper path. The case uses an offline
  `git::file://<checkout>//modules/<name>?ref=<HEAD>` source, which presents the
  whole repo as the package the way the registry does. Folder/organization IAM,
  the audit-config submodule and the `member_iam`/`service_accounts_iam`
  submodules are out of scope (the emulator is project-scoped, wire-contract
  §8); the root module creates the accounts it binds.

## TF3-6 — Cloud SQL module, Datastore and GKE plan-only tier (2026-11-07)

Three cases and a driver mode were added on one branch:

| case | module | pinned commit | tier | what it drives |
|---|---|---|---|---|
| `sql-db` | `terraform-google-modules/terraform-google-sql-db` (`modules/postgresql`) | `75b57dc6838e` (v28.3.0) | apply | the module's PostgreSQL instance + default database + user; tier and label update |
| `gke` | `terraform-google-modules/terraform-google-kubernetes-engine` | `a19597ac18ad` (v47.0.0) | plan-only | the regional cluster, default node pool, node service account, and the `google_container_engine_versions` read |
| `datastore` | `terraform-google-modules/terraform-google-cloud-datastore` | `1616a282bbf4` (v2.0.1) | plan-only | the rendered `index.yaml` and the gcloud `null_resource` |

The driver (`run-module.sh`) gained the **plan-only tier**: a case sets
`PLAN_ONLY=1` and calls `plan_only_phase` (and `plan_includes` for resource
addresses). A run is `init → validate → plan`; there is no apply, destroy or FC6
capture. A completed plan is the acceptance, not a clean one; a provider panic
or an unrouted request fails, an API diagnostic is recorded only when the case
opts in with `PLAN_ONLY_ALLOW_DIAGNOSTICS=1`. A plan-only run that creates state
is failed and destroyed. `PLAN_ONLY=1` in the environment flips the applying
`sql-db` case into the same mode (`make test-gcp-terraform-plan-only
CASE=sql-db`).

`sql-db` is a full apply: it converged on its first run (5 resources, then a
tier/label update), so the plan-only measurement was promoted to the standard
contract. `gke` and `datastore` stay plan-only: GKE's control plane is
metadata-only, and the Datastore module has no provider resource at all (it
shells out to `gcloud`). `gke`'s plan diagnostics are empty; its two
`google_container_engine_versions` reads reach Container `GetServerConfig`.

The FC6 capture learned the sqladmin v1beta4 paths (instance/database/user
create/update), and the mapping gained 207 `google_sql_*` rows with the 31
attributes the module actually sends classified `no-catalog` (sqladmin is not in
the FC1 field catalog; the sent fields are stored and echoed by the emulator).
`tools/tfcapture` has a path-rule test for the new routes.

Runs (each against a fresh ephemeral emulator; `STRICT=1`, so a SKIP fails):

| binary | result | case |
|---|---|---|
| Terraform 1.16.3 | PASS (15 passed, 0 failed) | `sql-db` |
| Terraform 1.16.3 | PASS (7 passed, 0 failed) | `gke` (plan-only) |
| Terraform 1.16.3 | PASS (6 passed, 0 failed) | `datastore` (plan-only) |
| Terraform 1.16.3 | PASS (22 passed, 0 failed, 0 skipped) | full aggregate (`make test-gcp-terraform-all STRICT=1`) |
| OpenTofu 1.12.6 | PASS (15 passed, 0 failed) | `sql-db` (`TF_BIN=tofu`) |
| OpenTofu 1.12.6 | PASS (7 passed, 0 failed) | `gke` (`TF_BIN=tofu`) |
| OpenTofu 1.12.6 | PASS (6 passed, 0 failed) | `datastore` (`TF_BIN=tofu`) |
| OpenTofu 1.12.6 | PASS (22 passed, 0 failed, 0 skipped) | full aggregate (`TF_BIN=tofu STRICT=1`) |

Boundaries, recorded rather than worked around:

- The Datastore Admin surface (`projects.indexes.*`, export/import and the admin
  operations) is not served; the CFT module manages indexes only through
  `gcloud datastore indexes`, so no provider path exists to exercise it — TF3-6-1
  and TF3-6-2 (`no fix` in the wave plan).
- The GKE module's apply path is not measured (plan-only): the fixture's raw
  `google_container_cluster` remains the apply gate for the served control
  plane — TF3-6-3 (`no fix`).
- `sql-db` sets `deletion_protection = false` (the provider guard, real Cloud
  SQL refuses the delete too) and leaves read replicas, private service access,
  CMEK/autokey, password policy, insights and PITR at their defaults; those
  reach surfaces outside this family's measured scope.

## TF5 — Lifecycle depth (2026-11-07)

One branch, one driver, five cases: `storage`, `pubsub`, `kms`, `iam-parallel`
and `tftest`. The aggregate starts one ephemeral emulator and runs the full
apply/destroy lifecycle set against it; the committed
`lifecycle-tftest/` module runs `terraform test` for the provider-visible
assertions.

```bash
make test-gcp-terraform-lifecycle STRICT=1                 # Terraform
make test-gcp-terraform-lifecycle TF_BIN=tofu STRICT=1     # OpenTofu
```

Runs (each against a fresh ephemeral emulator; `STRICT=1`):

| binary | result | cases |
|---|---|---|
| Terraform 1.16.3 | PASS (5 passed, 0 failed, 0 skipped) | `storage` 33 assertions, `pubsub` 35, `kms` 33, `iam-parallel` 11, `tftest` 2 |
| OpenTofu 1.12.6 | PASS (5 passed, 0 failed, 0 skipped) | the same five cases and assertion counts |

What each case proves:

| case | proof |
|---|---|
| `storage` | import → clean plan; `-replace` re-creates the bucket (new `timeCreated`, `metageneration=1`); a ForceNew rename leaves the old bucket 404; an out-of-band label PATCH is detected and the managed key converges; duplicate create 409, invalid name 400 and a provider-surfaced 409; destroy leaves 404 |
| `pubsub` | topic and subscription import → clean plan; `refresh-only` clean (labels normalization classified); `-replace` the subscription; ForceNew rename leaves the old subscription 404; `updateMask=labels` drift converges; duplicate 409, unsupported mask 400, invalid duration 400 and a provider-surfaced 409 |
| `kms` | key ring and key import → clean plan; label drift; crypto-key `-replace` is an expected 409 (the API has no delete; re-import and rotation repair); duplicate 409, invalid algorithm 400, unsupported mask 400; after destroy the key ring/key are still readable and the version is `DESTROY_SCHEDULED` |
| `iam-parallel` | 10 accounts + 10 project bindings at `-parallelism=10`; the policy holds all 10 (a lost update fails); destroy at parallelism 10 leaves no member |
| `tftest` | `terraform test` conditions on computed ids, `self_link`, `url`, `time_created`, topic/subscription ids, crypto-key `primary.state` and `version_template` |

### Import support (measured)

Generated by the lifecycle run into `<LOG_DIR>/<case>-import.tsv`; the nightly
artifact `gcp-terraform-nightly`/`gcp-opentofu-nightly` carries it.

| resource | import id | plan after import | classification |
|---|---|---|---|
| `google_storage_bucket` (no labels) | bucket name | clean | supported |
| `google_storage_bucket` (with labels) | bucket name | `labels`/`terraform_labels` only | provider-label-artifact |
| `google_pubsub_topic` (no labels) | `projects/{p}/topics/{t}` | clean | supported |
| `google_pubsub_topic` (with labels) | `projects/{p}/topics/{t}` | `labels`/`terraform_labels` only | provider-label-artifact |
| `google_pubsub_subscription` | `projects/{p}/subscriptions/{s}` | clean | supported |
| `google_kms_key_ring` | `projects/{p}/locations/{l}/keyRings/{r}` | clean | supported |
| `google_kms_crypto_key` | `…/cryptoKeys/{k}` | clean | supported |

The import gate is `state rm` → `terraform import` → `plan -detailed-exitcode`.
The strict form requires exit 0. `provider-label-artifact` accepts a non-empty
plan only when every changed attribute is `labels`, `terraform_labels` or
`effective_labels`; any other attribute fails. That split is the provider's
`tpgresource.SetLabels` behaviour (it keeps only keys already present in state,
and an import starts empty), not an emulator gap.

### Recorded gaps and boundaries

| ID | classification | evidence |
|---|---|---|
| TF5-1 | `fix` — `fix/gcp-storage-bucket-validation` | `storage.buckets.insert` accepts `storageClass=BOGUS`, `publicAccessPrevention=BOGUS` and `location=NOT-A-LOCATION` with HTTP 200; real GCS rejects each with 400. The case records the measurement and uses the invalid-bucket-name 400 that the emulator does validate. |
| TF5-2 | `fix` — `fix/gcp-storage-bucket-labels-merge` | `storage.buckets.patch` replaces the whole `labels` object instead of merging it: patching `{"a":"9"}` on `{a,b}` drops `b`, where real GCS keeps it (null deletes one key). Low impact through Terraform (the provider sends the effective map), wrong for a raw client. |
| TF5-3 | `no fix` (upstream) | A Cloud KMS crypto key cannot be deleted; the provider destroys its versions and drops it from state, so `-replace` re-creates the same name → 409. Real KMS behaves the same. The gate asserts the failure, re-imports and repairs the rotation schedule. |
| TF5-4 | `no fix` (no consumer) | A `DELETE` on a key ring answers 501 `UNIMPLEMENTED`; Cloud KMS defines no delete method and the provider never sends one. Recorded, not gated. |
| TF5-5 | `no fix` (provider artifact) | A `refresh-only` plan reports `labels = {}` for a resource with no managed labels (`SetLabels` normalizes an absent map). `refresh_phase labels-artifact` accepts exactly that attribute. |
| TF5-6 | `no fix` (provider artifact) | An unmanaged extra label key is filtered out of `labels` by `SetLabels` and preserved by the update; the gate asserts the managed key converges and records the extra key. |

Expansion after the representative set: `secretmanager`, `bigquery`,
`cloudrun`, `dataproc`, `cloudsql`, `firestore`, `clouddns` — each has an import
id and a ForceNew identity change; the table is in the suite README.

## What this does not prove

This is emulator-only evidence. It says nothing about authz/IAM conditions,
quotas/throttling, the org/folder hierarchy, the billing-account catalog, LRO
timing, or the data planes of the metadata-only services (Compute, GKE, Cloud
SQL, Memorystore). TF6 records the same corpus against real GCP and writes the
trust boundary in `docs/gcp/terraform-compatibility.md`; until then, a green row
means "converges against the emulator", not "identical to GCP".
