# GCP Terraform / OpenTofu compatibility suite

Opt-in integration suite that drives the real `hashicorp/google` Terraform
provider against a running `jaiscloud-gcp`, then asserts the resulting
resources through the emulator's REST API. It exists because Terraform is a
*stateful orchestrator*: it exercises cross-operation behavior (IAM
read-modify-write with etag OCC, update/patch semantics, read-after-write
consistency, and destroy/replacement lifecycle) that single-operation SDK tests
do not.

## Scope

Only services `jaiscloud-gcp` implements are declared in `main.tf`:

- Cloud Storage (bucket + object)
- IAM service account
- Secret Manager (secret + version + IAM member)
- Cloud KMS (key ring + crypto key)
- Pub/Sub (topic + subscription + topic/subscription IAM members)
- Cloud SQL for PostgreSQL (instance + database + user + client SSL certificate)
- Dataproc Metastore (service with an explicit `port` + federation + database
  IAM member)
- Dataproc (a Dataproc-on-GKE virtual cluster with two node-pool targets, plus a
  Compute-Engine `cluster_config` cluster)
- GKE (`google_container_cluster`, the Dataproc-on-GKE prerequisite)
- BigQuery (dataset + table, plus the NF31 dataset-scoped routine and
  table-scoped row access policy; the suite asserts create/get/list, an
  in-place update of both, and the read-after-delete)
- Cloud Resource Manager project-level IAM
  (`/v1/projects/{project}:getIamPolicy`) and project lookup
- Service Usage (`/v1/projects/{project}/services`)
- Cloud Billing — the `data "google_project"` `billing_account` read
  (`projects.getBillingInfo`) and `google_project`'s `billing_account`
  association round-trip (`projects.updateBillingInfo`)
- Cloud Run (a `google_cloud_run_v2_service`: the service + revision control
  plane, with a container `readiness_probe` — the field the pinned module's v2
  submodule does not expose; the consumer module case is
  `module-cases/cloudrun.sh`)
- Cloud Workflows (a `google_workflows_workflow`; the consumer module case is
  `module-cases/workflows.sh`)

Deliberately **not declared** because the emulator does not serve them:

- **Cloud Run Jobs, WorkerPools and the v1 surface** (`google_cloud_run_service`,
  v1 IAM, domain mappings). The v2 service/revision control plane is served over
  both transports (README-GCP and `docs/gcp/wire-contract.md` §5). A request to
  `/v2/.../jobs` or `/v2/.../workerPools` is not claimed by the emulator (404
  `UnknownService`), and a v1 domain-mapping request answers 404 "unsupported
  operation"; those are recorded boundaries, not untested gaps. Traffic targets
  are stored and reflected in `trafficStatuses`, but the split is not enforced,
  so the case leaves `traffic` empty (TF1-6 records the v1 boundary).

`google_container_cluster` was excluded until DPG37: the provider's cluster read
dereferences `legacyAbac.enabled`, `networkConfig.network`,
`shieldedNodes.enabled` and `defaultMaxPodsConstraint.maxPodsPerNode`
unconditionally and panicked (`SIGSEGV` in `resourceContainerClusterRead`) when
the emulator omitted them. The GKE cluster read now renders all four (plus the
`loggingService`/`monitoringService` defaults), so the resource is in `main.tf`,
and a second `plan` after `apply` must report no changes — that is the
permadiff assertion for those computed fields.

The GKE resource also needs a `container.`-prefixed host: the emulator routes
`container.v1` by host (or by a `/container/` path prefix) because the bare
`/v1/projects/{p}/locations/{l}/clusters` path belongs to Managed Kafka, and the
provider keeps only the scheme+host of `container_custom_endpoint` (its trailing
path segment exists only to satisfy the provider's endpoint regexp). `run.sh`
therefore derives `container.<host>` from `ENDPOINT` (e.g.
`http://container.localhost:8080/container/`) and fails loudly if it does not
resolve; add `127.0.0.1 container.localhost` to the hosts file on a host without
`*.localhost` resolution.

The plain `cluster_config` (Compute-Engine) shape is included: it used to panic
the provider's read-back (`flattenSoftwareConfig` on an absent
`config.softwareConfig`, DPG38), so the emulator now reports the service
defaults a real cluster always carries and `run.sh` asserts them.

Keep `main.tf` in sync with the emulator's supported surface: one unimplemented
resource aborts the whole `apply`, so nothing else runs.

## Requirements

- `terraform` (validated with 1.16.x) or `tofu` (validated with 1.12.x)
- `curl`

The provider is fetched from the registry on first `init` (`hashicorp/google`
`~> 7.36`; validated with 7.46.1).

## Run

Preferred — the Makefile starts and stops the emulator for you:

```bash
make test-gcp-terraform     # requires terraform
make test-gcp-opentofu      # requires tofu
make test-gcp-terraform-all # fixture + every pinned module case, one emulator
make test-gcp-terraform-all TF_BIN=tofu STRICT=1  # same set under OpenTofu; a SKIP fails
make test-gcp-terraform-lifecycle            # import/refresh/replace/drift/negative/parallel (TF5)
make test-gcp-terraform-lifecycle TF_BIN=tofu STRICT=1
make test-gcp-terraform-plan-only CASE=gke        # plan-only tier: no apply, no destroy
```

Both targets are opt-in: if the binary is not installed they print `SKIP` and
exit 0. They build `jaiscloud-gcp`, start it ephemeral (REST `:8080`,
gRPC `:8081`), run the suite, and stop it.

Against an already-running emulator:

```bash
tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project
TF_BIN=tofu tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project
```

## Real-GCP differential (TF6)

Every gate above is emulator-only. `differential/` closes that gap: a small
curated corpus is recorded against a real project (ADC, no custom endpoints) and
replayed against the emulator, diffing the normalized post-apply state, the
first plan's diff set and the re-plan. Every difference must be classified
(`emulator-gap` with a ledger row, `provider-quirk`, `environment` or
`accepted-risk`); an unclassified one fails the replay.

```bash
make test-gcp-terraform-differential          # offline replay vs an ephemeral emulator
make test-gcp-terraform-differential STRICT=1 # nightly: a missing Terraform fails
make record-gcp-terraform-differential        # credentialed: re-record the goldens
make check-gcp-terraform-differential         # no Terraform: corpus/golden/manifest hygiene
```

See [differential/README.md](differential/README.md) for the corpus, golden
format and normalization rules, and
[`docs/gcp/terraform-compatibility.md`](../../../../docs/gcp/terraform-compatibility.md)
for the trust boundary: what local Terraform evidence proves and what it does
not.

## Pinned consumer-module gates

`run.sh` drives a fixture *we* wrote, so it only proves the surface we already
declared. `run-module.sh` drives a **third-party** module instead: the real
consumer is the specification. It is a driver with one case file per module
under `module-cases/`; each case pins a repo/commit, generates a throwaway root
module, and requires the module's lifecycle to converge.

| case | module | pinned commit | what the gate proves |
|---|---|---|---|
| `pubsub` | [`terraform-google-pubsub`](https://github.com/terraform-google-modules/terraform-google-pubsub) | `6cf1860` | `data.google_project` (`projects.getBillingInfo`), topic + pull subscription + IAM member, `plan` **No changes** |
| `storage` | [`terraform-google-cloud-storage`](https://github.com/terraform-google-modules/terraform-google-cloud-storage) | `c7effc0be0f4` | `autoclass`, `hierarchicalNamespace`, `website`, `logging`, `customPlacementConfig` and `ipFilter` read back; mutated `autoclass.enabled` + labels/website/logging, then labels/logging/`ip_filter` cleared; the `modules/simple_bucket` submodule forces the `google_storage_project_service_account` data source read (`projects.serviceAccount.get`) and a module-owned `google_storage_hmac_key` reads back `ACTIVE` (`projects.hmacKeys.*`); each phase ends with `plan` **No changes** |
| `memorystore` | [`terraform-google-memorystore`](https://github.com/terraform-google-modules/terraform-google-memorystore) | `5415d5ccca28` | `authEnabled`, `connectMode`, `transitEncryptionMode`, `readReplicasMode`, `maintenancePolicy` and `persistenceConfig` read back (the two ForceNew modes no longer replace the instance); AUTH enabled then disabled, maintenance window/snapshot period changed, labels changed then cleared; each phase ends with `plan` **No changes** |
| `functions` | [`terraform-google-event-function`](https://github.com/terraform-google-modules/terraform-google-event-function) | `9f2e26ae3b73` | `ingressSettings` (`ALLOW_ALL` → `ALLOW_INTERNAL_ONLY`), the event trigger's default `failurePolicy` and `failurePolicy.retry`, and `environment_variables` read back; ingress/env changed and cleared, retry toggled; each phase ends with a `plan` whose only remaining diff is the recorded storage-object gap |
| `clouddns` | [`terraform-google-cloud-dns`](https://github.com/terraform-google-modules/terraform-google-cloud-dns) | `35cd6de8e647` | `privateVisibilityConfig` (VPC networks + GKE clusters), `forwardingConfig` (targets incl. `forwardingPath`), `peeringConfig` and `dnssecConfig` (state, `nonExistence`, both default key specs) read back; private description/labels/network list changed, then forwarding, peering and public-with-DNSSEC zones created and DNSSEC turned on; each phase ends with `plan` **No changes** |
| `cloud-operations` | [`terraform-google-cloud-operations`](https://github.com/terraform-google-modules/terraform-google-cloud-operations) (`modules/simple-uptime-check`) | `8c3e37951bb4` | the alert policy's `alertStrategy` (`autoClose`, `notificationRateLimit.period`) and `userLabels` read back alongside the uptime check; `auto_close`/rate limit and labels changed, then the strategy and labels cleared; each phase ends with `plan` **No changes** |
| `kms` | [`terraform-google-kms`](https://github.com/terraform-google-modules/terraform-google-kms) | `ef6e0eda7032` | the crypto key's `versionTemplate.algorithm` read back (the module always emits the `version_template` block); `key_algorithm` changed `HMAC_SHA256` → `HMAC_SHA512` (mask `versionTemplate.algorithm`) and back, with a `cryptoKeyVersion` created after the change carrying `HMAC_SHA512`; each phase ends with `plan` **No changes** |
| `firestore` | [`terraform-google-firestore`](https://github.com/GoogleCloudPlatform/terraform-google-firestore) | `50d170b9ebfc` | the database's `cmekConfig.kmsKeyName` read back (the module emits `cmek_config` only when `kms_key_name != null`), so the ForceNew block stops forcing a replacement; a non-ForceNew PITR change re-applies, and a second module instance with `kms_key_name = null` reports no `cmekConfig` at all; a third phase in its own working directory/state creates the implicit `(default)` database (`database_id = "(default)"`, the Firebase shape) and reads `locationId` back; a fourth phase destroys a database and re-applies the same id (TGM13: immediate reuse, where real GCP blocks it for ~5 minutes); each phase ends with `plan` **No changes** |
| `cloudrun` | [`terraform-google-cloud-run`](https://github.com/GoogleCloudPlatform/terraform-google-cloud-run) (`modules/v2`) | `4ce640a2a519` | the v2 service, its module-created service account and the `roles/run.invoker` IAM member apply; the service reads back labels/ingress/description, the container image/env/port and the service account; `revisions.list` carries revision `00001`; a second `plan` is **No changes**; changing ingress, a service label and the container env PATCHes in place, mints revision `00002` and re-plans **No changes**; destroy leaves the service and its revisions `404` |
| `bigquery` | [`terraform-google-bigquery`](https://github.com/terraform-google-modules/terraform-google-bigquery) | `e11db0dcd597` | the dataset + table create with the module's defaults and the `access` ACL; the dataset reads back its description, location, labels, `defaultTableExpirationMs` and the OWNER/`projectOwners` entry, the table its reference, friendly name, labels and schema; a second `plan` is **No changes**; a new schema column, dataset/table labels, the description and a second ACL entry (READER/`projectReaders`) update in place and re-plan **No changes**; destroy leaves the dataset and table `404` |
| `secret-manager` | [`terraform-google-secret-manager`](https://github.com/GoogleCloudPlatform/terraform-google-secret-manager) | `77fca80ccd6a` | two module secrets: one automatic-replication secret with a `global` CMEK key, a rotation schedule and its Pub/Sub topic, one user-managed secret with two replicas (one with a regional CMEK key), a version each and an authoritative IAM binding with two members; GET reads back the replication arm/keys, labels, topics, rotation and the version payload, `getIamPolicy` reads the members; a second `plan` is **No changes**; labels, the rotation period, the automatic secret's payload (a version replacement) and the IAM member list update in place and re-plan **No changes**; destroy leaves the secrets and versions `404` |
| `workflows` | [`terraform-google-cloud-workflows`](https://github.com/GoogleCloudPlatform/terraform-google-cloud-workflows) (`modules/simple_workflow`) | `4c431381b5d8` | the workflow applies with a description, labels, source and a root-module-owned service account + `roles/workflows.invoker` project grant; GET reads back name, `state: ACTIVE`, description, labels, source, service account and revision `000001`; a second `plan` is **No changes**; a new source, description and label update in place, mint revision `000002` and re-plan **No changes**; the workflow is deleted through the API (the submodule exposes no `deletion_protection`) and the service account and grant are destroyed |
| `scheduler` | [`terraform-google-cloud-scheduler`](https://github.com/GoogleCloudPlatform/terraform-google-cloud-scheduler) | `ece5c3e87412` | two jobs — a header-less HTTP target and a Pub/Sub target — apply enabled with far-future schedules; GET reads back each schedule, `timeZone`, `state: ENABLED`, the retry config, the HTTP target's `uri`/`httpMethod`/`body` and the server-filled `headers` (`User-Agent` + `Content-Type`), and `pubsubTarget.topicName`/`data`/`attributes`; a second `plan` is **No changes**; a schedule/description/retry/target-payload change updates in place and re-plans **No changes**; pausing calls `:pause` and resuming calls `:resume`, each re-planning clean; destroy leaves both jobs and the topic `404` |
| `tasks` | [`terraform-google-cloud-tasks`](https://github.com/GoogleCloudPlatform/terraform-google-cloud-tasks) | `ba7dbc000bfb` | a queue with rate limits, retry config, Stackdriver logging config and one `iam_member` applies; GET reads back `state: RUNNING`, every limit and the IAM binding, and the task list is empty (no dispatch); a second `plan` is **No changes**; a rate limit, a retry limit and the IAM member update in place and re-plan **No changes**; destroy leaves the queue `404` |
| `dataproc` | [`terraform-google-dataproc`](https://github.com/GoogleCloudPlatform/terraform-google-dataproc) | `56c7c7ed630f` | the module renders a GCE-shaped cluster (master/worker counts and machine types, software image version, labels) and a root-module-owned pyspark job is submitted into it; GET reads back the cluster name/labels/config and the job placement/pyspark driver/`DONE` state; a second `plan` is **No changes**; labels and the worker count update in place and re-plan **No changes**; destroy leaves the cluster and job `404`. The module's internal default-service-account data source is pruned with `-target` (Compute `projects.get` is served; the IAM read on the account is TF2-1-1) |
| `service-accounts` | [`terraform-google-service-accounts`](https://github.com/terraform-google-modules/terraform-google-service-accounts) | `570caafff32d` | three service accounts and a project binding per (account, role) apply; the accounts read back their email/display name, and the project policy contains every member under `roles/viewer` and `roles/storage.objectViewer`; a second `plan` is **No changes**; the role set changes and the new role gains every member while `roles/viewer` keeps them and the replaced role is gone; every phase runs with `-parallelism=10`; destroy leaves the accounts `404` |
| `iam` | [`terraform-google-iam`](https://github.com/terraform-google-modules/terraform-google-iam) (`modules/projects_iam`, `modules/storage_buckets_iam`, `modules/pubsub_topics_iam`) | `9b9eacfa2b4f` | project, bucket and Pub/Sub topic IAM are driven in additive mode with multiple members per role; the three policies read back every member; a second `plan` is **No changes**; a role-set change adds the new members without dropping the survivors (the etag OCC path), and the replaced roles are gone; every phase runs with `-parallelism=10`; destroy leaves the bucket, topic and accounts `404` |
| `sql-db` | [`terraform-google-sql-db`](https://github.com/terraform-google-modules/terraform-google-sql-db) (`modules/postgresql`) | `75b57dc6838e` | a PostgreSQL instance with the module's default `settings` block (tier, availability, backup configuration, maintenance window, disk settings, labels) applies through `google-beta`; the instance reads back `RUNNABLE` with its tier and labels, the default database and user exist, a second `plan` is **No changes**; a tier + label change PATCHes in place and re-plans **No changes**; destroy leaves the instance `404`. `PLAN_ONLY=1` runs the same module as init → validate → plan only |
| `gke` | [`terraform-google-kubernetes-engine`](https://github.com/terraform-google-modules/terraform-google-kubernetes-engine) | `a19597ac18ad` | **plan-only**: the regional cluster, the default node pool and the node service account are in the plan, and the module's `data.google_container_engine_versions.{region,zone}` read reaches Container `GetServerConfig`; no apply/destroy (GKE's control plane is metadata-only) |
| `datastore` | [`terraform-google-cloud-datastore`](https://github.com/terraform-google-modules/terraform-google-cloud-datastore) | `1616a282bbf4` | **plan-only**: the module renders the caller's `index.yaml` (`local_file`) and its gcloud `null_resource`; the plan carries the requested index kind and properties. No apply: the module shells out to `gcloud datastore indexes` and the Datastore Admin index surface is not served (TF3-6-1) |

The storage, memorystore, functions, clouddns, cloud-operations, kms, firestore, cloudrun, bigquery, secret-manager, dataproc, service-accounts and iam cases pin provider `hashicorp/google = 7.46.2` (the clouddns module also declares `hashicorp/google-beta`, pinned to the same version; the cloudrun module creates the service through `google-beta` and creates its service account through the IAM API, so it pins and points both providers at the emulator; the secret-manager module requires google-beta as well). The `sql-db` case pins `google` and `google-beta` `= 7.46.2` plus `random = 3.7.2` and `null = 3.2.4`; `gke` pins `google = 7.46.2`, `kubernetes = 2.38.0` and `random = 3.7.2`; `datastore` has no provider resource, so it pins only the client-side `local = 2.5.2` and `null = 3.2.4`. The `workflows`, `scheduler` and `tasks` cases pin `= 6.49.3` instead: their pinned modules declare `hashicorp/google < 7.0`, so 7.46.2 cannot satisfy them (the scheduler module also requires `hashicorp/null`, pinned to `3.3.2`). The FC6 capture is scoped to that case's own resource (`CAPTURE_RESOURCES`) and tolerates mapping rows the 6.x schema does not declare (`CAPTURE_ALLOW_MISSING`) — the shared mapping carries the union of both provider versions, and re-checking unrelated 7.x-only attributes under 6.x would fail for reasons the case cannot measure. The cloud-operations case exercises `alertStrategy`; the `severity` enum the same change added is covered by the unit/transport tests, since the module exposes no variable for it. The kms case passes `key_rotation_period = null` because CryptoKey rotation is ENCRYPT_DECRYPT-only (the module's `7776000s` default would be rejected for a MAC key by real GCP too); the rotation mask path is covered by the unit/transport tests. The firestore case creates the CMEK key with `google_kms_key_ring` + `google_kms_crypto_key` and passes its id as the module's `kms_key_name`, mirroring a real config; it sets `delete_protection_state = "DELETE_PROTECTION_DISABLED"` because the module's `DELETE_PROTECTION_ENABLED` default would block destroy (real GCP refuses too). Its `(default)` phase runs first and in its own working directory/state because a read of the default materialises it, and a materialised default is a live record — a later create would be `ALREADY_EXISTS`. Its recreate phase destroys a plain database and re-applies the same id in its own state; the emulator allows immediate reuse (TGM13), whereas real GCP refuses to reuse a deleted database id for about five minutes.

Per-case targets are **opt-in** for two reasons: each needs a one-time module
clone (this repo never clones from the network in a gate), and they resolve the
provider from the registry. They are still safe to invoke unconditionally — with
no checkout or no `terraform` they print `SKIP` and exit 0.

Clone a module once (the only step that needs the network), then run its target:

```bash
git clone https://github.com/terraform-google-modules/terraform-google-pubsub \
  "$HOME/.cache/terraform-modules/terraform-google-pubsub"
git -C "$HOME/.cache/terraform-modules/terraform-google-pubsub" checkout 6cf1860
make test-gcp-terraform-pubsub

git clone https://github.com/terraform-google-modules/terraform-google-cloud-storage \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-storage"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-storage" checkout c7effc0be0f4
make test-gcp-terraform-storage

git clone https://github.com/terraform-google-modules/terraform-google-memorystore \
  "$HOME/.cache/terraform-modules/terraform-google-memorystore"
git -C "$HOME/.cache/terraform-modules/terraform-google-memorystore" checkout 5415d5ccca28
make test-gcp-terraform-memorystore

git clone https://github.com/terraform-google-modules/terraform-google-event-function \
  "$HOME/.cache/terraform-modules/terraform-google-event-function"
git -C "$HOME/.cache/terraform-modules/terraform-google-event-function" checkout 9f2e26ae3b73
make test-gcp-terraform-functions

git clone https://github.com/terraform-google-modules/terraform-google-cloud-dns \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-dns"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-dns" checkout 35cd6de8e647
make test-gcp-terraform-clouddns

git clone https://github.com/terraform-google-modules/terraform-google-cloud-operations \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-operations"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-operations" checkout 8c3e37951bb4
make test-gcp-terraform-cloud-operations

git clone https://github.com/terraform-google-modules/terraform-google-kms \
  "$HOME/.cache/terraform-modules/terraform-google-kms"
git -C "$HOME/.cache/terraform-modules/terraform-google-kms" checkout ef6e0eda7032
make test-gcp-terraform-kms

git clone https://github.com/GoogleCloudPlatform/terraform-google-firestore \
  "$HOME/.cache/terraform-modules/terraform-google-firestore"
git -C "$HOME/.cache/terraform-modules/terraform-google-firestore" checkout 50d170b9ebfc
make test-gcp-terraform-firestore

git clone https://github.com/GoogleCloudPlatform/terraform-google-cloud-run \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-run"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-run" checkout 4ce640a2a519
make test-gcp-terraform-cloudrun

git clone https://github.com/terraform-google-modules/terraform-google-bigquery \
  "$HOME/.cache/terraform-modules/terraform-google-bigquery"
git -C "$HOME/.cache/terraform-modules/terraform-google-bigquery" checkout e11db0dcd597
make test-gcp-terraform-bigquery

git clone https://github.com/GoogleCloudPlatform/terraform-google-secret-manager \
  "$HOME/.cache/terraform-modules/terraform-google-secret-manager"
git -C "$HOME/.cache/terraform-modules/terraform-google-secret-manager" checkout 77fca80ccd6a
make test-gcp-terraform-secret-manager

git clone https://github.com/GoogleCloudPlatform/terraform-google-cloud-workflows \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-workflows"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-workflows" checkout 4c431381b5d8
make test-gcp-terraform-workflows

git clone https://github.com/GoogleCloudPlatform/terraform-google-cloud-scheduler \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-scheduler"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-scheduler" checkout ece5c3e87412
make test-gcp-terraform-scheduler

git clone https://github.com/GoogleCloudPlatform/terraform-google-cloud-tasks \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-tasks"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-tasks" checkout ba7dbc000bfb
make test-gcp-terraform-tasks

git clone https://github.com/GoogleCloudPlatform/terraform-google-dataproc \
  "$HOME/.cache/terraform-modules/terraform-google-dataproc"
git -C "$HOME/.cache/terraform-modules/terraform-google-dataproc" checkout 56c7c7ed630f
make test-gcp-terraform-dataproc

git clone https://github.com/terraform-google-modules/terraform-google-service-accounts \
  "$HOME/.cache/terraform-modules/terraform-google-service-accounts"
git -C "$HOME/.cache/terraform-modules/terraform-google-service-accounts" checkout 570caafff32d
make test-gcp-terraform-service-accounts

git clone https://github.com/terraform-google-modules/terraform-google-iam \
  "$HOME/.cache/terraform-modules/terraform-google-iam"
git -C "$HOME/.cache/terraform-modules/terraform-google-iam" checkout 9b9eacfa2b4f
make test-gcp-terraform-iam

git clone https://github.com/terraform-google-modules/terraform-google-sql-db \
  "$HOME/.cache/terraform-modules/terraform-google-sql-db"
git -C "$HOME/.cache/terraform-modules/terraform-google-sql-db" checkout 75b57dc6838e
make test-gcp-terraform-sql-db

git clone https://github.com/terraform-google-modules/terraform-google-kubernetes-engine \
  "$HOME/.cache/terraform-modules/terraform-google-kubernetes-engine"
git -C "$HOME/.cache/terraform-modules/terraform-google-kubernetes-engine" checkout a19597ac18ad
make test-gcp-terraform-gke

git clone https://github.com/terraform-google-modules/terraform-google-cloud-datastore \
  "$HOME/.cache/terraform-modules/terraform-google-cloud-datastore"
git -C "$HOME/.cache/terraform-modules/terraform-google-cloud-datastore" checkout 1616a282bbf4
make test-gcp-terraform-datastore
```

`TF_MODULE_DIR` overrides the checkout location for whichever case you run
(default `$HOME/.cache/terraform-modules/<module repo>`); the Makefile targets
point it at `TF_MODULE_DIR` / `TF_STORAGE_MODULE_DIR` /
`TF_MEMORYSTORE_MODULE_DIR` / `TF_FUNCTIONS_MODULE_DIR` /
`TF_CLOUDDNS_MODULE_DIR` / `TF_CLOUD_OPERATIONS_MODULE_DIR` /
`TF_KMS_MODULE_DIR` / `TF_FIRESTORE_MODULE_DIR` / `TF_CLOUD_RUN_MODULE_DIR` /
`TF_BIGQUERY_MODULE_DIR` / `TF_SECRET_MANAGER_MODULE_DIR` /
`TF_WORKFLOWS_MODULE_DIR` / `TF_SCHEDULER_MODULE_DIR` /
`TF_TASKS_MODULE_DIR` / `TF_DATAPROC_MODULE_DIR` /
`TF_SERVICE_ACCOUNTS_MODULE_DIR` / `TF_IAM_MODULE_DIR` /
`TF_SQLDB_MODULE_DIR` / `TF_GKE_MODULE_DIR` /
`TF_DATASTORE_MODULE_DIR` respectively.

### The plan-only tier

Not every module can complete the apply/destroy contract, and forcing one to
would either abort the aggregate or produce a green that proves nothing. A case
opts into the plan-only tier by setting `PLAN_ONLY=1` and calling
`plan_only_phase` instead of the apply phases:

```
init -> validate -> plan (saved) -> plan_includes <address>...
```

There is no apply, no destroy and no FC6 capture (a plan sends no write request
bodies). Acceptance is a **completed** plan that carries the addresses the case
asserts — not necessarily a clean one, because a module may legitimately plan
against a metadata-only control plane. A provider panic or an unrouted request
(the endpoint map does not reach the emulator) is a failure; a plan that stops
on an API diagnostic is recorded only when the case sets
`PLAN_ONLY_ALLOW_DIAGNOSTICS=1`, and its header says which data plane is
missing. `PLAN_ONLY=1` in the environment also flips a case that can be applied
(the `sql-db` case) into plan-only mode, which is what the generic target does.
A plan-only run that somehow creates state is failed and destroyed, so the mode
cannot leave resources behind:

```bash
make test-gcp-terraform-plan-only CASE=sql-db   # measure the diff, no apply
make test-gcp-terraform-plan-only CASE=gke
```

Plan-only cases run in the aggregate and the nightly like any other case; they
are reported as `plan-only:<case>` in `docs/fidelity/terraform-coverage.csv`
when their root module declares provider resources.

Against an already-running emulator, with a checkout you control:

```bash
TF_MODULE_DIR=/path/to/terraform-google-cloud-storage \
  tests/integration/gcp/terraform/run-module.sh storage http://localhost:8080 test-project
```

### What a case does

The driver sources `module-cases/<case>.sh` (one file per module) and supplies
the temp work dir, the module checkout, the Terraform wrapper, `pass`/`fail`,
`check` (substring assert on a REST body) and the phase helpers. A case sets
`MODULE_REPO` / `MODULE_COMMIT` / `MODULE_GIT`, defines `case_main`, and
generates its root module there. Every case ends with
`plan -detailed-exitcode` = 0: that is the acceptance, not the apply exit code.

**pubsub** — the generated root module carries the per-service custom endpoints
the module touches (`pubsub`, `resource_manager`, `cloud_billing`, `iam`,
`service_usage`, each with exactly one trailing slash) and a `module` block with
`project_id`, `topic` and one `pull_subscriptions` entry, then runs:

1. `init` (provider from the registry)
2. `plan` — must complete, which proves `data.google_project` +
   `projects.getBillingInfo` work
3. `apply`
4. spot checks through the emulator's REST API: the topic, the pull
   subscription on that topic, and `projects/{p}/billingInfo` answering the
   no-account default (`billingAccountName: ""`, `billingEnabled: false`)
5. a second `plan -detailed-exitcode` — must report **no changes**
6. `destroy` (also on exit via a trap)

**storage** — three buckets in `location = "NAM4"` (`module-bucket` with the
optional blocks, `logs` as the logging destination, `dual-region` created with
the two `ForceNew` blocks), the `modules/simple_bucket` submodule, and one
module-owned `google_storage_hmac_key`, run the full gate:

1. `apply` the module with its defaults (`autoclass`, `hierarchical_namespace`,
   UBLA, `soft_delete_policy`), labels, `public_access_prevention = "enforced"`,
   `website`, `logging` and `ip_filter`; the apply log must contain no `400`,
   `501` or `unsupported update_mask`
2. assert through the REST API that the bucket carries labels, UBLA/PAP,
   `hierarchicalNamespace`, `website`, `logging` and the full `ipFilter`, and
   that the `dual-region` bucket reads back `hierarchicalNamespace.enabled=true`
   plus `customPlacementConfig.dataLocations` (the two `ForceNew` blocks — the
   create read-back is what stops the next plan replacing the bucket); that
   `projects/{p}/serviceAccount` answers `storage#serviceAccount` (the
   `simple_bucket` data source read) and its bucket exists; and that
   `projects/{p}/hmacKeys` lists the module's key with the service-account email
   and `state: ACTIVE`
3. `plan -detailed-exitcode` → **no changes**
4. flip `autoclass.enabled` to true, change a label, and change
   `website`/`logging`; `apply`; assert the new values and that the untouched
   `ipFilter` survived; `plan` → **no changes**
5. clear back: `labels = {}`, `logging = {}`, `autoclass` false and
   `ip_filter { mode = "Disabled" }`; `apply`; assert the caller labels and
   logging are gone and the filter's ranges are cleared; `plan` → **no changes**
6. `destroy` (the HMAC key is deactivated and then deleted)

The consumer paths added for the project service account and HMAC keys are in
every phase, so their computed fields must also re-plan clean:

- `modules/simple_bucket` declares
  `data "google_storage_project_service_account"` unconditionally, so the data
  source is read on every plan/apply (`projects.serviceAccount.get`).
- `set_hmac_access` + `hmac_service_accounts` create a
  `google_storage_hmac_key` for `jc-hmac@<project>.iam.gserviceaccount.com`
  (`google_service_account` is declared for the same account). The map is
  `map(string)`, so the email is a hardcoded plan-time literal — referencing the
  account's computed `email` would fail the plan. The provider also needs
  `iam_custom_endpoint`/`iam_beta_custom_endpoint` alongside
  `storage_custom_endpoint`, because `google_service_account` is served by the
  IAM Admin API.

Two behaviours of `hashicorp/google` 7.46.2 are recorded, not worked around:

- A create-time `autoclass { enabled = false }` block is **not sent** (the
  block's `Optional+Computed` `terminal_storage_class` makes `GetOk` see the
  whole block as zero). The emulator must not synthesize it either, and the
  schema's `DiffSuppressFunc` keeps the plan clean. The mutate phase proves the
  field round-trips once it is actually sent.
- `website` has no clearing path in the provider (unlike `logging`, which sets
  `NullFields`), so removing the block would PATCH nothing and real GCS would
  keep the old value too. The clear phase changes `website` instead of removing
  it; this is a provider limitation, not an emulator gap.

**memorystore** — one `STANDARD_HA` instance created through the module with
its real inputs (`memory_size_gb`, `redis_version`, labels,
`transit_encryption_mode = "SERVER_AUTHENTICATION"`,
`read_replicas_mode = "READ_REPLICAS_DISABLED"`, `replica_count = 1`, an RDB
`persistence_config` and a Monday 01:00 `maintenance_policy`) runs the gate:

1. `apply` the module and assert through the REST API that GET returns
   `authEnabled:false`, `connectMode`, `transitEncryptionMode`,
   `readReplicasMode`, the maintenance window and the persistence config — the
   two ForceNew modes' create read-back is what stops the next plan replacing
   the instance; the apply log must contain no `400`, `501` or
   `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. flip `auth_enabled` to true, move the maintenance window, change the
   snapshot period and labels; `apply` (the provider reads
   `GET …/instances/{i}/authString` once AUTH is on); assert the new values and
   that the untouched ForceNew modes survived; `plan` → **no changes**
4. clear back: `auth_enabled = false`, `maintenance_policy = null`,
   `persistence_mode = "DISABLED"`, `labels = {}`; `apply`; assert AUTH false,
   the policy gone, persistence disabled and the labels cleared; `plan` →
   **no changes**
5. `destroy`

Two behaviours of `hashicorp/google` 7.46.2 are recorded, not worked around:

- `auth_enabled = false` is never sent (both create and update drop a false
  bool), and the update that disables AUTH sends `updateMask=authEnabled` with
  **no** `authEnabled` member in the body. An update mask is authoritative in
  proto3 semantics, so the emulator clears the masked path; a handler keyed on
  "field present in body" would keep AUTH on and permadiff.
- `persistence_config` is `Optional+Computed`: removing the block keeps the
  prior state and sends no mask at all (real GCP keeps the config too), so the
  clear phase disables persistence instead. `maintenance_policy` is plain
  `Optional`, so removing it does PATCH `updateMask=maintenancePolicy`.

**functions** — the v1 event-function module (`google_cloudfunctions_function`)
is created with its defaults (`ingress_settings = "ALLOW_ALL"`, a Pub/Sub
`event_trigger` with `failure_policy.retry = false`, `create_bucket = false`
against a bucket the case supplies) and runs the gate:

1. `apply`; assert through the REST API that GET returns
   `ingressSettings: "ALLOW_ALL"`, the Pub/Sub event trigger and the empty
   `failurePolicy` message; the apply log must contain no `400`, `501` or
   `unsupported update_mask`
2. a second `plan`; its only permitted diff is the recorded storage-object gap
   below
3. change `ingress_settings` to `ALLOW_INTERNAL_ONLY` and add
   `environment_variables`; `apply`; assert the new values (the ingress change
   is PATCHed in place by 7.46.2; the log records which path was taken);
   `plan` → no function diff
4. toggle `event_trigger_failure_policy_retry = true`; `apply`; assert
   `eventTrigger.failurePolicy.retry`; `plan` → no function diff
5. clear `environment_variables` and restore `ALLOW_ALL`; `apply`; assert the
   env var is gone and ingress is back; `plan` → no function diff
6. `destroy`

Three behaviours are recorded, not worked around:

- The provider names the v1 endpoint `cloud_functions_custom_endpoint`
  (`cloudfunctions2_custom_endpoint` is the v2 one); the case uses the v1 name.
- A v1 event trigger with the default `retry = false` is created with no
  `failurePolicy` member, but the API answers with the empty `failurePolicy`
  message (its documented "ignore failures" default). The emulator renders that
  empty message; omitting it made the provider flatten to no block and plan
  `+ failure_policy { retry = false }` forever.
- The module always manages a `google_storage_bucket_object` for the archive,
  and 7.46.2 permadiffs it because the emulator's storage-object renderer does
  not model `content_disposition`/`metadata`. That is a storage gap (TGM1's
  surface, not TGM3's), so the no-changes check accepts a plan whose **only**
  changed address is that object and fails on anything else; the case also
  supplies an existing uppercase-location bucket (`create_bucket = false`) so
  the bucket's location case does not add a second unrelated diff.

**clouddns** — the module's default `type = "private"` zone is created with
two VPC networks, one GKE cluster and labels; then the same zone's
description/labels change and a third network is added; then a `forwarding`
zone (two targets, one with `forwarding_path = "private"`), a `peering` zone
(a target network) and a `public` zone with `dnssec_config` (`state = "off"`,
then turned `"on"`) plus query logging are applied in turn. Every phase asserts
through the REST API that GET returns the accepted block — the create read-back
is what stops the next plan re-adding it — and then requires
`plan -detailed-exitcode` = 0; every apply log must be free of `400`, `501` and
`unsupported update_mask`.

The blocks are echoed verbatim (no synthesized defaults): a public zone with a
minimal `dnssec_config` reads back only what was sent, and the forwarding zone's
optional `forwardingPath`/`ipv6Address` survive because the emulator stores the
decoded object. `reverse_lookup` and `service_directory` zones are not exercised
— they are `google-beta`-only variants, out of scope for this case.

**cloud-operations** — the `modules/simple-uptime-check` module is applied with
`protocol = "HTTPS"`, a `monitored_resource` of type `uptime_url`,
`auto_close = "18000s"`, `notification_rate_limit_period = "300s"` and
`alert_policy_user_labels = { team = "platform" }`, which makes the module emit
the alert policy's `alert_strategy` block. The gate:

1. `apply`, then assert through the REST API that the uptime check exists, the
   alert policy GET returns `alertStrategy.autoClose`, the
   `alertStrategy.notificationRateLimit.period` and the user labels, and that no
   `severity` was synthesized (a policy that never set it must not read one
   back); the apply log must contain no `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change `auto_close` to `"36000s"`, the rate limit to `"600s"` and the labels
   to `{ team = "sre" }`; `apply` (the provider PATCHes
   `updateMask=alertStrategy`); assert the new values; `plan` → **no changes**
4. clear the strategy (`auto_close = null`,
   `notification_rate_limit_period = null`) and the labels; `apply`; assert the
   `alertStrategy` block and the labels are gone while the uptime check
   survived; `plan` → **no changes**
5. `destroy`

The provider's `monitoring_custom_endpoint` is the API **base** URL — it appends
`v3/` itself, so the case points it at `${endpoint}/`; a versioned endpoint
(`${endpoint}/v3/`) produces `/v3/v3/...` and a 404. The `severity` enum the
same change added has no module variable, so it is covered by the service,
store, REST and gRPC tests rather than this gate.

**cloudrun** — the pinned module is driven through its `modules/v2` submodule
(the repo's ROOT module still creates the v1 `google_cloud_run_service`, which
the emulator does not serve). It creates the service plus its own service
account and an `allUsers` invoker member, and the gate:

1. `apply` the module with `INGRESS_TRAFFIC_ALL`, a service label, a container
   image/env/port; assert through REST that the service, its template and the
   revision `00001` read back, and that the service IAM carries
   `roles/run.invoker`; the apply log must contain no `400`, `501` or
   `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change `ingress` to `INGRESS_TRAFFIC_INTERNAL_ONLY`, add a label and change
   the container env; `apply`; assert the new values, that the template change
   minted revision `00002`, and `plan` → **no changes**
4. `destroy`; the service and its `revisions` list are `404`

`create_service_account = true` (the module default) is deliberate: the other
path reads `data.google_compute_default_service_account`, whose IAM read on the
returned account is not served (TF2-1-1; the Compute `projects.get` half is
served). `cloud_run_deletion_protection` is set
to `false` because the module default is a provider-side destroy guard (real GCP
refuses the delete too). IAP, custom domains (v1), Jobs, WorkerPools and traffic
targets are outside the served surface and are not configured; the case header
records each boundary.

**bigquery** — the pinned module is the CFT `terraform-google-bigquery` root
module, which creates one `google_bigquery_dataset` and one
`google_bigquery_table` per `tables` entry. The gate:

1. `apply` the dataset with a description, `US` location, labels, a
   `default_table_expiration_ms` and the `access` ACL (OWNER/`projectOwners`),
   and one table with a two-column schema and labels; assert through REST that
   the dataset and table read back (`datasetReference`, `tableReference`,
   `friendlyName`, labels, `defaultTableExpirationMs`, the schema fields); the
   apply log must contain no `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. add a schema column, change the dataset and table labels and the dataset
   description, and add a READER/`projectReaders` ACL entry; `apply`; assert the
   new values; `plan` → **no changes**
4. `destroy`; the dataset and table are `404`

Dataset and table IAM (`google_bigquery_dataset_iam_*` /
`google_bigquery_table_iam_*`) is not served — `getIamPolicy`/`setIamPolicy` are
not in the operation registry (TF1-1, `fix/gcp-bigquery-resource-iam`) — so the
case drives the served dataset ACL through the module's `access` input instead.
Views, materialized/external tables, routines, CMEK and resource tags stay at
their defaults; the case header records each boundary.

**secret-manager** — the CFT `terraform-google-secret-manager` root module is
applied with two secrets and drives the full gate:

1. `apply` with
   - `jc-tf-sm-auto`: automatic replication with a `global` CMEK key, a
     `86400s` rotation schedule and the Pub/Sub topic the schedule requires,
     one label and one version payload;
   - `jc-tf-sm-user`: user-managed replication with two replicas
     (`us-central1` using a regional CMEK key, `us-east1` unencrypted), one
     version payload;
   - one authoritative `google_secret_manager_secret_iam_binding` per secret
     carrying a `roles/secretmanager.secretAccessor` reader;

   assert through REST that each secret reads back its replication arm and keys,
   the automatic secret reads back its label, rotation schedule and topic, the
   user-managed secret both replica locations, and each `versions.access` call
   returns the expected payload; `getIamPolicy` must carry the reader; the apply
   log must contain no `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change the label, the rotation period to `172800s`, the automatic secret's
   payload (a version replacement) and the IAM member list to two members;
   `apply`; assert the new values and `versions.access` on `latest`; `plan` →
   **no changes**
4. `destroy`; both secrets and the versions are `404`

`add_kms_permissions`/`add_pubsub_permissions` stay empty because both drive
`google_project_service_identity` (google-beta), whose Service Usage
`generateServiceIdentity` surface is not served; the CMEK keys are passed
directly, which is the part the secret API validates. Regional secrets are not
served (TF1-4); the case only creates global `projects.secrets` resources.

**workflows** — the CFT `terraform-google-cloud-workflows` root module has no
default for `workflow_trigger` and always creates a Scheduler or Eventarc
trigger, so the case drives its `modules/simple_workflow` submodule (the
blueprint's standalone workflow resource) and the generated root module supplies
what the blueprint's root would create through the registry service-accounts
module: a `google_service_account` and a project-level
`roles/workflows.invoker` grant. The gate:

1. `apply` the workflow with a description, labels, a YAML source and the
   service account; assert through REST that it reads back with
   `state: ACTIVE`, the description, labels, source, service account and
   revision `000001`; assert the service account exists and the project IAM
   policy carries the invoker grant; the apply log must contain no `400`, `501`
   or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change the source, description and a second label; `apply`; assert the new
   values and revision `000002`; `plan` → **no changes**
4. delete the workflow through the API, then `destroy` the service account and
   grant; all are `404`

The module pins `hashicorp/google < 7.0`, so the case resolves 6.49.3 and scopes
the capture to `google_workflows_workflow` (see the provider-version note
above). The submodule does not expose `deletion_protection`, whose provider
default is `true`, so the case removes the workflow from state and issues the
DELETE the provider guards; that is an upstream module limitation, not an
emulator gap. Workflow CMEK and resource-level IAM are not implemented
(README-GCP) and the provider has no workflow IAM resource type.

**scheduler** — the CFT `terraform-google-cloud-scheduler` root module is applied twice: one job with a header-less **HTTP target** (the module's input exposes no `headers` field) and one with a **Pub/Sub target** (the case's root module owns the topic), both on a far-future schedule (`0 0 1 1 *`), `UTC`, and a `retry_config`. The gate:

1. `apply`; assert through REST that each GET returns the reference, description, schedule, `timeZone`, `state: ENABLED`, `scheduleTime` and the retry config; the HTTP job also returns `httpTarget.uri`/`httpMethod`/`body` and the **server-filled headers map** (`User-Agent: Google-Cloud-Scheduler`, plus `Content-Type: application/octet-stream` because the job has a body), while the Pub/Sub job returns `pubsubTarget.topicName`/`data`/`attributes`; the apply log must contain no `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change the schedule, description, retry config, HTTP body and Pub/Sub payload/attributes; `apply`; assert the new values; `plan` → **no changes**
4. pause (`paused = true`): the provider calls `:pause`; assert `state: PAUSED` on both jobs; `plan` → **no changes**
5. unpause (`paused = false`): the provider calls `:resume`; assert `state: ENABLED` and that `scheduleTime` is present; `plan` → **no changes**
6. `destroy`; both jobs and the topic are `404`

The header-less HTTP target is the shape provider 6.49.3 crashed on when the
response omitted `httpTarget.headers`; real Cloud Scheduler always answers the
map with its delivery defaults (the provider strips those exact values before
diffing), and the emulator now synthesizes them (TF3-4-1,
`fix/gcp-scheduler-http-target-headers`).

One boundary is recorded, not worked around:

- The module's default `retry_config = { retry_count = 0 }` is not sent (the provider drops an all-empty block) and the emulator does not synthesize the documented RetryConfig defaults, so that input re-plans `+ retry_config { retry_count = 0 }`; the case creates a non-default retry config and the default synthesis is TF3-4-2 (`fix/gcp-scheduler-retry-config-defaults`).

**tasks** — the CFT `terraform-google-cloud-tasks` root module is applied with a queue, rate limits, a retry config, a Stackdriver logging config and one queue IAM member (`queue_iam_choice = "iam_member"`). No task is created, so the dispatch engine never runs. The gate:

1. `apply`; assert through REST that GET returns the reference, `state: RUNNING`, every rate/retry/logging value and the IAM binding, and that the queue's task list is empty; the apply log must contain no `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change a rate limit, a retry limit and the IAM member; `apply`; assert the new values, that the replaced member is gone, and `plan` → **no changes**
4. `destroy`; the queue is `404`

`rate_limits.max_burst_size` is computed-only in the provider's 6.x schema (7.x made it Optional+Computed), so the module input stays null; the emulator reports `maxBurstSize` 0 when the create body omits it while real Cloud Tasks defaults it to 100 — a recorded reader gap (TF3-4-3, `fix/gcp-tasks-queue-rate-limit-defaults`). `app_engine_routing_override` and the queue-level `http_target` are left null: they are defaults for App Engine/HTTP tasks, and the case creates no task.

**dataproc** — the CFT `terraform-google-dataproc` root module is applied with a GCE-shaped `cluster_config` (one master, two workers, `n1-standard-4`, image `2.2-debian12`, labels), and the generated root module submits a pyspark job into the module's cluster. The gate:

1. `apply`; assert through REST that the cluster GET returns `clusterName`, the labels, `config.masterConfig`/`workerConfig.numInstances` and `config.softwareConfig.imageVersion`, and that the job GET (the root module's `job_id` output) returns its `placement.clusterName`, labels, `pysparkJob.mainPythonFileUri`/args and `status.state: DONE`; the apply log must contain no `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change the cluster labels and grow the workers 2 → 3; `apply`; assert the read-back and `plan` → **no changes**
4. `destroy`; the cluster and job are `404`

The module's `main.tf` unconditionally reads `data.google_compute_default_service_account` and grants that account `roles/dataproc.worker` through `google_project_iam_member`. The data source's Compute half (`projects.get`, `GET /compute/v1/projects/{project}`) is served, but its follow-up IAM read on the returned account (`projects/-/serviceAccounts/{email}`) is not — TF2-1-1 (`fix/gcp-iam-default-service-accounts`) — so both resources are pruned with `-target` on plan, apply and destroy. The case still measures the module's cluster rendering and the job path; the pruned IAM prerequisite is recorded rather than worked around. The module's `virtual_cluster_config` is left null: the fixture already covers the GKE-shaped cluster and its NodePool reconciliation.

**service-accounts** — the CFT `terraform-google-service-accounts` root module creates three accounts with two project roles, then the case changes the role set. The module's `project_roles` format is `"project=>role"`; a bare role string would make the module parse the role as both the project and the role, so the case always passes `"${var.project}=>role"`. Every phase runs with `-parallelism=10`. The gate:

1. `apply`; assert each account GET (email, display name, `disabled: false`) and that the project policy has every account under `roles/viewer` and `roles/storage.objectViewer`; no `400`/`501`
2. `plan -detailed-exitcode` → **no changes**
3. replace `roles/storage.objectViewer` with `roles/pubsub.viewer`; `apply`; assert the new role holds all three accounts, `roles/viewer` still does, and the replaced role is gone; `plan` → **no changes**
4. `destroy`; the accounts are `404`

`display_name` is set explicitly: the provider's attribute is Optional (not Computed) while the emulator defaults an omitted `serviceAccount.displayName` to the account id, so an omitted value permadiffs — TF2-14 (`fix/gcp-iam-serviceaccount-display-name`). The module's own default is non-empty, so a real consumer does not hit it. Billing/org IAM (`grant_billing_role`, `grant_xpn_roles`) and key generation are disabled: they are outside the project-scoped surface this case measures.

**iam** — the `terraform-google-iam` repo is a collection of scope submodules, so the case pins `modules/projects_iam` and drives three of them on one project: project IAM, bucket IAM (one bucket) and Pub/Sub topic IAM (one topic), all in additive mode with multiple members per role. The submodules reference `../helper`; a plain local-path module source makes Terraform treat the submodule directory as the package root and rejects the relative helper path, so the case uses `git::file://<checkout>//modules/<name>?ref=<HEAD>` (offline; the checkout already exists) and presents the whole repo as the package, exactly as the registry does. Every phase runs with `-parallelism=10`. The gate:

1. `apply`; assert each account and that the project, bucket and topic policies carry every configured member; no `400`/`501`
2. `plan -detailed-exitcode` → **no changes**
3. change the role sets: `roles/viewer` gains a third member, a new `roles/iam.securityReviewer` appears, bucket `roles/storage.objectViewer` and topic `roles/pubsub.publisher` gain the third member, and the replaced roles disappear; `apply`; assert every survivor and new member is present (no lost updates) and the replaced roles are gone; `plan` → **no changes**
4. `destroy`; the bucket, topic and accounts are `404`

Folder and organization IAM, the audit-config submodule and the module's `member_iam`/`service_accounts_iam` submodules are out of scope: the emulator is project-scoped (wire-contract §8) and the root module creates the accounts it binds.

**sql-db** — the CFT `terraform-google-sql-db` `modules/postgresql` submodule is applied
through `google-beta` with its real defaults (tier `db-custom-1-3840`, ZONAL
availability, the module's backup configuration, maintenance window, disk
settings and `user_labels`) and the case's `deletion_protection = false`:

1. `apply`; assert through the REST API (`sqladmin` v1beta4) that the instance
   is `RUNNABLE`, echoes `POSTGRES_18`, its tier and labels, and that the
   module's default database and user exist; the apply log must contain no
   `400`, `501` or `unsupported update_mask`
2. `plan -detailed-exitcode` → **no changes**
3. change the tier (`db-custom-1-3840` → `db-custom-2-7680`) and the labels;
   `apply` (an in-place settings update); assert the new values; `plan` →
   **no changes**
4. `destroy`; the instance GET is `404`

The module reads `data.google_compute_zones.available` from `read_replica.tf`,
so the case points the compute endpoint at the emulator as well. Read replicas,
private service access, CMEK/autokey, password policy, insights and PITR stay at
their defaults: they reach surfaces outside this family's measured scope. The
case also supports `PLAN_ONLY=1` (init → validate → plan only), which the
`test-gcp-terraform-plan-only CASE=sql-db` target uses.

**gke** — the CFT `terraform-google-kubernetes-engine` ROOT module is measured
with the **plan-only tier** (it sets `PLAN_ONLY=1` itself):

1. `init`, `validate`, `plan`; assert the plan changes the cluster
   (`module.gke.google_container_cluster.primary`), the default node pool
   (`…pools["default-node-pool"]`) and the node service account
   (`…cluster_service_account[0]`)
2. assert the plan reached Container `GetServerConfig` for
   `data.google_container_engine_versions.{region,zone}` — the read the module
   cannot avoid — so a routing regression is visible as a failed read, not only
   as a generic plan failure
3. print the plan's own `Warning:`/`Error:` diagnostics as the recorded gap list

No apply or destroy runs: GKE's control plane is metadata-only in the emulator,
and the module's default surface reaches firewall/service-account resources
whose apply behavior is not what this family measures. `zones` is supplied so
the module does not read `data.google_compute_zones`; the provider's
`container_custom_endpoint` carries the `container.` host (the provider keeps
only scheme+host), and the compute/IAM/Resource Manager endpoints cover the
plan-time data sources and rendered IAM resources.

**datastore** — the CFT `terraform-google-cloud-datastore` module is measured
with the **plan-only tier** (it sets `PLAN_ONLY=1` itself), because it has no
provider resource at all: per its own README it "supports index creation by
shelling out to gcloud". `plan` must render the caller's `index.yaml`
(`module.datastore.local_file.cloud-datastore-index-file`) and the gcloud
`null_resource`, and the plan must carry the requested index kind and
properties. No apply runs: it would execute `gcloud datastore indexes` against a
real endpoint, and the Datastore Admin `projects.indexes.*` surface is not
served by the emulator (recorded as TF3-6-1). The client-side `local` and
`null` providers are pinned by the case because the module has no
`versions.tf`.

Each case prints one `PASS`/`FAIL` line per step and exits non-zero on any
failure.

## Lifecycle-depth gate (TF5)

`run.sh` and the module cases prove `apply -> re-plan -> update -> destroy`.
`run-lifecycle.sh` proves the operations a real Terraform user performs on
existing state. It is a second driver with one case per representative service
under `lifecycle-cases/` (a case is sourced and supplies `case_main`, like
`run-module.sh`), plus a committed `terraform test` module.

```bash
make test-gcp-terraform-lifecycle                    # Terraform, ephemeral emulator
make test-gcp-terraform-lifecycle TF_BIN=tofu STRICT=1
tests/integration/gcp/terraform/run-lifecycle.sh http://localhost:8080 test-project
CASES="storage kms" tests/integration/gcp/terraform/run-lifecycle.sh
```

It runs a full apply/destroy cycle, so it is **nightly-only**; it is not part of
`gcp-ci-preflight`. `STRICT=1` is what the nightly uses: a SKIP (missing binary)
is a failure, not a pass.

| case | phases |
|---|---|
| `storage` | import (clean plan), `refresh-only`, `-replace` (new `timeCreated`/metageneration), ForceNew rename (old bucket 404), out-of-band label drift, label-import artifact, duplicate 409 / invalid name 400 / provider-surfaced 409 |
| `pubsub` | import (topic + subscription), `refresh-only`, `-replace` the subscription, ForceNew rename (old subscription 404), out-of-band label drift through `updateMask=labels`, label-import artifact, duplicate 409 / unsupported mask 400 / invalid duration 400 / provider-surfaced 409 |
| `kms` | import (key ring + crypto key), `refresh-only`, out-of-band label drift, crypto-key `-replace` as an **expected 409** (the provider never calls the documented delete) with re-import and rotation repair, duplicate 409 / invalid algorithm 400 / unsupported mask 400 / live-version `cryptoKeys.delete` 400, and the post-destroy "key ring and key survive, version is `DESTROY_SCHEDULED`" assertions |
| `iam-parallel` | 10 service accounts + 10 project bindings at `-parallelism=10`; the policy must hold all 10 (a lost update fails), re-plan clean, destroy at parallelism 10 |
| `tftest` | `terraform test` over `lifecycle-tftest/`: provider-visible computed ids, self links, nested objects |

**Import support.** The driver records one row per imported address in
`<LOG_DIR>/<case>-import.tsv` (the nightly artifact keeps them), and the
nightly log is the source of the table in
[`docs/fidelity/terraform-scorecard.md`](../../../../docs/fidelity/terraform-scorecard.md).
A clean plan after import is the strong form: the emulator's GET is fed back into
state and re-planned. Import is not universal upstream, so two classifications
are recorded instead of failing:

- **provider-label-artifact**: `tpgresource.SetLabels` filters the response's
  labels against the labels already in state, and an import starts from an empty
  map, so the next plan reconstructs `labels`/`terraform_labels`. The gate
  accepts exactly those attributes (and nothing else); every other changed
  attribute fails.
- **empty-labels normalization**: a `refresh-only` plan reports `labels = {}` for
  a resource with no managed labels (the provider normalizes an absent map).
  `refresh_phase labels-artifact` accepts that one attribute.

**Both assertion layers exist on purpose.** `terraform test` conditions see what
a configuration can observe (computed ids, `self_link`, `url`, nested objects);
the shell cases use the REST API for the wire fields the provider hides or
normalizes (`metageneration`, `locationType`, `primary.state`,
`DESTROY_SCHEDULED`, policy etags, the raw 409/400 bodies). Neither layer
replaces the other.

**Recorded boundaries (not worked around):**

- `google_storage_bucket`'s `-replace` re-creates the same name; its server-side
  `timeCreated`/`metageneration` are the proof the object was re-created.
- The provider never calls Cloud KMS's documented delete methods: a key-ring
  delete is state-only and a crypto-key delete destroys the versions, so
  `-replace` on a crypto key is an expected 409 (re-create under the same name).
  The gate asserts that failure and repairs rotation after re-import;
  `cryptoKeys.delete` itself is served and enforces the live-version 400 (TF5-4
  is the unimplemented key-ring delete).
- An unmanaged extra label key is filtered out of `labels` by the provider's
  `SetLabels` (only keys already in state are planned) and preserved by the
  update; the gate asserts the managed key converges and records the extra key.
- The storage bucket's invalid `storageClass`/`publicAccessPrevention`/`location`
  are accepted today (real GCS rejects them) — TF5-1; the case records the
  measurement and uses the invalid-bucket-name 400 that GCS does validate.

**Expansion list (the next services, in order).** Each needs an import id and,
where the API supports it, a ForceNew identity change:

| service | resource(s) | import | replace |
|---|---|---|---|
| `secretmanager` | `google_secret_manager_secret` + version | secret resource name | name is ForceNew |
| `bigquery` | `google_bigquery_dataset` / `_table` | `projects/p/datasets/d`, `…/tables/t` | dataset/table id ForceNew |
| `cloudrun` | `google_cloud_run_v2_service` | `projects/p/locations/l/services/s` | location/name ForceNew |
| `dataproc` | `google_dataproc_cluster` | `projects/p/regions/r/clusters/c` | cluster name ForceNew |
| `cloudsql` | `google_sql_database_instance` | `projects/p/instances/i` | name ForceNew |
| `firestore` | `google_firestore_database` | `projects/p/databases/d` | database id ForceNew |
| `clouddns` | `google_dns_managed_zone` | `projects/p/managedZones/z` | name ForceNew |

## Composition patterns (TF4)

The fixture is a bag of resources and each module case is one module. Neither
proves a *stack*: several services wired together by cross-resource references,
applied and destroyed as one graph. The patterns under `patterns/<NN-name>/` are
small root modules written here, one per canonical provisioning stack, each
with a `run.sh` that follows the standard contract:

```text
init -> apply -> REST read-back asserts -> plan -detailed-exitcode == 0
     -> (optional in-place mutation -> apply -> asserts -> plan == 0)
     -> (optional targeted destroy -> partial-teardown asserts)
     -> (optional per-workspace teardown -> isolation asserts -> plan == 0)
     -> destroy -> post-destroy asserts (404 / DELETE_REQUESTED)
```

```bash
make test-gcp-terraform-patterns STRICT=1                 # all patterns, one emulator
make test-gcp-terraform-patterns TF_BIN=tofu STRICT=1     # OpenTofu
make test-gcp-terraform-pattern PATTERN=00-bootstrap      # one, own emulator
tests/integration/gcp/terraform/run-patterns.sh http://localhost:8080 test-project
```

It is a full apply/destroy run, so it is **nightly-only**, not part of
`gcp-ci-preflight`. `STRICT=1` (what the nightly uses) makes a SKIP a failure.
Per-pattern logs and a summary land in
`/tmp/jaiscloud-gcp-terraform-patterns/`.

Each pattern directory holds `main.tf` + `outputs.tf`; `patterns/provider.tf`
carries the shared provider block and the offline endpoint defaults, and
`patterns/lib.sh` assembles the root module in a throwaway work dir with the
per-service custom endpoints emitted from `terraform-endpoints.yaml` — the same
map the fixture, the module driver and the corpus tool consume. A bare
`terraform plan` inside a pattern directory therefore still works without Go
(the `provider.tf` locals are the offline defaults).

| pattern | stack | proves |
|---|---|---|
| `00-bootstrap` | `google_project` (with `billing_account`) + four `google_project_service` APIs + service account + project IAM member + GCS bucket + Pub/Sub topic/subscription + Secret Manager secret/version + KMS key ring/key | every resource resolves against the project Terraform just created; the provider's post-create reads of the new project succeed; the whole graph re-plans clean; destroy removes every resource before the project, and the project ends `DELETE_REQUESTED` (real GCP reserves the id too) |
| `01-cmek` | regional KMS ring/key pair + a global ring/key + GCS bucket `encryption.default_kms_key_name` + BigQuery dataset `default_encryption_configuration` + Secret Manager automatic replication CMEK | each response echoes the key it was configured with; switching the bucket and dataset to the second regional key updates them **in place** (bucket `timeCreated` kept, metageneration increments; dataset `creationTime` kept) and re-plans clean |
| `02-events` | a Pub/Sub topic + push subscription + a paused Cloud Scheduler job with a `pubsub_target` + a Pub/Sub-triggered function + a storage-triggered function + a standalone Eventarc GCS trigger with an `http_endpoint` destination | `jobs.run` publishes through the topic to the push subscription, and the delivery **arrives at a local echo sink** (`patterns/helpers/echo-sink`) with the configured payload and attributes; the same message invokes the Pub/Sub function (the emulator's delivery record is `delivered`); an object write reaches the Eventarc trigger's HTTP destination with `ce-type: google.cloud.storage.object.v1.finalized`, the bucket source and the object metadata, and invokes the storage-triggered function; both functions materialize their backing Eventarc triggers, and destroy leaves no orphan topic, subscription, job, trigger or function |
| `03-datalake` | input/output/staging buckets + a BigQuery dataset/table/routine/row-access-policy + a GKE cluster + a Dataproc-on-GKE virtual cluster with two node-pool targets + a Dataproc pyspark job + a Dataproc Metastore service/federation with a database IAM grant | the whole data graph re-plans clean; both node pools are readable through `container.v1` (the virtual-cluster read omits the input-only `nodePoolConfig`) and the virtual cluster is attached to the Metastore service through `auxiliaryServicesConfig`; the job reaches `DONE` with the input/output URIs it was given; the federation names the Metastore service at backend rank `0` and the database policy carries the job account's grant; destroying the job and the virtual cluster leaves the pools readable, and the full destroy leaves no orphan cluster/pool, bucket, dataset, Metastore service/federation or IAM grant. Its boundaries (the Spark data plane and the BigQuery load path stay in the k3d/Helm/Java demo and out of a local emulator) are in [`patterns/03-datalake/README.md`](patterns/03-datalake/README.md) |
| `04-multienv` | one root module applied once per workspace (`staging`, `prod`): a `for_each` bucket pair + a Pub/Sub topic + a two-version Secret Manager secret, plus an audit topic in a second project through an aliased provider | both workspaces co-exist with different names and `env` labels; `for_each`/`count` materialize the per-environment resources; the aliased provider puts the audit topic in `<project>-shared` (and not in the default project); destroying `staging` leaves every `prod` resource readable and `prod`'s plan clean, proving the environments are isolated states over one configuration. Its boundaries (project ids are served without a `google_project` row; secret payloads are read via `versions.access`) are in [`patterns/04-multienv/README.md`](patterns/04-multienv/README.md) |

Recorded boundaries, not worked around:

- **`auto_create_network`.** The bootstrap project keeps the provider default
  (`true`). Setting it `false` makes provider 7.46.2 probe Service Usage for the
  dummy project `00000000000`; real GCP answers `403` and the provider's
  pre-requisite switch assumes an error, but the emulator answers `200` with
  `DISABLED`, so the provider dereferences a nil error and panics. That is
  TF4-1-1 (`fix/gcp-serviceusage-unknown-project`); Compute networking is
  outside this stack's scope either way.
- **KMS objects survive destroy.** The provider never calls the documented
  `keyRings.delete`/`cryptoKeys.delete` (TF5-3), and
  `projects.locations.keyRings.delete` is a `501` in the emulator (TF5-4), so
  the patterns assert the rings are still readable after destroy while every
  resource the provider does delete is `404`. Neither pattern uses a fixed key
  ring name for that reason: names carry a per-run suffix (the bootstrap
  project id is unique per run too).
- **Secret Manager replication key rotation** is not driven: the provider models
  the `replication` block as ForceNew, so a Terraform change would replace the
  secret. The emulator's PATCH path for that field is covered by the
  service/REST tests, and the pattern asserts the key it was created with.
- **The scheduler job in `02-events` is paused.** Provider 7.46.2 calls
  `jobs:resume` after every create when `paused` is false, and the emulator
  rejects resuming an ENABLED job, so an enabled `google_cloud_scheduler_job`
  cannot be created yet (TF2-12, `fix/gcp-scheduler-resume-enabled`). The
  pattern creates the job paused with a far-future cron and fires it with
  `projects.locations.jobs.run`, Cloud Scheduler's documented "force it now"
  call, which goes through the same target dispatcher the cron engine uses.
  `retry_config` is omitted for the same reason: the provider only sends a
  non-zero expanded block and the emulator does not synthesize the API's retry
  defaults on a read yet (TF3-4-2, `fix/gcp-scheduler-retry-config-defaults`),
  so an explicit zero block would permadiff.
- **Event deliveries are asserted at a local HTTP sink.** `patterns/helpers/echo-sink`
  is a small Go program (`patterns/lib.sh` builds it, so Go is already a
  prerequisite of the driver) that records every request it receives. The push
  subscription and the Eventarc `http_endpoint` both point at it, so the pattern
  checks the real payload and CloudEvents headers rather than that a resource
  exists. A binary built without the console does not serve the Cloud Functions
  delivery log (`/api/ui/.../deliveries`); the pattern therefore proves the
  function invocation through the state export
  (`GET /_jaiscloud/export` → `envelope.json`), whose functions store records
  each delivery's `status`. The delivery log is an emulator observability
  extension with no real-GCP wire equivalent, so there is nothing to fix there
  (TF4-2-2).
- **`google_bigquery_table` guards its destroy by default.** Provider 7.x sets
  `deletion_protection = true`, a client-side guard real GCP does not require for
  a composition that tears its own stack down; `03-datalake` sets it `false`.
- **A Metastore federation's `backend_metastores` is a map keyed by rank.** The
  Discovery document defines `backendMetastores` as an object with
  `additionalProperties: BackendMetastore`, and `BackendMetastore` has no rank
  member, so the provider's `rank` becomes the map key (`"backendMetastores":
  {"0": …}`). `03-datalake` asserts that shape rather than a repeated list.
- **A Metastore service's `port` is requestable.** The provider's `port` is
  Optional+Computed and real Metastore echoes a requested value (TF2-13), so
  the fixture requests `9080` and asserts the read-back `port`/`endpointUri`;
  a Service that assigned its own port would permadiff on the second plan. On
  the emulator the port is also the catalog listener, so it must be free and
  must not be the base `--hms-port` (9083, the legacy shared catalog); a
  container/k3d deployment has to expose any port a config requests.

## What it does

`run.sh` runs `init -> validate -> plan -> apply`, performs the spot checks
below, then `destroy` (also on exit via a trap, so failures still clean up):

- A second `plan` reports **no changes** (no permadiff on the computed
  `google_container_cluster` fields)
- GCS bucket exists and its `labels` round-trip (`"env":"compat-test"`)
- GCS object exists with `contentType: text/plain`
- IAM service account exists
- Secret Manager secret exists with automatic replication; version 1 is
  `ENABLED`
- Secret Manager CMEK secret reads back its `global`-location key under
  `replication.automatic.customerManagedEncryption.kmsKeyName`, and the
  user-managed secret reads back both replica locations and its per-replica
  CMEK key verbatim (DC45)
- KMS key ring + crypto key (`purpose: ENCRYPT_DECRYPT`) exist, plus the
  `global` key ring/key the automatic-replication CMEK secret requires
- Pub/Sub topic + subscription exist
- Pub/Sub topic IAM keeps **both** member grants across the provider's
  `getIamPolicy -> merge -> setIamPolicy` flow (fresh-etag OCC)
- Pub/Sub subscription and Secret Manager IAM grants are present
- Cloud SQL instance is `RUNNABLE`, database and user exist, and the
  `google_sql_ssl_cert` fingerprint resolves under the instance (`sslCerts.get`),
  with the instance exposing its synthesized `serverCaCert`
- The Dataproc Metastore service exists with the **requested** `port` (9080)
  and an `endpointUri` naming that port, the federation is `ACTIVE`, the
  database IAM grant is readable, and `destroy` leaves the service `404`
- The CRM project lookup resolves (`lifecycleState: ACTIVE`)
- The project IAM policy keeps the `google_project_iam_member` grant across
  the provider's `getIamPolicy -> merge -> setIamPolicy` flow (fresh-etag OCC),
  and project `:getIamPolicy` accepts the provider's POST
- The `google_project_service` API is `ENABLED` and appears in
  `GET .../services?filter=state:ENABLED`
- The GKE cluster read carries the nested messages real GKE always renders —
  `legacyAbac.enabled: false`, `networkConfig.network`/`.subnetwork`, the
  `shieldedNodes.enabled: true` default and
  `defaultMaxPodsConstraint.maxPodsPerNode: "110"` — which is what the provider
  dereferences
- The Dataproc-on-GKE virtual cluster is `RUNNING`, echoes both `nodePoolTarget`
  names and the GKE target, and its read carries **no** `nodePoolConfig`
  (input-only in the real API)
- The node pools it named exist through the GKE API
  (`GET /container/v1/…/nodePools`): the caller-shaped one with `machineType`
  `n1-standard-4`, `maxNodeCount: 3` and the `goog-dataproc-cluster-name` label,
  the named-only one with the 1/10 autoscaler defaults
- The Cloud Run v2 service exists with its labels, `INGRESS_TRAFFIC_INTERNAL_ONLY`
  ingress, container image/service account and the container `readiness_probe`
  (the raw-only field; revision `00001` is listed); an in-place env change
  updates it, preserves the probe, mints revision `00002` and re-plans clean,
  and `destroy` leaves the service and its revisions `404`
- After the virtual cluster alone is destroyed (`destroy -target`), the pool is
  still readable — real Dataproc does not delete a virtual cluster's node pools

## Provenance

Originally adapted from the floci-gcp `compat-terraform` / `compat-opentofu`
suites, pruned to the implemented services and made self-contained (no BATS, no
external compat repo). It is now an independent Terraform/OpenTofu provider
conformance suite; floci-gcp is not its spec (real GCP is). See
`plan_docs/gcp-terraform-compat-2026-09-22.md` for the findings that motivated
it.

## Provider request capture (FC6)

Every module gate captures the provider's outgoing requests and joins them to
the field catalog, so a field the provider sends but the emulator does not serve
fails the gate instead of being chased as a mysterious plan diff:

- `run-module.sh` runs `terraform providers schema -json` after `init`, then sets
  `TF_LOG=DEBUG TF_LOG_PATH=$WORK/tf-debug.log` for the apply phases;
- `tools/tfcapture` parses the `---[ REQUEST ]---` blocks, maps each write to its
  provider resource, and joins the body attributes to
  `docs/fidelity/provider-attributes.csv` and `docs/fidelity/field-coverage.csv`;
- a sent-but-unsupported attribute, a sent attribute with no mapping row, and a
  mapping row the provider no longer declares all fail the case.

A case whose pinned module requires an older provider (the `workflows` case
pins 6.49.3) sets `CAPTURE_RESOURCES` to the provider resources it drives and
`CAPTURE_ALLOW_MISSING=1`. The first scopes the gate with
`tfcapture -only-resources`; the second downgrades a mapping row the case's
schema does not declare to a note (`-allow-missing-attributes`) instead of a
stale-row failure. Both are narrow: a sent-but-unsupported attribute, an
unclassified attribute the case's own schema declares, and a sent attribute
with no catalog row still fail.

`JC_TF_CAPTURE=0` disables the capture for local debugging; `JC_TF_KEEP=1` keeps
`$WORK` (debug log, schema, captured bodies) after the run. The weekly
`.github/workflows/gcp-provider-drift.yml` job re-runs the gates, diffs the
provider schema against `docs/fidelity/terraform-provider-schema.json`, and opens
an issue with the schema diff.

## Coverage inventory (TF1)

The suite is a curated snapshot, so `tools/tfcoverage` measures it against the
whole provider: one row per `hashicorp/google` resource type that maps to an
emulated service, with `service`, `emulator_state` (`served` / `limited` /
`unsupported` / `client-side`), `exercised_by` (`fixture:<name>`,
`module:<case>`, `plan-only:<case>` or `none`), `classification`, `reason` and
a `review_after` date. The committed output is
`docs/fidelity/terraform-coverage.csv`.

The hand-maintained half is `tools/tfcoverage/scope.yaml`: it maps resource-type
globs to services, exempts whole unserved subsystems (`out_of_scope`, wave-plan
§6), and records the fidelity-matrix state each service had when its rules were
reviewed. `exercised_by` is derived from the `resource` blocks in `main.tf` and
`module-cases/*.sh` — a module's *internal* resources are the module gate's
contract, not this inventory's. The TF4 composition patterns are not part of
that derivation yet, so a resource a pattern applies but the fixture/module
cases do not (for example `google_dataproc_metastore_service` in
`03-datalake`) still reads `none`: the inventory is a floor on exercised
coverage, not an exact set.

```bash
make check-gcp-terraform-coverage   # offline PR gate; also in gcp-ci-preflight
make gen-gcp-terraform-coverage     # rewrite the CSV after adding a resource/case
make gen-gcp-terraform-provider-schema-full  # new provider -> new snapshot
```

The gate fails when a provider resource type is neither classified nor
out of scope, a rule matches nothing (a removed resource type), the CSV does not
match a fresh computation, an unexercised/unsupported row in a `ga` service has
no reason or an expired review date, or a service the rules recorded as
`limited`/`unsupported` gains `ga` cells in the fidelity matrix (reclassify
instead of keeping a stale reason).

## Web corpus (TF2)

The fixture and the module cases are configurations this repository wrote. The
web corpus is the configuration surface the provider and Google publish:
`tools/tfcorpus` extracts every Example Usage block from
`hashicorp/terraform-provider-google`'s `website/docs/r` and every scenario
under `terraform-google-modules/terraform-docs-samples`, joins each block
against `docs/fidelity/terraform-coverage.csv`, and plans the in-scope ones.

Corpora are never vendored and no gate clones them: point the tool at local
checkouts. `corpus.yaml` pins the commit (and commit date) the committed report
was produced from.

```bash
git clone --filter=blob:none --sparse https://github.com/hashicorp/terraform-provider-google /tmp/tf-provider
git -C /tmp/tf-provider sparse-checkout set website/docs/r
git -C /tmp/tf-provider checkout v7.46.2
git clone --depth 1 https://github.com/terraform-google-modules/terraform-docs-samples /tmp/tf-samples
export TF_PROVIDER_DOCS=/tmp/tf-provider TF_SAMPLES_DIR=/tmp/tf-samples

make gen-gcp-terraform-corpus   # extract/refresh corpus.yaml (hand edits survive)
make test-gcp-terraform-corpus  # start the emulator, plan 5/service, run the apply allowlist
```

| artifact | role |
|---|---|
| `corpus.yaml` | generated manifest: pinned corpora plus one row per extracted block (`mode: plan|apply`, `status: candidate|excluded`, reason) |
| `corpus-report.md` / `.json` | committed triage: per-service plan counts, failure groups, apply results |
| `terraform-endpoints.yaml` | the single service -> provider custom-endpoint map |
| `tools/tfcorpus/` | discovery, plan classification, apply contract, endpoint emission |

A block that declares any resource type outside the TF1 served set is
`excluded` with `unsupported-service: …`, so an unserved subsystem never
becomes a case. `docs-samples` scenarios are recorded as `excluded` with a
deferred reason: they are multi-service stacks and running them is a follow-up.

Some published examples use the client-side `hashicorp/random` and
`hashicorp/time` providers for name uniqueness and waits; the generated module
pins them (`= 3.9.1`, `= 0.14.2`). A block that needs any other non-google
resource (e.g. `kubernetes_secret`) is `excluded` as `helper-provider: …`.

For a fast offline loop, point `TF_CLI_CONFIG_FILE` at a Terraform CLI config
whose `provider_installation` is a `filesystem_mirror` over the plugin cache and
whose `direct` method excludes the registry; otherwise every `init` re-resolves
the provider over the network (~1 minute per case instead of ~0.1s).

### Case modes

- `plan` (default): `init -backend=false` -> `validate` -> `plan -json`. The
  diagnostics are classified `plan-ok`, `unrouted` (404), `unimplemented` (501),
  `validation` (400), `auth` (401/403), `config`, `transport`,
  `provider-panic` or `other`. Plan-only results are triage data, not a gate:
  every failure group is owned by a row in the wave plan.
- `apply` (curated allowlist): `apply` -> REST asserts -> `plan
  -detailed-exitcode` == 0 -> `destroy` -> post-destroy asserts. `-apply`
  fails on a case that does not converge. An example that cannot converge — a
  recorded emulator gap or an upstream example defect — stays `mode: plan`, and
  the gap is a TF2-x row in the wave plan; the committed allowlist is green.
  It covers 20 of the 26 served services; the remaining six are plan-only
  because their published examples cannot complete the destroy half (TF2-15,
  TF2-18), while the fixture and module gates already apply those services.

The committed report is a 5-examples-per-service smoke (`-max-per-service 5`);
run the whole in-scope corpus with `TF_CORPUS_MAX_PER_SERVICE=0`.

### Endpoint map

`terraform-endpoints.yaml` is the single source for the provider's per-service
custom endpoints (field name, path, and the `container.` host token). `run.sh`
generates the fixture's `endpoints` tfvars from it, `run-module.sh` exposes
`endpoint_for <service>` to new cases, and `tfcorpus` emits the provider block
for every generated case. Print the user-facing block with:

```bash
make gcp-terraform-endpoints
```

`make test-gcp-terraform-corpus` SKIPs when `TF_PROVIDER_DOCS` is unset; set
`STRICT=1` to make a SKIP fatal.
