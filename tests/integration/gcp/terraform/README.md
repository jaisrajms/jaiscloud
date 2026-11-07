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
- Cloud SQL for PostgreSQL (instance + database + user)
- Dataproc Metastore (federation + database IAM member)
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
```

Both targets are opt-in: if the binary is not installed they print `SKIP` and
exit 0. They build `jaiscloud-gcp`, start it ephemeral (REST `:8080`,
gRPC `:8081`), run the suite, and stop it.

Against an already-running emulator:

```bash
tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project
TF_BIN=tofu tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project
```

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

The storage, memorystore, functions, clouddns, cloud-operations, kms, firestore, cloudrun and bigquery cases pin provider `hashicorp/google = 7.46.2` (the clouddns module also declares `hashicorp/google-beta`, pinned to the same version; the cloudrun module creates the service through `google-beta` and creates its service account through the IAM API, so it pins and points both providers at the emulator). The cloud-operations case exercises `alertStrategy`; the `severity` enum the same change added is covered by the unit/transport tests, since the module exposes no variable for it. The kms case passes `key_rotation_period = null` because CryptoKey rotation is ENCRYPT_DECRYPT-only (the module's `7776000s` default would be rejected for a MAC key by real GCP too); the rotation mask path is covered by the unit/transport tests. The firestore case creates the CMEK key with `google_kms_key_ring` + `google_kms_crypto_key` and passes its id as the module's `kms_key_name`, mirroring a real config; it sets `delete_protection_state = "DELETE_PROTECTION_DISABLED"` because the module's `DELETE_PROTECTION_ENABLED` default would block destroy (real GCP refuses too). Its `(default)` phase runs first and in its own working directory/state because a read of the default materialises it, and a materialised default is a live record — a later create would be `ALREADY_EXISTS`. Its recreate phase destroys a plain database and re-applies the same id in its own state; the emulator allows immediate reuse (TGM13), whereas real GCP refuses to reuse a deleted database id for about five minutes.

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
```

`TF_MODULE_DIR` overrides the checkout location for whichever case you run
(default `$HOME/.cache/terraform-modules/<module repo>`); the Makefile targets
point it at `TF_MODULE_DIR` / `TF_STORAGE_MODULE_DIR` /
`TF_MEMORYSTORE_MODULE_DIR` / `TF_FUNCTIONS_MODULE_DIR` /
`TF_CLOUDDNS_MODULE_DIR` / `TF_CLOUD_OPERATIONS_MODULE_DIR` /
`TF_KMS_MODULE_DIR` / `TF_FIRESTORE_MODULE_DIR` / `TF_CLOUD_RUN_MODULE_DIR` /
`TF_BIGQUERY_MODULE_DIR` respectively.

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
path reads `data.google_compute_default_service_account`, which needs the
unserved `compute.projects.get` (TF2-1). `cloud_run_deletion_protection` is set
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

Each case prints one `PASS`/`FAIL` line per step and exits non-zero on any
failure.

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
- Cloud SQL instance is `RUNNABLE`, database and user exist
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
contract, not this inventory's.

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
