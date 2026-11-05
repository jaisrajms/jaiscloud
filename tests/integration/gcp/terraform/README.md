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

Deliberately **excluded** because it makes `apply` fail (and abort the run):

- **Cloud Run v2** — not implemented; declared out of scope in
  `docs/gcp/wire-contract.md` §5.

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
| `storage` | [`terraform-google-cloud-storage`](https://github.com/terraform-google-modules/terraform-google-cloud-storage) | `c7effc0be0f4` | `autoclass`, `hierarchicalNamespace`, `website`, `logging`, `customPlacementConfig` and `ipFilter` read back; mutated `autoclass.enabled` + labels/website/logging, then labels/logging/`ip_filter` cleared; each phase ends with `plan` **No changes** |
| `memorystore` | [`terraform-google-memorystore`](https://github.com/terraform-google-modules/terraform-google-memorystore) | `5415d5ccca28` | `authEnabled`, `connectMode`, `transitEncryptionMode`, `readReplicasMode`, `maintenancePolicy` and `persistenceConfig` read back (the two ForceNew modes no longer replace the instance); AUTH enabled then disabled, maintenance window/snapshot period changed, labels changed then cleared; each phase ends with `plan` **No changes** |
| `functions` | [`terraform-google-event-function`](https://github.com/terraform-google-modules/terraform-google-event-function) | `9f2e26ae3b73` | `ingressSettings` (`ALLOW_ALL` → `ALLOW_INTERNAL_ONLY`), the event trigger's default `failurePolicy` and `failurePolicy.retry`, and `environment_variables` read back; ingress/env changed and cleared, retry toggled; each phase ends with a `plan` whose only remaining diff is the recorded storage-object gap |
| `clouddns` | [`terraform-google-cloud-dns`](https://github.com/terraform-google-modules/terraform-google-cloud-dns) | `35cd6de8e647` | `privateVisibilityConfig` (VPC networks + GKE clusters), `forwardingConfig` (targets incl. `forwardingPath`), `peeringConfig` and `dnssecConfig` (state, `nonExistence`, both default key specs) read back; private description/labels/network list changed, then forwarding, peering and public-with-DNSSEC zones created and DNSSEC turned on; each phase ends with `plan` **No changes** |
| `cloud-operations` | [`terraform-google-cloud-operations`](https://github.com/terraform-google-modules/terraform-google-cloud-operations) (`modules/simple-uptime-check`) | `8c3e37951bb4` | the alert policy's `alertStrategy` (`autoClose`, `notificationRateLimit.period`) and `userLabels` read back alongside the uptime check; `auto_close`/rate limit and labels changed, then the strategy and labels cleared; each phase ends with `plan` **No changes** |

The storage, memorystore, functions, clouddns and cloud-operations cases pin provider `hashicorp/google = 7.46.2` (the clouddns module also declares `hashicorp/google-beta`, pinned to the same version). The cloud-operations case exercises `alertStrategy`; the `severity` enum the same change added is covered by the unit/transport tests, since the module exposes no variable for it.

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
```

`TF_MODULE_DIR` overrides the checkout location for whichever case you run
(default `$HOME/.cache/terraform-modules/<module repo>`); the Makefile targets
point it at `TF_MODULE_DIR` / `TF_STORAGE_MODULE_DIR` /
`TF_MEMORYSTORE_MODULE_DIR` / `TF_FUNCTIONS_MODULE_DIR` /
`TF_CLOUDDNS_MODULE_DIR` / `TF_CLOUD_OPERATIONS_MODULE_DIR` respectively.

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
the two `ForceNew` blocks) run the full gate:

1. `apply` the module with its defaults (`autoclass`, `hierarchical_namespace`,
   UBLA, `soft_delete_policy`), labels, `public_access_prevention = "enforced"`,
   `website`, `logging` and `ip_filter`; the apply log must contain no `400`,
   `501` or `unsupported update_mask`
2. assert through the REST API that the bucket carries labels, UBLA/PAP,
   `hierarchicalNamespace`, `website`, `logging` and the full `ipFilter`, and
   that the `dual-region` bucket reads back `hierarchicalNamespace.enabled=true`
   plus `customPlacementConfig.dataLocations` (the two `ForceNew` blocks — the
   create read-back is what stops the next plan replacing the bucket)
3. `plan -detailed-exitcode` → **no changes**
4. flip `autoclass.enabled` to true, change a label, and change
   `website`/`logging`; `apply`; assert the new values and that the untouched
   `ipFilter` survived; `plan` → **no changes**
5. clear back: `labels = {}`, `logging = {}`, `autoclass` false and
   `ip_filter { mode = "Disabled" }`; `apply`; assert the caller labels and
   logging are gone and the filter's ranges are cleared; `plan` → **no changes**
6. `destroy`

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
- After the virtual cluster alone is destroyed (`destroy -target`), the pool is
  still readable — real Dataproc does not delete a virtual cluster's node pools

## Provenance

Originally adapted from the floci-gcp `compat-terraform` / `compat-opentofu`
suites, pruned to the implemented services and made self-contained (no BATS, no
external compat repo). It is now an independent Terraform/OpenTofu provider
conformance suite; floci-gcp is not its spec (real GCP is). See
`plan_docs/gcp-terraform-compat-2026-09-22.md` for the findings that motivated
it.
