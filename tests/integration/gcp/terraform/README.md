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

## Pinned consumer-module gate

`run.sh` drives a fixture *we* wrote, so it only proves the surface we already
declared. `run-module.sh` (and `make test-gcp-terraform-pubsub`) drives a
**third-party** module instead: the real consumer is the specification.

The pinned consumer is
[`terraform-google-modules/terraform-google-pubsub`](https://github.com/terraform-google-modules/terraform-google-pubsub)
at commit **`6cf1860`** (2026-10-07, `google >= 6.2, < 9`). Its `main.tf` reads
`data "google_project"` on every plan to build the default Pub/Sub service-agent
e-mail (`service-${number}@gcp-sa-pubsub.iam.gserviceaccount.com`). That data
source also exposes `billing_account`, so the read performs
`GET /v1/projects/{p}/billingInfo` — the call that used to answer
`404 UnknownService: cannot detect target GCP service` and made the module
unusable. The gate proves that call now succeeds and that the module's whole
lifecycle works against the emulator.

Clone it once (this is the only step that needs the network):

```bash
git clone https://github.com/terraform-google-modules/terraform-google-pubsub \
  "$HOME/.cache/terraform-modules/terraform-google-pubsub"
git -C "$HOME/.cache/terraform-modules/terraform-google-pubsub" checkout 6cf1860
make test-gcp-terraform-pubsub
```

`TF_MODULE_DIR` overrides the checkout location (default
`$HOME/.cache/terraform-modules/terraform-google-pubsub`). The target is
**opt-in** for two reasons: it needs that one-time clone (this repo never clones
from the network in a gate), and it resolves the provider from the registry. It
is still safe to invoke unconditionally — with no checkout or no `terraform` it
prints `SKIP` and exits 0.

Against an already-running emulator, with a checkout you control:

```bash
TF_MODULE_DIR=/path/to/terraform-google-pubsub \
  tests/integration/gcp/terraform/run-module.sh http://localhost:8080 test-project
```

### What the module gate does

The script generates a throwaway root module in a temp dir — a `provider
"google"` block with the per-service custom endpoints the module touches
(`pubsub`, `resource_manager`, `cloud_billing`, `iam`, `service_usage`, each
with exactly one trailing slash) and a `module` block pointing at the local
checkout with `project_id`, `topic` and one `pull_subscriptions` entry — then
runs:

1. `init` (provider from the registry)
2. `plan` — must complete, which is what proves
   `data.google_project` + `projects.getBillingInfo` work
3. `apply`
4. spot checks through the emulator's REST API: the topic, the pull
   subscription (on that topic), and `projects/{p}/billingInfo` answering the
   no-account default (`billingAccountName: ""`, `billingEnabled: false`)
5. a second `plan -detailed-exitcode` — must report **no changes** (no
   permadiff on the module's computed IAM member / topic / subscription)
6. `destroy` (also on exit via a trap, so a failure still cleans up)

It prints one `PASS`/`FAIL` line per step and exits non-zero on any failure.

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
