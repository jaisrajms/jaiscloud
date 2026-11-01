# GCP local-testability contract

This is the **local-testability contract** for `jaiscloud-gcp`. It states, per GCP
service, **what a local run may be trusted to prove** and **what must be verified
against real GCP**. An **AWS → GCP mapping** is included for reference.

It is a companion to, not a replacement for:

- [`docs/gcp/wire-contract.md`](wire-contract.md) — the wire contract: what
  `ga`/`limited`/`preview`/`unsupported` mean, stability, CI gates, client
  compatibility, accepted risks.
- [`README-GCP.md`](../../README-GCP.md) — the service overview and the authoritative
  [Known Limitations](../../README-GCP.md#known-limitations) list.
- [`docs/fidelity/fidelity-matrix.md`](../fidelity/fidelity-matrix.md) — the **source of
  truth** for per-operation state and reasons. This document derives every number from
  it; where they disagree, the matrix wins.

---

## 1. Purpose & scope

**Scope:** what `jaiscloud-gcp` can and cannot prove locally when used as a dev/CI
target.

**What the emulator can prove locally**

- An official Google client (REST service client or native gRPC client) pointed at
  `jaiscloud-gcp` sees the same request/response *shapes*, status codes, error
  envelopes, field names, and long-running-operation envelopes as the real API, within
  the surface declared by the fidelity matrix.
- Control-plane / IaC / metadata workflows round-trip and persist.
- For a **Green, data-plane** service, the data-plane operations and most semantics are
  exercised locally and gated in CI against captured real-GCP responses.

**What it cannot prove locally**

- That a client behaves correctly against the *real backend*. The emulator is not
  real GCP: several services are metadata-only, some long-running operations finish
  synchronously, identity authorization is not enforced (Cloud KMS crypto operations do
  honor a default-permissive cryptoKey IAM policy), and there is no quota/throttling plane.
- Anything behind a `preview` service (no engine ships locally), or the data plane of a
  metadata-only service.

**CI coverage (infra decision).** GitHub Actions runs on hosted `ubuntu-latest`
with no cluster, so the unit, conformance, persistence (`gcp_persistence` against
a Postgres service), SDK and async-LRO gates run in CI; the executor-backed
`*_e2e` suites do not. Two decisions stand:

- The **Docker-executor** e2e suites (`test-e2e-cloudrun-docker`,
  `test-e2e-functions-docker`, `test-e2e-dataproc-docker`,
  `test-e2e-managedkafka-broker-docker`) are intentionally **not** wired into CI.
  They re-assert the same control-plane flow as the k3s suites, their
  docker-specific logic already has unit coverage (`internal/gcp/runexec`,
  `internal/executor/container`, `internal/sparkhelpers`,
  `internal/gcp/broker/kafka`), and they need multi-GB images on a hosted runner
  — a duplicate signal at a high cost, and historically not where breakage is.
- The **k3s/k3d** e2e suites stay local-only by decision: gating them in CI would
  need a self-hosted cluster runner.

A local green run is evidence about the **wire contract**, not about real GCP
behaviour. Treat this document as the checklist for the gap.

---

## 2. Tier legend

Tiers classify each service from its fidelity-matrix cells, with two documented
overrides (`functions` → Yellow, `kms` → Green) so the tier reflects local **trust**
rather than a raw `ga` ratio.

| Tier | Matrix basis | Locally trustworthy? | What it means for local testing |
| --- | --- | --- | --- |
| 🟢 **Green** | `ga` cells, no `preview` | **Yes** — for data-plane services; shape only for metadata services | Build the GCP adapter and unit/CI-test it against the emulator. |
| 🟡 **Yellow** | only `limited` cells (or a large `limited` share) | **Shape only** | Control-plane/IaC metadata works; data-plane and authorization behaviour is not modelled. Gate on a real-GCP smoke test. |
| 🔴 **Red** | any `preview` cell | **No** | No engine/logic behind it (e.g. BigLake Iceberg). Local green would be meaningless. |

> `ga` means *supported and wire-conformant*, not *behaves like real GCP*. Read §5
> (behavioural depth) alongside the colour — a Green service can still be metadata-only.

---

## 3. Coverage snapshot

Read from [`docs/fidelity/fidelity-matrix.md`](../fidelity/fidelity-matrix.md) at the time
of writing:

| Layer | Cells | `ga` | `limited` | `preview` | `unsupported` | `ga` share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Overall** | 1166 | 987 | 123 | 14 | 42 | 85% |
| **gRPC** (official clients) | 491 | 443 | 18 | 0 | 30 | 90% |
| **REST** (Discovery-backed) | 675 | 544 | 105 | 14 | 12 | 81% |

Recorded evidence: **631/675** REST cells and **443/491** gRPC cells are
`verified`; every Discovery-mapped `ga` REST cell now has a validated transcript
response (the remaining unverified REST cells are `limited`/`preview`/
`unsupported`, or `ga` operations with no Discovery method — the shared
`google.longrunning.Operations`/`google.cloud.location` common APIs and the
function/Cloud Run data-plane URLs).

- gRPC-only wire services (no REST descriptor of their own): **Firestore Admin,
  Operations (long-running)**. Firestore Admin's REST control plane is served
  under the `firestore` service descriptor (see the `firestore` row).
- gRPC split = 443 `ga` + 18 `limited` + 30 `unsupported` = 491. REST split = 544 + 105 + 14 + 12 = 675.
  Overall = 491 + 675 = 1166.

**How to refresh.** The matrix is generated, not hand-edited. Run
`make gen-gcp-fidelity-matrix`, then re-read
[`docs/fidelity/fidelity-matrix.md`](../fidelity/fidelity-matrix.md) (or the canonical
[`fidelity-matrix.json`](../fidelity/fidelity-matrix.json)) and update the numbers here.
`make check-gcp-fidelity-matrix` fails CI if the committed matrix has drifted.

---

## 4. Per-service contract

`ga`/total is taken directly from the matrix. "Depth" is the behavioural-depth class
from §5. "Locally trustworthy?" answers the local-trust question, not the matrix state.

| Service | Transport(s) | `ga`/total | Tier | Depth | Locally trustworthy? | Note |
| --- | --- | ---: | --- | --- | --- | --- |
| `pubsub` | grpc, rest | 45/45 | 🟢 | Full | Yes | Topics, subscriptions, snapshots, seek, ordering, DLQ; subscription `retryPolicy` (`minimumBackoff`/`maximumBackoff`, 0–600s) is validated and round-tripped on `subscriptions.create`/`patch` over REST + gRPC, but has no effect on the emulator's fixed delivery backoff. |
| `storage` | grpc, rest | 52/52 | 🟢 | Full | Yes | Bucket/object/IAM/resumable; server-streaming `ReadObject` + bidirectional `BidiReadObject`. |
| `kms` | grpc, rest | 56/60 | 🟢 | Full | Yes | Symmetric/asym/MAC/raw + delete/import-job; 4 hard crypto leftovers. Crypto operations honor the cryptoKey IAM policy **default-permissively** (a key with no policy allows; a policy that omits the required role returns `PERMISSION_DENIED`). |
| `secretmanager` | grpc, rest | 30/32 | 🟢 | Full | Yes | Rotation schedule tracked; managed rotation needs Cloud SQL. |
| `firestore` | grpc, rest | 60/60 | 🟢 | Full | Yes | Documents/transactions/queries + Admin control plane (databases, collection-group fields, backup schedules, backups, user creds, and the `databases.operations` LRO poll family) over REST and gRPC; pipeline is a read-only subset. |
| `firestoreadmin` | grpc | 27/32 | 🟢 | Shape only | Shape only | `google.firestore.admin.v1.FirestoreAdmin` gRPC wire service: composite-index CRUD + control plane (databases/fields/schedules/backups/user creds), mirrored by the REST control plane under `firestore`. The 5 data-plane/DR RPCs (Export/Import/Restore/Clone/BulkDelete) are `unsupported` stubs. |
| `datastore` | grpc, rest | 16/16 | 🟢 | Full | Yes | Transactions are single entity-group with a read-set. |
| `monitoring` | grpc, rest | 48/48 | 🟢 | Full | Yes | Metrics/alerts/channels; `condition_threshold` + `condition_absent` are evaluated. |
| `logging` | grpc, rest | 10/11 | 🟢 | Full | Yes | Write/List/Delete; `TailLogEntries` is a bounded poll. |
| `iam` | grpc, rest | 24/52 | 🟢 | Shape only | Shape only | REST service accounts + keys + policy, mirrored over gRPC by the IAM Admin service (`google.iam.admin.v1.IAM`: service-account CRUD, all seven key RPCs including `keys.upload`, deprecated `signBlob`/`signJwt`, service-account IAM policy); the role/permission catalog RPCs are `unsupported` stubs and authz is **not enforced**. |
| `eventarc` | grpc, rest | 30/57 | 🟢 | Shape only | Shape only | Trigger/channel/provider CRUD over one core (gRPC + REST); a Pub/Sub-sourced trigger whose `destination.cloudFunction` names an existing function delivers matching events to it and provisions a backing Pub/Sub subscription (`transport.pubsub.subscription`, output-only) as its dead-letter surface (the other destinations are metadata only). The other 27 Eventarc RPCs are `unsupported` stubs. |
| `managedkafka` | grpc, rest | 34/44 | 🟢 | Shape only | Shape only | Metadata-only by default; an opt-in `JAISCLOUD_KAFKA_BROKER_MODE=docker\|k8s\|native` (or a global `JAISCLOUD_EXECUTOR_MODE`) starts a real Redpanda broker, so topic CRUD, consumer groups/offsets, and ACLs hit the Kafka wire and `bootstrapAddress` becomes live. `docker` runs the Redpanda image as a container on the local daemon (needs only the Docker daemon — no cluster, no host `rpk` toolchain); `k8s` runs a Pod + Service; `native` runs an `rpk` subprocess. With no broker (the default) consumer-group `list` returns an empty set and `get`/`update`/`delete` report `NOT_FOUND`. The real-Kafka paths are proven by the k3d gate `make test-managedkafka-broker-k8s` and the local-daemon gate `make test-managedkafka-broker-docker`. |
| `metastore` | grpc, rest | 26/38 | 🟢 | Shape only | Shape only | Control plane only; the Hive Thrift serving plane (:9083) is a separate surface (a single global catalog shared by all Services, not per-service — matching the AWS Glue Data Catalog's one-catalog-per-account+region model) that serves databases/tables/partitions/locks, the client-connect identity RPC (`set_ugi`), and the request-struct table reads (`get_table_req`/`get_table_objects_by_name_req`) Hive 2.3+/3.x clients use. An off-the-shelf Go HMS client drives it end-to-end (with Postgres restart) in `tests/persistent_mode/gcp/hms/`. The 5 deferred RPCs (export/restore/query/move/alter) are `unsupported` stubs by decision (admin/DR operations off the Spark job path, with no AWS Glue analogue); the shared `operations.get`/`list` path is disambiguated from Cloud Workflows by the service's own host token (`metastore.localhost` / `metastore.googleapis.com`). |
| `dataproc` | grpc, rest | 42/44 | 🟢 | Shape only | Shape only | Cluster/job metadata (GCE- or GKE-`virtualClusterConfig`-shaped) + real Spark on Docker/K8s executors; pollable async cluster/job LROs; real driver output/control files in GCS; optional Metastore attachment; `WorkflowTemplate` CRUD + `instantiate`/`instantiateInline` (inline DAG over the job core; only the latest template version is retained); `DiagnoseCluster` is an unsupported stub. Only the Spark family (`sparkJob`/`pysparkJob`/`sparkSqlJob`/`sparkRJob`) executes; `hadoopJob`/`hiveJob`/`pigJob`/`prestoJob`/`trinoJob`/`flinkJob` fail loud (job created then immediately `ERROR`). Long-running/streaming jobs (detected from `spark.*.streaming.*` properties; emulator approximation) stay `RUNNING` in mock mode, and `Job.scheduling` restarts a failed driver in K8s executor mode (thrash/limits, `ATTEMPT_FAILURE`); connector `jarFileUris` pass through to `spark-submit --jars`. `make test-dataproc-streaming` covers the scheduling/restart/lifecycle contract, and `make test-dataproc-streaming-k8s` runs a real k3d driver (`readStream.format("rate")` + `gs://` checkpoint) proving the job stays `RUNNING`, micro-batches commit, and cancel reaps the driver pod. `make test-dataproc-streaming-kafka` runs a Kafka-source driver against the live Managed Kafka broker (the connector supplied via `jarFileUris`) and asserts checkpoint offsets and the checkpoint-log consumer offsets advance before cancel reaps the driver. Auth/ADC fidelity must be tested on real GCP. |
| `operations` | grpc | 5/5 | 🟢 | Shape only | Shape only | Synchronous operation stub by default. Under `JAISCLOUD_LRO_MODE=async` the shared service is registry-backed for the services that persist operations (currently Service Usage): their persisted operations are `list`/`cancel`/`delete`-able (`cancel` validates but does not cancel) and an operation name no service owns is `NOT_FOUND` (real GCP semantics) instead of the lenient terminal stub. |
| `serviceusage` | grpc, rest | 12/12 | 🟢 | Shape only | Shape only | Accept-and-succeed enable/disable; no real API gating. |
| `resourcemanager` | grpc, rest | 8/15 | 🟢 | Shape only | Shape only | v1 REST + v3 gRPC project surfaces over one core: project lookup + project IAM (etag OCC); the 7 project lifecycle/lookup gRPC RPCs are unsupported stubs; authz not enforced. |
| `scheduler` | grpc, rest | 16/16 | 🟢 | Full | Yes | Job CRUD + `pause`/`resume`/`run` with a real cron engine that fires `httpTarget`/`pubsubTarget` jobs on the emulator clock (advance `/_jaiscloud/clock`, or `POST /_jaiscloud/scheduler-tick`/`jobs.run` to fire deterministically); failures retry with exponential backoff per `retryConfig`. `appEngineHttpTarget` is stored but never delivered (attempts recorded `Unimplemented`); English-like schedules and `oauthToken`/`oidcToken` real token minting are not modelled. |
| `tasks` | grpc, rest | 34/34 | 🟢 | Full | Yes | Cloud Tasks v2 over one core/store: queue CRUD + `pause`/`resume`/`purge` + queue IAM, task CRUD + REST `tasks:batchCreate`/`tasks:batchDelete`, and a dispatch engine that delivers due `httpRequest` tasks on the emulator clock (advance `/_jaiscloud/clock`, or `POST /_jaiscloud/tasks-tick`, to fire deterministically) with per-queue rate limits (`rateLimits`) and exponential-backoff retries (`retryConfig`); `RunTask`/`tasks:run` forces a synchronous attempt. `appEngineHttpRequest` is stored but never delivered (attempts recorded `Unimplemented`); `tasks:buffer`, 31-day expiry, IAM enforcement, and real `oauthToken`/`oidcToken` token minting are not modelled; rate/retry state is in-memory. |
| `workflows` | grpc, rest | 12/12 | 🟢 | Shape only | Shape only | Workflow definitions + executions + revision history; LROs complete synchronously by default, with an opt-in async mode (`JAISCLOUD_LRO_MODE=async`) that settles on `operations.get`. `ListWorkflowRevisions` serves the stored revisions (newest first), and `GetWorkflow` accepts an optional `revisionId`. |
| `workflowexecutions` | grpc, rest | 8/8 | 🟢 | Shape only | Shape only | Executions are synchronous. |
| `functions` | grpc, rest | 38/38 | 🟡 | Shape only | Shape only | Metadata CRUD + mock/docker/k8s call over REST and gRPC running the **GCP Functions Framework** contract (`POST /` on `$PORT`, source mounted at `/workspace`, `FUNCTION_TARGET`/`FUNCTION_SIGNATURE_TYPE`/`LOG_EXECUTION_ID`; the legacy Lambda-RIE contract is selectable with `JAISCLOUD_FUNCTIONS_EXECUTOR=lambda`); v2 runtime catalog (`/v2/.../runtimes`, gRPC `ListRuntimes`) served; common-API `GetLocation`/`CancelOperation`/`DeleteOperation`/`WaitOperation` served; function mutations persist a pollable `google.longrunning` operation store (memory + Postgres + snapshot), so `operations.get`/`list` and REST `:wait` return the typed response; GCS-referenced source archives (`sourceArchiveUrl` / `storageSource`) are fetched, persisted with a revision hash, and executed under Docker/K8s; v2 `generateUploadUrl` provisions a GCS-backed upload target so `gcloud functions deploy --gen2` runs end-to-end, and each deploy bumps a persisted revision counter so a deployed function renders `serviceConfig.revision` (the backing Cloud Run service revision) + `allTrafficOnLatestRevision` (no real container build); the v2 1st→2nd gen upgrade/traffic control plane (`setupFunctionUpgradeConfig`, `redirect`/`rollbackFunctionUpgradeTraffic`, `commitFunctionUpgrade`/`commitFunctionUpgradeAsGen2`, `abortFunctionUpgrade`, `detachFunction`) is served over REST with a persisted `upgradeInfo` state machine — REST-only in practice (documented as `FunctionService` RPCs, but absent from the public `googleapis` proto and all generated clients); the synthesized HTTPS trigger URL (`{location}-{project}.cloudfunctions.net/{id}`) is served (Host-scoped raw-body invocation, HTTP 500 on an executor error/timeout, 404 for an unknown or event-only function); event triggers deliver Pub/Sub publishes, GCS object finalize/delete, and matching Eventarc `cloudFunction` routes through the same executor, retry per `failurePolicy.retry`/`retryPolicy`, and persist a delivery record (`delivered`/`failed`/`dead_letter`); v2 `serviceConfig` instance/concurrency settings (`minInstanceCount`, `maxInstanceCount`, `maxInstanceRequestConcurrency`, `availableCpu`) are stored, range-validated against the real API (including the Cloud Run sub-1-vCPU → concurrency-1 rule), and surfaced; a configured `maxInstanceCount` (× `maxInstanceRequestConcurrency`) and a project-wide account cap are enforced by an invocation admission gate that returns HTTP 429 `RESOURCE_EXHAUSTED` when the capacity is exceeded, while no separate project quota/account-settings API is modelled (FD11). A Pub/Sub or Cloud Storage event trigger materializes a backing Eventarc trigger (`eventTrigger.trigger`, output-only) whose platform-provisioned `transport.pubsub.subscription` is user-configurable with `deadLetterPolicy` over `subscriptions.patch` (a Cloud Storage trigger's transport topic is auto-provisioned by the platform, mirroring real Eventarc); exhausted deliveries are republished to the dead-letter topic with the `CloudPubSubDeadLetterSource*` attributes. The v2 `ListRuntimes` filter evaluates the AIP-160 subset `=`/`!=`/`:`(contains)/`<`/`<=`/`>`/`>=` with `AND`/`OR`/`NOT` and parentheses over `name`/`displayName`/`stage`/`environment` (AIP-160 function calls are rejected with `InvalidArgument`). |
| `compute` | rest | 0/33 | 🟡 | Metadata only | Metadata only | No VM/disk/network data plane. |
| `cloudsql` | rest | 0/24 | 🟡 | Metadata only | Metadata only | No SQL engine or data plane. |
| `clouddns` | rest | 0/16 | 🟡 | Metadata only | Metadata only | No authoritative DNS server. |
| `memorystore` | rest | 0/8 | 🟡 | Metadata only | Metadata only | No Redis data plane. |
| `bigquery` | rest | 0/23 | 🟡 | Shape only | Shape only | A documented Standard SQL subset (`SELECT` + DDL/DML) runs on an in-process pure-Go SQLite engine in both memory and `--dsn` modes; `routines`/`models`/`rowAccessPolicies` are `501` stubs and the full GoogleSQL surface is not modelled. |
| `iceberg` | rest | 0/14 | 🔴 | None | No | BigLake Iceberg REST catalog; `preview`. |

Tier groups: **Green (21)** `dataproc`, `datastore`, `eventarc`, `firestore`,
`firestoreadmin`, `iam`, `kms`, `logging`, `managedkafka`, `metastore`, `monitoring`,
`operations`, `pubsub`, `resourcemanager`, `scheduler`, `secretmanager`, `serviceusage`, `storage`,
`tasks`, `workflowexecutions`, `workflows`. **Yellow (6)** `bigquery`, `clouddns`,
`cloudsql`, `compute`, `functions`, `memorystore`. **Red (1)** `iceberg`.

---

## 5. Behavioural depth — the honesty check

`ga` ≠ "behaves like real GCP". These four classes tell you how much real behaviour sits
behind a wire-conformant API.

> **Executor parity.** For a service that runs real work (Dataproc, Cloud Run,
> Functions, Managed Kafka), the real backends — `docker`, `k8s`, `native` — are
> interchangeable implementations of **one executor seam**: observable API
> behaviour must be identical across them, and only the **mock-vs-real**
> distinction changes depth. `docker` vs `k8s` is an orchestration choice, never a
> fidelity tier; a backend a service does not wire is a capability gap (listed in
> its cell and the console's availability matrix), not a shallower tier.
>
> **`docker` executor deployment.** The `docker` backend has two deployments,
> selected by the emulator's own environment and resolved once at startup by
> `docker.ResolveAndProbe`. **Host** (the default, and by default the deployment
> when nothing indicates otherwise) is the emulator process running on the same
> host as the Docker daemon: it reaches workloads on the host loopback
> (`127.0.0.1`/`localhost:<publishedPort>`) and bind-mounts emulator-local paths
> (code archives, layers, the platform TLS PEM bundle). **Container** is the
> emulator itself running as a container on that daemon (the socket
> bind-mounted, as `docker-compose.yml` does): it addresses workloads by
> container name on `JAISCLOUD_DOCKER_NETWORK` (publishing no host port, since a
> host-published port is unreachable from the emulator's own network namespace)
> and stages bind sources under `JAISCLOUD_DOCKER_BIND_ROOT`, a directory
> bind-mounted at the same path on the daemon host. The mode is auto-detected
> (Docker/Kubernetes container markers) and may be forced with
> `JAISCLOUD_DOCKER_MODE=host|container`; a host-networked container that shares
> the daemon host must set `host`. `docker-compose.yml` sets
> `JAISCLOUD_DOCKER_MODE=container` with the shared network and bind root, so its
> docker executor works rather than failing after startup.
>
> **Startup fallback probe.** A requested `docker`/`k8s` backend is probed once
> at startup and degrades to `mock` (reported truthfully in the console) when the
> probe fails: the `docker` probe pings the daemon socket **and** validates the
> deployment addressing (a containerized emulator without
> `JAISCLOUD_DOCKER_NETWORK` cannot reach its workloads, however reachable the
> daemon is), so it no longer reports a backend "reachable" that fails on the
> first invoke or bind mount; the `k8s` probe builds the Kubernetes client
> (`k8shelpers.NewClient`). The `k8s` probe is a client-construction check, not a
> reachability check — a *configured but unreachable* cluster still reports
> engine-backed and fails on first use.

| Depth | Services | What you can actually rely on locally |
| --- | --- | --- |
| **Full** | `pubsub`, `storage`, `kms`, `secretmanager`, `firestore`, `datastore`, `monitoring`, `logging` | Data-plane operations and most semantics, gated against captured real-GCP responses. |
| **Shape only** (wire-conformant, thin behaviour) | `iam` (authz not enforced), `resourcemanager` (v1 REST + v3 gRPC over one core; projects synthesized; authz not enforced; IAM policy is metadata), `serviceusage` (no real API gating), `eventarc` (Cloud Functions-only delivery), `firestoreadmin` (composite-index CRUD + the REST-mirrored control plane; data-plane/DR RPCs unsupported), `managedkafka` (metadata-only by default; an opt-in docker/k8s/native Redpanda broker backs `bootstrapAddress`), `metastore` (control plane shape only; the Hive Thrift plane serves databases/tables/partitions/locks plus Hive-3.x `get_table_meta`/`alter_table_with_cascade`, the identity RPC `set_ugi`, and the request-struct reads `get_table_req`/`get_table_objects_by_name_req`; niche partition methods unsupported), `operations` (LROs synchronous), `workflows` (LROs synchronous), `workflowexecutions` (LROs synchronous), `dataproc` (no real cluster locally unless an executor is wired), `functions` (no real container build; single revision; Docker/K8s execution applies the platform overlay — TLS PEM bundle, extra volumes/env — like the Cloud Run/Dataproc docker executors), `bigquery` (a documented Standard SQL subset — `SELECT` + DDL/DML — executes locally on an in-process SQLite engine; arrays/structs/`UNNEST`, scripting, wildcard tables and `INFORMATION_SCHEMA` fail loud, and the full GoogleSQL surface is not modelled) | Control-plane shape and metadata. Real behaviour must be tested on real GCP. |
| **Metadata only** | `compute`, `cloudsql`, `clouddns`, `memorystore` | Resource records + `get`/`list`; nothing actually runs. |
| **None (preview)** | `iceberg` | Nothing local counts as evidence. |

> **Rule of thumb:** build against **Full** services locally; treat **Shape only**,
> **Metadata only**, and **None** as "the API shape is right, the behaviour is not
> proven" and gate those on a real-GCP smoke test.

---

## 6. AWS → GCP mapping

| AWS (today) | GCP target | Emulator tier | Local trust |
| --- | --- | --- | --- |
| SQS | **Pub/Sub** | 🟢 `ga` (45/45) | High — full surface. |
| S3 | **Cloud Storage** | 🟢 `ga` (52/52) | High — full read surface (`ReadObject` + `BidiReadObject`). |
| DynamoDB | **Firestore** / Datastore | 🟢 `ga` (Firestore 60/60, Firestore Admin 27/32, Datastore 16/16) | High — watch transaction/OCC caveats ([Known Limitations](../../README-GCP.md#known-limitations)). |
| Lambda | **Cloud Functions** | 🟡 `limited` (38/38) | Control plane + GCS-referenced source execution (Docker/K8s); v2 `gcloud functions deploy --gen2` end-to-end (upload → create → poll); per-deploy revisions (Cloud Run shape) and the v2 1st→2nd gen upgrade/traffic control plane over REST; event triggers deliver Pub/Sub / GCS / Eventarc events with retry + dead-letter records (a Pub/Sub or GCS trigger's platform-provisioned backing subscription carries a user-configurable `deadLetterPolicy`). |
| KMS | **Cloud KMS** | 🟢 `ga` (56/60) | High; 4 hard crypto leftovers (`ImportCryptoKeyVersion`, trusted-key wraps, `Decapsulate`). |
| Secrets Manager | **Secret Manager** | 🟢 `ga` (30/32) | High; managed rotation needs Cloud SQL. |
| IAM | **Cloud IAM** | 🟢 `ga` (16/16) | Shape only — authz not enforced. |
| CloudWatch Logs / Metrics | **Cloud Logging / Monitoring** | 🟢 `ga` (Logging 31/54, Monitoring 48/48) | High; `TailLogEntries` is a bounded poll, sink routing is evaluated but not delivered, and the logging bucket/view/link/CMEK gRPC RPCs are `unsupported` stubs; `condition_threshold` + `condition_absent` evaluated. |
| EventBridge | **Eventarc** | 🟢 `ga` (30/57) | Trigger CRUD; Pub/Sub-sourced `cloudFunction` triggers deliver to their function with a backing subscription dead-letter surface. |
| Step Functions | **Workflows / Workflow Executions** | 🟢 `ga` (Workflows 12/12, Executions 8/8) | LROs complete synchronously by default; opt-in async timing via `JAISCLOUD_LRO_MODE=async`. |
| Athena / Redshift | **BigQuery** | 🟡 `limited` (0/23) | Shape only — a documented Standard SQL subset (`SELECT` + DDL/DML) executes locally; full GoogleSQL and real GCP semantics must be tested on real GCP. |
| RDS | **Cloud SQL** | 🟡 `limited` (0/24) | Metadata only. |
| EC2 | **Compute Engine** | 🟡 `limited` (0/33) | Metadata only. |
| ElastiCache | **Memorystore** | 🟡 `limited` (0/8) | Metadata only. |
| Route 53 | **Cloud DNS** | 🟡 `limited` (0/16) | Metadata only. |

*(The last four rows — Cloud SQL, Compute, Memorystore, Cloud DNS — are the
ones where AWS parity cannot be validated locally at all; budget real-GCP testing for
them up front.)*

---

## 7. Must test on real GCP

The local emulator deliberately does not model the following. A local green run is **not**
evidence for any of them.

- [ ] **Authz / IAM enforcement.** Identity permissions are not checked; Cloud IAM is
      shape-only across all services. The single exception is Cloud KMS, whose crypto
      operations honor a **default-permissive** cryptoKey resource policy. Any
      permission-sensitive path must be smoke-tested on real GCP. The differential
      harness's `*_noauth` scenarios record real GCP's unauthenticated response (401
      `UNAUTHENTICATED`) against the emulator's served response; those divergences are
      accepted by design so the gap is explicit and gated.
- [ ] **Async long-running-operation (LRO) timing.** Operations complete synchronously by
      default (`done: true` inline). An opt-in async mode (`JAISCLOUD_LRO_MODE=async`, in-flight
      window via `JAISCLOUD_LRO_DELAY`, default `250ms`) makes `workflows`, `functions`,
      `metastore`, `managedkafka`, `dataproc` and `serviceusage` return in-flight operations and
      settle them lazily on `operations.get`; the generic `google.longrunning.Operations` service
      then serves a registry-backed `list`/`cancel`/`delete` and reports an unowned name as
      `NOT_FOUND`. `make test-lro-async-gcp` exercises it end-to-end. It is a
      **local-testing affordance, not real-GCP timing**: code that assumes immediate readiness
      must still be tested on real, eventually-consistent GCP. The `google.longrunning.Operations`
      service is also endpoint-scoped: a client addressed at a service's endpoint
      (`{service}.localhost` / `{service}.googleapis.com`, via the gRPC `:authority`) lists only
      that service's operations, matching real GCP's per-endpoint Operations; an unregistered
      endpoint gets the shared name-based `Get`/`Wait` and an empty list.
- [ ] **Metadata-only services.** `compute`, `cloudsql`, `clouddns`, `memorystore` have
      no control/data plane locally — only resource records. Test the real data plane. The
      differential harness records read-only control-plane smoke for `compute`/`cloudsql`/
      `memorystore` (list + 404 get-missing), which validates routing, the empty-list shape
      and the error envelope — not a data plane.
- [ ] **BigQuery.** Only the documented Standard SQL subset executes locally — `SELECT`
      plus DDL/DML on an in-process SQLite engine. `ARRAY`/`STRUCT`/`UNNEST`, `GEOGRAPHY`,
      wildcard/`TABLE_SUFFIX` tables, `INFORMATION_SCHEMA`, scripting, `MERGE`, and legacy SQL
      fail loud (`400 invalidQuery`), and DDL/DML job timing / result paging / `getQueryResults`
      snapshot semantics are simplified. Full GoogleSQL behaviour must be tested against real
      BigQuery.
- [ ] **Dataproc Structured Streaming data path.** `make test-dataproc-streaming-k8s` proves the
      real-k3d lifecycle contract with the built-in `rate` source and a `gs://` sink/checkpoint
      (job stays `RUNNING`, micro-batches commit, cancel reaps the driver), and
      `make test-dataproc-streaming-kafka` proves the Kafka-source path against the live Managed
      Kafka broker (connector via `jarFileUris`; checkpoint and broker-side committed offsets
      advance). Pub/Sub / Pub/Sub Lite sources, driver-pod→emulator networking with real
      ADC/impersonation, and continuous (mid-batch) driver-output streaming are not modelled and
      must be exercised on real GCP.
- [ ] **Cloud Functions v2 build.** v2 request/response *shapes* are served, runtime resolution
      works (`/v2/.../runtimes`), `gcloud functions deploy --gen2` runs end-to-end
      (`generateUploadUrl` → GCS-backed upload → create → poll), and source referenced by a GCS
      object (`sourceArchiveUrl` / `storageSource`) is fetched and executed in Docker/K8s mode.
      There is **no real container build** (the archive is only checked non-empty); each deploy
      bumps a persisted revision counter so revision names change, and the v2 1st→2nd gen
      upgrade/traffic control plane (setup/redirect/rollback/commit/abort/detach) is modelled over
      REST, but build failures and a real Gen2 build pipeline must be exercised on real GCP.
- [ ] **Quotas, throttling, and retry/backoff.** No general rate-limit or quota plane is
      modelled, so most backoff and quota-exhaustion paths are never exercised locally. An
      **opt-in** injector (`JAISCLOUD_GCP_THROTTLE`, default `off`) refuses matching REST + gRPC
      requests before dispatch with a retryable `429 RESOURCE_EXHAUSTED`/`503 UNAVAILABLE`, a
      `Retry-After` header and a `google.rpc.RetryInfo` detail (`make test-throttle-gcp` exercises
      it end-to-end); it injects **synthetic** failures, not real quota values. The policy can be
      changed **mid-run** without a restart via `POST /_jaiscloud/throttle` (`GET` reports the
      current state); an unarmed injector is a no-op. Cloud
      Functions instance/concurrency configuration (`minInstanceCount`/`maxInstanceCount`/
      `maxInstanceRequestConcurrency`/`availableCpu`) is stored, range-validated, and surfaced,
      and a configured `maxInstanceCount` (× `maxInstanceRequestConcurrency`) plus a
      project-wide account cap is enforced by an invocation admission gate that returns HTTP
      429 `RESOURCE_EXHAUSTED` with no available instance; event delivery treats that refusal as
      back-pressure (re-polled without consuming a retry/dead-letter attempt; a persistent
      throttle is left `pending`). The project quota/account-settings
      API itself is still not modelled (FD11): Cloud Functions declares no account/quota method
      (real GCP exposes quotas only through the separate Cloud Quotas / Service Usage APIs), so
      Lambda's `GetAccountSettings` has no Cloud Functions analogue and is deliberately not faked.
- [ ] **Frozen-clock OCC / TTL.** With the clock frozen (`POST /_jaiscloud/clock`),
      clock-derived TTLs (Datastore transaction expiry) and DynamoDB-style TTL edge cases can
      behave differently. (Firestore's per-document `UpdateTime` and Datastore's per-entity
      `update_time` OCC tokens are kept strictly monotonic even under a frozen clock, so
      concurrent writes still conflict correctly.) The dedicated time-control suite
      (`tests/integration/gcp/time_control_test.go`) pins the contract deterministically — the
      three modes (`fixed`/`offset`/`real`), the admin endpoint validation, `reset` restoring real
      time, no cross-test leak, and the clock-driven expiry/dispatch paths: Scheduler due-time
      dispatch, Pub/Sub retention expiry, KMS scheduled-destroy and the async-LRO deadline (the
      frozen-timestamp, Scheduler and KMS cases are cross-checked REST + gRPC; the async-LRO case
      is REST-only). Advancing a frozen clock does **not** wake the wall-time
      tickers (Scheduler, Tasks, Monitoring, EventBridge), so a deterministic test fires those
      engines through their synchronous `/_jaiscloud/<engine>-tick` endpoint rather than sleeping;
      the lazy-on-read paths (KMS destroy promotion, LRO settlement, Pub/Sub retention) settle on
      a bare clock advance. Do not rely on frozen-clock results as production evidence.
- [ ] **Per-language SDK wire paths.** Different official SDKs exercise different wire
      paths. Add each client SDK/language in use to the conformance matrix rather
      than assuming one client's pass transfers.

### Clock correctness — business vs infrastructure time (TD15)

`CLAUDE.md` §Time splits the two clocks: `clock.Now()` is the controllable
**business** clock (frozen/offset by `POST /_jaiscloud/clock`), and
`clock.RealNow()` is the **infrastructure** wall clock — container lifecycle,
keepalive/GC tickers, request latency, TLS/token windows, rate-limiter refill.
Choosing wrong compiles and ships silently. `gcplint`'s `clock` rule bans bare
`time.Now()`/`time.Since`/`time.Until`; the `clock-realnow` rule flags a
`clock.Now()` that drives a wall-clock wait — a poll-loop condition, a derived
deadline compared inside a loop, or an argument to a wall-clock primitive
(`time.Sleep`/`After`/`NewTimer`/`NewTicker`/`Tick`, `context.WithDeadline`). The
rule is conservative; deliberate exceptions belong in
`tools/lint/gcplint/allowlist.txt`.

The TD15 audit classified every `clock.Now`/`clock.RealNow` call site under
`internal/gcp/**` and `cmd/jaiscloud-gcp`:

| category | example | verdict |
| --- | --- | --- |
| business timestamp (wire metadata, state transitions, LRO settle, TTL) | providers/stores, `lro.Pending`, Datastore txn expiry | `clock.Now()` — correct |
| token-bucket refill / rate limiting | `throttle`, Tasks dispatch bucket | `clock.RealNow()` — correct |
| container/pod readiness wait | `runexec`, `broker/kafka` | `clock.RealNow()` — correct |
| resumable-upload session GC/TTL | storage (REST + gRPC), BigQuery | `clock.RealNow()` — correct |
| bounded long-poll wait | Pub/Sub `longPoll` (REST + gRPC) | was `clock.Now()` — **fixed to `clock.RealNow()`** |

Two sites were genuine misuses: the Pub/Sub `longPoll` deadline in
`provider/pubsub` and `grpc/pubsub` derived its bounded window from the business
clock, so a frozen test clock made an empty long poll never return (the loop has
no other exit). Both now measure the request-bounded window with
`clock.RealNow()`; the `Pull` retention/ack-deadline inputs stay `clock.Now()`
(business). Regression tests: `TestPubSubLongPollBoundedFrozenClock` (REST) and
`TestLongPollBoundedFrozenClock` (gRPC).

**Deferral:** the `clock-realnow` rule does not statically catch the
token-bucket *elapsed-refill* shape (`now := clock.Now(); now.Sub(last)`), because
a business "elapsed since" is indistinguishable from an infrastructure one
without dataflow — the in-tree buckets (`throttle`, Tasks) already use
`clock.RealNow()` and are covered by review.

Time-driven engines expose a deterministic trigger so a test never sleeps: a
**real** wall-time ticker for production, plus a synchronous `TickNow` behind an
admin endpoint driven by the **business** clock.

| engine | deterministic trigger | production ticker | exercised by |
| --- | --- | --- | --- |
| Scheduler | `POST /_jaiscloud/scheduler-tick` | 1s `time.NewTicker` | `time_control_test.go`, `sdk-scheduler`, `scheduler_dataproc_hop_k8s` |
| Tasks | `POST /_jaiscloud/tasks-tick` (+ `RunTask`) | 1s `time.NewTicker` | `sdk-tasks` |
| Monitoring | `POST /_jaiscloud/monitoring-tick` | 30s `time.NewTicker` | `monitoring_alert_test.go`, `admin/monitoring_tick_test.go` |

Eventarc (the GCP analogue of AWS EventBridge) is **event-driven**, not
time-driven: delivery runs on the producing event with real HTTP timeouts, so it
has no tick engine to freeze.

Additional service-specific caveats live in
[`README-GCP.md` → Known Limitations](../../README-GCP.md#known-limitations); read the
section for every service you touch.

---

## 8. Do not depend on emulator-only behaviour

Production code paths must never rely on affordances that exist only in the emulator.
These are fixed by policy, not by the fidelity matrix:

- **`/_jaiscloud/*` admin endpoints** — `health`, `doctor`, `reset`, `export`, `import`,
  `snapshot*`, `clock`, `throttle`, `ttl-sweep`, `eb-tick`, `scheduler-tick`, `tasks-tick`, and
  `/metrics` are local control surfaces, not GCP APIs. Never call them from application code.
- **Reset / state wipe** — `POST /_jaiscloud/reset` and the `reset` CLI command exist for
  test isolation only; production code must not assume resettable state.
- **Lenient validation** — the emulator accepts and ignores more than real GCP (shallow
  create validation, dropped unmasked/unknown fields, synthesized placeholders). Do not
  encode emulator acceptance as a correctness assumption.
- **Absent authz** — requests succeed without credentials/permissions locally. Treat every
  IAM decision as unverified until tested on real GCP.
- **Frozen clock** — deterministic-time mode is a test affordance. Business logic must not
  depend on a frozen/offset clock.
- **Throttle/quota injection** — `JAISCLOUD_GCP_THROTTLE` is an emulator-only failure injector
  (default `off`) that injects synthetic retryable failures, never real quota values; the
  `POST /_jaiscloud/throttle` runtime control is equally emulator-only. Production code must not
  depend on its presence or its limits; quota behaviour must be verified on real GCP.

A useful guard is a lint/test that rejects references to `/_jaiscloud/` and clock control
in production packages.

---

## 9. Behavioural-suite contract — the `ga` Definition of Done (gated)

**The contract.** Every **`ga` service** must be exercised by **at least one
official-client behavioural suite**, or carry a **reasoned exemption** in
[`docs/gcpstatus-coverage-exemptions.yaml`](../gcpstatus-coverage-exemptions.yaml).
This is the behavioural-depth counterpart to the wire-shape layers in §1–§5: the
conformance / differential / parity harnesses prove the *shape* (the emulator
answers like real GCP), while this proves an official client can drive a
*lifecycle* (create → read → list → update/error paths → delete) through it.

- **`ga` service** = a service whose fidelity-matrix cells include at least one
  `ga` cell and **no `preview` cell**. This is a deliberately **mechanical, raw
  matrix rule** — a direct reading of the per-operation/per-transport states in
  `docs/gcp/wire-contract.md`, with **no overrides**. It is *not* §2's curated tier above: §2
  applies hand-picked overrides (e.g. `functions`→Yellow, `kms`→Green) and its
  snapshot is stale, so the inventory does not claim to reproduce it. A service
  may carry a few explicit `unsupported` stubs (`container`, `dataproc`,
  `metastore`) and still be `ga`. A `limited`-only service (zero `ga` cells —
  `bigquery`, `compute`, `clouddns`, `cloudsql`, `memorystore`) and any `preview`
  service (`iceberg`) is **not** `ga` and is **exempt** from this contract.
- **An official-client behavioural suite** = a `tests/integration/gcp` suite that
  drives an **official Google client** — a REST service client
  (`google.golang.org/api/...`) or a native gRPC client
  (`cloud.google.com/go/...`) — through a behavioural lifecycle. It is **not** a
  unit test, and it is **not** satisfied by the shape-only layers — conformance
  (`tests/gcpconformance`), real-GCP differential (`tests/gcpdifferential`),
  REST↔gRPC parity (`tests/gcpparity`), or the persistence-after-restart probe
  (AUD9) each prove something different. **Raw-REST coverage that drives no
  official client does not count**: e.g. `serviceusage` is exercised only through
  hand-rolled HTTP in `lro_async_test.go`, so it intentionally stays `uncovered`.
  A dedicated `sdk-<service>` module is the canonical form; a generic suite
  (`sdk`, `sdk-rest`, `sdk-gcs-grpc`) that imports a service's official client
  and drives it counts too.
- **A reasoned exemption** = an entry in
  [`docs/gcpstatus-coverage-exemptions.yaml`](../gcpstatus-coverage-exemptions.yaml)
  naming the service and *why* no `tests/integration/gcp` module is warranted —
  a stateless surface already covered by the IAM + conformance suites
  (`iamcredentials`). The gate accepts a service with either a suite **or** an
  exemption; a typo'd/unknown exempt service fails loud, as does a stale
  exemption for a service that now has a suite.

**How it is measured — derived, never hand-written.** `tools/gcpstatus` joins
three independent sources — the service registry (`internal/gcp/adapter`), the
fidelity matrix (`docs/fidelity/fidelity-matrix.json`), and the
`tests/integration/gcp/**` test tree — into one row per service (**the union of
the registry names and the matrix service names**, so a matrix-only service such
as the shared `operations`/`google.longrunning` service is not dropped):
`service → canonical → transports → suite(s) → test-func count → covered?`.
`make gcp-status` writes the inventory to `plan_docs/STATUS.md`
(**Behavioral coverage**) and prints the covered count;
`bin/gcpstatus -coverage-inventory` prints the table directly. In the table a
covered `ga` service shows `yes`, a reasoned exemption shows `exempt`, a real
gap (neither) shows `**no**`, and a non-`ga` service shows `n/a`. Because the
list is derived from the registry + matrix, a newly-registered service appears
automatically — which is the point: a service cannot land `ga` with conformance
+ parity only without the gate failing.

**Per-service test map (service → suite → transport).** Derived from the same
inventory (`make gcp-status-behavioral-gate` prints the summary;
`bin/gcpstatus -coverage-inventory` prints the full table). Services use their
canonical matrix name (`clouddns`/`memorystore`/`cloudsql`), which differs from
a few registry/wire names (`dns`/`redis`/`sqladmin`). `—` means no dedicated
behavioural module — the service is either non-`ga` (exempt by the matrix rule)
or carries a reasoned exemption.

| service | transport(s) | behavioural suite(s) | coverage |
| --- | --- | --- | --- |
| `storage` | grpc, rest | `sdk`, `sdk-gcs-grpc` | suite |
| `pubsub` | grpc, rest | `.` (root-suite client), `sdk-pubsub`, `sdk-rest`, `sdk-dataproc` | suite |
| `kms` | grpc, rest | `.` (root-suite client), `sdk-rest` | suite |
| `secretmanager` | grpc, rest | `sdk-rest` | suite |
| `firestore` | grpc, rest | `sdk-firestore`, `sdk-rest` | suite |
| `firestoreadmin` | grpc | `sdk-firestoreadmin` | suite |
| `datastore` | grpc, rest | `sdk-datastore` | suite |
| `monitoring` | grpc, rest | `sdk-monitoring` | suite |
| `logging` | grpc, rest | `sdk-logging` | suite |
| `iam` | grpc, rest | `sdk-rest` | suite |
| `iamcredentials` | grpc, rest | — | exempt (stateless; IAM `sdk-rest` + conformance) |
| `eventarc` | grpc, rest | `sdk-eventarc` | suite |
| `managedkafka` | grpc, rest | `sdk-managed-kafka` | suite |
| `metastore` | grpc, rest | `sdk-metastore` | suite |
| `dataproc` | grpc, rest | `sdk-dataproc` | suite |
| `operations` | grpc | `.` (root-suite client) | suite |
| `serviceusage` | grpc, rest | `sdk-rest` | suite |
| `resourcemanager` | grpc, rest | `sdk-rest` | suite |
| `scheduler` | grpc, rest | `.` (root-suite client), `sdk-scheduler` | suite |
| `tasks` | grpc, rest | `sdk-tasks` | suite |
| `workflows` | grpc, rest | `sdk-workflows` | suite |
| `workflowexecutions` | grpc, rest | `sdk-workflows` | suite |
| `functions` | grpc, rest | `sdk-rest` | suite |
| `container` | grpc, rest | `sdk-container` | suite |
| `run` | grpc, rest | `sdk-run` | suite |
| `compute` | rest | `sdk-compute` | limited — not required |
| `cloudsql` | rest | `sdk-cloudsql` | limited — not required |
| `clouddns` | rest | `sdk-clouddns` | limited — not required |
| `memorystore` | rest | `sdk-memorystore` | limited — not required |
| `bigquery` | rest | `sdk-bigquery` | limited — not required |
| `iceberg` | rest | `sdk-iceberg` | preview — not required |

**Enforcement.** The contract is a **hard gate**:
`make gcp-status-behavioral-gate` (`bin/gcpstatus -coverage-gate`) exits non-zero
if any `ga` service has neither a suite nor a reasoned exemption. It runs in CI
(the `test-gcp-unit` job) and as part of the `test-all-gcp` aggregate, and only
reads the fidelity matrix + the `tests/integration/gcp` tree (no server, no
Docker), so it is safe on a hosted runner. The exemption list is committed so a
waiver is a reviewed, evidence-carrying change rather than an invisible one.

> **Depth beyond the lifecycle baseline.** This contract proves a suite *exists*;
> §10 records how *deep* it goes (`p2`–`p5`) and gates that record too
> (`make gcp-status-depth-gate`).

---

## 10. Behavioural depth classes — the `p2`–`p5` record (gated)

§9 proves a `ga` service has **a** lifecycle suite; it says nothing about how
**deep** that suite goes — a service can be one test func deep and still pass.
This section adds the depth axis, borrowed from the AWS reference
implementation's test-suite taxonomy (its `p1`–`p5` behaviour classes). A
suite's *depth* is recorded as five behaviour classes:

| class | what the suite must exercise |
| --- | --- |
| `p1` | **baseline lifecycle** — create → get → list → update → delete through an official client. This is exactly §9's Definition of Done; `p1` stays **derived** from the coverage inventory (`coverageRow.Covered`), never recorded by hand. |
| `p2` | **concurrency & contention** — parallel writers under `-race`, OCC/etag/precondition conflicts (`409`/`412`), lost-update prevention through transactions, lease/ack contention (ack-deadline redelivery, ordering, exactly-once). |
| `p3` | **errors, negatives & retry/idempotency** — the real error envelope + codes (`googleapi.Error` / `codes.*`), `404`/`409`/`400`, `ALREADY_EXISTS` vs conflict semantics, invalid-input rejection, idempotent retry. |
| `p4` | **listing & paging** — `pageToken`/`pageSize` (or `nextPageToken`/`maxResults`) composed with `filter`/`orderBy`/`delimiter`/`prefix`, empty pages, cross-page stability. |
| `p5` | **scale & boundary** — large payload/object streaming (no full buffering), many resources, version/rotation lineage, long-running/streaming lifecycle, retention/TTL edges. |

**The definition of done.** Every `ga` service (the §9 rule: ≥1 `ga` cell, no
`preview` cell) must **account for each of `p2`–`p5`** — either as a class its
official-client suite(s) exercise, or as a **reasoned, dated exemption** (a
metadata-only service with no listing has no `p4`; a stateless token service has
none of them). A class that is neither present nor exempt is **unaccounted for**
and fails the gate.

**The record — one file per service, derived + validated.** The record lives at
[`docs/test-depth/<canonical>.yaml`](../test-depth/), conventionally one
`<canonical>.yaml` file per service (not a single central file, so per-service
sessions extend their own record without colliding). Each `p2`–`p5` cell is
either:

- `state: present` — with `suites` (the official-client suite(s) asserting it,
  each of which must be one of the service's §9 coverage suites) and a one-line
  `note`; or
- `state: exempt` — with a `reason`, a `date` (`YYYY-MM-DD`, when the exemption
  was recorded), an `owner` (the backlog ID that will close or re-review it,
  e.g. `DC3`) and a `review_by` (`YYYY-MM-DD`) — a dated promise to re-review,
  not a permanent pass.

The loader rejects a typo'd service, an unknown class key, a duplicate service
across files, a `present` cell with no note/suite or a suite the service does
not own, and an `exempt` cell with no reason, an unparseable `date`, no `owner`,
or an unparseable `review_by`. The gate additionally fails an exemption whose
`review_by` has passed, so an exemption cannot be inherited forever.

**How it is measured and enforced.** `tools/gcpstatus` joins the §9 inventory
with `docs/test-depth/` into a per-service `p1`–`p5` matrix: `make gcp-status`
writes it into the **Behavioral coverage** table in `plan_docs/STATUS.md`
(`✓` present, `exempt` reasoned gap, `**no**` unaccounted for, `n/a` non-`ga`),
and `bin/gcpstatus -coverage-inventory` prints it directly. The contract is a
**hard gate**: `make gcp-status-depth-gate` (`bin/gcpstatus -depth-gate`) exits
non-zero if any `ga` service has no record or leaves a `p2`–`p5` cell
unaccounted for. It runs in CI (the `test-gcp-unit` job, beside the §9 gate) and
in the `test-all-gcp` aggregate, and — like §9 — only reads the fidelity matrix,
the test tree, and the committed records (no server, no Docker).

Pre-existing thin services are **grandfathered**: their day-one record carries
dated exemptions (often `owner: TD<n>` from the test-depth wave), so a *newly
added* `ga` service with no `docs/test-depth` record fails the gate on landing.
The per-service test-depth sessions then replace those exemptions with `present`
cells as the depth lands.

**The shared depth-test kit (DC2).** The Wave-2 depth cells are closed through a
small, service-agnostic kit at
[`tests/integration/gcp/testdepth/`](../../tests/integration/gcp/testdepth/) — a
separate module (`jaiscloud-gcp-testdepth`, deps pinned to the oldest in-tree
versions so MVS never forces a bump) that the suites import, never edit. It has
three parts: a **list/paging contract** driver (`AssertPaging`/`Walk`/`CheckPages`
— run a list method with `pageSize`/`pageToken`, composed with `filter`/`orderBy`,
asserting every resource once, the documented ascending-name order **across page
boundaries** (the last item of a page is compared with the first item of the
next), an empty final page and cross-page stability — plus
`AssertInvalidPageSize` for the documented negative); **error-envelope** helpers (`StatusOf`/`CheckError`/`AssertError`/
`RunNegatives` with the `SpecNotFound`/`SpecAlreadyExists`/… catalogue — one spec
asserts both transports, since the `google.rpc` status name is the REST
envelope's `error.status`); and a **contention** helper (`Race`/`AssertOneWinner`
with a `-race` build-tag guard, since an un-raced writer run proves nothing). It
is official-client-based and carries no service specifics, so a `p2`–`p4` cell
costs a test function rather than a session — each service still asserts its own
Discovery-document-pinned contract (order, filter, paged-vs-unpaged) on top of
the shared driver.

---

## 11. Statement-coverage ratchet — the per-package floor (gated)

§9 proves a suite **exists** and §10 records which behaviour classes it
exercises; neither measures how much of a package's code those tests actually
**execute**. Statement coverage does not measure *assertion strength* — that is
the mutation-testing axis (`TD13`) — but it does make a largely-unexercised
surface visible, which is the axis this section adds: a committed **per-package
statement-coverage floor** that can only move up.

**How it is measured.** `make test-gcp-coverage` runs
`go test -count=1 -covermode=set -coverprofile=bin/gcp-coverage.out
./internal/gcp/...` and feeds the profile to `bin/gcpstatus -coverage-ratchet`.
The profile is aggregated per package (statements with a non-zero execution
count over total statements), and a per-package table is printed — wrapped in a
collapsed GitHub Actions `::group::` in CI.

**The floor.** [`docs/gcp-coverage-floor.yaml`](../gcp-coverage-floor.yaml) records,
per repo-relative package dir, the statement-coverage percentage at the time it
was last ratcheted. It is a **ratchet, not a target**: it starts at today's
measured value and only moves up. `make test-gcp-coverage-update` rewrites it
from a fresh profile and **raises** a recorded floor but refuses to **lower** one
unless `ALLOW_LOWER=1` is set (a held regression is reported, not silently
recorded) — so a coverage drop for a package that still produces data cannot be
laundered by re-running the update. Generated protobuf packages (a final path
element ending in `pb`, e.g. `grpc/storage/storagepb`) are excluded mechanically:
they carry no hand-written behaviour to cover. Packages with **no test files**
produce no coverage data and are therefore outside the ratchet (a regression
needs tests to regress); the §9 gate still requires a behavioural suite for every
`ga` service.

**Enforcement.** The contract is a **hard gate**: `make test-gcp-coverage`
(`bin/gcpstatus -coverage-ratchet`) exits non-zero when

- a measured package drops **more than 0.5pp below** its recorded floor (a small
  tolerance for a few timing-dependent tests whose statement coverage varies
  run-to-run — e.g. a gRPC receive loop racing `ctx.Done`);
- a measured package has **no floor entry** (a new package must be recorded); or
- a floor entry names a package that **no longer produces coverage data**
  (removed/renamed — re-run the update).

It runs in CI (the `test-gcp-unit` job, beside the §9/§10 gates) and in the
`test-all-gcp` aggregate, and — like §9/§10 — needs no server and no Docker. Its
violation branches (below / unrecorded / stale) and its ratchet / tolerance edges
are unit-tested in `tools/gcpstatus/coverageratchet_test.go`; a manual seeded
negative (deleting a suite's test file drops its package below the floor) also
fails the gate.

---

## 12. Load / concurrency gate — the hot-path soak (local + nightly)

§9–§11 prove a suite *exists*, records its *depth*, and *executes* code in each
package. None of them can catch a defect that only appears under contention: a
race that `-race` misses because the interleaving needs many writers, a 5xx
under load, or state that grows without bound as requests accumulate. This
section adds that axis — a bounded soak of three hot paths.

**The gate.** `make test-gcp-load` (`test/gcp-load` package under
`tests/integration/gcp/loadtest`) builds `jaiscloud-gcp`, boots it ephemeral on
private ports **with `--metrics`**, seeds a bucket, a topic + three pull
subscriptions and the Firestore database, then drives `-c 100` for a bounded
window against:

| hot path | what it hammers | per-request correctness |
| --- | --- | --- |
| GCS object PUT/GET | media upload + `?alt=media` download | every download matches the bytes just uploaded |
| Pub/Sub publish + fan-out | `topics:publish` and `subscriptions:pull`/`:acknowledge` on 3 subscriptions | publish returns one `messageId`; a bounded fan-out pass asserts **every** subscription received **every** published marker |
| Firestore concurrent commits | `documents:commit` | the commit envelope carries a `commitTime` and one `writeResult` |

It fails on any **5xx**, any transport error, any unexpected status, and any
goroutine or memory growth past a threshold across the window. Goroutine/memory
growth is read from the emulator's own Prometheus `/metrics`
(`go_goroutines`, `go_memstats_alloc_bytes`, `process_resident_memory_bytes`);
the client closes its idle keep-alive connections and the process settles before
the trailing snapshot, so per-connection goroutines do not read as a leak. The
thresholds are deliberately generous leak detectors, not budgets, and are
env-overridable (`JAISCLOUD_LOAD_MAX_GOROUTINES`, `…_MAX_HEAP_PCT`,
`…_MAX_RSS_PCT`; window/concurrency via `JAISCLOUD_LOAD_WINDOW` /
`JAISCLOUD_LOAD_CONCURRENCY`).

**Race-instrumented mode.** `make test-gcp-load LOAD_RACE=1` builds the emulator
with the Go race detector (`go build -race`) and, after the run, fails the gate
if the emulator log contains a `WARNING: DATA RACE` report — so a data race the
ordinary `-race` unit/integration suites miss (one that needs many concurrent
writers) is caught and surfaced. The scheduled workflow runs with `LOAD_RACE=1`;
the local default is the fast non-instrumented binary.

**Why it is not in PR CI.** Mirroring the documented decision for the k3d/Docker
e2e gates, a 100-way soak does not belong on a hosted per-PR runner. It runs
locally (`make test-gcp-load`) and on a **weekly + manual** workflow
([`.github/workflows/gcp-load.yml`](../../.github/workflows/gcp-load.yml)) — like
the Terraform/OpenTofu compat workflow, it is `workflow_dispatch` with a
schedule. The harness mechanics — the metric parser, the worker pool, and the
growth checks — are unit-tested server-free in `loadtest/harness_test.go`, with
seeded negatives (a `500`, a corrupted download, a dropped fan-out copy, a
`commit` missing its envelope) proving each assertion bites. The live gate skips
unless `JAISCLOUD_LOAD=1`, so `go test ./tests/integration/gcp/...` stays a
no-op for it.

---

## 13. Mutation testing — assertion strength (the local spike)

§9–§12 prove a suite **exists**, records its **depth**, **executes** code, and
survives a soak. None of them measures whether the assertions *bite*: a test
that calls a function and only checks `err == nil` executes the code (raising
statement coverage) yet kills no fault. Mutation testing is that axis — mutate
the source, replay the package's own tests against each mutant, and count the
mutants the tests **detect** (kill). A surviving mutant is either a real
assertion gap or a provably equivalent mutation; telling the two apart is the
triage.

**The tool and the invocation.** `make test-gcp-mutation PKG=…` installs the
pinned [`go-mutesting`](https://github.com/avito-tech/go-mutesting) (the
maintained fork of `zimmski/go-mutesting`) into `bin/` and runs it over one
package (default `./internal/gcp/service/datastore`). For each mutant it
rewrites one target file **in place**, runs `go test <package>`, restores the
file, and finally writes a JSON report — `bin/gcp-mutation-report.json` — whose
`stats` carry the mutation score and whose `escaped` array carries the diff of
every surviving mutant. The target is **local only** (a full package run is not a
per-PR budget): TD13 is a spike that seeds a ratchet, not a gate. The package run
that seeded the datastore floor took minutes at TD13 but now takes **hours** —
several lexer mutants in `gql.go` burn the full 60s exec timeout per mutant — so
TD21 measured its surface file-scoped instead (§13.3).

**The spike's result** (`internal/gcp/service/datastore`, the transport-neutral
Datastore core; 1246 mutants):

| run | killed | escaped | MSI |
| --- | --- | --- | --- |
| initial (as-shot, no test change) | 582 | 664 | **46.7%** |
| after the TD13 test pass | 856 | 390 | **68.7%** |

The pass added the missing assertion strength where the suite only *called*
code: value equality/ordering across every `Value` variant (`valueEqual` /
`valueCompare` / `entityEqual`, including the later-variant nil guards),
the aggregation reducer (`count`/`sum`/`avg` edges: empty set, `up_to` clamp,
double promotion, int64 overflow, non-numeric skips), the store-sentinel →
`ProviderError` mapping (previously 0% covered), the GQL lexer
(tokens, number/string literals, escapes, char classes), key/path validation
and projection, and the transaction TTL/OCC boundaries (`txnTTL` at exactly the
limit, `maxAggregations` at 5 vs 6, read-set sorting, abort-on-read-conflict).

**Surviving-mutant classes** (final run) and their triage:

| class | what the mutant changes | shares | triage |
| --- | --- | --- | --- |
| `branch/if` | a guard/`return` body emptied (condition forced false) | 121 | real gaps concentrate in the **GQL parser error arms** and the **metadata synthesizers**; the rest are lock/defer bookkeeping (below) |
| `expression/remove` | one operand of a `&&`/`\|\|` forced `false` | 82 | a few real (untested later-variant guards); many provably equivalent — e.g. `if aNum \|\| bNum` in `valueEqual` is equivalent because neither numeric variant can reach the `switch` |
| `numbers/incrementer`/`decrementer` | a literal or counter `±1` | 107 | the load-bearing constants/limits are now pinned (`txnTTL`, `maxAggregations`); the remainder are loop counters whose ±1 is absorbed |
| `statement/remove` | a statement deleted | 21 | mostly `s.txnMu.Lock()` / `defer Unlock()` — a real concurrency bug only a `-race`/multi-writer run would observe, which the non-race mutation exec cannot |
| `branch/case` | a `switch`-case body emptied | 20 | real where the arm is untested (`Commit` mode dispatch, `metadata` representation cases) |
| `expression/comparison` | `<`↔`<=`, `>`↔`>=`, `==`↔`!=` | 19 | real only at an untested boundary |
| `loop/break` | `continue`↔`break` | 11 | real only when a multi-match input is tested |
| `arithmetic/*`, `loop/range_break`, `branch/else` | `+`↔`-`, `+=`↔`-=`, a range break, an else body | 9 | mixed; none load-bearing |

The **equivalent** survivors (the `valueEqual`/`valueCompare` short-circuit
operands that no input can distinguish, the single-threaded lock statements) are
*not* assertion gaps and are deliberately left (recorded as TD19-a). The two
follow-ups TD13 named — a negative-suite pass on the GQL parser and metadata, and
a committed `docs/gcp-mutation-floor.yaml` ratchet — are **TD19**; the
codec/router closure is **TD20**; and the value-type ordering the value tests
surfaced is **TD21** (all below).

### 13.1 The negative tables and the committed floor (TD19)

TD13 left the score unguarded: a later test refactor could weaken an assertion
and let a mutant survive again with nothing failing. TD19 adds the two things
TD13 deferred:

- **Negative tables.** `internal/gcp/service/datastore/gql_test.go` names every
  fail-closed parser arm — the lexer's character classes, the SELECT/projection/
  FROM grammar, WHERE conditions, the DATETIME/KEY/BLOB literal parsers,
  IN/HAS/ORDER, LIMIT/OFFSET, and the binding resolver — plus the positive
  boundary inputs each sits next to, so a guard widened into rejecting a valid
  query is caught too. `metadata_test.go` pins the value→representation mapping
  for every `Value` variant and the array flattening, and asserts the
  `__kind__`/`__property__` synthesizers deduplicate, sort, flatten, and exclude
  the reserved kind, the empty kind, reserved properties and out-of-partition
  entities — plus the reserved key/mutation tables and the store-error mapping a
  failing `ListKind` exercises.
- **A committed floor.** `docs/gcp-mutation-floor.yaml` records a per-target
  mutation-score floor; `make test-gcp-mutation-ratchet PKG=…` runs the spike and
  gates the score against it (`tools/gcpstatus/mutationratchet.go`,
  `-mutation-ratchet`), and `make test-gcp-mutation-update` ratchets a floor up —
  refusing to lower one without `ALLOW_LOWER=1`. Like the coverage ratchet (§11)
  it is a *ratchet*, not a target, and it runs on demand rather than in PR CI (a
  full package run is minutes, TD13).

The floor is bootstrapped at the last **completed** measurement — TD13's 68.70%
(856/1246) — which TD19's tests can only raise: they add assertions over
unchanged production code, so the 1246-mutant set is unchanged and the killed
count cannot drop. A fresh
`make test-gcp-mutation-update PKG=./internal/gcp/service/datastore` ratchets it
to the measured post-TD19 score.

Writing the value-comparison tests surfaced a **real semantic gap** worth more
than the score: `valueEqual`/`valueCompare` treat INTEGER and DOUBLE as one
numeric type (`n = 3` matches a stored `3.0`; `7 < 3.2`), whereas Firestore in
Datastore mode documents them as *separate* types for ordering and equality.
That is a behaviour question, not a test-depth one, so the spike does not change
it — it is recorded as TD13-c (and the new tests say so) rather than silently
blessed. **TD21 (§13.3) closes it.**

The codec/router surface the plan also named — `internal/gcp/adapter/jsoncodec.go`,
where a wrong resource-type or action derivation silently misroutes a request —
was scored but not fixed in the spike (**64.3%** MSI, 502/781 killed; the
survivors are `expression/remove`/`branch/case` in the resource-type and action
derivation); TD20 closed it, below.

**DC10** extends the committed floor to the two shared surfaces the behavioural
depth suites' assertions ride on: `internal/gcp/paging` (the `pageSize`/
`pageToken` cursor walk every Wave-2 `p4` walk drives, floored at **67.80%**) and
`internal/gcp/gcperr` (the `google.rpc` status ↔ HTTP mapping behind every `p3`
error-envelope assertion, floored at **91.41%**) — so a paging- or error-mapping
regression weakens an assertion and fails the ratchet instead of passing silently.

### 13.2 The codec derivation table and a file-scoped floor (TD20)

TD13's second surface is the codec/router: `internal/gcp/adapter/jsoncodec.go`
derives the (resource type, action) pair for every `/v1/projects/...` REST
request the generic codec serves (Pub/Sub, Secret Manager, Cloud KMS, IAM and
IAM Credentials, Firestore and Firestore Admin, Cloud Functions, Cloud
Workflows, Eventarc, Memorystore Redis). A wrong derivation does not fail — it
silently misroutes, which is why the surface needs assertion strength rather
than coverage. It scored **64.28%** (502/781), and the 279 survivors were
dominated by the derivation itself:

| class | what the mutant changes | shares | where |
| --- | --- | --- | --- |
| `expression/remove` | one operand of a `&&`/`\|\|` dropped (`isCollection`, `custom == ""`, a method test) | 114 | the `switch { case … }` arms of `deriveAction` |
| `branch/case` | a case body emptied — an action an arm no longer returns | 69 | the custom-verb switches and the resource-type switches |
| `numbers/*` | a literal ±1 (mostly the 400/404/501/500 statuses, plus index arithmetic) | 55 | `Decode`, `Encode`, `EncodeError`, the location scans |
| `expression/comparison`, `branch/if`, `arithmetic/base` | `<`↔`<=`, a returned error body emptied, `i+1`↔`i-1` | 30 | `Decode`, `Encode*`, `gcpErrorDetails`, the location scans |
| `loop/break`, `loop/range_break` | `continue`↔`break` in a scan | 10 | the location scans and `segmentsAfterDocuments` |

**The table.** `internal/gcp/adapter/jsoncodec_derivation_test.go` closes the
killable classes with a closed-form table rather than per-service assertions:

- **Resource type × method × collection/item.** Every resource family the codec
  routes is asserted for all five methods at both levels (31 families, ~310
  cells), and a cell the resource does not serve is asserted **fail-loud** (501
  `UnsupportedOperation`). The negative cells are what pin the guards: a `case
  isCollection && method == GET:` arm widened to `isCollection && true` — or to
  `true && method == GET` — answers for a collection PATCH or a stray item GET
  the resource does not serve, and the grid's unsupported cell catches it.
- **Custom (`:verb`) methods**, per service, including the Firestore
  `documents:*` set, the Eventarc IAM verbs and the Cloud Functions 1st→2nd gen
  upgrade verbs. The HTTP verb on these rows is documentary: `deriveAction`
  derives from the verb segment alone, so each row carries the verb the
  Discovery declares.
- **The path parser's failure contract.** No `projects` segment → 404, a
  `projects` marker with no value → 404, an empty resource tail → 501, `projects`
  outside second position → decoded, the **first** `projects` marker wins (a
  resource literally named `projects`), and a one-segment `/v1` path → 404 (the
  mutant panics).
- **`detectResourceType`**: the precedence rules (cryptoKeyVersions > cryptoKeys
  > keyRings, documents > keys, executions > workflows) and the positional
  guards that keep Memorystore's `locations/{l}/backupCollections/{c}/backups`
  out of the Firestore `backups` bucket.
- **The response half**: `Encode`'s 200 default, its content type, the raw-JSON
  passthrough and the marshal-failure envelope; `EncodeError`'s 500 default, its
  code/status/message triple and the conditional `details` array;
  `gcpErrorDetails`' skip-untyped and omit-empty-field rules.
- **The Firestore segment-parity rule** (an even count after `documents` is a
  document, an odd count a collection, the bare marker the collection list), the
  first-`locations`-wins rule (including a resource literally named `locations`)
  and the three path helpers.

| run | killed | escaped | MSI |
| --- | --- | --- | --- |
| TD13 (as-shot) | 502 | 279 | **64.28%** |
| after TD20 | 746 | 35 | **95.52%** |
| after TD24 | 734 | 34 | **95.57%** |

**The surviving classes** are all equivalent mutants or recorded gaps — no
untested branch is left. The equivalents:

| class | shares | why it is equivalent |
| --- | --- | --- |
| the location-scan guard | 20 | `i-1`, `i+0`, `i+2`, `<=` and the dropped operand are all already true at the first segment — and every path that reaches those blocks starts `locations/` with at least three segments — so the scan matches and breaks there; no later iteration is reached |
| the constant-operand arms (`pi := -2`, `len(seg) >= 0`, `> -1`) | 4 | the sentinel is either the found index (≥ 0) or any negative value, and `splitEscaped` never returns an empty slice — only the `-0` variants the malformed-path table kills are observable |
| the `executions` arm of the workflow location guard | 1 | `workflows` and `workflowexecutions` are separate services, so the JSON codec never derives `executions`; the action is unsupported before the param matters |
| the v2 operations list/get arms | 2 | emptying them falls through to the v1-shaped `operations` arm of the same function, which derives the same list/get action |
| `len(seg) > 1` on the v2 check, `segs > 1` on the documents parity, `len(segs) >= 2` on the `databasesOperations` guard | 3 | each one's difference is only reachable on a path the earlier guard has already returned from (a one-segment `/v2`, or a two-segment `databases/*`) |
| the documents `method == GET && !isDoc` arm | 1 | the `GET && isDoc` arm above it matches every document GET first, so widening the arm cannot answer for one |
| the custom-strip detectors (`len(rest) > 0`, the `IndexByte(':') >= 0` colon check) | 3 | with the fail-loud gate (TD24) a stray verb on a single-segment path is refused whether or not the detector strips it, so weakening them cannot change the observable (501) outcome |

**Closed as TD24** — the one family the table would not bless: an unrecognized
custom verb on a **collection** path used to fall through to the resource's
plain create verb for most resource types (`POST /v1/projects/p/secrets:zzz` →
`Create`, `POST …/databases/{db}/documents:zzz` → `CreateDocument`,
`POST …/serviceAccounts/{sa}/keys:zzz` → `ServiceAccountKeyCreate`). Real GCP
has no such method and the codec's contract is fail-loud, so `deriveAction` now
returns `""` for any custom verb that reaches the end of its custom-verb switch.
The method-switch arms below it therefore never see a custom verb, so the
per-arm `custom == ""` guards (cryptoKeys, cryptoKeyVersions) and the `databases`
fail-loud case they duplicated were removed as dead code — the single
custom-verb switch is now the one gate. The only remaining pass-through is
Firestore `databasesOperations`, whose `:cancel` the method switch derives; Cloud
Functions v2's operations `:cancel`/`:wait` are derived above the block. The
collection shapes — plus a stray verb on each operations surface and on the
`getAllowedLocations` Discovery shape — are pinned by new negative rows in
`jsoncodec_derivation_test.go`. The related method arms that are *not*
`isCollection`-guarded, where a collection-level `PATCH`/`DELETE` still derives
the item verb, are left as they are (recorded as TD35): the provider rejects the
bogus name, so the wire result stays compliant.

**Three gap records the table pins rather than hides** were written while
building it (all verified against the vendored Discovery, all recorded):

- **TD25** — `cryptoKeys.patch` (the real REST key update) is not derived, and
  no REST route registers it, while gRPC serves `UpdateCryptoKey` and the
  fidelity matrix records `grpc UpdateCryptoKey = ga` with no REST cell: a
  REST↔gRPC gap, pinned by the cryptoKeys `PATCH` cells.
- **TD26** — the Cloud Workflows long-running-operations surface over REST:
  `operations.delete` is declared by the vendored Discovery but not derived, and
  the derived `ListOperations` has no handler (the REST workflows provider
  registers only `Workflow.GetOperation`), so the registry answers
  `UnknownAction`.
- **TD27** — the rest of the REST custom-method surface the Discovery declares
  and the gRPC transport already serves (Pub/Sub `subscriptions.seek` and
  `modifyPushConfig`, the KMS `importJobs` IAM verbs, `rawEncrypt`/`rawDecrypt`,
  `locations.generateRandomBytes`, Firestore `documents.partitionQuery`/`listen`/
  `write`/`executePipeline`, Memorystore `instances.import`/`export`/`failover`/
  `rescheduleMaintenance`/`authString`, Eventarc `locations.operations:cancel`).
  Those paths already fail loud with a stray verb, so the gap is
  feature-completeness rather than silent misrouting.

**A file-scoped floor.** `docs/gcp-mutation-floor.yaml` gains a second target
keyed by the **file** (`internal/gcp/adapter/jsoncodec.go`) rather than by the
package. The file is the surface TD13 scored; a whole-package run would sweep
the other codecs (GCS, BigQuery, Compute, Cloud SQL, Cloud DNS) and multiply the
run's minutes without adding signal to this target. This is the first
file-scoped entry, so the floor's tooling comment and the `test-gcp-mutation*`
help text were widened from "package directory" to "package or file". Like
TD19's it is a *ratchet*: `make test-gcp-mutation-ratchet
PKG=./internal/gcp/adapter/jsoncodec.go` fails when the measured score drops more
than 0.5pp below the recorded floor, and `make test-gcp-mutation-update` raises
it — never lowers it.

### 13.3 The value-type ordering and the fail-open filter shapes (TD21)

The value-comparison tests TD13 wrote (§13.1) surfaced behaviour the emulator had
pinned rather than blessed, and the TD13-c note recorded the one that mattered:
`valueEqual`/`valueCompare` treated INTEGER and DOUBLE as a single numeric type.
TD21 closes that against real Datastore's authority — the **"Value type
ordering"** table on [Entities, Properties, and
Keys](https://cloud.google.com/datastore/docs/concepts/entities#value_type_ordering)
for equality and ordering, and the vendored Datastore **Discovery** for a
filter's static shape.

**Ground truth.** A Datastore mode database orders a property whose values have
mixed types by the value's *type* first, then by value within the type:
`null < integers < dates/times < booleans < byte strings < unicode strings <
floats < NaN < geo points < keys` (`Fixed-point numbers` is one entry carrying
`Integers` then `Dates and times`), with the explicit note that "Integers and
floating-point numbers are considered separate types in Datastore mode. If an
entity uses a mix of integers and floats for the same property, all integers will
be sorted before all floats, for example: `7` < `3.2`". Only arrays and embedded
entities have **Sort order: None**. On the filter side the Discovery requires
`PropertyFilter.op = IN` to carry a *non-empty* `ArrayValue` and
`CompositeFilter.filters` to have *at least one* filter.

**What was wrong.** Three divergences, all on the comparison path:

- `valueEqual`/`valueCompare` folded the numerics: `n = 3` matched a stored
  `3.0`, ordering mixed them numerically, and *every* cross-type pair returned
  `ok = false` ("not orderable") instead of comparing by the documented type
  order — so a range filter silently matched nothing across types.
- `IN` with a non-array value returned no-match, where real Datastore rejects it.
- An `AND` composite with **no** sub-filters matched every entity (fail open),
  contrary to the file's own fail-closed contract, and an empty `IN` array was
  accepted.

**The fix** (`internal/gcp/service/datastore/query.go`, `core.go`):

- `valueRank` encodes the documented type order and `valueCompare` is total
  across it — `cmp.Compare` within the ordered type, `bytes.Compare` for blobs,
  `false < true` for booleans, chronological for timestamps, latitude-then-
  longitude for geo points. Only arrays, embedded entities and an absent value
  stay unorderable, matching the docs' "None".
- `valueEqual` is type-strict: `3 != 3.0` and `3.0 != 3`, while a NaN equals a
  NaN (NaN is a single distinct value type).
- `validateFilter`/`validatePropertyFilter` are the **single authority** for a
  filter's static shape and reject `IN` without a non-empty array and an empty
  `AND` with InvalidArgument. `RunQuery` and `RunAggregationQuery` call it once
  before scanning any candidate — so the request fails loud on an empty kind,
  where the per-entity matcher never fires — and `matchesFilter` calls it too, so
  no caller can reach a fail-open path; `matchValidFilter`/`matchPropertyFilter`
  are pure evaluators over an already-validated filter. The shape checks were
  deliberately *not* left duplicated between the validator and the matcher, which
  is how the empty-array and empty-AND cases drifted.

**Tests.** `TestValueEqual`/`TestValueCompare` pin the full rank order, its
antisymmetry, and the type-strict equality; `TestValueEqualNumeric` the separated
numerics both ways; `TestValidateFilter` the static failure contract (non-array
and empty `IN`, empty `AND`, `OR`, the `HAS_ANCESTOR` guards); and
`TestRunQueryValidatesFilterBeforeScan` plus the `RunAggregationQuery` case in
`TestRunAggregationQuery` the empty-kind end-to-end rejection at both call sites.

| run | killed | escaped | MSI |
| --- | --- | --- | --- |
| TD21, as first written | 226 | 71 | **76.09%** |
| after closing the survivors below | 243 | 54 | **81.82%** |

The file-scoped run (`make test-gcp-mutation
PKG=./internal/gcp/service/datastore/query.go`, 297 mutants) was used rather than
the whole package: the package token is TD13/TD19's measurement and a re-run is
hours, because several `gql.go` lexer mutants burn the full 60s exec timeout. The
survivors separate cleanly into

- **genuine assertion gaps, now pinned**: the `GREATER_THAN` /
  `LESS_THAN_OR_EQUAL` arms had no equal/miss value (`c > 0` → `c >= 0`, `c <= 0`
  → `c <= 1` survived); a range comparison against a value with no sort order
  (`if !comparable`) was never exercised, so emptying that guard — which then
  reads `c` as 0 and matches — survived; `valueEqual`'s double nil-guard was never
  reached from the absent-value side (removing it is a nil deref);
  `keyHasAncestor`'s "not a proper prefix" guard was never exercised with the
  entity's own key or an unparseable key; and the sum reducer's mid-list hole,
  negative contribution and int64 *underflow* arms were untested;
- **provably equivalent**, deliberately left: the `rankNull = iota + N` constant
  (only the ranks' relative order matters, so a uniform shift is invisible), the
  `int` returned alongside `ok = false` (every caller ignores it), the redundant
  `X != nil ||` operand of each `valueEqual` arm (every arm's body already
  requires both sides), `true &&` on the EQUAL arm (an absent property never
  equals any value), `if !ok { return false }` in the matcher (the zero value
  fails every arm anyway), and `continue`/`break` inside a `switch` (identical to
  the enclosing loop); plus the overflow detector's `iv > 0`/`iv < 0` variants
  whose difference needs `iv == 0`.

`docs/gcp-mutation-floor.yaml` gains the file-scoped entry
(`internal/gcp/service/datastore/query.go` at 81.82), the second file-keyed target
after TD20's codec.

**Recorded rather than fixed.** Reviewing this change surfaced three
pre-existing divergences *adjacent* to the comparison path, each now carrying its
own backlog row rather than being folded into this fix:

- **TD28** — the documented array-valued property semantics ("the entity
  satisfies the filter if any of the values compares") are not modelled: a filter
  is compared against the stored array as a whole.
- **TD29** — the reserved `__key__` property is wired only for `HAS ANCESTOR`,
  so the documented key filters (`__key__ > KEY(...)`) never match; and the key
  comparator compares the length-prefixed canonical key *string*, an
  approximation of the documented by-path-elements ordering.
- **TD30** — timestamp equality compares the raw RFC 3339 `timestampValue`
  string while ordering parses and compares instants, so two strings naming the
  same instant with different offsets are order-equal but not equal.

They are distinct behaviours, not regressions, and each is scheduled with its own
branch. The one shape the fix deliberately still leaves unmodelled is the
Discovery's disjunction limits ("subject to disjunction limits") on `IN`/`NOT_IN`
— the emulator evaluates the array directly, with no limit. It could not be
closed here: the Discovery names no number, so it is recorded as **TD31** rather
than guessed at.




