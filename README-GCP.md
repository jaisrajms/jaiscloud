# JaisCloud for GCP

> **Early Development Notice**
> `jaiscloud-gcp` is under active development on the `gcp` branch (current version **v1.1.0**) and has not yet been released as a packaged binary (see the [main README](README.md), which currently lists GCP as "In pipeline"). Build it from source. Some operations may have incomplete implementations, behavioural differences from real GCP, or known bugs — see [Known Limitations](#known-limitations) below, and please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) for anything not already listed there.

**JaisCloud — a free GCP emulator for developers and CI.** It implements real GCP wire protocols — both the REST/JSON APIs and the native gRPC APIs official Google clients use (Storage, Pub/Sub, Firestore, Datastore, KMS, Secret Manager, Logging, Monitoring, Dataproc, Eventarc, Functions, Managed Kafka, Metastore, Service Usage, Workflows, Workflow Executions, Resource Manager, IAM) — no SDK shims, no proxy rewrites. Point an official Google client library at it and it works. Where real GCP is REST-only (Compute, Cloud SQL, Cloud DNS, BigQuery, BigLake Iceberg, Memorystore), only REST is exposed; each service row below states its transports.

**One binary per cloud.** `jaiscloud-gcp` is fully self-contained — no `--cloud` flag, no shared runtime with `jaiscloud-aws`. See the [main README](README.md) for the project-wide picture (AWS is the reference implementation; this document covers the GCP binary specifically).

---

## Supported GCP Services

| Service | Transport | Notes |
|---|---|---|
| Cloud Storage (GCS) | REST + gRPC v2 | Buckets, objects, resumable/multipart uploads, CMEK, CSEK |
| Cloud Pub/Sub | REST + gRPC | Topics, subscriptions, snapshots, seek, push/pull delivery, ordering keys, DLQ, exactly-once delivery (pull) |
| Secret Manager | REST + gRPC | Secrets, versions, rotation, CMEK envelope encryption |
| Cloud KMS | REST + gRPC | Key rings, crypto keys/versions, symmetric + asymmetric, rotation |
| Cloud IAM | REST + gRPC | Service accounts, service account keys; gRPC `IAMPolicy` for project/resource policies (authz not enforced) |
| Service Usage | REST + gRPC | Project service enable/disable/get/list (`services.enable`/`disable`/`batchEnable`), `filter=state:ENABLED` |
| Cloud Resource Manager | REST + gRPC | Project lookup + project-level IAM policy (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) — authz not enforced |
| Cloud Firestore (Native mode) | REST + gRPC | Documents, transactions, structured/aggregation/partition queries, composite indexes, `BatchWrite`/`Write`/`Listen` streaming, pipelines (read-only subset) |
| Cloud Datastore mode | REST + gRPC | Entities, queries, ID allocation, `ReserveIds`/`RunAggregationQuery`, transactions (read-set OCC) — see [Known Limitations](#known-limitations) |
| Cloud Functions (v1 + v2) | REST + gRPC | Deploy (LRO), invoke (mock echo by default, Docker/K8s execution modes), locations, source URLs |
| Cloud Workflows | REST + gRPC | Workflow definitions + executions, real YAML expression engine |
| Cloud Dataproc | REST + gRPC | Clusters + jobs, **real Spark execution** in Docker/K8s executor mode (same model as AWS EMR) |
| Dataproc Metastore | REST + gRPC | Control-plane CRUD (services/backups/metadata-imports) + Hive Metastore Thrift serving plane (:9083); partitions/Hive-3.x stubbed, see [Known Limitations](#known-limitations) |
| BigLake Iceberg REST Catalog | REST | `org.apache.iceberg.rest.RESTCatalog` surface mounted at `/iceberg/` — namespaces, tables, atomic `CommitTableRequest` requirements/updates, see [Known Limitations](#known-limitations) |
| Managed Kafka | REST + gRPC | Metadata-only clusters/topics — see [Known Limitations](#known-limitations) |
| BigQuery | REST | Metadata + stored rows — no SQL engine, see [Known Limitations](#known-limitations) |
| Cloud Monitoring | REST + gRPC | Metrics, alert policies (evaluated), notification channels + incidents — see [Known Limitations](#known-limitations) |
| Cloud Logging | REST + gRPC | Log entries, filtering, tailing, monitored-resource descriptors, log-based routing |
| Eventarc | REST + gRPC | Metadata-only triggers/channels + provider discovery — no event-delivery engine, see [Known Limitations](#known-limitations) |
| Cloud DNS | REST | Metadata-only managed zones + record sets/changes — no authoritative DNS server, see [Known Limitations](#known-limitations) |
| Memorystore for Redis | REST | Metadata-only instances + location discovery — no Redis data plane, see [Known Limitations](#known-limitations) |
| Cloud SQL Admin | REST | Metadata-only instances/databases/users — no SQL engine or data plane, see [Known Limitations](#known-limitations) |
| Compute Engine | REST | Metadata-only instances/disks/networks/firewalls/subnetworks — no VM, disk, or network data plane, see [Known Limitations](#known-limitations) |

**Not implemented (out of scope):** Artifact Registry, Cloud Run, Cloud Endpoints, Deployment Manager.

### Fidelity matrix

Every operation (per transport) is classified **ga / limited / preview / unsupported**. The
matrix is *derived* — from the emulator's operation registry, the official Discovery schemas,
and the wire-conformance harness — so it can't drift from the code:

- [`docs/fidelity/fidelity-matrix.md`](docs/fidelity/fidelity-matrix.md) — human-readable, grouped by service
- [`docs/fidelity/fidelity-matrix.json`](docs/fidelity/fidelity-matrix.json) — machine-readable canonical form
- [`docs/fidelity/fidelity-matrix.csv`](docs/fidelity/fidelity-matrix.csv) — flat, for spreadsheets/CI
- [`docs/fidelity-overrides.yaml`](docs/fidelity-overrides.yaml) — the curated contract (the only hand-maintained input)

Regenerate with `make gen-gcp-fidelity-matrix`; CI fails (`make check-gcp-fidelity-matrix`) if the
committed matrix drifts.

### GA readiness

The published GA contract — what `ga` means here, the rollup, the CI gates, client
compatibility, deploy artifacts, and the surfaces that are explicitly non-GA — is
[`docs/GA.md`](docs/GA.md). The one-command aggregate gate is `make ga-check`.

[`docs/GCP-TESTABILITY.md`](docs/GCP-TESTABILITY.md) is the per-service local-testability
contract: what a local run proves, and what must be verified on real GCP.

The release decisions and explicitly **accepted risks** for v1.1.0 — authz not enforced, LROs
complete synchronously, the metadata-only tier, and the real-GCP smoke requirement for Yellow/Red
surfaces — are recorded in [GA.md §10](docs/GA.md#10-release-decisions-and-accepted-risks-v110).

---

## Quick Start

### 1. Build

```bash
git clone https://github.com/jaisrajms/jaiscloud.git && cd jaiscloud
go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/
# or: make build-gcp
```

### 2. Start

```bash
./jaiscloud-gcp start
# Listening on http://localhost:8080  (gRPC on :8081)
```

### 3. Connect

Point any official Google client library at the emulator. Most services use plain endpoint overrides; gRPC-native clients connect to `:8081`, and the services with official emulator-host environment variables (Firestore, Datastore, Pub/Sub, Storage) can use them directly.

```bash
export GCP_EMULATOR_ENDPOINT=http://localhost:8080/    # REST services (GCS, BigQuery, Dataproc, Workflows, ...)
export FIRESTORE_EMULATOR_HOST=localhost:8081           # Firestore (gRPC)
export STORAGE_EMULATOR_HOST=http://localhost:8080      # GCS REST client
```

**Service-account credentials.** The emulator serves an OAuth2 token endpoint at the root (`POST /token`, also `/v1/token`) so `google-auth-library` service-account credentials can mint an access token instead of calling Google. Point `ServiceAccountCredentials`'s token server URI at `<endpoint>/token`; the JWT-bearer grant is implemented (`grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`), and the response is the standard `{access_token, token_type, expires_in}`. This is independent of the opt-in metadata server (`--gcp-metadata`); it is available whenever the REST transport is selected.

---

## Connect your SDK

### Go — REST-based services (GCS, BigQuery, Dataproc, Workflows, Secret Manager REST, ...)

```go
import "google.golang.org/api/option"

opts := []option.ClientOption{
    option.WithEndpoint("http://localhost:8080/"),
    option.WithoutAuthentication(),
}
```

### Go — Firestore (gRPC, real emulator-host support)

```go
import (
    "cloud.google.com/go/firestore"
    "google.golang.org/api/option"
)

// export FIRESTORE_EMULATOR_HOST=localhost:8081
client, err := firestore.NewClient(ctx, projectID, option.WithoutAuthentication())
```

### Go — Cloud Datastore (gRPC, real emulator-host support)

```go
import "cloud.google.com/go/datastore"

// export DATASTORE_EMULATOR_HOST=localhost:8081
client, err := datastore.NewClient(ctx, projectID)
```

### Go — Cloud Monitoring (gRPC, manual endpoint — no official emulator-host var for this client)

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

conn, err := grpc.NewClient("localhost:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
```

---

## Configuration

The most common flags — all have an equivalent `JAISCLOUD_*` env var.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `JAISCLOUD_PORT` | `8080` | HTTP listen port — serves the GCP REST API when `rest` is selected, plus the always-on admin plane |
| `--grpc-port` | — | `8081` | gRPC (h2c, plaintext) listen port — bound only when `grpc` is selected |
| `--transports` | `JAISCLOUD_TRANSPORTS` | `rest,grpc` | Wire transports to expose globally: `rest`, `grpc`, `both`/`all`, or `none` |
| `--transport-overrides` | `JAISCLOUD_TRANSPORT_OVERRIDES` | — | Per-service transport override, e.g. `storage=grpc,pubsub=rest,redis=none`; each value is `rest`, `grpc`, `both`, or `none` |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN; when set all state is stored in PostgreSQL |
| `--ephemeral` | `JAISCLOUD_EPHEMERAL` | `false` | Disable all persistence — state is lost on exit (CI / unit tests) |
| `--data-dir` | `JAISCLOUD_DATA_DIR` | `~/.jaiscloud/jaiscloud-gcp` | Directory for state.json saves and named snapshots |
| — | `JAISCLOUD_GCP_PROJECT_ID` | — | Default GCP project when a request carries none |
| — | `JAISCLOUD_GCP_SERVICE_ACCOUNT` | — | Default service-account identity returned by the metadata emulator |
| `--gcp-metadata` | `JAISCLOUD_GCP_METADATA_ENABLED` | `false` | Enable the GCP metadata-server emulator |
| `--kms-master-key` | `JAISCLOUD_KMS_MASTER_KEY` | — | 32-byte hex KEK wrapping the KMS DEK at rest |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | — | `false` | Expose Prometheus metrics at `/metrics` |

**Transport selection.** `--transports` sets the global default and `--transport-overrides` refines it per service (an override wins). A service is constructed and registered only when at least one transport is selected for it, and only the selected listeners bind — `--transports=rest` opens no `:8081`, while `--transports=grpc` serves the GCP REST API nowhere but keeps the always-on `/_jaiscloud/*` admin plane (and `/metrics`) on `--port`. Per-service selection gates construction and which gRPC services register; `none` removes a service from both transports (a later request for it fails with a registry `no handler` error). The REST API is mounted as one route set, so it is gated by the global `--transports` setting rather than per service. Per-service override names are the **wire** names: `storage`, `pubsub`, `secretmanager`, `kms`, `iam`, `firestore`, `firestoreadmin`, `datastore`, `logging`, `monitoring`, `functions`, `workflows`, `workflowexecutions`, `dataproc`, `managedkafka`, `metastore`, `eventarc`, `serviceusage`, `resourcemanager`, `bigquery`, `dns`, `sqladmin`, `compute`, `iceberg`, `redis` (Memorystore). An unknown service or transport token fails startup loudly.

```bash
./jaiscloud-gcp start --transports=rest                                  # REST + admin only (no :8081)
./jaiscloud-gcp start --transports=grpc                                  # gRPC + admin only (no GCP REST surface)
./jaiscloud-gcp start --transport-overrides=storage=grpc,pubsub=rest     # mixed per service
./jaiscloud-gcp start --transport-overrides=redis=none                   # disable one service
```

Storage model (identical semantics to the AWS binary — see the [main README](README.md#configuration)): default is memory + periodic `state.json` saves; `--dsn` stores everything in PostgreSQL; `--ephemeral` is purely in-memory with no disk writes.

```bash
./jaiscloud-gcp start                                              # default: memory + periodic saves
./jaiscloud-gcp start --dsn "postgres://user:pass@localhost:5433/jaiscloud"
./jaiscloud-gcp start --ephemeral                                  # CI / unit tests
```

---

## CLI Reference

```bash
jaiscloud-gcp start                             # start the emulator
jaiscloud-gcp version                           # print version
jaiscloud-gcp env                               # print effective config as env vars
jaiscloud-gcp doctor                            # verify the emulator is reachable
jaiscloud-gcp reset                             # wipe all state
jaiscloud-gcp export -o snapshot.tar.gz         # save full state to a snapshot tarball
jaiscloud-gcp import -i snapshot.tar.gz         # restore state from a snapshot tarball
jaiscloud-gcp snapshot create --name <name>     # create a named on-disk snapshot
jaiscloud-gcp snapshot list                     # list all named snapshots
jaiscloud-gcp snapshot revert <name>            # revert to a named snapshot
jaiscloud-gcp snapshot delete <name> --yes      # delete a named snapshot
jaiscloud-gcp snapshot inspect <name>           # show snapshot metadata
```

## Admin API

Identical shared endpoints to the AWS binary (see the [main README](README.md#admin-api)) — `/_jaiscloud/health`, `/_jaiscloud/reset`, `/_jaiscloud/export`, `/_jaiscloud/import`, `/_jaiscloud/snapshot*`, `/_jaiscloud/clock`, `/metrics` — all served on the REST port (`8080` by default).

```bash
curl -X POST http://localhost:8080/_jaiscloud/reset
```

---

## Known Limitations

This section documents deliberate simplifications and known correctness edge cases — distinct from ordinary bugs, these are behaviours a developer relying on this emulator should know about up front.

### OAuth2: the service-account assertion signature and audience are not verified

The token endpoint (`POST /token`) parses the JWT-bearer assertion and enforces that it is a well-formed, unexpired JWT identifying a service account, but it does **not** verify the assertion's signature or its `aud` claim: the client signs with a key the emulator has never registered and hardcodes the audience to `https://oauth2.googleapis.com/token` regardless of the configured token server, and JaisCloud likewise never verifies the signature of the bearer tokens it accepts (identity is decoded, not authenticated — see the metadata server). The minted access token is a JaisCloud HS256 JWT carrying the service-account email and project, not an opaque Google token. `refresh_token` is accepted but there is no real refresh-token store — any non-empty value mints a fresh access token.

### Cloud Storage: range reads are served from a whole-object decrypt

The Cloud Storage v2 gRPC service implements both read surfaces: the server-streaming `ReadObject` (metadata + bounded 2 MiB data chunks, with `read_offset`/`read_limit`) and the bidirectional `BidiReadObject` used by the official Go client's `Reader` (with `experimental.WithGRPCBidiReads`) and `MultiRangeDownloader`. `BidiReadObject` serves multiple independent ranges per stream (negative offsets count back from EOF, `read_length` 0 reads to EOF, an offset past EOF is `OutOfRange`), honors `generation` and the `if_generation_match`/`if_generation_not_match`/`if_metageneration_match`/`if_metageneration_not_match` preconditions, and mints an opaque `read_handle` the client can pass back to open a subsequent stream without repeating the object identity. Unsupported request surface (the deprecated `read_mask`, the redirect-only `routing_token`) fails loud with `codes.Unimplemented`. The bucket/object write, copy, restore, IAM, and resumable-upload surfaces are all implemented.

**Correctness caveat (shared with `ReadObject`):** objects are stored as a single AES-256-GCM blob (`iv || ciphertext+tag`) for both CSEK and the server/CMEK envelope, and one GCM tag authenticates the whole ciphertext. A byte window therefore cannot be decrypted without processing the entire object, so true partial decryption is not possible for the on-disk format. A range read reads the blob via `blobfs.GetStream`, decrypts it (at most once per `BidiReadObject` stream, reusing the plaintext across that stream's ranges), and streams the requested window in bounded chunks. The full ciphertext and the full decrypted plaintext are therefore both in memory for the read, and a very large object costs one full decrypt per read stream.

`RestoreObject` models GCS soft-delete as versioning tombstones (`timeDeleted` set): restoring a non-live generation flips it live and marks the current live generation non-live, honoring the `if_generation_match`/`if_metageneration_match` preconditions atomically. `restore_token` and `copy_source_acl` are accepted but ignored (the emulator has no ACL plane and no hierarchical namespaces). `UpdateBucket` applies only the field-mask paths it models — `labels`, `versioning`, `storage_class`, `location`, and `retention_policy` — and bumps the bucket metageneration; unknown paths are ignored rather than rejected.

### Firestore: optimistic-concurrency conflict detection

Firestore's `documents.patch` and transform-carrying `commit`/`batchWrite` writes are protected against concurrent lost updates via an optimistic-concurrency check: the document's `UpdateTime` at read time is captured and re-validated, atomically with applying the write, against the live document's current `UpdateTime` — a conflicting concurrent write causes the loser to receive `409 ABORTED` instead of silently overwriting the winner.

`UpdateTime` is the version token, and it is kept **strictly monotonic per document** even when the emulator's clock is frozen (`POST /_jaiscloud/clock` with `{"mode":"fixed",...}`, used for deterministic tests). Every write stamps `max(clock.Now(), current.UpdateTime + 1µs)` (truncated to microseconds — the precision of the persistent `TIMESTAMPTZ` column), so two writers that read the same document simultaneously can no longer both present the same token: the first commit advances it and the second aborts. Under a live (or offset) clock `clock.Now()` already advances between writes, so the conditional advance is a no-op.

We evaluated content-hash version tokens and rejected them (a document that changes and then reverts produces the same hash — the classic ABA problem), and a second internal version counter decoupled from `UpdateTime` (real GCP's `Precondition` proto carries only `exists`/`update_time`, and official clients read and pass forward the real `updateTime`), so the monotonic `UpdateTime` guard is the token both transports already agree on.

### Firestore: `ExecutePipeline` implements a read-only subset

The Firestore pipeline RPC (`google.firestore.v1.Firestore/ExecutePipeline`) is implemented over the same query engine as `RunQuery`, covering the read-only relational subset: the `collection`, `collection_group`, `database`, `documents` and `literals` source stages followed by `where` (comparison `FieldFilter`s and `and`/`or` composites), `sort`/order-by, `select`/projection, `distinct`, `limit` and `offset`. The pipeline DSL is open-ended, so any other stage (`aggregate`, `update`, `delete`, `unnest`, `find_nearest`, …) fails loudly with `codes.Unimplemented`, as do expressions the relational engine cannot represent (non-field sort keys, computed or otherwise aliased projections, and boolean functions outside the comparison/composite subset) rather than fabricating a result. `PartitionQuery` runs the request's structured query and splits the ordered result set into up to `partition_count` cursors, returning none when the query has too few documents to partition (the proto's documented signal for a single full-range partition).

### Datastore: transactions are optimistic with a read-set

The Datastore gRPC service supports real transactions using GCP's optimistic read-set model. `BeginTransaction` returns an opaque transaction handle; `Lookup`, `RunQuery`, and `RunAggregationQuery` issued with that handle record a read-set; a `Commit` with the `TRANSACTIONAL` mode re-validates every read key against current state and applies all mutations atomically. A concurrent modification to a read key aborts the whole commit with `ABORTED` (no mutations applied), and a per-mutation `base_version`/`update_time` precondition mismatch fails with `FAILED_PRECONDITION`. `Rollback` discards the transaction, and a `TRANSACTIONAL` commit without a handle (or with an unknown/expired one) is `INVALID_ARGUMENT`. Non-transactional `Commit`, `Lookup`, and `RunQuery` are fully supported. `ReserveIds` advances the ID allocator so reserved IDs are never reissued by `AllocateIds`. `RunAggregationQuery` evaluates `count`/`sum`/`avg` over the nested query (kind + filter), honors aliases, and participates in transactions.

Documented approximations: the query read-set tracks the returned entities' versions (real Datastore validates the query's read *range*), transactions are single entity-group, and an unused transaction handle expires after ~270 seconds (real Datastore also expires transactions).

### BigQuery: metadata only, no SQL engine

`jobs.query` never evaluates SQL — it stores the query and reports `jobComplete: true` with empty results. `tabledata.insertAll` validates rows against the table schema (missing `REQUIRED` fields and unknown fields are rejected unless `ignoreUnknownValues` is set), honors `skipInvalidRows`, and best-effort suppresses duplicate `insertId`s over a bounded, in-memory window (the dedup state is not persisted across restarts). Field *types* are not enforced, and `templateSuffix` is not supported. `tabledata.list` honors `startIndex` (offset pagination). `projects.getServiceAccount` returns a synthetic `bq-{project}@gcp-sa-bigquery.iam.gserviceaccount.com` rather than a real service account.

### Managed Kafka: metadata only, no real broker

A cluster is a logical record only — the emulator never stands up a real Kafka broker. Consumer groups are not tracked: `ListConsumerGroups` always returns an empty list, and get/update/delete operations on a consumer group return `NOT_FOUND`.

Cluster create/update/delete return a proper `google.longrunning.Operation` (`name`, `metadata` with `@type=type.googleapis.com/google.cloud.managedkafka.v1.OperationMetadata`, `done: true`, `response`) inline; the operation is also persisted under `projects/{project}/locations/{location}/operations/{id}` and served by the registered `GetOperation`/`ListOperations` handlers. That operations path is shared with Cloud Workflows' LRO surface and is routed to Workflows on the single emulator host, but every Managed Kafka operation is returned already done, so no client needs to poll it.

### Cloud Monitoring: alert policies are evaluated for `condition_threshold`

Alert policies are evaluated by a background worker (30s tick, matching the AWS CloudWatch alarm evaluator). Only `condition_threshold` conditions are evaluated; `condition_absent`, `condition_matched_log`, `condition_monitoring_query_language`, `condition_prometheus_query_language`, and `condition_sql` are stored but never evaluated (and never fire). The evaluated filter subset is equality clauses joined by `AND` on `metric.type`, `resource.type`, `metric.label.{key}`, and `resource.label.{key}`; unknown clauses are ignored. The matching series' latest point in each alignment window is reduced across series (`REDUCE_SUM`/`MEAN`/`MAX`/`MIN`/`COUNT`, default mean), then compared to `threshold_value`; `duration` requires the comparison to hold across every alignment window it spans, and `trigger` count/percent is honored when no reducer is set. A firing policy opens an incident and delivers notifications; a below-threshold evaluation or no matching data closes its open incident and delivers a resolution notification. Incidents have no public API, so they are observable via the snapshot/export surface and logs. `pubsub` channels receive a CloudEvents-style JSON message on `labels.topic`; `email`/`webhook`/`sms` notifications are recorded on the incident and logged but not sent.

`GetMonitoredResourceDescriptor` serves a canonical catalog of well-known types (`gce_instance`, `gcs_bucket`, `pubsub_topic`, `pubsub_subscription`, `k8s_container`, `k8s_pod`, `k8s_node`, `dataproc_cluster`, `cloudsql_database`, `api`); an unknown type returns `NotFound`. `CreateServiceTimeSeries` mirrors `CreateTimeSeries` — in real GCP the two differ only in the identity/permission used, and the emulator has no authz plane. `DISTRIBUTION`-typed point values (linear/exponential/explicit buckets, bucket counts, count, mean, sum of squared deviations, optional range and exemplars) round-trip through `CreateTimeSeries`/`ListTimeSeries`. `ListMetricDescriptors`/`ListMonitoredResourceDescriptors` honor a discovery filter subset — equality (`type = "x"`, `metric.type = "x"`, `resource.type = "x"`) and `starts_with("prefix")` clauses joined by `AND`, over the descriptor type or its `name`; any other key, operator, or malformed clause fails with `InvalidArgument`. `ListTimeSeries` supports only the `metric.type` / `resource.type` equality filter subset — the full Monitoring Query Language is not implemented. `NotificationChannelService` supports CRUD for `pubsub`/`email`/`webhook`/`sms` channels. `ListNotificationChannelDescriptors`/`GetNotificationChannelDescriptor` serve a static catalog of well-known channel types (`email`, `sms`, `pubsub`, `webhook_tokenauth`, `slack`); an unknown type returns `NotFound`. `SendNotificationChannelVerificationCode` is a no-op success (the emulator delivers nothing), `GetNotificationChannelVerificationCode` returns a deterministic synthetic code, and `VerifyNotificationChannel` marks the channel `VERIFIED`.

### Cloud Logging: `TailLogEntries` is a bounded polling approximation

`ListMonitoredResourceDescriptors` returns the canonical catalog of well-known monitored resource types shared with the Monitoring service (`gce_instance`, `gcs_bucket`, `pubsub_topic`/`subscription`, `k8s_container`/`pod`/`node`, `dataproc_cluster`, `cloudsql_database`, `api`), each with `type`, `display_name`, `description`, and `labels`; `page_size`/`page_token` are honored. Unlike Monitoring, the Logging descriptors leave `name` unset, and the `ListMonitoredResourceDescriptorsRequest` in this proto revision carries no `parent` or `filter` fields (only page size/token), so the catalog is global and unfiltered.

`TailLogEntries` is implemented as a **bounded, store-polling** tail rather than a true push feed. After accepting the initial `resource_names`/`filter`/`buffer_window`, the server records the store's monotonic write id as its cursor and re-reads the store on a timer derived from `buffer_window`, streaming entries written after the stream began whose fields satisfy the same filter engine `ListLogEntries` uses. This is the closest correct behavior the emulator's store allows, and it never returns `Unimplemented` and never busy-spins. The documented deviations from real Cloud Logging are:

- **Delivery is at-most-once against each read snapshot**, not the real API's at-least-once. An entry inserted with a higher store id is emitted exactly once; a client that disconnects before the next poll does not get it, and there is no server-side retention or replay.
- **`buffer_window` is realized as the poll interval**, not as a reordering buffer. The emulator's store already returns entries in `(timestamp, id)` order, so there are no late-arriving out-of-order entries to absorb; the window only controls latency. Absent/`nil` uses the real 2 s default, explicit `0` is clamped to 50 ms to avoid a spin, and values are capped at 60 s (the spec's maximum).
- **Filter (and project) changes on further client requests are applied** to subsequent polls; a project change re-seeds the id cursor so the new project's pre-existing backlog is not replayed.
- The session terminates cleanly on client half-close (`Recv` → `EOF`) or context cancel.

### Cloud KMS: rotation schedule is executed lazily on read

`CryptoKey` `labels`, `rotationPeriod`, and `nextRotationTime` are persisted and returned by both the REST and gRPC surfaces. A `rotationPeriod` is an automatic rotation schedule: when set, `nextRotationTime` is derived (`createTime + rotationPeriod` on create, `now + rotationPeriod` on update) and clearing the period clears `nextRotationTime`. `UpdateCryptoKey` honors the `labels` and `rotation_period` update-mask paths (unmasked fields are preserved); any other path fails loud with `Unimplemented`.

The schedule **is** executed, lazily on read (the emulator has no background scheduler): when a due key is read (`GetCryptoKey`/`ListCryptoKeys`, and `Encrypt` before it resolves the primary), a new ENABLED `CryptoKeyVersion` is created, made the primary, and `nextRotationTime` advances to `now + rotationPeriod`. A key therefore rotates on its next read after the schedule comes due. Because the schedule advances to `now + rotationPeriod` rather than by one period per elapsed interval, a clock jump far past the due time is coalesced into a single rotation — a deliberate approximation. Cloud KMS **managed/external** rotation (imported key versions, external key managers, `rotate-master-key`) remains out of scope and is not implemented.

The gRPC KMS surface also implements the delete side (`DeleteCryptoKeyVersion`, `DeleteCryptoKey`, plus the `RetiredResource` records that block name reuse), the raw AES-GCM primitives (`RawEncrypt`/`RawDecrypt`, purpose `RAW_ENCRYPT_DECRYPT`), and the `ImportJob` control plane (`CreateImportJob`/`GetImportJob`/`ListImportJobs`). `ImportCryptoKeyVersion` (wrapped-key import), the trusted-key-wrapped import/export pair, and `Decapsulate` (no KEM algorithm support) are not implemented and return `Unimplemented`.

### Secret Manager: scheduled rotation is stored, managed rotation is not implemented

A `Secret`'s `rotation` schedule (`nextRotationTime` + `rotationPeriod`) is persisted and honored lazily: a read (`GetSecret`/`AccessSecretVersion`) after `nextRotationTime` creates a new version and advances the schedule. Cloud SQL **managed rotation**, however, is `Unimplemented` on the gRPC surface: `EnableManagedRotation` and `RotateSecret` generate a password, apply it to a Cloud SQL user, and store it as a new secret version, and the emulator has no Cloud SQL data plane to update. Both RPCs fail loud with `codes.Unimplemented` rather than fabricating a version.

### Cloud Workflows: synchronous execution, no filter/orderBy

Cloud Workflows is implemented over a real YAML expression engine: workflow definitions and executions are stored, and `executions.create` runs the workflow synchronously and returns it already in a terminal state (there is no asynchronous execution queue). List `filter`/`orderBy` are ignored; a `switch` with no matching condition fails the execution loudly; `http.*` auth is not modelled; `retry.predicate` fails loud; and subworkflows, `listRevisions`, IAM, and CMEK are not implemented.

### Dataproc: `Reset` does not drain in-flight Spark job goroutines

`POST /_jaiscloud/reset` wipes the Dataproc store but does not cancel or wait for jobs currently executing (Docker/K8s executor mode). This matches AWS EMR's own `Reset` behaviour in this codebase, which is a no-op for the same reason — not a GCP-specific gap. If you reset while a job is mid-execution and then resubmit a job with the *same* `(project, region, jobId)` before the stale run finishes, the stale run's completion could overwrite the new job's state. Avoid reusing job IDs across a reset boundary while a prior run may still be in flight.

### Dataproc Serverless: not implemented

The Dataproc Serverless (Batch) API is not implemented. Dataproc is clusters + jobs with mock, Docker, or K8s executors only; serverless batches have no emulator surface.

### Dataproc Metastore: control plane + Hive Thrift serving plane (partitions stubbed)

The management plane is implemented (`Service` / `Backup` / `MetadataImport` CRUD with long-running operations). The table-metadata plane is served by a Hive Metastore **Thrift** listener on `:9083` (a single global catalog) that answers the database, table, and lock methods Spark's Hive/Iceberg clients use; the per-Service `endpointUri` is synthesized (`thrift://<id>.<location>.metastore.jaiscloud.local:9083`) because the listener is shared, not per-service. Partition methods and `get_table_meta`/Hive-3.x paths are not implemented; the gRPC control plane (`Service`/`Backup`/`MetadataImport` CRUD) is implemented over the official `DataprocMetastore` proto. `ExportMetadata`, `RestoreService`, `QueryMetadata`, `MoveTableToDatabase`, and `AlterMetadataResourceLocation` return `Unimplemented`. On the single host, `locations/{l}/operations/{id}` is path-identical to Cloud Workflows' LRO surface and therefore routes to Workflows — Metastore's own operations are returned inline (`done: true`), so no client needs to poll them.

### BigLake Iceberg REST Catalog: DB-backed, standard REST spec

The BigLake Iceberg REST Catalog is mounted at `/iceberg/` (Spark configures `uri=http://host:port/iceberg/`) and speaks the standard `org.apache.iceberg.rest.RESTCatalog` protocol. It models GCP's real BigLake Metastore managed-Iceberg product, whose catalog is the standard Apache Iceberg REST spec — Spark, Trino, and Flink attach over that spec. (Dataproc Metastore's own table plane is the Hive Metastore Thrift server, a separate product.) The catalog is database-backed (Polaris-style): it stores the `TableMetadata` JSON and a synthesized `metadata-location` pointer (`{location}/metadata/{version:05d}-{uuid}.metadata.json`) but never writes `metadata.json`/`version-hint.text` to object storage itself — the client's `FileIO` does that. `CommitTableRequest` requirements (`assert-table-uuid`, `assert-ref-snapshot-id`, schema/spec/sort-order assertions, …) and updates (`assign-uuid`, `add-schema`, `add-snapshot`, `set-properties`, …) are applied atomically, so concurrent commits cannot lose updates. `set-statistics`/`remove-statistics`/`remove-partition-statistics` are accepted as no-ops, and `GET /tables/{table}/metrics` returns `501 Not Implemented`.

### Eventarc: metadata only, no event-delivery engine

Eventarc is implemented as trigger-based metadata CRUD, **not** the AWS EventBridge "bus + rule + event-pattern + target" fan-out model (the two are different models despite the shared "event routing" purpose). A `Trigger` is a stored record with a `destination` (Cloud Run service, Workflows, HTTP endpoint, or GKE — accepted as metadata references since Cloud Run does not exist in the emulator), a `transport.pubsub.topic` source, and `eventFilters` (at least one of which must have `attribute: "type"`, as real Eventarc requires); a `Channel` is a 3rd-party-source registration record referencing a `provider`; and `Provider` resources are read-only discovery (a small catalogue of real providers — `pubsub.googleapis.com`, `storage.googleapis.com` — not invented ones). `destination.cloudFunction` is **output-only/read-only and rejected on create** (Cloud Functions v2 triggers are created through the Cloud Functions API, not by an Eventarc create call), so it is not offered as an accepted destination. No events are ever delivered: there is no Pub/Sub subscription, Cloud Run deployment, or Workflow execution created, and no event-matching engine. Source/destination references are validated structurally — a `transport.pubsub.topic` must name an existing Pub/Sub topic and a `destination.workflow` must name an existing Workflow (`NotFound` otherwise) — and a channel's `pubsubTopic`/`activationToken` are synthesized placeholders.

Trigger and Channel writes honor `updateMask` (masked paths take the incoming value, unmasked paths retain the stored value, merged inside the store's atomic mutate closure), support `validateOnly=true` (validate then skip the write), and enforce `etag` optimistic concurrency control: a deterministic content-checksum etag is recomputed on every create/update, and a stale etag on patch/delete is rejected with **409 `ABORTED`**. A trigger/channel `uid` is a UUID4, and a newly created unconnected Channel reports `state: PENDING` (it only becomes `ACTIVE` once a provider connects). Trigger/Channel IAM (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) is implemented via the shared policy store (etag OCC included), not `Unimplemented`.

Known debt for Eventarc: list `filter`/`orderBy` are **not** honored (`ListTriggers`/`ListChannels` ignore them and return the full page); output-only fields (`name`/`uid`/`etag`/times/`state`/`activationToken`/`pubsubTopic`) are overlaid on read but a client-supplied output-only field in a create/patch body is not rejected, it is echoed into the stored config; and unknown Eventarc v1 resources/custom methods (e.g. `ChannelConnection`, `GoogleApiSource`, `MessageBus`, `Pipeline`, `Enrollment`) are not routed, so they fall through to the adapter's generic 404 envelope rather than an Eventarc-specific `NOT_FOUND`.

### Cloud Functions: metadata CRUD, mock/docker execution, no real build pipeline

Cloud Functions v1 is implemented as metadata CRUD over the shared function store, plus `call` invocation through the Lambda executor (mock echo by default, Docker/K8s under `JAISCLOUD_EXECUTOR_MODE`). Source archives are **not** fetched, unpacked, or built: `sourceUploadUrl`/`sourceArchiveUrl` are stored as opaque references, `generateUploadUrl`/`generateDownloadUrl` return synthesized `storage.googleapis.com` URLs that nothing serves, and a function is always born `state: ACTIVE` with a synthesized HTTPS trigger URL (unless an `eventTrigger` is supplied). `Create`, `Update` (PATCH), and `Delete` return a **done `google.longrunning.Operation` inline** — `done: true`, `metadata` = `google.cloud.functions.v1.OperationMetadata` (`@type`, `createTime`/`endTime`, `target`, `verb`, `operationType`, `apiVersion`), and `response` = the resulting `CloudFunction` for create/update or `{}` for delete — matching real v1. The operation is never persisted or pollable: the shared `locations/{location}/operations/{id}` path is path-identical to Cloud Workflows' LRO surface and routes there, so no client needs to poll it.

`UpdateFunction` honors the `updateMask` query parameter (accepted spellings include lowerCamel and snake_case, with an optional `function.` prefix): only masked fields are overlaid on the stored function (unmasked fields are retained), an empty mask overlays every mutable field present in the body, and the merge runs inside the store's atomic read-modify-write (`UpdateFunctionAtomic`) so concurrent disjoint-field PATCHes cannot lose updates. An unrecognized mask path fails loud with **501 `UNIMPLEMENTED`** rather than being silently ignored. `locations.list`/`locations.get` return a synthesized region set and honor `pageSize`/`pageToken`; the bare `/v1/projects/{p}/locations[/{l}]` path is shared with Memorystore and is served by the Memorystore handler (which returns the same `google.cloud.location.Location` records), so the route does not collide.

Input validation is deliberately shallow: a create requires a non-empty `runtime` and a function id (from `?functionId=` or the body `name`), and `name`/`path` resource names must match `locations/{l}/functions/{id}` (full `projects/{p}/…` form also accepted) — otherwise **400 `INVALID_ARGUMENT`**. Source archive contents/URLs, runtime/entry-point compatibility, and memory/timeout bounds are **not** validated. Function IAM (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) is implemented via the shared policy store. `call` is synchronous and returns the executor's echo/error inline (an executor failure is surfaced as `error` with HTTP 200, matching v1's `CallFunctionResponse`); no Cloud Functions v2 surface, Eventarc triggers, traffic splitting, or revisions are implemented.

### Cloud DNS: metadata only, no authoritative DNS server

Cloud DNS is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the AWS Route53 provider. `ManagedZone`s, `ResourceRecordSet`s, and `Change`s are stored records — the emulator never stands up an authoritative DNS server, so `nameServers` are synthesized `ns-cloud-*.googledomains.com.` placeholders and zones never resolve queries. `managedZones.{create,get,list,patch,update,delete}` and `resourceRecordSets.{create,get,list,patch,delete}` are supported, and `changes.create` applies its `additions`/`deletions` to the stored record sets synchronously and returns `status: "done"`. `projects.get` returns a synthesized `dns#project` with a `quota` block. The `id` of a managed zone and the project `number` are stable numeric strings derived from the resource name.

Known debt for Cloud DNS: DNSSEC (`dnsKeys`), `policies`/`responsePolicies`, and the managed-zone IAM custom methods (`:getIamPolicy`/`:setIamPolicy`/`:testIamPermissions`) are **not** implemented and fail loud with `501 UNIMPLEMENTED`. List pagination honors `maxResults`/`pageToken`; `managedZones.patch` and `managedZones.update` share merge semantics (description/visibility/labels are overlaid, unmasked fields retained, and `dnsName` is immutable/ignored). Only `name`, `dnsName`, `description`, `visibility`, and `labels` are persisted — the other managed-zone config blocks (`dnssecConfig`, `forwardingConfig`, `peeringConfig`, `privateVisibilityConfig`) are accepted but dropped, so no DNSSEC, forwarding, peering, or private-zone visibility behavior is applied.

### Memorystore for Redis: metadata only, no Redis data plane

Memorystore for Redis is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the Cloud DNS provider and keyed by project + location (region). The emulator never stands up a Redis server, so an instance is born in `state: READY` with a synthesized `host` (a stable `10.x.y.z` placeholder) and `port: 6379` that nothing listens on — there is no data plane, no `AUTH`, no persistence, and no failover. `instances.create` takes `?instanceId=`, defaults `tier` to `BASIC` (rejecting anything but `BASIC`/`STANDARD_HA` with `400`), defaults `memorySizeGb` to `1` and `redisVersion` to `REDIS_7_0`, and stores `displayName`, `labels`, `redisConfigs`, and the location. `instances.get/list/delete` and `instances.patch` (which applies `updateMask`; unsupported mask paths fail loud with `400`) are supported, and `instances.upgrade` applies the requested `redisVersion`. List pagination honors `pageSize`/`pageToken`, and `locations.get`/`locations.list` return synthesized `projects/{p}/locations/{l}` records.

Create/Update/Delete return the `google.longrunning.Operation` **inline** with `done: true` and the resulting instance in `response` (the Dataproc/Metastore convention): the shared `locations/{location}/operations/{id}` path is path-identical to Cloud Workflows' LRO surface on the single emulator host, so Memorystore does not claim it and operations are never persisted or pollable. Location discovery is the one bare `/v1/projects/{p}/locations[/{l}]` path claimed by Memorystore; no other emulator service serves it. Deferred surfaces — `instances.import`/`export`/`failover`/`rescheduleMaintenance`/`getAuthString`, backup collections, and the Redis Cluster surface — are not routed and fall through to the generic `404` rather than silently succeeding.

### Cloud SQL Admin: metadata only, no SQL engine

Cloud SQL Admin (`sqladmin.googleapis.com/sql/v1beta4`) is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the Cloud DNS/Memorystore providers and the AWS RDS provider, keyed by the project at `store.GlobalRegion`. The emulator never stands up a database server, so an instance is born in `state: RUNNABLE` with a synthesized `connectionName` (`{project}:{region}:{instance}`), `selfLink`, `serviceAccountEmailAddress`, `settings` defaults (`tier` `db-f1-micro`, `dataDiskSizeGb` `"10"`, `availabilityType` `ZONAL`, ...), and a cosmetic `PRIMARY` IP that nothing listens on — there is no SQL engine, no data plane, and no AuthN/AuthZ. `instances.{insert,get,list,update,patch,delete,restart}`, `databases.{insert,get,list,update,patch,delete}`, and `users.{insert,get,list,update,delete}` are supported, plus the synthesized `flags.list`, `tiers.list`, and `connectSettings.get` discovery surfaces.

Every mutation returns Cloud SQL's own `Operation` envelope (`kind: "sql#operation"`, with `operationType`, `targetId`, `targetLink`, `selfLink`, and `insertTime`) inline with `status: "DONE"` — **not** a `google.longrunning.Operation` — and the same operation is persisted so `operations.get`/`operations.list` read it back. List responses use Cloud SQL's envelopes (`sql#instancesList`, `sql#databasesList`, `sql#usersList`, `sql#operationsList`), and pagination honors `maxResults`/`pageToken`.

Known debt for Cloud SQL: the engine-less design means `Instances.Update` and `Instances.Patch` share merge semantics (body fields overlay the stored instance, `settings` sub-fields are deep-merged, and the server-owned `region`/`state`/`selfLink`/`ipAddresses` are immutable/ignored); a user's `password` is accepted but never stored or echoed; `settings` blocks other than the fields the emulator defaults are preserved verbatim without any behavioral effect; and deferred surfaces — `sslCerts`, `backupRuns`, `clone`, `failover`, `promoteReplica`, `import`/`export`, `demote`, `resetSslConfig`, `operations.cancel`, and the instance certificate custom methods (`instances:generateEphemeralCert`, ...) — fail loud with `501 UNIMPLEMENTED` rather than silently succeeding. The `v1beta4` path shape is recognized by the shared GCP identity extractor (`/v{N}beta{M}/projects/{project}`).

### Compute Engine: metadata only, no VM/disk/network data plane

Compute Engine (`compute.googleapis.com/compute/v1`) is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the Cloud DNS/Memorystore/Cloud SQL providers and the AWS EC2 provider. Records are keyed by the project (AccountID) and the request scope — the zone for zonal resources, the region for regional resources, and `store.GlobalRegion` for global resources — so instances and disks with the same name in different zones never collide. The emulator never boots a VM or attaches a real disk: an instance is born in `status: RUNNING` with synthesized ids, `selfLink`s, `creationTimestamp`s, a cosmetic `nic0` interface that nothing listens on, and a stable `cpuPlatform`; `start`/`stop`/`reset` only flip the stored status (`RUNNING`/`TERMINATED`). There is no data plane and no AuthN/AuthZ.

Supported surface: zonal `instances.{insert,get,list,delete,start,stop,reset}` plus `instances.aggregatedList`; zonal `disks.{insert,get,list,delete}`; global `networks.{insert,get,list,delete}` and `firewalls.{insert,get,list,delete}`; regional `subnetworks.{insert,get,list,delete}`; synthesized `machineTypes.get/list` (zonal), `zones.get/list`, and `regions.get/list`; and zonal/regional/global `operations.get/list`. Every mutation returns Compute Engine's own `Operation` envelope (`kind: "compute#operation"`, with `operationType`, `targetId`, `targetLink`, `selfLink`, `insertTime`, and `progress: 100`) inline with `status: "DONE"` — **not** a `google.longrunning.Operation` — and the same operation is persisted at its scope so `operations.get`/`operations.list` read it back. List responses use the `compute#*List` envelopes, and pagination honors `maxResults`/`pageToken`.

Known debt for Compute Engine: the data-plane fields are accepted and preserved but have no effect (attached disks are stored verbatim, no boot disk is provisioned, `metadata`/`tags`/`labels` are cosmetic); `id`/`targetId` are stable numeric strings derived from the resource name (the real API assigns server-side integers); the default network is only a synthesized name, not a seeded resource. Everything outside the supported surface — addresses, images, snapshots, instance templates and groups, routers, backend services, `instances.setMetadata`/`attachDisk` and the other custom methods, `operations.wait`, and `aggregatedList` for resource types other than instances — resolves to `501 UNIMPLEMENTED` rather than a `404`. The `compute/v1` path shape is recognized by the shared GCP service detector by its `/compute/v1/` prefix, and the client `BasePath` must include that prefix.

---

## Contributing

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) and [CLAUDE.md](CLAUDE.md) for build setup, architecture conventions, and the AWS-vs-GCP isolation model. Please open an issue before starting large changes.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
