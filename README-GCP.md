# JaisCloud for GCP

> **Early Development Notice**
> `jaiscloud-gcp` is under active development on the `gcp` branch (current version **v1.1.0**) and has not yet been released as a packaged binary (see the [main README](README.md), which currently lists GCP as "In pipeline"). Build it from source. Some operations may have incomplete implementations, behavioural differences from real GCP, or known bugs — see [Known Limitations](#known-limitations) below, and please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) for anything not already listed there.

**JaisCloud — a free GCP emulator for developers and CI.** It implements real GCP wire protocols — both the REST/JSON APIs and the native gRPC APIs official Google clients use (Storage, Pub/Sub, Firestore, Datastore, KMS, Secret Manager, Logging, Monitoring, Dataproc, Eventarc, Functions, Managed Kafka, Metastore, Service Usage, Workflows, Workflow Executions, Resource Manager, IAM, Cloud Scheduler, Cloud Tasks) — no SDK shims, no proxy rewrites. Point an official Google client library at it and it works. Where real GCP is REST-only (Compute, Cloud SQL, Cloud DNS, BigQuery, BigLake Iceberg, Memorystore), only REST is exposed; each service row below states its transports.

**One binary per cloud.** `jaiscloud-gcp` is fully self-contained — no `--cloud` flag, no shared runtime with `jaiscloud-aws`. See the [main README](README.md) for the project-wide picture (AWS is the reference implementation; this document covers the GCP binary specifically).

---

## Supported GCP Services

| Service | Transport | Notes |
|---|---|---|
| Cloud Storage (GCS) | REST + gRPC v2 | Buckets, objects, resumable/multipart uploads, CMEK, CSEK |
| Cloud Pub/Sub | REST + gRPC | Topics, subscriptions, snapshots, seek, push/pull delivery, ordering keys, DLQ, exactly-once delivery (pull) |
| Secret Manager | REST + gRPC | Secrets, versions, rotation, CMEK envelope encryption |
| Cloud KMS | REST + gRPC | Key rings, crypto keys/versions, symmetric + asymmetric, rotation; crypto ops honor a default-permissive cryptoKey IAM policy |
| Cloud IAM | REST + gRPC | Service accounts, service account keys; gRPC `IAMPolicy` for project/resource policies (authz not enforced) |
| Service Usage | REST + gRPC | Project service enable/disable/get/list (`services.enable`/`disable`/`batchEnable`), `filter=state:ENABLED` |
| Cloud Resource Manager | REST + gRPC | Project lookup + project-level IAM policy (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) — authz not enforced |
| Cloud Scheduler | REST + gRPC | Cron jobs (`jobs` CRUD + `pause`/`resume`/`run`); a real cron engine fires `httpTarget`/`pubsubTarget` jobs on the emulator clock — see [Known Limitations](#known-limitations) |
| Cloud Tasks | REST + gRPC | Queues (`queues` CRUD + `pause`/`resume`/`purge`, queue IAM) and tasks (`tasks` CRUD + REST `tasks:batchCreate`/`tasks:batchDelete`); a dispatch engine delivers due `httpRequest` tasks with rate limits and retries, and `run` forces an attempt — see [Known Limitations](#known-limitations) |
| Cloud Firestore (Native mode) | REST + gRPC | Documents, transactions, structured/aggregation/partition queries, composite indexes, `BatchWrite`/`Write`/`Listen` streaming, pipelines (read-only subset) |
| Cloud Datastore mode | REST + gRPC | Entities, queries (structured + GQL), ID allocation, `ReserveIds`/`RunAggregationQuery`, transactions (read-set OCC) — see [Known Limitations](#known-limitations) |
| Cloud Functions (v1 + v2) | REST + gRPC | Deploy (LRO), invoke via `:call` or the deployed HTTPS trigger URL (mock echo by default, Docker/K8s execution modes), locations, source URLs, v2 `serviceConfig` instance/concurrency config (`minInstanceCount`/`maxInstanceCount`/`maxInstanceRequestConcurrency`/`availableCpu`, validated, surfaced + admission-enforced) |
| Cloud Workflows | REST + gRPC | Workflow definitions + executions, real YAML expression engine; LROs complete synchronously by default, with an opt-in async mode (`JAISCLOUD_LRO_MODE=async`) |
| Cloud Dataproc | REST + gRPC | Clusters (GCE- or GKE-`virtualClusterConfig`-shaped) + jobs + workflow templates (`WorkflowTemplate` CRUD; `instantiate`/`instantiateInline` execute the inline DAG over the job core), **real Spark execution** in Docker/K8s executor mode (same model as AWS EMR); pollable async cluster/job long-running operations; `Job.scheduling` restart policy (k8s restart loop) and long-running/streaming job lifecycle; `SparkJob`/`PySparkJob` `jarFileUris` → `spark-submit --jars`; real driver output/control files in GCS; optional Metastore attachment; optional cluster/job lifecycle events on Pub/Sub |
| Dataproc Metastore | REST + gRPC | Control-plane CRUD (services/backups/metadata-imports) + Hive Metastore Thrift serving plane (:9083); databases/tables/partitions/locks served (including the Hive-3.x `get_table_meta` and `alter_table_with_cascade` paths), see [Known Limitations](#known-limitations) |
| BigLake Iceberg REST Catalog | REST | `org.apache.iceberg.rest.RESTCatalog` surface mounted at `/iceberg/` — namespaces, tables, atomic `CommitTableRequest` requirements/updates, see [Known Limitations](#known-limitations) |
| Managed Kafka | REST + gRPC | Metadata-only clusters/topics — see [Known Limitations](#known-limitations) |
| BigQuery | REST | Datasets/tables/jobs/rows + a documented Standard SQL subset (`SELECT`, DDL/DML) on an in-process SQLite engine, see [Known Limitations](#known-limitations) |
| Cloud Monitoring | REST + gRPC | Metrics, alert policies (evaluated), notification channels + incidents — see [Known Limitations](#known-limitations) |
| Cloud Logging | REST + gRPC | Log entries, filtering, tailing, monitored-resource descriptors, sinks + exclusions (routing evaluated, not delivered), logs-based metrics |
| Eventarc | REST + gRPC | Triggers/channels + provider discovery; Pub/Sub-sourced triggers with a `cloudFunction` destination deliver to that function — see [Known Limitations](#known-limitations) |
| Cloud DNS | REST | Metadata-only managed zones + record sets/changes — no authoritative DNS server, see [Known Limitations](#known-limitations) |
| Memorystore for Redis | REST | Metadata-only instances + location discovery — no Redis data plane, see [Known Limitations](#known-limitations) |
| Cloud SQL Admin | REST | Metadata-only instances/databases/users — no SQL engine or data plane, see [Known Limitations](#known-limitations) |
| Compute Engine | REST | Metadata-only instances/disks/networks/firewalls/subnetworks — no VM, disk, or network data plane, see [Known Limitations](#known-limitations) |

**Not implemented (out of scope):** Artifact Registry, Cloud Run, Cloud Endpoints, Deployment
Manager, Firebase Auth (Identity Toolkit), GKE (`container.googleapis.com`).

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

The release decisions and explicitly **accepted risks** for v1.1.0 — identity authz not enforced
(Cloud KMS crypto ops honor a default-permissive cryptoKey policy), LROs complete synchronously by
default (an opt-in async mode is available for local testing),
the metadata-only tier, and the real-GCP smoke requirement for Yellow/Red surfaces — are recorded
in [GA.md §10](docs/GA.md#10-release-decisions-and-accepted-risks-v110).

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
| — | `JAISCLOUD_LAMBDA_CODE_URL` | — | Admin base (including `/_jaiscloud`) a K8s code-fetch init container downloads function source archives from; defaults to `JAISCLOUD_GCS_EMULATOR_ENDPOINT` + `/_jaiscloud` |
| `--gcp-metadata` | `JAISCLOUD_GCP_METADATA_ENABLED` | `false` | Enable the GCP metadata-server emulator |
| `--kms-master-key` | `JAISCLOUD_KMS_MASTER_KEY` | — | 32-byte hex KEK wrapping the KMS DEK at rest |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| — | `JAISCLOUD_LRO_MODE` | `sync` | Long-running-operation timing: `sync` (default) completes every operation inline with `done: true`; `async` returns operations in flight and settles them lazily when a client polls `operations.get`, the generic `operations` service serves registry-backed `list`/`cancel`/`delete`, and an operation name no service owns is `NOT_FOUND` (local testing only — see [Known Limitations](#known-limitations)) |
| — | `JAISCLOUD_LRO_DELAY` | `250ms` | In-flight window before an operation settles in `JAISCLOUD_LRO_MODE=async`; a Go duration (`0` settles on the first read) |
| — | `JAISCLOUD_GCP_THROTTLE` | `off` | Opt-in throttle/quota fault injection for backoff testing: `off` (default), `rate`, `fault`, or `both`. Matching REST **and** gRPC requests are refused before dispatch with a retryable `429 RESOURCE_EXHAUSTED` (or `503 UNAVAILABLE` via `_STATUS`), a `Retry-After` header, and a `google.rpc.RetryInfo` detail (local testing only — see [Known Limitations](#known-limitations)) |
| — | `JAISCLOUD_GCP_THROTTLE_RPS` | `10` | Token-bucket refill rate (requests/second) for `rate`/`both` |
| — | `JAISCLOUD_GCP_THROTTLE_BURST` | `ceil(RPS)` | Token-bucket capacity for `rate`/`both` |
| — | `JAISCLOUD_GCP_THROTTLE_SERVICES` | all | Comma-separated allow-list of wire services / gRPC methods to throttle (case-insensitive substring; `all` matches everything) |
| — | `JAISCLOUD_GCP_THROTTLE_FAIL_FIRST` | `0` | Fail the first N matching requests |
| — | `JAISCLOUD_GCP_THROTTLE_FAIL_EVERY` | `0` | After the first N, fail every Mth matching request |
| — | `JAISCLOUD_GCP_THROTTLE_FAIL_COUNT` | `0` | Fail K consecutive matching requests then recover (applies when the two above are `0`) |
| — | `JAISCLOUD_GCP_THROTTLE_STATUS` | `429` | Injected HTTP status: `429` (`RESOURCE_EXHAUSTED`) or `503` (`UNAVAILABLE`) |
| — | `JAISCLOUD_GCP_THROTTLE_RETRY_DELAY` | `1s` | Advertised retry delay (Go duration), surfaced as `Retry-After` + `RetryInfo` |
| `--metrics` | — | `false` | Expose Prometheus metrics at `/metrics` |

**Transport selection.** `--transports` sets the global default and `--transport-overrides` refines it per service (an override wins). A service is constructed and registered only when at least one transport is selected for it, and only the selected listeners bind — `--transports=rest` opens no `:8081`, while `--transports=grpc` serves the GCP REST API nowhere but keeps the always-on `/_jaiscloud/*` admin plane (and `/metrics`) on `--port`. Per-service selection gates construction and which gRPC services register; `none` removes a service from both transports (a later request for it fails with a registry `no handler` error). The REST API is mounted as one route set, so it is gated by the global `--transports` setting rather than per service. Per-service override names are the **wire** names: `storage`, `pubsub`, `secretmanager`, `kms`, `iam`, `firestore`, `firestoreadmin`, `datastore`, `logging`, `monitoring`, `functions`, `workflows`, `workflowexecutions`, `dataproc`, `managedkafka`, `metastore`, `eventarc`, `serviceusage`, `resourcemanager`, `bigquery`, `dns`, `sqladmin`, `compute`, `iceberg`, `redis` (Memorystore), `scheduler`, `tasks`. An unknown service or transport token fails startup loudly.

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

### Throttle/quota: injected failures, not real quota values

No real quota plane is modelled. With `JAISCLOUD_GCP_THROTTLE` unset (the default), no path is ever throttled and backoff/quota-exhaustion handling never runs locally — that is the normal state, and production code must not depend on the injector. When the injector is armed it refuses matching REST and gRPC requests before dispatch with a synthetic `429 RESOURCE_EXHAUSTED` or `503 UNAVAILABLE`, together with a `Retry-After` header and a `google.rpc.RetryInfo` detail, so a client's retry/backoff/idempotency path can be exercised deterministically (`fault` mode is counter-based and needs no timing; `rate` mode is a per-project token bucket refilled on the real clock). It injects **configured** limits, never real per-project/per-metric quota values; quota behaviour must still be verified on real GCP. `make test-throttle-gcp` exercises the fault mode end-to-end.

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

### Datastore: `update_time` is a strict optimistic-concurrency token

A Datastore mutation may carry a `base_version` or an `update_time` conflict-detection precondition. `update_time` is the timestamp token, and it is kept **strictly monotonic per entity** even when the emulator's clock is frozen (`POST /_jaiscloud/clock` with `{"mode":"fixed",...}`): every write stamps `max(clock.Now(), current.UpdateTime + 1µs)`, truncated to microseconds (the precision of the persistent `TIMESTAMPTZ` column). Two writers that read the same entity simultaneously can therefore no longer both present the same token — the first commit advances it and the second is reported as `conflict_detected` (non-transactional) or `FAILED_PRECONDITION` (transactional) instead of silently overwriting the winner. `update_time` is returned on `EntityResult` (`lookup`/`runQuery`) and `MutationResult` (`commit`) over both REST and gRPC, so a client can read the token from a response and pass it forward as the next mutation's precondition. Under a live (or offset) clock `clock.Now()` already advances between writes, so the conditional advance is a no-op.

GQL is supported on both transports: `runQuery`/`RunQuery` accept a `gqlQuery` (`SELECT` with `WHERE`, `IN`, `IS [NOT] NULL`, `DATETIME`/`KEY`/`BLOB` literals, and named `@name`/positional `@N` bindings), and `runAggregationQuery`/`RunAggregationQuery` accept `AGGREGATE COUNT(*) | COUNT_UP_TO(n) | SUM(p) | AVG(p) OVER (SELECT ...)`. A GQL filter is translated to the same structured filter the engine already executes. Documented approximations: the emulator's query engine models only kind/filter/offset/limit, so GQL projection, `ORDER BY`, and cursor bindings are parsed/ignored, not applied; `HAS ANCESTOR`/`HAS DESCENDANT`, `CONTAINS`, and `NOT` are rejected (the same limitations the structured query surface has). Literals are accepted only when `allow_literals` is set, matching real Datastore.

Documented approximations: the query read-set tracks the returned entities' versions (real Datastore validates the query's read *range*), transactions are single entity-group, and an unused transaction handle expires after ~270 seconds (real Datastore also expires transactions).

### BigQuery: documented Standard SQL subset (single in-process SQLite engine)

`jobs.query` and `jobs.insert` (query configuration) evaluate a bounded BigQuery **Standard SQL subset** on a single in-process, pure-Go SQLite engine (`modernc.org/sqlite`) in both memory and `--dsn` modes, and encode results into the Discovery `jobs.query`/`getQueryResults` shapes with plan-inferred column types. Supported: `SELECT`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`/`LIMIT`/`OFFSET`/`DISTINCT`, `INNER`/`LEFT` joins, subqueries, CTEs, `UNION ALL`, aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), window functions, a bounded string/math/null function set (`LENGTH`/`UPPER`/`LOWER`/`SUBSTR`/`TRIM`/`LTRIM`/`RTRIM`/`REPLACE`/`INSTR`/`ABS`/`ROUND`/`COALESCE`/`NULLIF`/`IFNULL`), FLOAT64 division, and DDL/DML (`CREATE`/`DROP` dataset/table, `CREATE TABLE … AS SELECT`, `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE`). The **store stays the source of truth**: SQL writes go through the store and SQLite is disposable per-query scratch, never persisted, so snapshots/export/import/`--dsn` durability are unaffected.

Unsupported constructs — `UNNEST`/`ARRAY`/`STRUCT`, `GEOGRAPHY`, wildcard/`TABLE_SUFFIX` tables, `INFORMATION_SCHEMA`, scripting/stored procedures, `MERGE`, `SAFE.` functions, `QUALIFY`, date/format functions, DDL/DML beyond the accepted set, and legacy SQL — **fail loud** with `400 invalidQuery` instead of returning wrong rows. Documented simplifications: DDL/DML jobs complete synchronously (`jobComplete: true`); `getQueryResults` re-executes a stored `SELECT` but returns the persisted statistics for a DDL/DML job (so a write is never double-applied); `maxResults`/`pageToken` paging is ignored (the full bounded result set is returned); job-statistics/timing fields (`cacheHit`, `creationTime`, `queryId`, bytes/latency counters, ...) are not synthesized; and cross-type comparison and division-by-zero semantics follow the SQLite-backed subset rather than real BigQuery. `routines`, `models`, and `rowAccessPolicies` are explicit `501` stubs.

`tabledata.insertAll` validates rows against the table schema (missing `REQUIRED` fields and unknown fields are rejected unless `ignoreUnknownValues` is set), honors `skipInvalidRows`, and best-effort suppresses duplicate `insertId`s over a bounded, in-memory window (the dedup state is not persisted across restarts). Field *types* are not enforced, and `templateSuffix` is not supported. `tabledata.list` honors `startIndex` (offset pagination). `projects.getServiceAccount` returns a synthetic `bq-{project}@gcp-sa-bigquery.iam.gserviceaccount.com` rather than a real service account.

### Cloud Scheduler: real cron engine; App Engine targets stored only

Cloud Scheduler v1 is served over REST (`cloudscheduler.googleapis.com/v1`) and gRPC
(`google.cloud.scheduler.v1.CloudScheduler`) from one transport-neutral core and store. Jobs support
`create`/`get`/`list`/`patch`/`delete` plus `pause`/`resume`/`run`. A real cron engine fires due
`httpTarget` and `pubsubTarget` jobs on the **emulator clock** (`clock.Now()`): advance the clock
(`POST /_jaiscloud/clock`) and the engine delivers on its next tick, or call
`POST /_jaiscloud/scheduler-tick` (or `jobs.run`) to fire deterministically. An `httpTarget`
delivery performs the documented HTTP request (method, headers, body, with the `X-CloudScheduler*`
headers) and treats a 2xx as success; a `pubsubTarget` reuses the Pub/Sub publish path, so the
message fans out to the topic's pull subscriptions. Failures are retried with exponential backoff
honoring `retryConfig.retryCount`/`minBackoffDuration`/`maxBackoffDuration`, then fall back to the
cron schedule.

Limitations: `appEngineHttpTarget` is stored and echoed but **never delivered** (the emulator has no
App Engine router) — its attempts are recorded as `Unimplemented`; English-like schedules ("every 5
minutes") are not parsed (unix-cron plus `@` descriptors only; anything else is `InvalidArgument`);
`oauthToken`/`oidcToken` attach a synthetic emulator-local bearer token rather than a real Google
token; and `updateCmekConfig` is not implemented.

### Cloud Tasks: dispatch engine over the control plane

Cloud Tasks v2 is served over REST (`cloudtasks.googleapis.com/v2`) and gRPC
(`google.cloud.tasks.v2.CloudTasks`) from one transport-neutral core and store. Queues support
`create`/`get`/`list`/`patch`/`delete` plus `pause`/`resume`/`purge` and queue IAM
(`getIamPolicy`/`setIamPolicy`/`testIamPermissions`). Tasks support `create`/`get`/`list`/`delete`
plus the REST-only `tasks:batchCreate`/`tasks:batchDelete`. Both `httpRequest`
(url/method/headers/body/`oauthToken`/`oidcToken`) and `appEngineHttpRequest` are accepted and
echoed, and created tasks get a `scheduleTime`, `createTime`, and default `dispatchDeadline`.

A dispatch engine delivers due `httpRequest` tasks on RUNNING queues against the emulator clock:
advance `/_jaiscloud/clock` (or `POST /_jaiscloud/tasks-tick`) to fire deterministically. Delivery
attaches the documented `X-CloudTasks-QueueName`/`-TaskName`/`-TaskRetryCount`/`-TaskExecutionCount`
/`-TaskETA` and `User-Agent: Google-Cloud-Tasks` headers, is gated by the queue's
`rateLimits` (`maxDispatchesPerSecond`/`maxBurstSize` token bucket and `maxConcurrentDispatches`),
and a 2xx response deletes the task while a failure retries with exponential backoff
(`minBackoff`/`maxBackoff`/`maxDoublings`) until `maxAttempts` (-1 = unlimited) or
`maxRetryDuration` is exhausted. `RunTask` (gRPC) / `tasks:run` (REST) forces one synchronous
attempt, bypassing the queue's paused state and rate limits.

Limitations: `appEngineHttpRequest` is stored and echoed but never delivered (no App Engine
router) — its attempts are recorded as `Unimplemented` failures. REST `tasks:buffer`, task
auto-expiry (31 days), IAM enforcement (queue policies are stored but not enforced),
task-level `retryConfig` overrides (the queue's `retryConfig` governs), and
`oauthToken`/`oidcToken` Google-token minting (a synthetic emulator-local bearer token is attached
instead) are not modelled. Rate-limit and retry state is in-memory and not persisted across
restarts; the engine polls once per second, so sustained throughput is bounded by
`maxBurstSize` per second rather than `maxDispatchesPerSecond`.

### Managed Kafka: metadata only, no real broker

A cluster is a logical record only — the emulator never stands up a real Kafka broker. Consumer groups are not tracked: `ListConsumerGroups` always returns an empty list, and get/update/delete operations on a consumer group return `NOT_FOUND`.

Cluster create/update/delete return a proper `google.longrunning.Operation` (`name`, `metadata` with `@type=type.googleapis.com/google.cloud.managedkafka.v1.OperationMetadata`, `done: true`, `response`) inline; the operation is also persisted under `projects/{project}/locations/{location}/operations/{id}` and served by the registered `GetOperation`/`ListOperations` handlers. That operations path is shared with Cloud Workflows' LRO surface and is routed to Workflows on the single emulator host; by default every Managed Kafka operation is returned already done, so no client needs to poll it, while in the opt-in async mode (`JAISCLOUD_LRO_MODE=async`) a poll is resolved against the Managed Kafka store.

### Cloud Monitoring: alert policies are evaluated for `condition_threshold` and `condition_absent`

Alert policies are evaluated by a background worker (30s tick, matching the AWS CloudWatch alarm evaluator). Two condition types are evaluated: **`condition_threshold`** and **`condition_absent`**. `condition_matched_log`, `condition_monitoring_query_language`, `condition_prometheus_query_language`, and `condition_sql` are stored but never evaluated (and never fire).

For `condition_threshold`, the evaluated filter subset is equality clauses joined by `AND` on `metric.type`, `resource.type`, `metric.label.{key}`, and `resource.label.{key}`; unknown clauses are ignored. The matching series' latest point in each alignment window is reduced across series (`REDUCE_SUM`/`MEAN`/`MAX`/`MIN`/`COUNT`, default mean), then compared to `threshold_value`; `duration` requires the comparison to hold across every alignment window it spans, and `trigger` count/percent is honored when no reducer is set.

For `condition_absent`, the same filter subset is used: a matching series is treated as **absent** when it has no data point within the condition's `duration` window, and `trigger` count/percent selects how many absent series are required (when unset, any absent series fires — the real "Any time series violates" default). Mirroring real Cloud Monitoring, a metric that has **never** produced a measurement does not fire (absence requires prior data), and the condition's per-series aligner / cross-series reducer / `group_by_fields` are not applied to the absence evaluation.

A firing policy opens an incident and delivers notifications. An open incident is closed — and a resolution notification delivered — when the policy stops firing: for `condition_threshold`, a below-threshold evaluation or a window with no matching data; for `condition_absent`, fresh data (a point inside the window). Incidents have no public API, so they are observable via the snapshot/export surface and logs. `pubsub` channels receive a CloudEvents-style JSON message on `labels.topic`; `email`/`webhook`/`sms` notifications are recorded on the incident and logged but not sent.

`GetMonitoredResourceDescriptor` serves a canonical catalog of well-known types (`gce_instance`, `gcs_bucket`, `pubsub_topic`, `pubsub_subscription`, `k8s_container`, `k8s_pod`, `k8s_node`, `dataproc_cluster`, `cloudsql_database`, `api`); an unknown type returns `NotFound`. `CreateServiceTimeSeries` mirrors `CreateTimeSeries` — in real GCP the two differ only in the identity/permission used, and the emulator has no authz plane. `DISTRIBUTION`-typed point values (linear/exponential/explicit buckets, bucket counts, count, mean, sum of squared deviations, optional range and exemplars) round-trip through `CreateTimeSeries`/`ListTimeSeries`. `ListMetricDescriptors`/`ListMonitoredResourceDescriptors` honor a discovery filter subset — equality (`type = "x"`, `metric.type = "x"`, `resource.type = "x"`) and `starts_with("prefix")` clauses joined by `AND`, over the descriptor type or its `name`; any other key, operator, or malformed clause fails with `InvalidArgument`. `ListTimeSeries` supports only the `metric.type` / `resource.type` equality filter subset — the full Monitoring Query Language is not implemented. `NotificationChannelService` supports CRUD for `pubsub`/`email`/`webhook`/`sms` channels. `ListNotificationChannelDescriptors`/`GetNotificationChannelDescriptor` serve a static catalog of well-known channel types (`email`, `sms`, `pubsub`, `webhook_tokenauth`, `slack`); an unknown type returns `NotFound`. `SendNotificationChannelVerificationCode` is a no-op success (the emulator delivers nothing), `GetNotificationChannelVerificationCode` returns a deterministic synthetic code, and `VerifyNotificationChannel` marks the channel `VERIFIED`.

### Cloud Logging: `TailLogEntries` is a bounded polling approximation

`ListMonitoredResourceDescriptors` returns the canonical catalog of well-known monitored resource types shared with the Monitoring service (`gce_instance`, `gcs_bucket`, `pubsub_topic`/`subscription`, `k8s_container`/`pod`/`node`, `dataproc_cluster`, `cloudsql_database`, `api`), each with `type`, `display_name`, `description`, and `labels`; `page_size`/`page_token` are honored. Unlike Monitoring, the Logging descriptors leave `name` unset, and the `ListMonitoredResourceDescriptorsRequest` in this proto revision carries no `parent` or `filter` fields (only page size/token), so the catalog is global and unfiltered.

`TailLogEntries` is implemented as a **bounded, store-polling** tail rather than a true push feed. After accepting the initial `resource_names`/`filter`/`buffer_window`, the server records the store's monotonic write id as its cursor and re-reads the store on a timer derived from `buffer_window`, streaming entries written after the stream began whose fields satisfy the same filter engine `ListLogEntries` uses. This is the closest correct behavior the emulator's store allows, and it never returns `Unimplemented` and never busy-spins. The documented deviations from real Cloud Logging are:

- **Delivery is at-most-once against each read snapshot**, not the real API's at-least-once. An entry inserted with a higher store id is emitted exactly once; a client that disconnects before the next poll does not get it, and there is no server-side retention or replay.
- **`buffer_window` is realized as the poll interval**, not as a reordering buffer. The emulator's store already returns entries in `(timestamp, id)` order, so there are no late-arriving out-of-order entries to absorb; the window only controls latency. Absent/`nil` uses the real 2 s default, explicit `0` is clamped to 50 ms to avoid a spin, and values are capped at 60 s (the spec's maximum).
- **Filter (and project) changes on further client requests are applied** to subsequent polls; a project change re-seeds the id cursor so the new project's pre-existing backlog is not replayed.
- The session terminates cleanly on client half-close (`Recv` → `EOF`) or context cancel.

### Cloud Logging: sinks and exclusions are configured, routing is evaluated (not delivered)

The config plane is implemented over both transports for **sinks** (`sinks.create`/`get`/`list`/`update`/`patch`/`delete`, gRPC `CreateSink`/`GetSink`/`ListSinks`/`UpdateSink`/`DeleteSink`) and **resource-level exclusions** (`exclusions.create`/`get`/`list`/`patch`/`delete`, gRPC `ConfigServiceV2` exclusions). Sinks and exclusions are project-scoped and persisted (memory + Postgres + snapshots); the sink's inline `exclusions` and the `writer_identity` output field round-trip, and masked updates honor the REST `updateMask`/gRPC `FieldMask` paths (`destination`, `filter`, `description`, `disabled`, `exclusions`, `include_children`). A filter that the emulator's advanced-log subset cannot parse is rejected at write time (`InvalidArgument`) rather than silently routing nothing.

`WriteLogEntries` **evaluates** routing: resource-level exclusions are applied first, then each enabled sink's inline exclusions, then the sink filter, and the matched sink resource names are recorded (debug log). The emulator has **no export destination delivery** — no bytes are written to the sink's bucket/dataset/topic — so a sink is configuration + routing evaluation only. Sink/exclusion filters use the same filter engine as `ListLogEntries` (the documented subset), so filters the engine does not understand are rejected loud. The gRPC `ConfigServiceV2` bucket/view/link, CMEK/settings, and `CopyLogEntries` RPCs are explicit `Unimplemented` stubs (the emulator has no log-bucket storage plane).

### Cloud Logging: logs-based metrics are definitions, not computed time series

Logs-based metrics are implemented over both transports (`metrics.create`/`get`/`list`/`update`/`delete`, gRPC `MetricsServiceV2` `CreateLogMetric`/`GetLogMetric`/`ListLogMetrics`/`UpdateLogMetric`/`DeleteLogMetric`). A metric is a project-scoped, persisted definition (`filter`, `description`, `disabled`, `bucket_name`, `value_extractor`, `label_extractors`, `bucket_options`, and the descriptor's `metric_kind`/`value_type`/`unit`/`display_name`/`labels`) stored in the memory and Postgres backends and included in snapshots/export/import. The filter is compiled by the same advanced-log engine as `ListLogEntries`, so an unparseable filter is rejected at write time (`InvalidArgument`); a `DISTRIBUTION` metric requires a `value_extractor`, and every descriptor label must have a matching `label_extractors` entry (and vice versa).

The output-only `metric_descriptor.name`/`type`/`description` are synthesized from the metric id/description (`logging.googleapis.com/user/{METRIC_ID}`), and `metric_kind`/`value_type` are immutable across updates (existing label value types are too; new labels may be added). The emulator **does not compute metric time series** — there is no Monitoring writer on the log write path — so a logs-based metric is a definition only. Metric ids may contain slashes (e.g. `nginx/requests`); the id is percent-encoded (`%2F`) in the canonical resource name and accepted either encoded or raw on a read.

### Cloud KMS: rotation schedule is executed lazily on read

`CryptoKey` `labels`, `rotationPeriod`, and `nextRotationTime` are persisted and returned by both the REST and gRPC surfaces. A `rotationPeriod` is an automatic rotation schedule: when set, `nextRotationTime` is derived (`createTime + rotationPeriod` on create, `now + rotationPeriod` on update) and clearing the period clears `nextRotationTime`. `UpdateCryptoKey` honors the `labels` and `rotation_period` update-mask paths (unmasked fields are preserved); any other path fails loud with `Unimplemented`.

The schedule **is** executed, lazily on read (the emulator has no background scheduler): when a due key is read (`GetCryptoKey`/`ListCryptoKeys`, and `Encrypt` before it resolves the primary), a new ENABLED `CryptoKeyVersion` is created, made the primary, and `nextRotationTime` advances to `now + rotationPeriod`. A key therefore rotates on its next read after the schedule comes due. Because the schedule advances to `now + rotationPeriod` rather than by one period per elapsed interval, a clock jump far past the due time is coalesced into a single rotation — a deliberate approximation. Cloud KMS **managed/external** rotation (imported key versions, external key managers, `rotate-master-key`) remains out of scope and is not implemented.

The gRPC KMS surface also implements the delete side (`DeleteCryptoKeyVersion`, `DeleteCryptoKey`, plus the `RetiredResource` records that block name reuse), the raw AES-GCM primitives (`RawEncrypt`/`RawDecrypt`, purpose `RAW_ENCRYPT_DECRYPT`), and the `ImportJob` control plane (`CreateImportJob`/`GetImportJob`/`ListImportJobs`). `ImportCryptoKeyVersion` (wrapped-key import), the trusted-key-wrapped import/export pair, and `Decapsulate` (no KEM algorithm support) are not implemented and return `Unimplemented`.

A cryptoKey's IAM policy is enforced on its crypto operations (`encrypt`/`decrypt`/`asymmetricSign`/`asymmetricDecrypt`/`macSign`/`macVerify`/`getPublicKey`), **default-permissively**: a key with no policy — or with no bindings — allows every operation, and only a policy scoped to roles that omit the required `cloudkms.cryptoKeyVersions.useTo*` permission returns `403 PERMISSION_DENIED`. Binding **members** and **conditions** are not evaluated (the emulator does not resolve the caller's identity), mirroring the AWS KMS emulator's key-policy check. Identity IAM remains shape-only (see [GA.md §10](docs/GA.md#10-release-decisions-and-accepted-risks-v110)).

### Secret Manager: scheduled rotation is stored, managed rotation is not implemented

A `Secret`'s `rotation` schedule (`nextRotationTime` + `rotationPeriod`) is persisted and honored lazily: a read (`GetSecret`/`AccessSecretVersion`) after `nextRotationTime` creates a new version and advances the schedule. Cloud SQL **managed rotation**, however, is `Unimplemented` on the gRPC surface: `EnableManagedRotation` and `RotateSecret` generate a password, apply it to a Cloud SQL user, and store it as a new secret version, and the emulator has no Cloud SQL data plane to update. Both RPCs fail loud with `codes.Unimplemented` rather than fabricating a version.

### Cloud Workflows: synchronous execution, no filter/orderBy

Cloud Workflows is implemented over a real YAML expression engine: workflow definitions and executions are stored, and `executions.create` runs the workflow synchronously and returns it already in a terminal state (there is no asynchronous execution queue). List `filter`/`orderBy` are ignored; a `switch` with no matching condition fails the execution loudly; `http.*` auth is not modelled; `retry.predicate` fails loud; and subworkflows, `listRevisions`, IAM, and CMEK are not implemented.

### Dataproc on GKE: `virtualClusterConfig` is metadata only

A cluster may be created with a `virtualClusterConfig` (Dataproc-on-GKE) instead of a `config` (Dataproc-on-Compute-Engine). The emulator validates the GKE shape structurally — `kubernetesClusterConfig.gkeClusterConfig.gkeClusterTarget` or a non-empty `nodePoolTarget` is required — rejects supplying both `config` and `virtualClusterConfig` with `InvalidArgument`, and round-trips the raw `virtualClusterConfig` (including unknown sub-fields) on REST and gRPC. It is **metadata only**: there is no GKE/Container API, no node-pool CRUD, and no real GKE control plane — both placements run the same client-mode Spark pods, so a GKE-backed cluster is not scheduled onto an external GKE cluster.

### Dataproc: asynchronous cluster/job state machine

Cluster create/update/start/stop/delete return a pollable `google.longrunning.Operation` (`Done=false`) whose status advances lazily on each `operations.get` read — there is no background goroutine. A create shows the cluster `CREATING` then `RUNNING` (the readiness delay is clock-driven and overridable via `JAISCLOUD_DATAPROC_CLUSTER_READY_DELAY`); a delete keeps the `DELETING` record until the transition fires, then removes it. `ERROR` is reachable only through the test failure-injection hook, because real provisioning failures are not observable. Likewise `jobs.submit` writes `PENDING` and advances `SETUP_DONE → RUNNING → DONE` as the job is polled, and `jobs.cancel` walks `CANCEL_PENDING → CANCEL_STARTED → CANCELLED`. In K8s executor mode `jobs.cancel` also deletes the client-mode driver k8s Job (and, by cascade, its pod), so a running driver actually stops rather than continuing after the job reports `CANCELLED`. On the gRPC surface the generic `google.longrunning.Operations` stub does not read the Dataproc store, so a `SubmitJobAsOperation` LRO can only be `Wait`-polled once the job is terminal in the default mock executor; the REST `operations.get` path always reflects the store.

### Dataproc: long-running and restartable jobs

A job is treated as **long-running (streaming)** when its type-job `properties` set a `spark.sql.streaming.*` or `spark.streaming.*` key: in mock mode it stays `RUNNING` (never auto-settling to `DONE`) until `jobs.cancel`; under the Docker/K8s executor it is an ordinary job that stays `RUNNING` while the driver pod lives and reaches `DONE` only on driver exit `0`. Real GCP has no streaming marker — detection from `properties` is an emulator approximation (a streaming job is simply one whose driver never exits, so a never-exiting driver that sets no streaming property is not detected). The real-k3d smoke `make test-dataproc-streaming-k8s` stages a `readStream.format("rate")` driver with a `gs://` checkpoint and sink, asserts the job stays `RUNNING` while micro-batches commit, then cancels it and asserts `CANCELLED` plus the driver k8s Job/pod being reaped.

`Job.scheduling` (`maxFailuresPerHour` / `maxFailuresTotal`; REST and gRPC) is parsed, validated against the documented maxima (`0..10` / `0..240`; `0` or omitted = no restart, the API default) and echoed back. In K8s executor mode a non-zero driver exit restarts the driver while within both limits and not thrashing (`>4` non-zero exits in a 10-minute window), recording `ATTEMPT_FAILURE` between attempts; success is strictly driver exit `0` and exhaustion reaches `ERROR`. Restart counters live only for the job's current execution (they are not persisted across a restart/resubmission of the same `jobId`) and the per-hour window is modelled as a fixed window; the mock executor echoes the fields but never restarts. `SparkJob.jarFileUris` and `PySparkJob.jarFileUris` are passed to `spark-submit --jars` (additional driver/executor classpath jars — the documented way to add connector jars); `SparkRJob` has no such field and `SparkSqlJob.jarFileUris` was already wired.

### Dataproc: job-type support matrix

`jobs.submit` runs the Spark family (including Spark SQL); every other type-job is fail-loud. The emulator is metadata-not-engine for the non-Spark engines — the Hive Metastore plane is served separately (see [Dataproc Metastore: control plane + Hive Thrift serving plane](#dataproc-metastore-control-plane--hive-thrift-serving-plane)) — so it does not attempt to execute Hive/Pig/Presto/Trino/Flink work.

| Type-job | Behaviour |
|---|---|
| `sparkJob` / `pysparkJob` / `sparkRJob` | Run via the Spark executor (mock or K8s mode; the Docker Spark executor is not wired, so `docker` mode falls back to mock). |
| `sparkSqlJob` | Runs the real Spark SQL CLI (`spark-sql`) over the same executor in K8s mode (mock mode simulates it; `docker` mode falls back to mock): `queryList.queries` via `-e`, `queryFileUri` via `-f` (a `gs://` script is read through the wired GCS connector), `jarFileUris` via `--jars`, `scriptVariables` via `--hivevar`, and `properties` via `--conf`. Fail-loud if neither `queryFileUri` nor `queryList.queries` is set. |
| `hadoopJob`, `hiveJob`, `pigJob`, `prestoJob`, `trinoJob`, `flinkJob` | Fail loud (see below). |

Submitting an unsupported type does **not** return an RPC error: the job is created and immediately lands in `ERROR` with details `job type <X> is not supported by the emulator`, so the client observes a terminal job rather than a transport failure. In particular **hiveJob** is fail-loud because execution is engine-bearing (a Hive/Pig/Presto/Trino/Flink runtime or query engine) while the emulator is metadata-not-engine; the Hive Metastore plane is a separate surface (see the Metastore subsection below).

Because the emulator is metadata-not-engine, **hiveJob** stays fail-loud rather than being silently approximated: HiveQL is a superset of Spark SQL, so running it on Spark would diverge on Hive-specific constructs. If you need Hive execution, point your own engine at the emulator: the Hive Metastore Thrift plane on `:9083` serves the catalog (a single global catalog, see the Metastore subsection below), so a HiveServer2 or Spark deployment attached to it can execute queries while the emulator stores the table metadata.

### Dataproc: driver output is captured from the client-mode driver

A job's `driverOutputResourceUri`/`driverControlFilesUri` are allocated at submit and point into a staging bucket (`config.tempBucket`/`config.configBucket`, else a derived `dataproc-staging-<project>-<region>-<hash8>`; real Dataproc uses `dataproc-staging-<region>-<projectNumber>-<random>`, but the name is opaque to clients). At terminal state the driver's stdout/stderr is written to `<driverOutputResourceUri>.000000000` and a control file under `driverControlFilesUri`, so `gcloud dataproc jobs wait` and log readers resolve real objects in the emulated GCS. Capture is from the client-mode `spark-submit` main container only (no YARN/cluster-mode driver logs, no multi-container multiplexing) and is byte-capped at 4 MiB; in mock mode a small synthetic line is written so the advertised URI still resolves. Driver output is staged only at termination (there is no continuous/streamed output), so a long-running job's advertised URI resolves to nothing until the job is cancelled or its driver exits. Executor pods receive the `fs.gs.*` connector config plus `spark.executorEnv.*` mirrored from the driver, and caller `properties` cannot strip the injected connector confs.

### Dataproc: `Reset` does not drain in-flight Spark job goroutines

`POST /_jaiscloud/reset` wipes the Dataproc store but does not cancel or wait for jobs currently executing (Docker/K8s executor mode). This matches AWS EMR's own `Reset` behaviour in this codebase, which is a no-op for the same reason — not a GCP-specific gap. If you reset while a job is mid-execution and then resubmit a job with the *same* `(project, region, jobId)` before the stale run finishes, the stale run's completion could overwrite the new job's state. Avoid reusing job IDs across a reset boundary while a prior run may still be in flight.

### Dataproc Serverless: not implemented

The Dataproc Serverless (Batch) API is not implemented. Dataproc is clusters + jobs with mock, Docker, or K8s executors only; serverless batches have no emulator surface.

### Dataproc WorkflowTemplates: only the latest version is retained

`WorkflowTemplateService` is served over REST + gRPC (`create`/`get`/`list`/`update`/`delete`, plus `instantiate`/`instantiateInline`). Only the **latest** version of a template is stored: `create` stores version 1, `update` requires the request version to match the current one (`ABORTED` on mismatch) and bumps it, and a `get`/`delete` naming a non-current explicit version returns `NotFound` (real GCP keeps a version history). Instantiation runs the template's `OrderedJob` DAG through the same job core, so only the Spark-family job types (`sparkJob`/`pysparkJob`/`sparkSqlJob`/`sparkRJob`) execute; other job types fail loud exactly as `SubmitJob` already does (see [Dataproc: job-type support matrix](#dataproc-job-type-support-matrix)). The `workflowTemplates` IAM trio (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) is not served.

### Dataproc lifecycle events on Pub/Sub (emulator-defined)

Every cluster and job state transition publishes one message to a configured Cloud Pub/Sub topic, so event-driven consumers can observe the Dataproc lifecycle without polling `operations.get`/`jobs.get`. **This is an emulator-defined contract** — real GCP has no native Dataproc Pub/Sub event source — so it is opt-in and off by default.

- **Enable:** start the emulator with `JAISCLOUD_DATAPROC_EVENTS_TOPIC=<topic>` (a topic ID or full `projects/{p}/topics/{t}` name; a bare ID is scoped to the transition's project). Publishing is disabled unless that variable is set or a cluster carries the per-cluster label below.
- **Per-cluster override:** set the cluster label `jaiscloud-events-topic` to route that cluster's — and its jobs' — events to a different topic. The label alone enables publishing for that cluster even when the env default is unset.
- **Message body:** a structured [CloudEvents 1.0](https://cloudevents.io) JSON envelope (`specversion`, `id`, `source` `//dataproc.googleapis.com/projects/{p}/regions/{r}/clusters/{c}`, `type`, `datacontenttype`, `time`, `data`). Types are `google.cloud.dataproc.v1.job.v1.stateChange` and `google.cloud.dataproc.v1.cluster.v1.stateChange`; `data` carries `projectId`, `region`, `clusterName`, `jobId`/`clusterUuid`, `previousState`, `state`, `stateStartTime` (and `attempt` for jobs).
- **Message attributes:** the same key fields are mirrored as string attributes (`eventType`, `projectId`, `region`, `clusterName`, `jobId`, `previousState`, `state`, `attempt`) so a subscription `filter` can select them.
- **States:** jobs emit the full machine (`PENDING`, `SETUP_DONE`, `RUNNING`, `CANCEL_PENDING`, `CANCEL_STARTED`, `CANCELLED`, `DONE`, `ERROR`, `ATTEMPT_FAILURE`); clusters emit `CREATING`, `UPDATING`, `STARTING`, `STOPPING`, `RUNNING`, `STOPPED`, and a terminal `DELETED` when the record is removed (`DELETED` is emulator-only — `ClusterStatus` has no such state). A cluster reaches `ERROR` only through the failure-injection path the tests use (real provisioning failures are not observable), and `attempt` on a job event is `1` today because the Spark engine does not retry (the emulator never begins a second attempt).
- **Best-effort:** a transition never fails because of eventing; a publish error (e.g. the topic does not exist) is logged and dropped. If Cloud Functions is enabled, each transition is also dispatched directly to any function whose event trigger names the matching Dataproc event type **and** the specific job/cluster resource (the trigger `resource` must match the event's `jobs/{id}` / `clusters/{name}`).

### Dataproc cluster Metastore attachment

A cluster may attach an existing Dataproc Metastore service through `config.metastoreConfig` (GCE) or `virtualClusterConfig.auxiliaryServicesConfig.metastoreConfig` (GKE). The `dataprocMetastoreService` reference is validated at cluster create/update: the canonical `projects/{p}/locations/{l}/services/{s}` name and the short forms `locations/{l}/services/{s}`, `services/{s}` and a bare `{s}` are accepted (missing project/location default to the cluster's), a malformed name is `InvalidArgument`, an unknown service is `NotFound`, and a service in a region other than the cluster's is rejected (`InvalidArgument`) — real Dataproc requires the cluster and its Metastore service to share a region. The raw reference is echoed back on get/list. At job submit the attachment is injected into the Spark driver/executor confs as `spark.hadoop.hive.metastore.uris` plus `spark.sql.catalogImplementation=hive`; caller job `properties` override the injected values (Spark last-value-wins). The per-service endpoint is synthesized as `thrift://<id>.<location>.metastore.jaiscloud.local:9083`, which is not resolvable from Spark pods, so set `JAISCLOUD_DATAPROC_HMS_ENDPOINT=host:9083` (or a full `thrift://host:port` URI) to inject the pod-reachable Hive Metastore address instead.

### Dataproc Metastore: control plane + Hive Thrift serving plane

The management plane is implemented (`Service` / `Backup` / `MetadataImport` CRUD with long-running operations). The table-metadata plane is served by a Hive Metastore **Thrift** listener on `:9083` (a single global catalog) that answers the database, table, partition, and lock methods Spark's Hive/Iceberg clients use; the per-Service `endpointUri` is synthesized (`thrift://<id>.<location>.metastore.jaiscloud.local:9083`) because the listener is shared, not per-service. Consequently **all** control-plane Services share that one catalog: two Services — in the same project or in different projects/locations — that create a database of the same name collide, the `default` database and the Iceberg commit locks are shared, and pipelines must namespace their databases. This matches the AWS Glue Data Catalog in this emulator, which keeps one catalog per account+region shared by every job with no per-service/per-job catalog, so the per-Service `endpointUri` is cosmetic naming (the residual difference is that Glue is region-scoped while the HMS plane is location-agnostic). Partition support covers the generic Hive/Hadoop-migration path — add/get/list (by name, prefix, filter, spec), alter, rename, and drop, over a `jc_hms_partitions` store (memory + Postgres + snapshot) — while Iceberg keeps its own partition metadata. The Hive-3.x paths — `get_table_meta` (the pattern-based bulk table-metadata lookup) and `alter_table_with_cascade` (an alias of `alter_table`; stored partition descriptors are not cascaded) — are served; the client-connect identity method `set_ugi` and the request-struct table reads `get_table_req`/`get_table_objects_by_name_req` are served too, so Hive 2.3+/3.x clients (and off-the-shelf Go HMS clients) connect and read tables without relying on `UNKNOWN_METHOD` fallbacks; the niche partition methods (`exchange_partition(s)`, `drop_partitions_req`, `get_partition_values`, `get_partitions_by_expr`, event marking) return an explicit unsupported error; the gRPC control plane (`Service`/`Backup`/`MetadataImport` CRUD) is implemented over the official `DataprocMetastore` proto. `ExportMetadata`, `RestoreService`, `QueryMetadata`, `MoveTableToDatabase`, and `AlterMetadataResourceLocation` return `Unimplemented` by decision, because they are admin/DR operations off the Spark job path (the medallion ETL surface is the Hive Thrift / Iceberg table plane) and have no AWS Glue analogue: `QueryMetadata` would need a SQL engine over the metastore backend RDBMS schema (`DBS`/`TBLS`/…) plus a Cloud Storage result-manifest writer; `ExportMetadata`/`RestoreService` would need a metadata-dump format (`MYSQL`/`AVRO`) and a catalog restore; and `MoveTableToDatabase`/`AlterMetadataResourceLocation` are catalog mutations whose common ETL equivalents (`ALTER TABLE … RENAME` / `SET LOCATION`) already flow through the Hive Thrift `alter_table` path. Each fails loud rather than fabricating a result. On the single host, `locations/{l}/operations/{id}` is path-identical to Cloud Workflows' LRO surface and therefore routes to Workflows — by default Metastore's own operations are returned inline (`done: true`), so no client needs to poll them, while in the opt-in async mode (`JAISCLOUD_LRO_MODE=async`) a poll is resolved against the Metastore store over REST and gRPC.

### BigLake Iceberg REST Catalog: DB-backed, standard REST spec

The BigLake Iceberg REST Catalog is mounted at `/iceberg/` (Spark configures `uri=http://host:port/iceberg/`) and speaks the standard `org.apache.iceberg.rest.RESTCatalog` protocol. It models GCP's real BigLake Metastore managed-Iceberg product, whose catalog is the standard Apache Iceberg REST spec — Spark, Trino, and Flink attach over that spec. (Dataproc Metastore's own table plane is the Hive Metastore Thrift server, a separate product.) The catalog is database-backed (Polaris-style): it stores the `TableMetadata` JSON and a synthesized `metadata-location` pointer (`{location}/metadata/{version:05d}-{uuid}.metadata.json`) but never writes `metadata.json`/`version-hint.text` to object storage itself — the client's `FileIO` does that. `CommitTableRequest` requirements (`assert-table-uuid`, `assert-ref-snapshot-id`, schema/spec/sort-order assertions, …) and updates (`assign-uuid`, `add-schema`, `add-snapshot`, `set-properties`, …) are applied atomically, so concurrent commits cannot lose updates. `set-statistics`/`remove-statistics`/`set-partition-statistics`/`remove-partition-statistics` persist to `TableMetadata.statistics`/`partition-statistics` (upsert and remove keyed by `snapshot-id`), and the `POST /tables/{table}/metrics` report endpoint validates the table and `report-type` and acknowledges with `204` (reports are not retained — the spec exposes no read-back, so aggregation is left to the caller/backend).

### Eventarc: trigger CRUD + Pub/Sub → Cloud Functions delivery

Eventarc is implemented as trigger-based metadata CRUD, **not** the AWS EventBridge "bus + rule + event-pattern + target" fan-out model (the two are different models despite the shared "event routing" purpose). A `Trigger` is a stored record with a `destination` (Cloud Run service, Workflows, HTTP endpoint, GKE, or Cloud Functions), a `transport.pubsub.topic` source, and `eventFilters` (at least one of which must have `attribute: "type"`, as real Eventarc requires); a `Channel` is a 3rd-party-source registration record referencing a `provider`; and `Provider` resources are read-only discovery (a small catalogue of real providers — `pubsub.googleapis.com`, `storage.googleapis.com` — not invented ones). `destination.cloudFunction` is accepted (it must name an existing function, else `404`) and is executed: when a message is published to the trigger's `transport.pubsub.topic`, the emulator matches the trigger's `eventFilters` (including the `type`) and invokes the destination function (see [Cloud Functions](#cloud-functions-metadata-crud-real-execution-v2-deploy-end-to-end)). The other destinations (Cloud Run, GKE, Workflows, HTTP endpoint) remain stored metadata references only — no Cloud Run deployment or Workflow execution is created; only a `destination.cloudFunction` trigger provisions a backing Pub/Sub subscription, exposed as the output-only `transport.pubsub.subscription` and used as the dead-letter surface (see [Cloud Functions](#cloud-functions-metadata-crud-real-execution-v2-deploy-end-to-end)). Source/destination references are validated structurally — a `transport.pubsub.topic` must name an existing Pub/Sub topic, a `destination.workflow` an existing Workflow, and a `destination.cloudFunction` an existing function (`NotFound` otherwise) — and a channel's `pubsubTopic`/`activationToken` are synthesized placeholders.

Trigger and Channel writes honor `updateMask` (masked paths take the incoming value, unmasked paths retain the stored value, merged inside the store's atomic mutate closure), support `validateOnly=true` (validate then skip the write), and enforce `etag` optimistic concurrency control: a deterministic content-checksum etag is recomputed on every create/update, and a stale etag on patch/delete is rejected with **409 `ABORTED`**. A trigger/channel `uid` is a UUID4, and a newly created unconnected Channel reports `state: PENDING` (it only becomes `ACTIVE` once a provider connects). Trigger/Channel IAM (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) is implemented via the shared policy store (etag OCC included), not `Unimplemented`.

Known debt for Eventarc: list `filter`/`orderBy` are **not** honored (`ListTriggers`/`ListChannels` ignore them and return the full page); output-only fields (`name`/`uid`/`etag`/times/`state`/`activationToken`/`pubsubTopic`) are overlaid on read but a client-supplied output-only field in a create/patch body is not rejected, it is echoed into the stored config; and unknown Eventarc v1 resources/custom methods (e.g. `ChannelConnection`, `GoogleApiSource`, `MessageBus`, `Pipeline`, `Enrollment`) are not routed, so they fall through to the adapter's generic 404 envelope rather than an Eventarc-specific `NOT_FOUND`.

### Cloud Functions: metadata CRUD, real execution, v2 deploy end-to-end

Cloud Functions v1 and v2 are implemented as metadata CRUD over the shared function store, plus `call` invocation through the Lambda executor (mock echo by default, Docker/K8s under `JAISCLOUD_EXECUTOR_MODE`). A function whose source is referenced by a **GCS object** — v1 `sourceArchiveUrl` (`gs://bucket/object`) or v2 `buildConfig.source.storageSource` — has that archive fetched through the emulated Cloud Storage (decrypted via the storage provider), persisted in the blob store with a sha256 revision hash, and mounted into the container in Docker/K8s mode, so a deployed function really executes. In K8s mode the archive is fetched by a `code-fetch` init container over the admin API (`{JAISCLOUD_LAMBDA_CODE_URL}/lambda/code/{project}/{location}.{id}/$LATEST`); the mount is enabled only when `JAISCLOUD_LAMBDA_CODE_URL` (or a cluster-reachable `JAISCLOUD_GCS_EMULATOR_ENDPOINT`, with `/_jaiscloud` appended) is configured, otherwise the init container is skipped. v2 `generateUploadUrl` provisions a `gcf-v2-sources-{project}-{location}` GCS bucket and returns a `storageSource` plus an **emulator-reachable** `uploadUrl` (so a raw PUT lands in the emulated GCS; real GCP returns a `storage.googleapis.com` URL), which makes `gcloud functions deploy --gen2` run end-to-end (upload → create → poll). Each deploy bumps a persisted revision counter, so a deployed function renders `serviceConfig.revision` — the backing Cloud Run service revision (`…/services/{id}/revisions/{id}-{NNNNN}-{sha8}`) — and `allTrafficOnLatestRevision`; there is no real container build (the archive is only checked non-empty). A function is always born `state: ACTIVE` with a synthesized HTTPS trigger URL (unless an `eventTrigger` is supplied). That URL is served: a request addressed to the trigger host (`{location}-{project}.cloudfunctions.net/{id}`, detected by Host, any HTTP method; the rest of the path is ignored) invokes the function with the raw request body as its payload and returns the result, a `500` for an executor error or function timeout, and `404` for an unknown or event-only function. Function-level CORS is not modelled. `Create`, `Update` (PATCH), and `Delete` return a **done `google.longrunning.Operation`** — `done: true`, typed `metadata`/`response` `Any`s — and the operation is persisted (memory + Postgres + snapshot), so `operations.get`/`list` and REST `:wait` return it after a restart.

`UpdateFunction` honors the `updateMask` query parameter (accepted spellings include lowerCamel and snake_case, with an optional `function.` prefix): only masked fields are overlaid on the stored function (unmasked fields are retained), an empty mask overlays every mutable field present in the body, and the merge runs inside the store's atomic read-modify-write (`UpdateFunctionAtomic`) so concurrent disjoint-field PATCHes cannot lose updates. An unrecognized mask path fails loud with **501 `UNIMPLEMENTED`** rather than being silently ignored. `locations.list`/`locations.get` return a synthesized region set and honor `pageSize`/`pageToken`; the bare `/v1/projects/{p}/locations[/{l}]` path is shared with Memorystore and is served by the Memorystore handler (which returns the same `google.cloud.location.Location` records), so the route does not collide.

Input validation is deliberately shallow: a create requires a non-empty `runtime` and a function id (from `?functionId=` or the body `name`), and `name`/`path` resource names must match `locations/{l}/functions/{id}` (full `projects/{p}/…` form also accepted) — otherwise **400 `INVALID_ARGUMENT`**. Source archive contents/URLs (other than a non-empty check) and runtime/entry-point compatibility are **not** validated. Memory and the v2 `serviceConfig` instance/concurrency settings **are** range-validated against the real API — v1 `availableMemoryMb` must be one of 128/256/512/1024/2048/4096/8192, v2 `availableMemory` 128M–32Gi, `minInstanceCount`/`maxInstanceCount` 0–1000 with `min ≤ max`, `maxInstanceRequestConcurrency` 1–1000, and `availableCpu` one of 1/2/4/6/8 vCPU or a sub-vCPU value 0.08–0.99 in increments of 0.01 (a milli suffix is normalized; below 1 vCPU the request concurrency must be 1) — and surfaced on the v2 `serviceConfig`; a configured `maxInstanceCount` (× `maxInstanceRequestConcurrency`) and a project-wide account cap (the shared executor's `JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT`, default 1000) are enforced on every invocation entry point by an admission gate that returns **429 `RESOURCE_EXHAUSTED`** once the capacity is exceeded, but no separate project quota/account-settings API is modelled (FD11): Cloud Functions declares no account/quota method, so Lambda's `GetAccountSettings` has no GCP analogue to model. Function IAM (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) is implemented via the shared policy store. `call` is synchronous and returns the executor's echo/error inline (an executor failure is surfaced as `error` with HTTP 200, matching v1's `CallFunctionResponse`); the synthesized HTTPS trigger URL (`{location}-{project}.cloudfunctions.net/{id}`) is served (Host-scoped, raw body → `CallFunction`, HTTP 500 on an executor error/timeout, 404 for an unknown or event-only function); the v2 surface, `ListRuntimes`, per-deploy revisions, `allTrafficOnLatestRevision`, and event-trigger delivery (retry + dead-letter records) are implemented. The v2 1st→2nd gen upgrade/traffic control plane — `setupFunctionUpgradeConfig` (config overrides), `redirectFunctionUpgradeTraffic` / `rollbackFunctionUpgradeTraffic` (traffic target), `commitFunctionUpgrade` / `commitFunctionUpgradeAsGen2` (finalize), `abortFunctionUpgrade`, and `detachFunction` — is served over REST with a persisted `upgradeInfo` state machine; traffic is moved only by the upgrade redirect/rollback, since real Cloud Functions exposes no general traffic-percentage API. Those seven methods are served **REST-only in practice**. Google's RPC reference documents them as `google.cloud.functions.v2.FunctionService` RPCs, but the public `googleapis` proto mirror (unchanged since April 2025) and every generated client (Go, Java, Python, Node) omit them, so the emulator's generated-proto gRPC server cannot register them; the Discovery-generated REST client `google.golang.org/api/cloudfunctions/v2` exposes all seven.

Event triggers are live. A function deployed with an `eventTrigger` (v1 `{eventType, resource, service, failurePolicy.retry}` or v2 `{eventType, pubsubTopic, retryPolicy}`) is invoked when a matching event is produced: a **Pub/Sub publish** (v1 `google.pubsub.topic.publish` / legacy `providers/cloud.pubsub/eventTypes/topic.publish`), a **GCS object finalize/delete** (`google.storage.object.finalize`/`.delete` or the v1 `object.change` catch-all), or an **Eventarc trigger** whose destination is the function. Both the legacy v1 and v2/Eventarc event-type spellings, and the short and fully-qualified resource forms, are matched. Delivery runs on a bounded background worker: the function is invoked through the same executor as `call`, a failure is retried (v1 `failurePolicy.retry`; v2 `RETRY_POLICY_RETRY`) with bounded exponential backoff, and every attempt is recorded in a persisted delivery record (`delivered` / `failed` / `dead_letter`) that survives a restart under `--dsn`. A Pub/Sub event trigger materializes a **backing Eventarc trigger** — the v2 render exposes it as the output-only `eventTrigger.trigger`, and `projects/{p}/locations/{l}/triggers/{id}` shows its `transport.pubsub.subscription` — whose platform-provisioned Pub/Sub subscription (`eventarc-{location}-{trigger}`) is the dead-letter surface: set `deadLetterPolicy.deadLetterTopic`/`maxDeliveryAttempts` on it with `subscriptions.patch` (REST `PATCH /v1/projects/{p}/subscriptions/{s}`, gRPC `UpdateSubscription`), and an exhausted delivery is republished to the dead-letter topic with the `CloudPubSubDeadLetterSource{Subscription,SubscriptionProject,DeliveryCount}` attributes, recorded as `dead_letter` with its `deadLetterTopic`. The subscription's `maxDeliveryAttempts` (5–100) bounds the retries in place of the emulator's default cap. A delivery the invocation admission gate refuses (**429 `RESOURCE_EXHAUSTED`**, see above) is treated as back-pressure, not a failure: the function was never invoked, so the engine re-polls with bounded backoff **without consuming an attempt** and never dead-letters it. A throttle that outlasts that bounded in-process re-poll leaves the record `pending` (attempt budget untouched) and is not resumed on its own — a deliberate emulator divergence from Pub/Sub, which would keep counting attempts toward the subscription's `maxDeliveryAttempts`. Delivery is unauthenticated (as everywhere), and the retry window is bounded rather than real GCP's seven days.

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
