# JaisCloud for GCP

> **Early Development Notice**
> `jaiscloud-gcp` is under active development on the `gcp` branch and has not yet been released as a packaged binary (see the [main README](README.md), which currently lists GCP as "In pipeline"). Build it from source. Some operations may have incomplete implementations, behavioural differences from real GCP, or known bugs — see [Known Limitations](#known-limitations) below, and please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) for anything not already listed there.

**JaisCloud — a free GCP emulator for developers and CI.** It implements real GCP wire protocols (both the REST/JSON APIs and the native gRPC APIs Firestore, Pub/Sub, Datastore, KMS, Secret Manager, Cloud Logging, and Cloud Monitoring actually use) — no SDK shims, no proxy rewrites. Point an official Google client library at it and it works.

**One binary per cloud.** `jaiscloud-gcp` is fully self-contained — no `--cloud` flag, no shared runtime with `jaiscloud-aws`. See the [main README](README.md) for the project-wide picture (AWS is the reference implementation; this document covers the GCP binary specifically).

---

## Supported GCP Services

| Service | Transport | Notes |
|---|---|---|
| Cloud Storage (GCS) | REST + gRPC v2 | Buckets, objects, resumable/multipart uploads, CMEK, CSEK |
| Cloud Pub/Sub | REST + gRPC | Topics, subscriptions, push/pull delivery, ordering keys, DLQ |
| Secret Manager | REST + gRPC | Secrets, versions, rotation, CMEK envelope encryption |
| Cloud KMS | REST + gRPC | Key rings, crypto keys/versions, symmetric + asymmetric, rotation |
| Cloud IAM | REST | Service accounts, service account keys |
| Cloud Firestore (Native mode) | REST + gRPC | Documents, transactions, queries, composite indexes, `Listen` streaming |
| Cloud Datastore mode | gRPC | Entities, queries, ID allocation — see [Known Limitations](#known-limitations) for transaction support |
| Cloud Functions (v1) | REST | Deploy, invoke (mock echo by default, Docker/K8s execution modes) |
| Cloud Workflows | REST | Workflow definitions + executions, real YAML expression engine |
| Cloud Dataproc | REST | Clusters + jobs, **real Spark execution** in Docker/K8s executor mode (same model as AWS EMR) |
| Managed Kafka | REST | Metadata-only clusters/topics — see [Known Limitations](#known-limitations) |
| BigQuery | REST | Metadata + stored rows — no SQL engine, see [Known Limitations](#known-limitations) |
| Cloud Monitoring | gRPC | Metrics + alert policies — see [Known Limitations](#known-limitations) |
| Cloud Logging | gRPC | Log entries, filtering, log-based routing |

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

Point any official Google client library at the emulator. Most services use plain endpoint overrides; the gRPC-native services (Firestore, Pub/Sub, Datastore, Monitoring) use the real Google client libraries' own emulator-host environment variables where those exist.

```bash
export GCP_EMULATOR_ENDPOINT=http://localhost:8080/    # REST services (GCS, BigQuery, Dataproc, Workflows, ...)
export FIRESTORE_EMULATOR_HOST=localhost:8081           # Firestore (gRPC)
export STORAGE_EMULATOR_HOST=http://localhost:8080      # GCS REST client
```

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
| `--port` | `JAISCLOUD_PORT` | `8080` | REST listen port |
| `--grpc-port` | — | `8081` | gRPC (h2c, plaintext) listen port |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN; when set all state is stored in PostgreSQL |
| `--ephemeral` | `JAISCLOUD_EPHEMERAL` | `false` | Disable all persistence — state is lost on exit (CI / unit tests) |
| `--data-dir` | `JAISCLOUD_DATA_DIR` | `~/.jaiscloud/jaiscloud-gcp` | Directory for state.json saves and named snapshots |
| — | `JAISCLOUD_GCP_PROJECT_ID` | — | Default GCP project when a request carries none |
| — | `JAISCLOUD_GCP_SERVICE_ACCOUNT` | — | Default service-account identity returned by the metadata emulator |
| `--gcp-metadata` | `JAISCLOUD_GCP_METADATA_ENABLED` | `false` | Enable the GCP metadata-server emulator |
| `--kms-master-key` | `JAISCLOUD_KMS_MASTER_KEY` | — | 32-byte hex KEK wrapping the KMS DEK at rest |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | — | `false` | Expose Prometheus metrics at `/metrics` |

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

### Firestore: optimistic-concurrency conflict detection can miss a race under a frozen clock

Firestore's `documents.patch` and transform-carrying `commit`/`batchWrite` writes are protected against concurrent lost updates via an optimistic-concurrency check: the document's `UpdateTime` at read time is captured and re-validated, atomically with applying the write, against the live document's current `UpdateTime` — a conflicting concurrent write causes the loser to receive `409 ABORTED` instead of silently overwriting the winner. This is correct and race-free under the real (or offset) clock, where `UpdateTime` values are effectively unique to nanosecond resolution.

**Edge case:** if the emulator's clock is *frozen* (`POST /_jaiscloud/clock` with `{"mode":"fixed",...}`, used for deterministic tests), two writes to the same document landing while the clock is frozen can be stamped with the *identical* `UpdateTime`. In that specific case, the conflict-detection check can fail to notice that a write happened in between — the version token didn't move, even though the document did. This is a testing-mode-only concern (frozen clocks aren't a realistic production condition — the real GCP backend's own `update_time` is generated by Spanner/TrueTime, not an application clock, so it has no equivalent failure mode). If your test suite freezes the clock *and* exercises concurrent writes to the same document in the same test, be aware the emulator's lost-update protection is not guaranteed in that combination.

We evaluated an alternative (a checksum of document content as the version token) and rejected it: a content hash reintroduces the classic ABA problem — a document that changes and then reverts to its original bytes produces the same hash as if nothing had happened, which would make conflict detection *less* reliable, not more, since it would miss real intervening writes whenever content happened to return to a prior state. We also considered a purely internal monotonic version counter decoupled from `UpdateTime`, but real GCP's `Precondition` proto only supports `exists`/`update_time` — no opaque version token — and client libraries read and pass forward the real `updateTime` value for their own `currentDocument.updateTime` preconditions. Introducing a second, different notion of "version" internally, even without changing the wire-facing `UpdateTime` field itself, was judged higher-risk than documenting the known edge case for the (real-world-rare) frozen-clock scenario.

### Datastore: no transaction support

The Datastore gRPC service is intentionally non-transactional. `BeginTransaction` returns an opaque id (so SDK init paths that poll the transaction surface don't error) and `Rollback` is a no-op, but a `Commit` carrying a transaction selector is rejected with `Unimplemented` rather than being silently applied as a non-transactional write. Non-transactional `Commit`, `Lookup`, and `RunQuery` are fully supported.

### BigQuery: metadata only, no SQL engine

`jobs.query` never evaluates SQL — it stores the query and reports `jobComplete: true` with empty results. `tabledata.insertAll` streams rows without validating them against the table schema and does not honor `insertId`-based deduplication, `skipInvalidRows`, `ignoreUnknownValues`, or `templateSuffix`. `tabledata.list` ignores `startIndex`. `projects.getServiceAccount` returns a synthetic `bq-{project}@gcp-sa-bigquery.iam.gserviceaccount.com` rather than a real service account.

### Managed Kafka: metadata only, no real broker

A cluster is a logical record only — the emulator never stands up a real Kafka broker. Consumer groups are not tracked: `ListConsumerGroups` always returns an empty list, and get/update/delete operations on a consumer group return `Unimplemented`.

### Cloud Monitoring: alert policies are stored, never evaluated

Alert policies can be created, listed, updated, and deleted, but their conditions are never evaluated and no notifications are ever triggered. `GetMonitoredResourceDescriptor` and `CreateServiceTimeSeries` are `Unimplemented`. `ListMetricDescriptors`/`ListMonitoredResourceDescriptors` ignore the `filter` field. `DISTRIBUTION`-typed point values are rejected. `ListTimeSeries` supports only the `metric.type` / `resource.type` equality filter subset — the full Monitoring Query Language is not implemented.

### Dataproc: `Reset` does not drain in-flight Spark job goroutines

`POST /_jaiscloud/reset` wipes the Dataproc store but does not cancel or wait for jobs currently executing (Docker/K8s executor mode). This matches AWS EMR's own `Reset` behaviour in this codebase, which is a no-op for the same reason — not a GCP-specific gap. If you reset while a job is mid-execution and then resubmit a job with the *same* `(project, region, jobId)` before the stale run finishes, the stale run's completion could overwrite the new job's state. Avoid reusing job IDs across a reset boundary while a prior run may still be in flight.

---

## Contributing

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) and [CLAUDE.md](CLAUDE.md) for build setup, architecture conventions, and the AWS-vs-GCP isolation model. Please open an issue before starting large changes.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
