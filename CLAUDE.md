# JaisCloud — Developer Reference

## Project

JaisCloud is a local cloud emulator written in Go. Each cloud ships as its own self-contained binary (`jaiscloud-aws`, `jaiscloud-azure`, `jaiscloud-gcp`). The binary IS the cloud — there is no runtime `--cloud` flag and no shared provider logic between clouds. AWS wire protocols (Query/XML, JSON/Target, REST) are implemented so any AWS SDK can point at `jaiscloud-aws` without modification.

**Design decision: per-cloud binaries, not a cloud-agnostic core.**
Every cloud has its own adapter, its own provider implementations, and its own entry point. Only infrastructure utilities are shared (`gateway`, `store`, `model`, `admin`, `blobfs`, `clock`, `events`, `executor`, `config`). When Azure or GCP services are implemented, they will live entirely in `internal/azure/` or `internal/gcp/` — no provider code will be shared with AWS.

**Implemented AWS services:** SQS, SNS, IAM/STS, DynamoDB (+ Streams), S3, Lambda, KMS, SecretsManager, SSM, API Gateway, CloudFormation, EMR (on EC2), EMR on EKS, EventBridge, CloudWatch, EKS, EC2, Route53, RDS, ElastiCache, ECS, Glue.

---

## Module

```
module jaiscloud   # go.mod
go 1.26.3
```

---

## Directory layout

```
cmd/
  jaiscloud-aws/main.go    # AWS binary — full provider wiring, Cobra CLI
  jaiscloud-azure/main.go  # Azure binary stub (501 for all ops)
  jaiscloud-gcp/main.go    # GCP binary stub (501 for all ops)
docs/                      # Architecture, LLD, design documents
internal/
  # ── SHARED INFRASTRUCTURE (cloud-neutral utilities) ──────────────────────
  adapter/
    adapter.go             # CloudAdapter + Codec interfaces only — no implementations
  provider/
    provider.go            # HandlerFunc type, OK() helper
    registry.go            # Registry — Dispatch (exact match → error)
  model/                   # NormalizedRequest, ProviderResponse, ProviderError, Cloud enum
  gateway/                 # HTTP server (Chi), middleware, request dispatch
    server.go              # Server — holds single CloudAdapter; handleCloudRequest
    middleware/            # Recovery, RequestID, Logging, Metrics
  admin/                   # /_jaiscloud/* endpoints; Resetter, Snapshotter interfaces
  blobfs/                  # BlobStore (Memory/LocalFS); BlobFetcher (S3BlobFetcher)
  clock/                   # Clock interface: RealClock, FixedClock, OffsetClock
  config/                  # Config struct; Viper loading; env prefix JAISCLOUD_
  events/                  # In-process EventBus (subscribe/publish)
  executor/                # Container executor abstraction
    lambda/                # Lambda executor (mock / docker / k8s)
  k8shelpers/              # Generic K8s helpers (BuildPodSpec, IdentityMutator, OwnershipPatcher)
  k8stypes/                # K8s type defs
  sparkhelpers/            # Spark-specific K8s helpers
  platform/                # PlatformConfig — TLS init containers, env fragments, volume mounts
  reqctx/                  # Request context helpers
  resourcemgr/             # Deletion guards: CheckParent, AcquireDelete, DeleteGuardRule
  certstore/               # TLS cert storage
  snapshottypes/           # Snapshotter/Resetter/PostRestoreHook interfaces (cycle-breaking)
  persistence/
    version/               # CodeSnapshotVersion, CodeDBSchemaVersion, Envelope struct, FingerprintKEK
    snapshot/              # SnapshotLoop — periodic state.json persistence
    platform/              # flock_darwin.go / flock_linux.go — data-dir OS lock
  store/                   # ResourceStore interface + memory/postgres implementations
    migrations/            # SQL migration files (001–015)
    object/                # Generic ObjectStore
    stream/                # MemoryStreamStore (DynamoDB Streams)
  workers/                 # Worker interface + Registry — centralised lifecycle for background goroutines

  # ── AWS-SPECIFIC (internal/aws/) ─────────────────────────────────────────
  aws/
    adapter/               # AWSAdapter, router.go, services.go
      services/            # Per-service Codec implementations (27 files)
    provider/              # All AWS business logic
      apigw/               # APIGatewayProvider
      cache/               # ElastiCache (metadata only)
      catalog/             # Glue Data Catalog provider
      cloudwatch/          # CloudWatchProvider — metrics ring, alarms
        logs/              # CloudWatch Logs
      compute/             # EC2 (metadata only)
      container/           # ECS (metadata only)
      dns/                 # Route53 (metadata only)
      eks/                 # EKS (metadata only)
      emr/                 # EMRProvider — RunJobFlow, steps, bootstrap, Spark K8s/Docker
      emroneks/            # EMRContainersProvider — virtual clusters, job runs
      events/              # EventBridgeProvider
      function/            # FunctionProvider — Lambda
      iam/                 # IAMProvider + STS
      lambda/esm/          # Lambda event source mappings
      notification/        # SNSProvider
      object/              # ObjectProvider — S3
      queue/               # QueueProvider — SQS (all 17 operations)
      rds/                 # RDS (metadata only)
      sparkaws/            # AWS emulator wiring injected into Spark driver pods
      stack/               # CloudFormation — CreateStack, intrinsics, topsort, dispatch
      table/               # TableProvider — DynamoDB
    key/                   # KeyProvider — KMS
    secret/                # SecretProvider — SecretsManager
    parameter/             # ParameterProvider — SSM
    arn/                   # AWS ARN formatters (moved from internal/config)
    store/
      dynamodb/            # DynamoDBItemStore (memory + postgres)
      object/              # S3ObjectMetaStore (memory + postgres)
      s3/                  # S3 blob metadata helpers
      sqs/                 # SQSMessageStore (memory + postgres)
      stream/              # MemoryStreamStore (DynamoDB Streams)
      bundle/              # Generic per-scope store wrappers (LocalBundle, CrossRegion, CrossAccount)

  # ── AZURE-SPECIFIC (stub, returns 501) ───────────────────────────────────
  azure/
    adapter/               # AzureAdapter stub

  # ── GCP-SPECIFIC (stub, returns 501) ─────────────────────────────────────
  gcp/
    adapter/               # GCPAdapter stub

tests/
  integration/          # End-to-end tests using aws-sdk-go-v2 (SQS, IAM, SNS, DynamoDB, S3, Lambda)
  persistent_mode/aws/  # Persistent-mode e2e tests (build-tagged): lambda, cfn, kms, emr, emrcontainers,
                        # eventbridge, dpc, iceberg
```

---

## Architecture

### Per-cloud binary model

Each binary is self-contained:

```
jaiscloud-aws    = internal/aws/adapter + internal/aws/provider/* + shared infra
jaiscloud-azure  = internal/azure/adapter (stub) + shared infra
jaiscloud-gcp    = internal/gcp/adapter (stub) + shared infra
```

`internal/aws/` may import `internal/*` (shared). `internal/*` (shared) must **never** import `internal/aws/` — this is enforced by `go build`.

When a new cloud is implemented (e.g. Azure), ALL of its logic — adapter, codecs, providers, resource ID formatters — goes into `internal/azure/`. No provider code is promoted to shared.

### Request flow

```
HTTP request
  → gateway.Server.handleCloudRequest
      → cloudAdapter.DetectAndDecode     (adapter hardcoded at binary startup)
          → <ServiceCodec>.Decode
      → inject: nr.Clock, nr.Region, nr.AccountID, nr.Cloud, nr.ResourceID
      → middleware.WithRequestLabels(ctx, cloud, service, action)
      → Registry.Dispatch("ProviderPrefix.Action", nr)
      → <ServiceCodec>.Encode
  → HTTP response
```

Key design rules:
- **No layer imports its caller.** `model` package breaks the cycle between `gateway` and `adapter`.
- **Single cloud per binary.** `cfg.Cloud` is set unconditionally in `main.go`; no `--cloud` flag, no per-request cloud detection.
- **No shared providers.** Each cloud owns its adapters and providers entirely. Storage backends (`internal/aws/store/*`) are AWS-specific data stores — they don't implement wire protocols.
- **Executors are wired at startup.** `JAISCLOUD_EXECUTOR_MODE` controls the container orchestrator for Spark and Lambda.

### CloudAdapter interface

```go
type CloudAdapter interface {
    Cloud() model.Cloud
    DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, Codec, error)
    ServiceToProvider(service string) string
}
```

`ServiceToProvider` maps wire service name (e.g. `"sqs"`) to provider registry prefix (e.g. `"Queue"`). AWS delegates to `serviceProviderMap` from `services.go`; Azure/GCP stubs return the service name unchanged.

### Service → Provider mapping (AWS)

Defined once in `internal/aws/adapter/services.go` — no switch statement anywhere.

| Wire service | Provider prefix | Codec |
|---|---|---|
| `sqs` | `Queue` | SQSCodec (JSON + Query) |
| `iam` | `IAM` | IAMCodec (Query/XML) |
| `sts` | `STS` | IAMCodec (Query/XML) |
| `sns` | `Notification` | SNSCodec (Query/XML) |
| `dynamodb` | `Table` | DynamoDBCodec (JSON/Target) |
| `dynamodbstreams` | `Streams` | DynamoDBStreamsCodec (JSON/Target) |
| `s3` | `Object` | S3Codec (REST/XML) |
| `lambda` | `Function` | LambdaCodec (REST/JSON) |
| `glue` | `Glue` | GlueCodec (JSON/Target) |
| `ec2` | `Compute` | EC2Codec (Query/XML) |
| `route53` | `DNS` | Route53Codec (REST/XML) |
| `rds` | `RDS` | RDSCodec (Query/XML) |
| `elasticache` | `ElastiCache` | ElastiCacheCodec (Query/XML) |
| `ecs` | `ECS` | ECSCodec (JSON/Target) |
| `eks` | `EKS` | EKSCodec (REST/JSON) |
| `cloudformation` | `CloudFormation` | CloudFormationCodec (Query/XML) |
| `monitoring` | `CloudWatch` | CloudWatchCodec (form-body + Granite path) |
| `emr` | `EMR` | EMRCodec (JSON/Target) |
| `emr-containers` | `EMRContainers` | EMRContainersCodec (REST/JSON) |
| `events` | `EventBridge` | EventBridgeCodec (JSON/Target) |
| `kms` | `KMS` | KMSCodec (JSON/Target) |
| `secretsmanager` | `SecretsManager` | SecretsManagerCodec (JSON/Target) |
| `ssm` | `SSM` | SSMCodec (JSON/Target) |
| `apigateway` | `APIGateway` | APIGatewayCodec (REST/JSON) |

**Adding a new AWS service:** add one `ServiceDescriptor` entry to `awsServices` in `internal/aws/adapter/services.go`. Detection, SigV4 allow-list, Action routing, and provider mapping all update automatically.

### Service detection order (router.go)

1. `X-Amz-Target` header → `targetPrefixToService` (JSON/Target services)
2. SigV4 `Authorization` scope → `knownSigV4Services`
3. `Action=` query/body param → `actionToService` (Query-protocol services)
4. Granite path `/service/<sigv4name>/operation/<action>` — AWS SDK v2 CloudWatch

---

## Storage model

Storage is determined by two flags — no `--mode` flag exists.

| Invocation | Storage | Survives restart? |
|---|---|---|
| `./jaiscloud-aws start` | Memory + periodic state.json saves | Yes (soft) |
| `./jaiscloud-aws start --dsn "postgres://..."` | PostgreSQL | Yes (durable) |
| `./jaiscloud-aws start --ephemeral` | Memory only, no disk writes | No |
| `--ephemeral` + `--dsn` | Startup error: mutually exclusive | — |

`JAISCLOUD_EXECUTOR_MODE`: `""` / `mock` / `docker` / `k8s` — applies to both Spark (EMR) and Lambda.

| Service | Default (memory+saves) | `--dsn` (PostgreSQL) | `docker` executor | `k8s` executor |
|---|---|---|---|---|
| SQS / SNS / IAM / STS / KMS / SecretsManager / SSM | In-memory + state.json | PostgreSQL | PostgreSQL | PostgreSQL |
| DynamoDB + Streams | In-memory + state.json | PostgreSQL | PostgreSQL | PostgreSQL |
| S3 | In-memory + session blob dir | PostgreSQL + LocalFSBlobStore (`~/.jaiscloud/blobs`) | same | same |
| Lambda | Echo (mock) | Echo (mock) | Docker container per function (warm pool) | K8s Pod + Service per function |
| EMR on EC2 steps | Instant COMPLETED | Instant COMPLETED | Docker container per step | K8s batch/v1 Job per step |
| EMR on EKS job runs | Instant COMPLETED | Instant COMPLETED | Docker container per job | K8s batch/v1 Job per job |
| CloudWatch | In-memory ring + alarms | In-memory ring + PostgreSQL alarms | — | — |
| EC2 / Route53 / EKS / RDS / ElastiCache / ECS | Metadata only | Metadata only | — | — |
| API Gateway | In-memory + state.json | PostgreSQL | — | — |
| CloudFormation | In-memory + real dispatch | PostgreSQL + real dispatch | — | — |
| Glue / EventBridge | In-memory + state.json | PostgreSQL | — | — |

---

## Build & run

```bash
# Always rebuild after code changes
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/

# Build all cloud binaries
make build-all   # → jaiscloud-aws, jaiscloud-azure, jaiscloud-gcp

# Default: state saved to ~/.jaiscloud/jaiscloud-aws/state.json, survives restarts
./jaiscloud-aws start

# Explicit data directory for state.json saves and named snapshots
./jaiscloud-aws start --data-dir /var/lib/jaiscloud

# PostgreSQL backend
./jaiscloud-aws start --dsn "postgres://user:pass@localhost:5433/jaiscloud"

# PostgreSQL + named snapshots on disk
./jaiscloud-aws start --dsn "postgres://..." --data-dir /var/lib/jaiscloud

# Ephemeral: no persistence, clean slate on every start (CI / tests)
./jaiscloud-aws start --ephemeral

# Docker-compose (postgres on 5433, jaiscloud-aws on 4566)
make up-docker
make down-docker

# K8s executors (Spark + Lambda)
JAISCLOUD_EXECUTOR_MODE=k8s ./jaiscloud-aws start --dsn "postgres://..."

# Docker executors
JAISCLOUD_EXECUTOR_MODE=docker ./jaiscloud-aws start --dsn "postgres://..."

# Prometheus metrics (at /metrics)
./jaiscloud-aws start --metrics
```

### Key environment variables

```bash
JAISCLOUD_PORT=4566
JAISCLOUD_EPHEMERAL=false            # true = no persistence (CI/tests); mutually exclusive with JAISCLOUD_DSN
JAISCLOUD_DSN=                       # PostgreSQL DSN; when set all state is stored in PostgreSQL
JAISCLOUD_REGION=us-east-1
JAISCLOUD_ACCOUNT_ID=000000000000
JAISCLOUD_LOG_LEVEL=info
JAISCLOUD_EXECUTOR_MODE=             # "" | mock | docker | k8s
JAISCLOUD_LAMBDA_IMAGE=              # override default Lambda runtime image
JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300
JAISCLOUD_KMS_MASTER_KEY=            # 32-byte hex KEK; if unset DEK stored plaintext
JAISCLOUD_S3_VIRTUAL_HOST_BASES=     # comma-separated host suffixes
JAISCLOUD_IMDS_ENABLED=false
JAISCLOUD_AWS_EMULATOR_ENDPOINT=     # JaisCloud endpoint reachable from Spark pods
JAISCLOUD_SPARK_K8S_CLUSTER_MODE=auto   # auto | always | never
JAISCLOUD_INSTANCE_ID=               # override auto-generated instance UUID

# Snapshot / persistence env vars
JAISCLOUD_DATA_DIR=                  # path for state.json saves and named snapshots (default: ~/.jaiscloud/jaiscloud-aws)
JAISCLOUD_FRESH_START=false          # if true, skip loading state.json on startup
JAISCLOUD_SNAPSHOT_INTERVAL=5m      # how often SnapshotLoop saves state.json (default 5m)
JAISCLOUD_SNAPSHOT_SAVE_TIMEOUT=30s # per-save context deadline (default 30s)
JAISCLOUD_EXPORT_SOFT_LIMIT=        # max bytes in export tarball before warning (default unlimited)
```

Note: `JAISCLOUD_CLOUD` is **not** an env var. The binary determines the cloud.

> **Config loading:** all `viper.BindPFlag(...)` calls in `startCmd` must use the global `viper` package (not `viper.New()`) or flags are silently ignored.

### CLI commands

| Command | Description |
|---|---|
| `start` | Start the emulator |
| `version` | Print version |
| `env` | Print effective config as env vars |
| `doctor` | Check emulator reachability |
| `reset` | Wipe all state via HTTP |
| `export [-o file]` | Export state as a gzip tarball (stdout if no `-o`) |
| `import [-i file] [--dry-run]` | Restore state from a snapshot tarball |
| `rotate-master-key --new-key <hex>` | Re-wrap KMS DEK with new KEK |
| `services` | Print service implementation levels |
| `snapshot create --name <n>` | Create a named on-disk snapshot |
| `snapshot list` | List named snapshots |
| `snapshot revert <name>` | Revert to a named snapshot |
| `snapshot delete <name> --yes` | Delete a named snapshot |
| `snapshot inspect <name>` | Show snapshot metadata |

---

## Tests

```bash
# Unit + store tests
go test -race ./internal/...

# Integration tests — lite mode
./jaiscloud-aws start &
go test -race ./tests/integration/

# Full-mode e2e (docker-compose handles server + postgres)
make test-e2e-lambda-docker      # tag: lambda_e2e
make test-e2e-lambda-k8s         # tag: lambda_e2e
make test-e2e-emr-docker         # tag: spark_e2e
make test-e2e-emrcontainers-k8s  # tag: spark_e2e
make test-e2e-cloudformation     # tag: cfn_fullmode
make test-e2e-kms                # tag: kms_fullmode
make test-e2e-eventbridge        # tag: spark_e2e
make test-e2e-iceberg            # tag: iceberg_e2e
```

Persistent-mode e2e tests live under `tests/persistent_mode/aws/{lambda,cloudformation,kms,emr,emrcontainers,eventbridge,dpc,iceberg}/`.

Integration tests call `POST /_jaiscloud/reset` between each test via `resetState(t)`.

---

## Key conventions

### GCP parity status ledger: run `make gcp-status` before planning

The GCP backlog spans many plan docs; don't decide "what's implemented" from
memory. `make gcp-status` rebuilds `plan_docs/STATUS.md` + `status.json` from
every `plan_docs/**/*.md` table joined with git/GitHub state, and
`make gcp-status-check Q="<service> <keywords>"` assesses a proposed change
(exit 2 = already done, 3 = in flight). See [AGENTS.md](AGENTS.md).

### Resource IDs: use nr.ResourceID, never hardcode ARN formats

Providers must use `nr.ResourceID("type", name)` — never `fmt.Sprintf("arn:aws:...")`.

- `model.NormalizedRequest.ResourceID` — injected by gateway, always non-nil
- `config.awsARNFormatters` — add one entry per new AWS resource type
- Future Azure/GCP binaries will inject their own formatters; this is the only cross-cutting concern

### Time: NEVER call `time.Now()` directly — use `clock.Now()` or `clock.RealNow()`

This is a hard rule. The `"time"` package must not be imported for time-of-day in provider, store, or worker code. Use `internal/clock` exclusively.

There are exactly two functions:

| Function | When to use |
|---|---|
| `clock.Now()` | Business timestamps — anything that appears on the AWS wire or in a resource's metadata. Respects the global clock; tests can freeze or advance it via `POST /_jaiscloud/clock`. Always UTC. |
| `clock.RealNow()` | Infrastructure wall time — container lifecycle, keepalive timers, GC eviction, request latency, TLS cert windows, rate-limiter token refill. Always real wall clock, always UTC. |

The distinction makes intent explicit at the call site: `clock.RealNow()` means "I deliberately need real elapsed time here."

**Required replacements:**

```go
// Before                          After
time.Now()                    →    clock.Now()       // business timestamp
time.Now()                    →    clock.RealNow()   // infrastructure / real elapsed time
time.Now().UTC()              →    clock.Now()
time.Since(x)                 →    clock.Now().Sub(x)
time.Until(x)                 →    x.Sub(clock.Now())
```

`clock.RealNow()` is used in (do not convert these to `clock.Now()`):
- `gateway/middleware/` — request latency measurement
- `gateway/server.go` — TLS cert validity windows
- `executor/lambda/`, `executor/ecs/` — container/pod lifecycle waits and keepalive GC
- `store/postgres.go` retry backoff (`time.After`)
- Infrastructure cleanup tickers: `store/sqs/memory.go`, `store/sqs/postgres.go`
- `store/dynamodb/throttle.go` — token-bucket rate limiter; frozen clock → zero elapsed → tokens never refill
- `config/config.go` — `cfg.TimeStart` server uptime tracking
- `certstore/certstore.go` — TLS cert expiry check
- `provider/stepfunctions/engine/retry.go`, `task_state.go` — real retry/timeout delays
- `provider/compute/compute.go` — EC2 state-transition delays (`time.AfterFunc`)
- `provider/queue/waiters.go` — SQS long-poll real wait duration
- `provider/kinesis/mock_server.go` — Kinesis mock container readiness polling
- `provider/ecr/registry_proxy.go` — ECR registry proxy container readiness polling
- `k8shelpers/` — Spark job execution wall time tracking
- `internal/clock/clock.go` — `OffsetClock` and `RealClock` implementations (the clock package itself)

### DynamoDB x-amz-crc32

Every DynamoDB response **must** include `x-amz-crc32: <crc32_of_body>`. AWS SDK v2 validates it; missing → SDK fails to drain body. Set in `DynamoDBCodec.Encode`.

### S3 delete ordering

`DeleteObject`/`DeleteObjects` delete **metadata before blob**. `GetObject` rechecks metadata when blob is missing: if metadata gone → 404 (concurrent delete); metadata present + blob absent → 500 (corruption).

### HTTP Content-Length

`gateway.writeResponse` sets `Content-Length` explicitly before `WriteHeader`. Without it, Go uses chunked encoding, causing AWS SDK body-close warnings.

### SQS FIFO deduplication

`MemoryMessageStore.Send` returns `(dedupMessageID string, err error)`. Non-empty `dedupMessageID` means duplicate — return the original `MessageId`.

### SNS fan-out

Each SQS delivery gets a **new unique `MessageID`**. The SNS notification ID is in the envelope body only.

### DynamoDB pagination

Both memory and postgres stores sort items by `itemPKHash` before applying cursor/limit. `LastEvaluatedKey` is set when `len(page) == limit` (page full), not when pre-filter count equals limit.

### EMR goroutine lifecycle

EMR/EMRContainers providers capture `handlerCtx{cloud, region, accountID}` at handler entry so background goroutines can publish events without holding `NormalizedRequest`. All step/job goroutines use `p.wg.Add(1)` + `defer p.wg.Done()`; `Shutdown()` calls `p.cancel()` then `p.wg.Wait()`.

### Spark conf precedence

`BuildClientModeArgs` emits: fixed JaisCloud confs → `ExtraSparkConfs` → `SparkSubmitArgs`. Spark last-value-wins, so user `SparkSubmitArgs` always override JaisCloud defaults.

### Admin endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | Liveness check |
| `/_jaiscloud/doctor` | GET | Emulator diagnostics (version, mode, instance ID, uptime) |
| `/_jaiscloud/reset` | POST | Wipe all state |
| `/_jaiscloud/export` | GET | Export state as gzip tarball (`application/x-tar`) |
| `/_jaiscloud/import` | POST | Restore from snapshot tarball or legacy JSON; `?dry_run=true` validates only |
| `/_jaiscloud/snapshot` | POST | Create named snapshot (`{"name":"<n>","description":"<d>"}`) |
| `/_jaiscloud/snapshots` | GET | List named snapshots |
| `/_jaiscloud/snapshot/{name}` | GET | Inspect snapshot metadata |
| `/_jaiscloud/snapshot/{name}/revert` | POST | Revert to named snapshot (`?reset_first=true` to clear state first) |
| `/_jaiscloud/snapshot/{name}` | DELETE | Delete named snapshot (`?yes=true` required) |
| `/_jaiscloud/clock` | GET | Return current clock state `{"mode","time"}` |
| `/_jaiscloud/clock` | POST | Set clock mode: `{"mode":"fixed","time":"..."}` / `{"mode":"offset","time":"..."}` / `{"mode":"real"}` |
| `/_jaiscloud/ttl-sweep` | POST | Synchronous DynamoDB TTL sweep (for deterministic tests) |
| `/_jaiscloud/eb-tick` | POST | Synchronous EventBridge scheduler evaluation (for deterministic tests) |
| `/metrics` | GET | Prometheus (requires `--metrics`) |
