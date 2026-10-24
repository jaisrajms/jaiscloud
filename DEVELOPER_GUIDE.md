# Developer Guide

JaisCloud ships one self-contained binary per cloud provider. Each binary speaks the exact wire protocol for that cloud — no shared adapter, no runtime flag to switch clouds. The AWS binary (`jaiscloud-aws`) is the reference implementation; Azure and GCP stubs follow the same structure.

This guide covers the AWS binary. It walks you from a fresh clone to a running server, then through progressively more realistic setups: in-memory for unit tests, PostgreSQL-backed for persistence, and real Spark jobs submitted to a Kubernetes cluster.

---

## Which setup do I need?

Read this table first. Pick the row that matches your goal, then jump to that section.

You can run JaisCloud three ways — pick the one that fits your situation:

| How | When to use |
|-----|-------------|
| **Pre-built binary** (Homebrew / Scoop / GitHub Releases) | You just want to run JaisCloud locally. No Go, no Docker required. |
| **Docker / Kubernetes** | CI pipelines, or full-mode with Postgres persistence. |
| **Build from source** | You are developing JaisCloud itself or need to iterate on code changes. |

### Installing the pre-built binary (no Go required)

**macOS (Homebrew)**
```bash
brew tap jaisrajms/homebrew-tap
brew install --cask jaiscloud-aws
jaiscloud-aws start
```

**Windows (Scoop)**
```powershell
scoop bucket add jaiscloud https://github.com/jaisrajms/scoop-jaiscloud
scoop install jaiscloud-aws
jaiscloud-aws start
```

**All platforms — manual download**

Download the binary for your platform from the [GitHub Releases page](https://github.com/jaisrajms/jaiscloud/releases), extract it, and put it on your `PATH`.

```bash
# macOS arm64 (Apple Silicon)
curl -LO https://github.com/jaisrajms/jaiscloud/releases/latest/download/jaiscloud-aws_darwin_arm64.tar.gz
tar -xzf jaiscloud-aws_darwin_arm64.tar.gz
sudo mv jaiscloud-aws /usr/local/bin/

# Linux amd64
curl -LO https://github.com/jaisrajms/jaiscloud/releases/latest/download/jaiscloud-aws_linux_amd64.tar.gz
tar -xzf jaiscloud-aws_linux_amd64.tar.gz
sudo mv jaiscloud-aws /usr/local/bin/

# Linux amd64 — Debian/Ubuntu package
curl -LO https://github.com/jaisrajms/jaiscloud/releases/latest/download/jaiscloud-aws_linux_amd64.deb
sudo dpkg -i jaiscloud-aws_linux_amd64.deb
```

Once installed, start the emulator (state is saved to `~/.jaiscloud/jaiscloud-aws/state.json` by default):

```bash
jaiscloud-aws start
curl http://localhost:4566/_jaiscloud/health   # {"status":"ok"}
```

### Running via Docker (no Go required)

```bash
docker pull jaisraj/jaiscloud-aws:latest
docker run -p 4566:4566 jaisraj/jaiscloud-aws:latest
curl http://localhost:4566/_jaiscloud/health   # {"status":"ok"}
```

### Building from source (requires Go 1.26+)

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
./jaiscloud-aws start
```

Use this path if you are developing JaisCloud itself or need to iterate quickly on code changes.

---

### Setup matrix

| Goal | Storage | Pre-built binary | Docker / K8s | From source |
|---|---|---|---|---|
| Run unit tests / CI pipelines | `--ephemeral` (no disk writes) | ✅ | ✅ | ✅ |
| Build and test AWS integrations locally | Default (state.json) or `--dsn` | ✅ | ✅ | ✅ |
| State survives server restarts | Default (state.json) or `--dsn` | ✅ | ✅ | ✅ |
| Run EMR/Spark API calls that return mock results instantly | Any storage + mock executor | ✅ | ✅ | ✅ |
| Actually run a Spark job end-to-end | `--dsn` + K8s executor | — | ✅ | ✅ |

---

## Contents

- [Which setup do I need?](#which-setup-do-i-need)
  - [Installing the pre-built binary](#installing-the-pre-built-binary-no-go-required)
  - [Running via Docker](#running-via-docker-no-go-required)
  - [Building from source](#building-from-source-requires-go-126)
- [Service Reference](#service-reference)
- [Prerequisites](#prerequisites)
- [Storage Model](#storage-model)
- [Setup 1 — Default (memory + periodic saves)](#setup-1--default-memory--periodic-saves)
- [Setup 2 — PostgreSQL backend](#setup-2--postgresql-backend)
- [Setup 3 — JaisCloud on Kubernetes](#setup-3--jaiscloud-on-kubernetes)
- [EMR Spark — Mock Mode (instant results)](#emr-spark--mock-mode-instant-results)
- [EMR Spark — Kubernetes Executor (real Spark jobs)](#emr-spark--kubernetes-executor-real-spark-jobs)
- [Multi-Account Support](#multi-account-support)
  - [How account identity is derived](#how-account-identity-is-derived)
  - [LSIA key encoding](#lsia-key-encoding)
  - [Using multiple accounts in tests](#using-multiple-accounts-in-tests)
  - [Cross-account dispatch](#cross-account-dispatch)
  - [KMS cross-account protection](#kms-cross-account-protection)
  - [Scoped state reset](#scoped-state-reset)
  - [Snapshot export and import](#snapshot-export-and-import)
  - [Spark driver pod credentials](#spark-driver-pod-credentials)
- [Running Tests](#running-tests)
  - [Unit tests](#unit-tests-no-server-needed)
  - [Integration tests](#integration-tests)
  - [Multi-account integration tests](#multi-account-integration-tests)
  - [Postgres backend integration tests](#postgres-backend-integration-tests)
  - [Spark e2e tests](#spark-e2e-tests-spark_e2e-build-tag)
  - [Lambda e2e tests](#lambda-e2e-tests-lambda_e2e-build-tag)
    - [Lambda Docker mode](#lambda-docker-mode)
    - [Lambda Kubernetes mode](#lambda-kubernetes-mode)
  - [KMS · SecretsManager · SSM e2e tests](#kms--secretsmanager--ssm-e2e-tests-kms_fullmode-build-tag)
  - [CloudFormation e2e tests](#cloudformation-e2e-tests-cfn_fullmode-build-tag)
  - [Kinesis e2e tests](#kinesis-e2e-tests-kinesis_fullmode-build-tag)
  - [Step Functions e2e tests](#step-functions-e2e-tests-sfn_e2e-build-tag)
  - [Apache Iceberg e2e tests](#apache-iceberg-e2e-tests-iceberg_e2e-build-tag)
- [Platform Runtime Layer](#platform-runtime-layer)
  - [TLS CA injection](#tls-ca-injection)
  - [Extra volumes](#extra-volumes)
  - [Extra environment variables](#extra-environment-variables)
  - [Docker mode behaviour](#docker-mode-behaviour)
  - [Kubernetes mode behaviour](#kubernetes-mode-behaviour)
- [Spark Kubernetes Configuration Reference](#spark-kubernetes-configuration-reference)
  - [JAISCLOUD_EXECUTOR_MODE](#jaiscloud_executor_mode)
  - [JAISCLOUD_K8S_SA vs JAISCLOUD_K8S_SPARK_SA](#jaiscloud_k8s_sa-vs-jaiscloud_k8s_spark_sa)
  - [JAISCLOUD_K8S_NAMESPACE](#jaiscloud_k8s_namespace)
  - [JAISCLOUD_SPARK_K8S_CLUSTER_MODE](#jaiscloud_spark_k8s_cluster_mode)
  - [JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN](#jaiscloud_spark_k8s_cluster_shutdown)
  - [JAISCLOUD_SPARK_K8S_CLUSTER_RESTART_POLICY](#jaiscloud_spark_k8s_cluster_restart_policy)
  - [JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT](#jaiscloud_spark_k8s_reconcile_timeout)
  - [JAISCLOUD_INSTANCE_ID](#jaiscloud_instance_id)
- [AWS Emulator Wiring for Spark Driver Pods](#aws-emulator-wiring-for-spark-driver-pods)
  - [IMDS emulator](#imds-emulator)
  - [Spark conf precedence](#spark-conf-precedence)
- [S3 Virtual-Hosted Style Routing](#s3-virtual-hosted-style-routing)
- [Multi-Cloud Spark Transforms](#multi-cloud-spark-transforms)
  - [Azure](#azure-spark-transform)
  - [GCP](#gcp-spark-transform)
- [Platform Setup](#platform-setup)

---

## Service Reference

### Implementation tiers

| Tier | Meaning |
|---|---|
| ✅ Full | Real business logic. Passes the AWS SDK integration test suite. State persists in Postgres when `--dsn` is set. |
| ⚙️ Metadata-only | Wire protocol + resource CRUD (create, describe, delete, tag). No execution engine — instances don't run, clusters don't provision VMs. |
| 🔌 Stub | Endpoint exists, returns plausible responses. Limited operation coverage. |

> The tier for each service is exposed to the UI as the `Tier` field on
> `ServiceDescriptor` (defined in `internal/ui/types.go`, served by
> `GET /api/ui/v1/services`). Each cloud's catalog is contributed by its UI
> registrar — AWS descriptors live in `internal/aws/ui/services.go`, GCP in
> `internal/gcp/ui/`. Keep that table in sync with this one; the AWS
> classification is pinned by `internal/aws/ui/services_test.go`.

### Service implementation matrix

| Service | Tier | Default storage | Postgres storage (`--dsn`) | Integration tests |
|---|---|---|---|---|
| Amazon S3 | ✅ Full | In-memory + MemoryBlobStore | PostgreSQL + LocalFS blobs | `tests/integration/s3_*.go` |
| Amazon SQS | ✅ Full | In-memory | PostgreSQL | `tests/integration/sqs_*.go` |
| Amazon DynamoDB | ✅ Full | In-memory | PostgreSQL | `tests/integration/dynamo_*.go` |
| Amazon DynamoDB Streams | ✅ Full | In-memory stream store | In-memory stream store | — |
| Amazon SNS | ✅ Full | In-memory | PostgreSQL | `tests/integration/sns_test.go` |
| Amazon EventBridge | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/eventbridge/` |
| AWS IAM | ✅ Full | In-memory | PostgreSQL | `tests/integration/iam_test.go` |
| AWS STS | ✅ Full | In-memory | PostgreSQL | `tests/integration/sts_test.go` |
| AWS Lambda | ✅ Full | In-memory | PostgreSQL | `tests/integration/lambda_*.go`, `tests/persistent_mode/aws/lambda/` |
| AWS Glue Data Catalog | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/iceberg/` |
| Amazon Kinesis | ✅ Full | In-memory | In-memory | `tests/integration/kinesis_*.go`, `tests/persistent_mode/aws/kinesis/` |
| Amazon EMR (on EC2) | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/emr/` |
| Amazon EMR on EKS | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/emrcontainers/` |
| AWS KMS | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/kms/` |
| AWS Secrets Manager | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/kms/` |
| AWS SSM Parameter Store | ✅ Full | In-memory | PostgreSQL (labels: in-memory only) | `tests/integration/ssm_*.go` |
| AWS API Gateway (REST) | ✅ Full | In-memory | PostgreSQL | `tests/integration/apigw_test.go` |
| AWS CloudFormation | ✅ Full | In-memory | PostgreSQL | `tests/persistent_mode/aws/cloudformation/` |
| Amazon CloudWatch | ✅ Full | In-memory ring | In-memory ring + PostgreSQL alarms | `tests/integration/cloudwatch_test.go` |
| Amazon CloudWatch Logs | ✅ Full | In-memory | In-memory | `tests/integration/cloudwatchlogs_test.go` |
| AWS Step Functions | ✅ Full | In-memory | In-memory | `tests/integration/stepfunctions_test.go` |
| Amazon EC2 | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon Route 53 | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon RDS | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon ElastiCache | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon ECS | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon EKS | ⚙️ Metadata-only | In-memory | In-memory | — |
| AWS ELBv2 | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon ECR | ⚙️ Metadata-only | In-memory | In-memory | — |
| AWS ACM | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon Kinesis Firehose | ⚙️ Metadata-only | In-memory | In-memory | — |
| AWS Config | ⚙️ Metadata-only | In-memory | In-memory | — |
| AWS Resource Groups | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon Redshift | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon Athena | ⚙️ Metadata-only | In-memory | In-memory | — |
| Amazon SES | 🔌 Stub | In-memory | In-memory | — |
| Amazon Cognito | 🔌 Stub | In-memory | In-memory | — |

### Step Functions (real ASL engine)

JaisCloud runs a goroutine-per-execution ASL interpreter — not a stub. All 8 state types are supported:

| State type | Behavior |
|---|---|
| `Pass` | Passes input to output; supports InputPath, OutputPath, ResultPath, Parameters, Result |
| `Task` | Invokes a Lambda function (ARN resolved from the JaisCloud registry) |
| `Choice` | Evaluates comparison rules (StringEquals, NumericLessThan, IsNull, And, Or, Not, etc.) |
| `Wait` | Waits Seconds, SecondsPath, Timestamp, or TimestampPath |
| `Map` | Iterates ItemsPath with MaxConcurrency |
| `Parallel` | Runs branches in parallel goroutines, merges output |
| `Succeed` | Terminates successfully |
| `Fail` | Terminates with error + cause |

Retry and Catch blocks are fully evaluated with exponential backoff. The execution runs in-process — no Lambda warm-up cost for Task states, but requires a real Lambda function to exist in the registry.

### Lambda execution modes

| `JAISCLOUD_EXECUTOR_MODE` | Behavior |
|---|---|
| _(empty)_ / `mock` | Echo handler — returns the invocation payload unchanged; instant response |
| `docker` | Warm Docker container pool per function; containers are reused across invocations |
| `k8s` | Warm K8s Pod + ClusterIP Service per function; survives JaisCloud restarts |

### Lambda layer support (Docker mode)

When `JAISCLOUD_EXECUTOR_MODE=docker`, published Lambda layers are mounted at `/opt` inside the container. A layer's zip is extracted to `/opt` via a Docker bind-mount before invocation. Python code can import from `/opt/python`, Node.js from `/opt/nodejs/node_modules`, etc. — the standard AWS layout.

```python
# In the function code:
import foo  # resolved from /opt/python/foo.py published in a layer
```

To use layers:

1. `PublishLayerVersion` with a zip that follows the AWS runtime path convention
2. Pass the layer ARN in `CreateFunction` / `UpdateFunctionConfiguration` `Layers` list
3. Invoke — JaisCloud mounts the layer before the container starts

### Lambda log tail (X-Amz-Log-Result)

Invoke with `LogType: "Tail"` to receive the last 4 KB of function stdout/stderr as a base64-encoded `X-Amz-Log-Result` response header, matching the AWS SDK behavior:

```go
out, err := lambdaClient.Invoke(ctx, &lambda.InvokeInput{
    FunctionName: aws.String("my-fn"),
    Payload:      payload,
    LogType:      types.LogTypeTail,
})
// out.LogResult contains base64(last 4KB of logs)
```

### EMR / Spark execution modes

| `JAISCLOUD_EXECUTOR_MODE` | EMR on EC2 | EMR on EKS |
|---|---|---|
| _(empty)_ / `mock` | Steps complete instantly as `COMPLETED` | Job runs complete instantly as `COMPLETED` |
| `docker` | Each step runs in a Docker container | Each job run runs in a Docker container |
| `k8s` | Each step runs as a K8s `batch/v1 Job` | Each job run runs as a K8s `batch/v1 Job`; cluster-mode Spark available |

### What "metadata-only" means in practice

Metadata-only services implement the AWS management plane — you can:
- Create, describe, list, and delete resources
- Add and remove tags
- Describe resource attributes

They do **not** implement the data plane or execution engine:
- EC2 instances don't boot or run workloads
- RDS instances don't serve database connections
- ECS tasks don't schedule containers
- Route 53 does not resolve DNS queries

This is intentional: these services exist so that IaC tooling (CloudFormation, Terraform, CDK) can create and reference them without failing. Applications that only read resource metadata (e.g. discovering VPC IDs, listing subnets) work correctly. Applications that make data-plane calls to the resource itself will fail or need the real cloud.

---

## Prerequisites

You need different tools depending on which mode you are using. Install only what you need for your current goal.

### Required only if building from source

**Go 1.26+** — only needed if you are building from source. The pre-built binary and Docker image require no Go. Check your version:

```bash
go version
# Expected: go version go1.26.x ...
```

If Go is not installed or out of date: https://go.dev/dl/

### Required for PostgreSQL backend and Spark

**Docker** — needed to run PostgreSQL (`--dsn`) and to load Spark images onto a local K8s cluster. You do **not** need Docker to build the JaisCloud image — the public image is available at `jaisraj/jaiscloud-aws:latest`.

```bash
docker version
# Expected: Client: Docker Engine - Community, Version: 24.x or higher
```

### Required for Spark K8s executor only

**kubectl** — the Kubernetes CLI, used to check Spark job pods.

```bash
kubectl version --client
# Expected: Client Version: v1.28.x or higher
```

**A Kubernetes cluster** — Docker Desktop (easiest), kind, or minikube. See [Setup 3](#setup-3--jaiscloud-on-kubernetes) for setup.

### Optional (for smoke testing only)

**AWS CLI** — useful for manually calling JaisCloud via the command line, but not required to run the server.

```bash
aws --version
# Expected: aws-cli/2.x.x
```

---

## Storage Model

JaisCloud has three storage configurations — no `--mode` flag. Two flags drive the decision: `--dsn` and `--ephemeral`.

| Invocation | Storage | Survives restart? | When to use |
|---|---|---|---|
| `./jaiscloud-aws start` | Memory + periodic `state.json` saves | Yes (soft) | Local dev, team environments without Postgres |
| `./jaiscloud-aws start --dsn "postgres://..."` | PostgreSQL | Yes (durable) | Shared deployments, production-grade persistence |
| `./jaiscloud-aws start --ephemeral` | Memory only, no disk writes | No | CI pipelines, unit tests, guaranteed clean state |
| `--ephemeral` + `--dsn` | — | — | **Startup error: mutually exclusive** |

---

## Setup 1 — Default (memory + periodic saves)

**What it is:** JaisCloud keeps all state in memory and saves a `state.json` snapshot to `--data-dir` (default: `~/.jaiscloud/jaiscloud-aws/`) every 30 seconds. State is restored from `state.json` on the next startup — no database needed.

**What you need:** Go only (or the pre-built binary — no Go required).

### Step 1 — Build the binary

From the repo root:

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
```

This produces a `jaiscloud-aws` binary in the current directory. You must rebuild after any code change — never run a stale binary.

### Step 2 — Start the server

```bash
./jaiscloud-aws start
```

Expected output:
```
INFO  executor  lambda=mock  spark=mock
INFO  jaiscloud started  port=4566  mode=memory+saves
```

The server is now listening on port 4566. Leave this terminal open, or run it in the background with `./jaiscloud-aws start &`.

### Step 3 — Verify it is running

```bash
./jaiscloud-aws doctor
```

Expected:
```
OK: jaiscloud is running at http://localhost:4566
```

Or use curl directly:

```bash
curl http://localhost:4566/_jaiscloud/health
# {"status":"ok"}
```

### Step 4 — Try it with the AWS CLI

The AWS CLI talks to JaisCloud the same way it talks to real AWS. The only difference is `--endpoint-url`.

```bash
# Create a queue
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    sqs create-queue --queue-name hello-queue

# Send a message
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    sqs send-message \
    --queue-url http://localhost:4566/000000000000/hello-queue \
    --message-body "hello from jaiscloud"

# Receive it back
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    sqs receive-message \
    --queue-url http://localhost:4566/000000000000/hello-queue
```

You should see a JSON response containing your message body `"hello from jaiscloud"`.

### Step 5 — Reset state between tests

```bash
curl -X POST http://localhost:4566/_jaiscloud/reset
```

This wipes all in-memory state. Useful between test runs without restarting the server. Integration tests call this automatically.

### Useful flags

| Flag | What it does |
|---|---|
| `--port 9000` | Listen on a different port |
| `--region eu-west-1` | Change the region reported in responses |
| `--account-id 123456789012` | Set a custom default account ID in ARNs |
| `--data-dir /var/lib/jaiscloud` | Override the directory used for `state.json` and named snapshots |
| `--fresh-start` | Skip loading existing `state.json` on startup (start clean) |
| `--ephemeral` | Disable all saves — state is lost on exit (CI / unit tests) |
| `--log-level debug` | Print every request and response (very verbose) |
| `--metrics` | Enable Prometheus metrics at `http://localhost:4566/metrics` |

---

## Setup 2 — PostgreSQL backend

**What it is:** all resource state is stored in PostgreSQL. Use this for shared team environments, multi-instance deployments, or when you need full ACID durability.

**What you need:** Go (or the pre-built binary) and Docker (to run PostgreSQL).

### Step 1 — Start PostgreSQL

The repo includes a Docker Compose file that starts both PostgreSQL and JaisCloud together. That is the easiest path.

**Option A — Docker Compose (recommended)**

```bash
make up-docker
```

This pulls `jaisraj/jaiscloud-aws:latest`, starts PostgreSQL on port 5433, and starts JaisCloud on port 4566. Skip to the verify step.

> **Using a locally built image?** Run `make docker-aws` first, then pass the image override: `JAISCLOUD_IMAGE=jaiscloud-aws:latest make up-docker`.

To stop:
```bash
make down-docker
```

To wipe all data and start fresh:
```bash
docker-compose down -v    # -v removes the postgres data volume
make up-docker
```

---

**Option B — Run the binary yourself**

If you want to run the Go binary directly (e.g. for debugging or hot-reload during development), start PostgreSQL separately:

```bash
docker run -d \
  --name jaiscloud-pg \
  -e POSTGRES_USER=jaiscloud \
  -e POSTGRES_PASSWORD=jaiscloud \
  -e POSTGRES_DB=jaiscloud \
  -p 5433:5432 \
  postgres:16-alpine
```

Wait for it to be ready (takes about 5 seconds):
```bash
docker exec jaiscloud-pg pg_isready -U jaiscloud
# /var/run/postgresql:5432 - accepting connections
```

### Step 2 — Start JaisCloud with the PostgreSQL backend

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
./jaiscloud-aws start \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
```

Expected output:
```
INFO  executor      lambda=mock  spark=mock
INFO  blob storage  dir=/Users/yourname/.jaiscloud/blobs
INFO  jaiscloud started  port=4566  mode=postgres
```

> **Where do S3 blobs go?** With `--dsn` (PostgreSQL backend), S3 object *bodies* are written to a `LocalFSBlobStore` under `--data-dir` (default: `~/.jaiscloud/jaiscloud-aws`). They survive server restarts. S3 *metadata* (bucket names, keys, sizes, ETags) goes into PostgreSQL. Without `--dsn`, blobs are held in a session-scoped temporary directory and included in snapshot exports.

JaisCloud runs SQL migrations automatically on every startup — no manual schema setup is needed.

### Step 3 — Verify persistence

Create a resource, restart the server, and confirm it is still there:

```bash
# Create a queue
aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs create-queue --queue-name persist-test

# Stop and restart the server (Ctrl+C then restart)
./jaiscloud-aws start --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"

# Queue should still be there
aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs get-queue-url --queue-name persist-test
# {"QueueUrl":"http://localhost:4566/000000000000/persist-test"}
```

### Using environment variables

```bash
export JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud
./jaiscloud-aws start
```

### Relevant env vars

| Env var | Default | Description |
|---|---|---|
| `JAISCLOUD_DSN` | — | PostgreSQL connection string |
| `JAISCLOUD_DATA_DIR` | `~/.jaiscloud/jaiscloud-aws` | Directory for named snapshots on disk |

---

### Connection string format

```
postgres://<user>:<password>@<host>:<port>/<database>
```

| Part | Default in this guide | Notes |
|---|---|---|
| user | `jaiscloud` | The PostgreSQL role |
| password | `jaiscloud` | Change this in production |
| host | `localhost` | Use `host.docker.internal` from inside Docker |
| port | `5433` | Docker Compose maps container port 5432 → host port 5433 |
| database | `jaiscloud` | Must exist before starting |

---

## Setup 3 — JaisCloud on Kubernetes

**What this is:** running the JaisCloud server itself inside a Kubernetes cluster. This is separate from running Spark jobs on K8s (covered in [EMR Spark — Kubernetes Executor](#emr-spark--kubernetes-executor-real-spark-jobs)).

**When to use this:**
- You want a shared JaisCloud instance accessible to multiple pods in the same cluster
- You are testing services that run inside K8s and need to call AWS APIs
- You want Spark jobs to submit to K8s (the JaisCloud server needs to reach the K8s API)

**What you need:** Docker Desktop with Kubernetes enabled, or kind / minikube.

### Step 1 — Enable Kubernetes

**Docker Desktop (easiest):**

Open Docker Desktop → Settings → Kubernetes → check "Enable Kubernetes" → Apply & Restart. Wait for the Kubernetes status indicator to show green.

Verify:
```bash
kubectl config current-context
# docker-desktop
```

**kind or minikube:**

```bash
# kind
kind create cluster --name jaiscloud-dev

# minikube
minikube start
```

### Step 2 — Deploy with Make

```bash
make up-k8s
```

This runs in order:
1. Pulls `jaisraj/jaiscloud-aws:latest` from the GitHub Container Registry
2. Applies `deploy/k8s/namespace.yaml` — creates the `jaiscloud` namespace
3. Applies `deploy/k8s/rbac.yaml` — grants JaisCloud permission to create Spark Jobs and Lambda Pods
4. Applies `deploy/k8s/postgres.yaml` — starts a PostgreSQL pod
5. Applies `deploy/k8s/jaiscloud.yaml` — starts JaisCloud with the PostgreSQL backend, wired to the postgres pod

Wait for both pods to be running:
```bash
kubectl get pods -n jaiscloud
# NAME                         READY   STATUS    RESTARTS
# jaiscloud-xxxx               1/1     Running   0
# postgres-xxxx                1/1     Running   0
```

### Step 3 — Reach the server

Docker Desktop exposes the LoadBalancer service on localhost automatically. Check the port:

```bash
kubectl get svc -n jaiscloud jaiscloud
# NAME        TYPE           EXTERNAL-IP   PORT(S)
# jaiscloud   LoadBalancer   localhost     4566:xxxxx/TCP
```

The server is now at `http://localhost:4566`.

For kind or minikube (external IP stays `<pending>`), use port-forward instead:
```bash
kubectl port-forward -n jaiscloud svc/jaiscloud 4566:4566
```

### Step 4 — Verify

```bash
./jaiscloud-aws doctor
# OK: jaiscloud is running at http://localhost:4566

curl http://localhost:4566/_jaiscloud/health
# {"status":"ok"}
```

### Teardown

```bash
make down-k8s       # removes the deployment; keeps persistent volumes
make down-k8s WIPE=true   # removes deployment AND wipes all data
```

### Viewing logs

```bash
kubectl logs -n jaiscloud deployment/jaiscloud -f
kubectl logs -n jaiscloud deployment/postgres -f
```

---

## EMR Spark — Mock Mode (instant results)

**What this is:** EMR (`RunJobFlow`, `AddJobFlowSteps`) and EMR on EKS (`StartJobRun`) work as full AWS-compatible APIs, but Spark jobs complete instantly with `COMPLETED` status. No actual Spark process runs.

**When to use this:** whenever you need the EMR API to work (your code creates clusters, submits steps, polls status) but you do not need to execute real Spark code. This covers most unit tests and integration tests.

**What you need:** JaisCloud running (any storage configuration).

### How to enable

This is the default. You do not need to set anything:

```bash
./jaiscloud-aws start   # mock executor is on by default
```

To be explicit:
```bash
JAISCLOUD_EXECUTOR_MODE=mock ./jaiscloud-aws start
```

### Try it — EMR on EC2 (classic)

```bash
# 1. Create a cluster
CLUSTER_ID=$(aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr run-job-flow \
    --name "test-cluster" \
    --release-label emr-6.10.0 \
    --instance-groups '[{"InstanceRole":"MASTER","InstanceType":"m5.xlarge","InstanceCount":1}]' \
    --service-role EMR_DefaultRole \
    --query 'JobFlowId' --output text)

echo "Cluster ID: $CLUSTER_ID"

# 2. Add a Spark step
STEP_ID=$(aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[{
      "Name": "my-spark-job",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "s3://my-bucket/my-app.jar",
        "Args": ["--input", "s3://my-bucket/data"]
      }
    }]' \
    --query 'StepIds[0]' --output text)

echo "Step ID: $STEP_ID"

# 3. Check status — will be COMPLETED immediately in mock mode
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr describe-step \
    --cluster-id $CLUSTER_ID \
    --step-id $STEP_ID \
    --query 'Step.Status.State' --output text
# COMPLETED
```

### Try it — EMR on EKS (virtual clusters)

```bash
# 1. Create a virtual cluster
VC_ID=$(aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr-containers create-virtual-cluster \
    --name my-vc \
    --container-provider '{"id":"my-eks-cluster","type":"EKS","info":{"eksInfo":{"namespace":"spark-jobs"}}}' \
    --query 'id' --output text)

# 2. Start a job run
JOB_ID=$(aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr-containers start-job-run \
    --virtual-cluster-id $VC_ID \
    --name my-job \
    --execution-role-arn arn:aws:iam::000000000000:role/SparkRole \
    --release-label emr-6.10.0-latest \
    --job-driver '{"sparkSubmitJobDriver":{"entryPoint":"s3://my-bucket/app.jar","sparkSubmitParameters":"--class com.example.App"}}' \
    --query 'id' --output text)

# 3. Check status
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr-containers describe-job-run \
    --virtual-cluster-id $VC_ID \
    --id $JOB_ID \
    --query 'jobRun.state' --output text
# COMPLETED
```

---

## EMR Spark — Kubernetes Executor (real Spark jobs)

**What this is:** when you submit an EMR step or job run, JaisCloud creates a real Kubernetes `batch/v1 Job` that runs `spark-submit` inside your Spark Docker image. The Spark driver starts, spawns executor pods, runs your code, and produces real output.

**When to use this:** end-to-end testing of actual Spark logic — reading from S3, writing Parquet, running transformations. Not needed just to test EMR API calls.

**What you need:**
- JaisCloud running (any storage configuration)
- A Kubernetes cluster (Docker Desktop, kind, or minikube — see [Setup 3](#setup-3--jaiscloud-on-kubernetes))
- A Spark Docker image accessible from the cluster

### How it works

```
Your code calls EMR AddJobFlowSteps
  → JaisCloud receives the step
  → Creates a batch/v1 Job in Kubernetes
  → Job pod runs: spark-submit --master k8s://... --conf ...
  → Spark driver spawns executor pods
  → Executors complete; Job reaches Succeeded/Failed
  → JaisCloud polls Job status and updates step state to COMPLETED/FAILED
```

### Step 1 — Set up RBAC in Kubernetes

JaisCloud needs permission to create Jobs. The Spark driver (running inside the Job pod) needs permission to create executor pods. Apply the included manifest which handles both:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/rbac.yaml
```

This creates two service accounts in the `jaiscloud` namespace:

| Service account | Used by | Needs |
|---|---|---|
| `jaiscloud` | JaisCloud server | Create/delete batch/v1 Jobs and ConfigMaps |
| `spark-driver` | Spark driver pod | Create/delete Pods, Services, ConfigMaps |

### Step 2 — Pre-pull the Spark image (local clusters only)

Local clusters (Docker Desktop, kind, minikube) use `ImagePullPolicy: IfNotPresent`. The image must already exist on the node or pod scheduling will fail.

```bash
# Pull the image locally
docker pull apache/spark:3.5.0

# For kind: load the image into the cluster
kind load docker-image apache/spark:3.5.0

# For minikube
minikube image load apache/spark:3.5.0

# Docker Desktop: the image is automatically available (it shares Docker's image store)
```

### Step 3 — Start JaisCloud with K8s executor

You need the K8s API server URL. Get it from kubectl:

```bash
kubectl cluster-info
# Kubernetes control plane is running at https://127.0.0.1:6443
```

Create a service account token for JaisCloud to authenticate:

```bash
kubectl create token jaiscloud -n jaiscloud --duration=24h
# eyJhbGciOiJSUzI1NiIs...  (copy this)
```

Start the server:

```bash
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=<paste token here>
export JAISCLOUD_K8S_NAMESPACE=jaiscloud
export JAISCLOUD_K8S_SA=spark-driver              # SA for the spark-submit pod
export JAISCLOUD_K8S_SPARK_SA=spark-driver         # SA for executor pods

./jaiscloud-aws start \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
```

Expected output:
```
INFO  executor      lambda=k8s  spark=k8s
INFO  blob storage  dir=/home/yourname/.jaiscloud/blobs
INFO  jaiscloud started  port=4566  mode=postgres
```

> **Why two service accounts?** `JAISCLOUD_K8S_SA` is the service account the Spark *driver pod* runs as — it needs permission to create executor pods. `JAISCLOUD_K8S_SPARK_SA` is forwarded to Spark as the service account for *executor pods*. They can be the same SA (as above) or different ones if you want finer-grained RBAC. See [JAISCLOUD_K8S_SA vs JAISCLOUD_K8S_SPARK_SA](#jaiscloud_k8s_sa-vs-jaiscloud_k8s_spark_sa) for details.

### Step 4 — Submit a job and watch it run

```bash
# Create an EMR cluster
CLUSTER_ID=$(aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr run-job-flow \
    --name "real-spark-cluster" \
    --release-label emr-6.15.0 \
    --instance-groups '[{"InstanceRole":"MASTER","InstanceType":"m5.xlarge","InstanceCount":1}]' \
    --service-role EMR_DefaultRole \
    --query 'JobFlowId' --output text)

# Add a step using SparkPi (bundled with Apache Spark image)
STEP_ID=$(aws --endpoint-url http://localhost:4566 \
    --region us-east-1 --no-cli-pager \
    emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[{
      "Name": "SparkPi",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "--class", "org.apache.spark.examples.SparkPi",
                 "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar", "100"]
      }
    }]' \
    --query 'StepIds[0]' --output text)

echo "Watching step $STEP_ID..."
```

Watch Kubernetes in a second terminal:

```bash
# See the Job appear
kubectl get jobs -n jaiscloud -w

# See the pods
kubectl get pods -n jaiscloud -w

# Read the driver logs
kubectl logs -n jaiscloud -l spark-role=driver --tail=50 -f
```

Poll until the step completes (takes 1–3 minutes on first run due to image scheduling):

```bash
watch -n 3 "aws --endpoint-url http://localhost:4566 --region us-east-1 \
    emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID \
    --query 'Step.Status.State' --output text"
# PENDING → RUNNING → COMPLETED
```

### Step 5 — Wire S3 so Spark can read your data (optional)

If your Spark job reads from `s3://` URIs, Spark needs to know to use JaisCloud's S3 endpoint instead of real AWS.

Set this env var before starting JaisCloud:

```bash
export JAISCLOUD_AWS_EMULATOR_ENDPOINT=http://jaiscloud.jaiscloud.svc:4566
```

JaisCloud will automatically inject the right S3 configuration (`fs.s3a.endpoint`, credentials, etc.) into every Spark driver pod. See [AWS Emulator Wiring for Spark Driver Pods](#aws-emulator-wiring-for-spark-driver-pods) for the full details.

### Troubleshooting

| Symptom | What to check |
|---|---|
| Step stays `PENDING` forever | Check `kubectl get jobs -n jaiscloud` — if no Job appears, the server likely cannot reach the K8s API. Check `JAISCLOUD_K8S_APISERVER` and the token. |
| Step stays `RUNNING` forever | Check `kubectl get pods -n jaiscloud` — if pods are `Pending`, the image may not be loaded. Run `kubectl describe pod <name>` to see the event. |
| Step goes to `FAILED` immediately | Check driver logs: `kubectl logs -n jaiscloud -l spark-role=driver --tail=100` |
| `Forbidden` creating pods | The driver SA lacks RBAC. Re-apply `deploy/k8s/rbac.yaml`. Check: `kubectl auth can-i create pods -n jaiscloud --as=system:serviceaccount:jaiscloud:spark-driver` |
| Image pull error | Pre-pull with `docker pull apache/spark:3.5.0` and load with `kind load docker-image apache/spark:3.5.0` |

### Environment variable quick-reference

| Variable | What to set | Why |
|---|---|---|
| `JAISCLOUD_EXECUTOR_MODE` | `k8s` | Enables the K8s executor |
| `JAISCLOUD_K8S_APISERVER` | `https://127.0.0.1:6443` | K8s API server URL (from `kubectl cluster-info`) |
| `JAISCLOUD_K8S_TOKEN` | output of `kubectl create token` | Auth token to create Jobs |
| `JAISCLOUD_K8S_CA_FILE` | path to CA cert, or unset | TLS verification for the API server (unset = use system roots) |
| `JAISCLOUD_K8S_NAMESPACE` | `jaiscloud` | Namespace where Spark Jobs are created |
| `JAISCLOUD_K8S_SA` | `spark-driver` | Service account for the spark-submit Job pod |
| `JAISCLOUD_K8S_SPARK_SA` | `spark-driver` | Service account for executor pods |
| `JAISCLOUD_AWS_EMULATOR_ENDPOINT` | `http://jaiscloud.jaiscloud.svc:4566` | Inject S3 endpoint into driver pods (needed for `s3://` JAR URIs) |

For a full explanation of every variable, see [Spark Kubernetes Configuration Reference](#spark-kubernetes-configuration-reference).

---

## Spark Kubernetes Configuration Reference

This section explains each Spark-related env var in detail — why it exists, what to set it to, and what breaks when it is wrong.

---

### `JAISCLOUD_EXECUTOR_MODE`

**What it controls:** the compute backend used for every Spark job (EMR on EC2 steps and EMR on EKS job runs) and every Lambda invocation.

| Value | Behaviour |
|---|---|
| _(unset)_ or `mock` | Jobs complete immediately with `COMPLETED`; no real compute. Use for unit tests, CI, local dev when you just need the API to work. |
| `docker` | Each job runs in a Docker container on the local machine. Requires Docker. Use when you want to run a real Spark job on a dev laptop without a K8s cluster. |
| `k8s` | Each job creates a `batch/v1 Job` on a Kubernetes cluster. Use for devbox-on-K8s, staging, or any environment that has `kubectl` access. |

**Common mistake:** leaving `JAISCLOUD_EXECUTOR_MODE` unset and wondering why Spark jobs finish instantly — that is mock mode working as designed. Set `k8s` to get real execution.

```bash
# Explicit mock (same as leaving unset)
JAISCLOUD_EXECUTOR_MODE=mock ./jaiscloud-aws start

# Real K8s execution
JAISCLOUD_EXECUTOR_MODE=k8s ./jaiscloud-aws start
```

---

### `JAISCLOUD_K8S_SA` vs `JAISCLOUD_K8S_SPARK_SA`

These two variables look similar but control entirely different service accounts in different K8s namespaces.

| Variable | Which pod uses it | What it grants |
|---|---|---|
| `JAISCLOUD_K8S_SA` | The `spark-submit` Job pod itself (created by JaisCloud) | Must be able to create Pods and ConfigMaps, and list/watch Pods — so the Spark driver can spawn executor pods |
| `JAISCLOUD_K8S_SPARK_SA` | The Spark executor pods (created by the Spark driver inside the cluster) | Forwarded as `spark.kubernetes.authenticate.executor.serviceAccountName`; needs permissions to read ConfigMaps in the Spark namespace |

**In practice:**

```bash
# The spark-submit pod runs as this SA — must have RBAC to create executor pods
JAISCLOUD_K8S_SA=spark-driver-sa

# Executor pods run as this SA — must have RBAC to read ConfigMaps
JAISCLOUD_K8S_SPARK_SA=spark-executor-sa
```

If you only set `JAISCLOUD_K8S_SA` and leave `JAISCLOUD_K8S_SPARK_SA` unset, executor pods run as the `default` service account, which usually lacks the permissions to read Spark's executor ConfigMap. The symptom is executor pods starting but immediately crashing with a "Forbidden" error in the logs.

**Minimum RBAC for `JAISCLOUD_K8S_SPARK_SA`:**

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-executor
  namespace: jaiscloud
rules:
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch"]
```

---

### `JAISCLOUD_K8S_NAMESPACE`

**What it controls:** the Kubernetes namespace where JaisCloud creates Spark `batch/v1 Jobs`, executor ConfigMaps, Lambda Pods, and Lambda Services.

**Default:** `jaiscloud`

Set this when you want Spark and Lambda workloads isolated in a different namespace than the JaisCloud server itself, or when your cluster policy requires workloads in a specific namespace:

```bash
JAISCLOUD_K8S_NAMESPACE=spark-jobs ./jaiscloud-aws start
```

The namespace must already exist and have the RBAC from `deploy/k8s/rbac.yaml` applied. If the namespace does not exist, Job creation silently fails and steps stay in `RUNNING` forever until the reconcile timeout fires.

---

### `JAISCLOUD_SPARK_K8S_CLUSTER_MODE`

**What it controls:** when JaisCloud uses Spark's `--deploy-mode client` (driver runs inside the Job pod) vs engages the full K8s cluster deploy-mode (`--deploy-mode cluster`, where the driver spawns independently and Spark manages the pod lifecycle natively).

> **Background:** JaisCloud's `SubmitClientMode` always uses `--deploy-mode client`. "Cluster mode" in this context refers to JaisCloud's own cluster-mode policy — whether to inject pod-template `--conf` entries that activate Spark's native K8s pod-template mechanism.

| Value | Behaviour | When to use |
|---|---|---|
| `auto` *(default)* | Engage cluster deploy-mode only when the job already contains `spark.kubernetes.driver.podTemplateFile` or `spark.kubernetes.executor.podTemplateFile` `--conf` entries | Most setups — the caller controls whether templates are injected |
| `always` | Always inject pod-template confs, even for jobs that don't include them | When you want JaisCloud to manage pod templates for all jobs, e.g. enforcing a standard TLS template |
| `never` | Strip any pod-template `--conf` entries and always use bare `spark-submit` | Debugging, or when your Spark image does not support pod templates |

**When does `auto` activate?** When the step args or `sparkSubmitParameters` contain `--conf spark.kubernetes.driver.podTemplateFile=...` or `--conf spark.kubernetes.executor.podTemplateFile=...`. JaisCloud detects those and enables the full template pipeline.

---

### `JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN`

**What it controls:** what happens to running Spark `batch/v1 Jobs` when `jaiscloud` process exits cleanly (`Close()` is called, e.g. `SIGTERM`).

| Value | What happens on shutdown | When to use |
|---|---|---|
| `leave` *(default)* | Running Jobs are **suspended** (`spec.suspend: true`). Pods stop immediately but the Job object and its status are preserved. On the next startup, JaisCloud re-adopts them (see `CLUSTER_RESTART_POLICY`). | Dev/devbox — you want in-progress jobs to survive a server restart |
| `delete` | Running Jobs and all their Pods are **deleted immediately**. Steps in `RUNNING` will be marked `FAILED` by the reconcile timeout on next startup (they won't be in K8s anymore). | CI — clean teardown, no orphaned resources between test runs |

**What `leave` looks like:**

```bash
# After server shutdown with CLUSTER_SHUTDOWN=leave:
kubectl get jobs -n jaiscloud
NAME                        COMPLETIONS   DURATION   AGE   SUSPEND
jc-spark-abc-123            0/1           2m         5m    True   ← suspended, not deleted
```

On next startup JaisCloud unsuspends these jobs and continues polling them.

---

### `JAISCLOUD_SPARK_K8S_CLUSTER_RESTART_POLICY`

**What it controls:** what JaisCloud does with Spark Jobs it finds in Kubernetes at **startup** that belong to the current instance (matching `jaiscloud.io/instance-id` label) but are not yet terminal.

| Value | Behaviour | When to use |
|---|---|---|
| `adopt` *(default)* | Suspended Jobs are unsuspended and re-tracked in the poller. Running Jobs are adopted directly. State-change events fire when they complete. | Dev/devbox — you want interrupted jobs to resume and their EMR step states to update correctly |
| `reap` | All non-terminal Jobs are deleted immediately. The corresponding steps/job-runs are marked `FAILED` in the EMR store. | CI teardown, or when stale jobs from a previous run should never affect the new run |

**The interact with `CLUSTER_SHUTDOWN`:**

| `CLUSTER_SHUTDOWN` | `CLUSTER_RESTART_POLICY` | Net result on restart |
|---|---|---|
| `leave` | `adopt` *(default)* | In-progress jobs resume where they left off |
| `leave` | `reap` | In-progress jobs are deleted and steps marked FAILED |
| `delete` | `adopt` | Nothing to adopt — no jobs survive shutdown |
| `delete` | `reap` | Nothing to reap — no jobs survive shutdown |

For most dev workflows, leave both at their defaults (`leave` + `adopt`).

---

### `JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT`

**What it controls:** how long the poller waits before giving up on a Spark Job that has disappeared from Kubernetes.

**Default:** `10m`

**When it fires:** if a tracked Job returns 404 from the K8s API (e.g. deleted externally with `kubectl delete job`), the poller records `missingSince`. After `RECONCILE_TIMEOUT` of continuous absence, the corresponding EMR step or job run is marked `FAILED`.

This exists because Kubernetes does not guarantee immediate consistency — a 404 might be a brief API hiccup rather than a real deletion. The timeout distinguishes a transient blip from a genuinely missing job.

**When to change it:**

```bash
# Shorter timeout for CI — fail fast when jobs are externally cleaned up
JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT=2m ./jaiscloud-aws start

# Longer timeout for flaky clusters with intermittent API server unavailability
JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT=30m ./jaiscloud-aws start
```

If steps are flipping to `FAILED` unexpectedly in a healthy cluster, increase this value. If steps stay `RUNNING` too long after being externally deleted, decrease it.

---

### `JAISCLOUD_INSTANCE_ID`

**What it controls:** the UUID stamped as `jaiscloud.io/instance-id` on every K8s resource JaisCloud creates (Spark Jobs, Lambda Pods, Lambda Services).

**Default:** auto-generated UUID, persisted to `<stateDir>/instance-id` so it survives restarts. `stateDir` is resolved automatically using this priority: `JAISCLOUD_STATE_DIR` env var → `~/.jaiscloud` → `/var/lib/jaiscloud` → `/tmp/jaiscloud-<uid>` (ephemeral fallback — logs a warning). On any normal developer machine `~/.jaiscloud` is used, so the UUID is always stable across restarts without any configuration.

**Why it exists:** if two JaisCloud instances run against the same K8s cluster (e.g. two developers sharing a devbox cluster, or parallel CI jobs), `cleanupOrphans` on startup would otherwise delete each other's resources. The instance ID ensures each instance only touches its own resources.

**When to set it manually:**

```bash
# CI — give each CI job a stable, reproducible instance ID so cleanup is deterministic
JAISCLOUD_INSTANCE_ID=ci-run-${CI_JOB_ID} ./jaiscloud-aws start

# Staging — fix the ID so that rolling restarts re-adopt the same set of jobs
JAISCLOUD_INSTANCE_ID=staging-primary ./jaiscloud-aws start
```

**Do not** set this to the same value for two concurrently running instances — they will fight over each other's K8s resources.

---

## AWS Emulator Wiring for Spark Driver Pods

When Spark jobs run in K8s mode, the driver pod needs to reach JaisCloud's S3 endpoint and obtain AWS credentials. Set `JAISCLOUD_AWS_EMULATOR_ENDPOINT` to the endpoint that Spark pods can reach from inside the cluster:

```bash
export JAISCLOUD_AWS_EMULATOR_ENDPOINT=http://jaiscloud.jaiscloud.svc:4566
export JAISCLOUD_EXECUTOR_MODE=k8s
./jaiscloud-aws start --dsn "postgres://..."
```

JaisCloud then injects the following into every spark-submit pod:

| Injected item | What it sets |
|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars | Fixed dev credentials matching `JAISCLOUD_ACCOUNT_ID` |
| `HADOOP_AWS_S3_ENDPOINT` env var | `JAISCLOUD_AWS_EMULATOR_ENDPOINT` |
| `spark.hadoop.fs.s3a.impl` `--conf` | `org.apache.hadoop.fs.s3a.S3AFileSystem` |
| `spark.hadoop.fs.s3a.endpoint` `--conf` | `JAISCLOUD_AWS_EMULATOR_ENDPOINT` |
| `spark.hadoop.fs.s3a.aws.credentials.provider` `--conf` | `SimpleAWSCredentialsProvider` |
| `spark.hadoop.fs.s3a.path.style.access` `--conf` | `true` |
| `spark.executorEnv.*` `--conf` | mirrors all driver env vars to executor pods |

The `spark.hadoop.fs.s3a.impl` conf is required — without it Hadoop uses the default S3A provider which may not be present or may try to resolve real AWS endpoints.

### IMDS emulator

The IMDS emulator exposes `GET /latest/meta-data/` endpoints so Spark jobs that read region or credentials from IMDS work correctly against JaisCloud.

Enable it:

```bash
./jaiscloud-aws start --imds-enabled
# or
JAISCLOUD_IMDS_ENABLED=true ./jaiscloud-aws start
```

When `--aws-emulator-endpoint` is also set, JaisCloud injects `AWS_EC2_METADATA_SERVICE_ENDPOINT=<endpoint>` into Spark driver pods so they hit the local IMDS emulator instead of `169.254.169.254`.

When IMDS is **disabled** (the default), JaisCloud injects `AWS_EC2_METADATA_DISABLED=true` into Spark driver pods so Hadoop's credential chain skips IMDS and falls through to the explicitly configured `SimpleAWSCredentialsProvider`.

### Spark conf precedence

JaisCloud's injected `--conf` tokens (`ExtraSparkConfs`) are prepended before user-supplied `--conf` entries (`SparkSubmitArgs`). Because Spark processes `--conf` left to right and last-value-wins, user confs always override JaisCloud defaults:

```
spark-submit
  --conf spark.hadoop.fs.s3a.endpoint=http://jaiscloud:4566   ← JaisCloud default
  --conf spark.hadoop.fs.s3a.endpoint=http://my-minio:9000   ← user SparkSubmitArgs (wins)
```

This applies to both EMR on EC2 (`HadoopJarStep` args) and EMR on EKS (`sparkSubmitParameters`).

---

## S3 Virtual-Hosted Style Routing

### Why you need this

AWS S3 supports two URL styles:

| Style | URL pattern | Example |
|---|---|---|
| Path-style | `http://<host>/<bucket>/<key>` | `http://localhost:4566/mybucket/data.csv` |
| Virtual-hosted | `http://<bucket>.<host>/<key>` | `http://mybucket.s3.us-east-1.amazonaws.com/data.csv` |

**AWS SDK v2 defaults to virtual-hosted style.** When the SDK is pointed at a custom endpoint (e.g. JaisCloud), it constructs requests like:

```
PUT http://mybucket.jaiscloud.devbox.local:4566/data.csv
Host: mybucket.jaiscloud.devbox.local:4566
```

JaisCloud's S3 codec needs to know that `jaiscloud.devbox.local` is the base hostname so it can strip the `mybucket.` prefix and route the request correctly. Without `JAISCLOUD_S3_VIRTUAL_HOST_BASES`, the codec sees no matching pattern and falls back to path-style — which means `mybucket` is treated as the first path segment of `jaiscloud.devbox.local`, and the request fails.

**You do NOT need this if:**
- Your SDK is configured to use `UsePathStyle = true` (explicit path-style override)
- Your SDK endpoint is `http://localhost:4566` and you are using Go SDK v2 with `UsePathStyle = true` or boto3 with `addressing_style='path'`

**You DO need this if:**
- You are running JaisCloud in Kubernetes with a DNS name (e.g. `jaiscloud.jaiscloud.svc.cluster.local`) and the SDK defaults to virtual-hosted style
- You are running a Spark job that uses `spark.hadoop.fs.s3a.*` — Hadoop's S3A client uses virtual-hosted style by default unless `fs.s3a.path.style.access=true` is set
- You are testing code that constructs S3 URLs by hand using virtual-hosted format

---

### What value to pass

The base is the hostname that JaisCloud is reachable at, **without** any bucket prefix. Pass exactly the DNS name your clients will use:

| Deployment | Clients use | Set base to |
|---|---|---|
| Local dev, localhost | `http://localhost:4566` | _(not needed — use path-style)_ |
| K8s in-cluster | `http://jaiscloud.jaiscloud.svc.cluster.local:4566` | `jaiscloud.jaiscloud.svc.cluster.local` |
| K8s with short DNS | `http://jaiscloud.jaiscloud.svc:4566` | `jaiscloud.jaiscloud.svc` |
| Custom devbox DNS | `http://s3.devbox.internal:4566` | `s3.devbox.internal` |
| Multiple aliases | Both of the above | `jaiscloud.jaiscloud.svc,s3.devbox.internal` |

The port is **not** part of the base. JaisCloud strips `:port` from the Host header before matching.

---

### Configuration

```bash
# CLI flag
./jaiscloud-aws start --s3-virtual-host-bases "jaiscloud.jaiscloud.svc.cluster.local"

# Multiple bases (comma-separated)
./jaiscloud-aws start --s3-virtual-host-bases "jaiscloud.jaiscloud.svc.cluster.local,s3.devbox.local"

# Environment variable (same syntax)
JAISCLOUD_S3_VIRTUAL_HOST_BASES=jaiscloud.jaiscloud.svc.cluster.local ./jaiscloud-aws start
```

---

### How it works

For each incoming request, the codec:
1. Takes the `Host` header (e.g. `mybucket.jaiscloud.jaiscloud.svc.cluster.local:4566`)
2. Strips any `:port` suffix → `mybucket.jaiscloud.jaiscloud.svc.cluster.local`
3. Checks whether the result ends with `.<base>` for each configured base
4. If it matches, the prefix before `.<base>` is the bucket name; the URL path is the key

```
Host: mybucket.jaiscloud.jaiscloud.svc.cluster.local:4566
Path: /prefix/data.csv

  → strip port → mybucket.jaiscloud.jaiscloud.svc.cluster.local
  → ends with ".jaiscloud.jaiscloud.svc.cluster.local"? yes
  → bucket = "mybucket"
  → key    = "prefix/data.csv"
```

A request to the bare base hostname (no bucket prefix, e.g. `jaiscloud.jaiscloud.svc.cluster.local`) falls through to path-style routing, so `ListBuckets` and similar bucket-level operations work correctly.

---

### AWS SDK configuration examples

#### Go SDK v2

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithBaseEndpoint("http://jaiscloud.jaiscloud.svc.cluster.local:4566"),
)
s3Client := s3.NewFromConfig(cfg)
// SDK v2 uses virtual-hosted style by default — no extra option needed.
// JaisCloud sees: Host: mybucket.jaiscloud.jaiscloud.svc.cluster.local:4566
```

To force path-style instead (no base needed):

```go
s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.UsePathStyle = true
})
// JaisCloud sees: GET http://jaiscloud.jaiscloud.svc.cluster.local:4566/mybucket/key
```

#### Python boto3

```python
import boto3

# Virtual-hosted style (requires JAISCLOUD_S3_VIRTUAL_HOST_BASES)
s3 = boto3.client(
    's3',
    endpoint_url='http://jaiscloud.jaiscloud.svc.cluster.local:4566',
    region_name='us-east-1',
    config=boto3.session.Config(s3={'addressing_style': 'virtual'}),
)

# Path-style (no base config needed)
s3 = boto3.client(
    's3',
    endpoint_url='http://jaiscloud.jaiscloud.svc.cluster.local:4566',
    region_name='us-east-1',
    config=boto3.session.Config(s3={'addressing_style': 'path'}),
)
```

#### Hadoop / Spark S3A

Spark's S3A client uses virtual-hosted style by default. Set the endpoint and either configure the base in JaisCloud, or disable virtual-hosted style on the Spark side:

```
# Option A — configure the base in JaisCloud (recommended for cluster-mode)
JAISCLOUD_S3_VIRTUAL_HOST_BASES=jaiscloud.jaiscloud.svc.cluster.local

# Option B — tell Spark to use path-style (no base needed)
spark.hadoop.fs.s3a.path.style.access=true
```

When using `JAISCLOUD_AWS_EMULATOR_ENDPOINT`, JaisCloud automatically injects `spark.hadoop.fs.s3a.path.style.access=true` into Spark driver pods, so you typically do **not** need to set `JAISCLOUD_S3_VIRTUAL_HOST_BASES` for Spark jobs submitted through EMR.

---

### Priority over AWS SDK form

Real AWS virtual-hosted URLs match `*.s3.<region>.amazonaws.com`. JaisCloud checks that pattern first. Custom base matching only applies when the AWS form does not match, so there is no risk of collision even if you set a base like `s3.us-east-1.amazonaws.com`.

---

## Multi-Account Support

JaisCloud emulates real AWS multi-account behaviour. Every resource — queues, tables, buckets, keys, secrets, Lambda functions — is scoped to an `(account, region)` pair. Two clients with different access keys see completely isolated state, even if they hit the same port.

### How account identity is derived

JaisCloud parses the SigV4 `Authorization` header on every request and extracts the **access key**. The account ID is resolved from the key using this priority order:

| Access key format | Resolved account |
|---|---|
| `ASIA<base32-encoded-account>` | Decoded 12-digit account ID (LSIA encoding) |
| Any 12-digit numeric string | Taken as account ID literally |
| Anything else (`test`, `AKIA…`) | `JAISCLOUD_ACCOUNT_ID` (server default, `000000000000`) |

No server configuration or restart is needed to use additional accounts. Any client that presents an LSIA-encoded access key (or a 12-digit literal) automatically routes to an isolated store scope.

### LSIA key encoding

```
account_int = int(account_id_string)      // e.g. 111111111111
value       = account_int + 549755813888  // constant offset
key         = "ASIA" + base32(value)      // 20-char access key
```

The helper is in `internal/aws/identity/`:

```go
import "jaiscloud/internal/aws/identity"

key, err := identity.EncodeLSIA("111111111111")   // → "ASIAxxx..."
account, err := identity.DecodeLSIA(key)           // → "111111111111"
```

### Using multiple accounts in tests

The multi-account integration tests in `tests/integration/multiaccount/` show the canonical pattern. The fixture helper `accessKeyFor(account)` mints LSIA keys:

```go
// fixtures.go pattern
func clientFor(t *testing.T, account string) *sqs.Client {
    key, _ := identity.EncodeLSIA(account)
    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, "test", "")),
    )
    return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.BaseEndpoint = aws.String("http://localhost:4566")
    })
}

sqsA := clientFor(t, "111111111111")
sqsB := clientFor(t, "222222222222")
// sqsA and sqsB see completely separate queues, messages, etc.
```

### Cross-account dispatch

Resources that reference other resources by ARN are routed to the correct account store automatically:

| Cross-account operation | How it works |
|---|---|
| SNS topic (A) → SQS queue (B) | `InternalSend` parses the SQS ARN and routes to B's queue store |
| EventBridge rule (A) → Lambda (B) | Target ARN parsed; invocation dispatched to B's function store |
| STS AssumeRole for role in B | Returns LSIA-encoded credentials; calls resolve to B thereafter |
| Lambda `InvokeFunction` with full ARN | `nameAccountRegionFromARN` extracts account/region; routes to function owner |
| ESM (event source mapping) | Source ARN parsed; poller reads from source account's queue/stream |

### KMS cross-account protection

KMS ciphertext blobs carry a v2 format that embeds the encrypting account ID:

```
[0x02][keyUUID(36)][account_len(2)][account][region_len(2)][region][ciphertext]
```

A `Decrypt` call from a different account returns `IncorrectKeyException` — matching real AWS behaviour. The v1 format (no version byte) is read transparently for backward compatibility.

### Scoped state reset

The admin reset endpoint accepts optional query params to narrow the scope:

```bash
# Wipe all state (standard behaviour)
curl -X POST http://localhost:4566/_jaiscloud/reset

# Wipe one account across all regions
curl -X POST "http://localhost:4566/_jaiscloud/reset?account=111111111111"

# Wipe one (account, region) pair — leave other accounts and regions untouched
curl -X POST "http://localhost:4566/_jaiscloud/reset?account=111111111111&region=us-east-1"
```

Stores that implement `ScopedResetter` (all bundled stores) honour the narrow scope. Non-scoped stores (e.g. stream stores without per-account state) fall back to a global reset.

### Snapshot export and import

The snapshot envelope stores each resource collection under its `(account, region)` key. Exporting a two-account emulator and re-importing it restores both accounts' state intact.

The export format is a **gzip tarball** (`application/gzip`), not JSON. Use the CLI commands or the admin endpoints:

```bash
# Export via CLI (recommended)
jaiscloud-aws export -o snapshot.tar.gz

# Export via HTTP
curl http://localhost:4566/_jaiscloud/export -o snapshot.tar.gz

# Wipe everything
curl -X POST http://localhost:4566/_jaiscloud/reset

# Restore via CLI (recommended)
jaiscloud-aws import -i snapshot.tar.gz

# Restore via HTTP
curl -X POST http://localhost:4566/_jaiscloud/import \
     -H "Content-Type: application/gzip" \
     --data-binary @snapshot.tar.gz
```

Add `?dry_run=true` to the import endpoint to validate the tarball without applying it.

### Spark driver pod credentials

When an EMR step or EMR on EKS job run is submitted by account B, JaisCloud injects account B's credentials into the Spark driver pod environment. The driver and executor pods SigV4-sign as account B, so all S3, STS, and IAM calls from within the Spark job land in B's resource stores:

```
EMR RunJobFlow (account B)
  → Spark driver pod env:
      AWS_ACCESS_KEY_ID  = "222222222222"   ← account B's 12-digit ID
      AWS_REGION         = "us-east-1"      ← step's region
      AWS_ENDPOINT_URL   = "http://jaiscloud:4566"
```

This is handled automatically by the EMR provider — no manual credential injection is needed.

---

## Running Tests

### Unit tests (no server needed)

```bash
go test -race ./internal/...
```

### Integration tests

Start the server first, then run:

```bash
./jaiscloud-aws start &
go test -race -count=1 ./tests/integration/

# Run a specific service
go test -race -run TestSQS ./tests/integration/
go test -race -run TestEMR ./tests/integration/
go test -race -run TestEventBridge ./tests/integration/
```

Integration tests automatically call `POST /_jaiscloud/reset` between each test case via `resetState(t)`. You do not need to restart the server between runs.

Current integration test coverage: SQS, IAM/STS, SNS, DynamoDB, S3, Lambda, EC2, Route53, RDS, ElastiCache, ECS, Glue, CloudFormation, DynamoDB Streams, EMR, EMR Containers, EventBridge.

### Multi-account integration tests

Multi-account tests live in `tests/integration/multiaccount/` and require the same running server as the standard integration tests. They use LSIA-encoded access keys to simulate multiple accounts hitting the same emulator concurrently.

```bash
./jaiscloud-aws start &
go test -race -count=1 ./tests/integration/multiaccount/

# Run a specific surface
go test -race -run TestGetCallerIdentity ./tests/integration/multiaccount/
go test -race -run TestSQS_AccountIsolation ./tests/integration/multiaccount/
go test -race -run TestKMS ./tests/integration/multiaccount/
go test -race -run TestResetScope ./tests/integration/multiaccount/
go test -race -run TestExport ./tests/integration/multiaccount/
```

| Test file | Coverage |
|---|---|
| `sts_test.go` | `GetCallerIdentity` per-account, `AssumeRole` cross-account, assumed-role ARN shape |
| `isolation_test.go` | SQS queue/message isolation, DynamoDB table/item isolation, KMS cross-account blob rejection |
| `reset_scope_test.go` | Scoped reset by account+region, by account, global reset |
| `cross_service_test.go` | SNS→SQS cross-account fan-out, SecretsManager isolation |
| `export_round_trip_test.go` | Schema v3 verification, export→reset→import round-trip |

---

### Postgres backend integration tests

Postgres backend tests require a running PostgreSQL instance and a JaisCloud server started with `--dsn`. They share the same test files as the standard integration tests (`tests/integration/`) but run against a PostgreSQL store.

#### Prerequisites

| Tool | Purpose |
|---|---|
| PostgreSQL 14+ | Persistent store backend |
| `go` 1.26+ | Test runner |

#### 1. Start PostgreSQL

```bash
docker run -d \
  --name jaiscloud-pg \
  -e POSTGRES_USER=jaiscloud \
  -e POSTGRES_PASSWORD=jaiscloud \
  -e POSTGRES_DB=jaiscloud \
  -p 5432:5432 \
  postgres:16-alpine

# Wait for it to be ready
docker exec jaiscloud-pg pg_isready -U jaiscloud
```

#### 2. Build and start JaisCloud with the PostgreSQL backend

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
./jaiscloud-aws start \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud" &
```

Migrations run automatically on startup. You should see:
```
INFO jaiscloud started port=4566 mode=postgres
```

#### 3. Run the integration tests

```bash
go test -race -count=1 ./tests/integration/
```

The tests call `POST /_jaiscloud/reset` between cases, which truncates all PostgreSQL tables. No manual wipe is needed between runs.

#### 4. Verify persistence across restarts

```bash
# Create a resource
aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs create-queue --queue-name persist-test

# Kill and restart the server
kill %1
./jaiscloud-aws start \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud" &

# Queue should still be there
aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs get-queue-url --queue-name persist-test
```

#### Environment variable override

```bash
# Point at a different host (e.g. a remote JaisCloud instance)
JAISCLOUD_HOST=http://my-remote-host:4566 go test -race -count=1 ./tests/integration/
```

---

### Spark e2e tests (`spark_e2e` build tag)

The Spark end-to-end tests cover the full EMR + EMR-on-EKS + EventBridge notification lifecycle. They live under `tests/persistent_mode/aws/emr/`, `tests/persistent_mode/aws/emrcontainers/`, and `tests/persistent_mode/aws/eventbridge/`, and are **excluded from normal CI runs** by the `//go:build spark_e2e` tag. Use the Makefile targets which handle server lifecycle via docker-compose or K8s.

#### Prerequisites (both modes)

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.26 | Test runner |
| Docker | 20+ | Required for Docker mode; used by K8s mode to build images |
| PostgreSQL | 14+ | JaisCloud full-mode store |
| JaisCloud server | latest | Running with `--dsn` (PostgreSQL backend) |

Use the Makefile to start the server and run tests:

```bash
# EMR Docker tests (server starts via docker-compose)
make test-e2e-emr-docker SPARK_IMAGE=apache/spark:3.5.0

# EMR Containers K8s tests (server deployed to K8s)
make test-e2e-emrcontainers-k8s SPARK_IMAGE=apache/spark:3.5.0

# EventBridge tests
make test-e2e-eventbridge

# Narrow to a specific test
make test-e2e-emrcontainers-k8s TEST_RUN=TestSparkJob_K8s_CancelJobRun
```

Or run manually:

#### Docker mode — EMR steps running SparkPi in a container

```bash
# Start server via docker-compose
make up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_K8S_SPARK_IMAGE=apache/spark:3.5.0

# Run tests
SPARK_E2E_DOCKER_IMAGE=apache/spark:3.5.0 JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags spark_e2e -timeout 10m ./tests/persistent_mode/aws/emr/

make down-docker
```

| Environment variable | Default | Description |
|---|---|---|
| `SPARK_E2E_DOCKER_IMAGE` | — | **(required)** Spark Docker image to run |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `3s` | How often to poll step/job state |
| `SPARK_E2E_JOB_TIMEOUT` | `5m` | Max time to wait for a job to reach terminal state |

#### Kubernetes mode — EMR Containers job runs on a real cluster

```bash
# Deploy server to K8s
make up-k8s

# Run EMR Containers tests
SPARK_E2E_SPARK_IMAGE=apache/spark:3.5.0 SPARK_E2E_K8S_NAMESPACE=jaiscloud \
  JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags spark_e2e -timeout 15m ./tests/persistent_mode/aws/emrcontainers/

make down-k8s
```

| Environment variable | Default | Description |
|---|---|---|
| `SPARK_E2E_SPARK_IMAGE` | — | **(required)** Spark image for K8s executor |
| `SPARK_E2E_K8S_NAMESPACE` | `jaiscloud` | Kubernetes namespace for Spark pods |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `3s` | Polling interval |
| `SPARK_E2E_JOB_TIMEOUT` | `5m` | Job timeout |

#### EventBridge notification tests (no real Spark needed)

```bash
make test-e2e-eventbridge
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=mock
JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags spark_e2e -timeout 10m ./tests/persistent_mode/aws/eventbridge/
make down-docker
```

---

### Lambda e2e tests (`lambda_e2e` build tag)

Lambda e2e tests live under `tests/persistent_mode/aws/lambda/` and are **excluded from normal CI** by the `//go:build lambda_e2e` tag. Use the Makefile targets which start the server via docker-compose or K8s:

```bash
make test-e2e-lambda-docker   # Docker warm-pool executor
make test-e2e-lambda-k8s      # K8s warm-pod executor
```

All tests follow these conventions:
- `resetState(t)` calls `POST /_jaiscloud/reset` before each test.
- Skip guards (`requireLambdaDockerEnv`, `requireLambdaK8sEnv`) skip when the executor is not configured.
- Timeouts controlled via environment variables.

---

#### Lambda Docker mode

In Docker mode the Lambda executor starts a real Docker container per function (warm pool). Each invocation posts to the container's Lambda Runtime Interface Emulator (RIE) endpoint on port 8080. The container is reused across invocations until deleted or idle.

##### Prerequisites

| Requirement | Notes |
|---|---|
| Docker running | `docker info` must succeed |
| `JAISCLOUD_EXECUTOR_MODE=docker` | Controls executor selection at startup |
| A Lambda container image | Must implement the Lambda RIC. Use the included echo image or build your own. |

##### Using the included echo image

The repo includes a ready-to-use echo handler at `deploy/images/lambda-echo/`:

```bash
docker build -t jaiscloud-lambda-echo deploy/images/lambda-echo/
```

The handler simply echoes the event back: `def handler(event, context): return event`. Tests pass `Handler: "handler.handler"` when creating functions.

##### Running Docker mode tests

```bash
make test-e2e-lambda-docker LAMBDA_IMAGE=jaiscloud-lambda-echo
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_LAMBDA_IMAGE=jaiscloud-lambda-echo
LAMBDA_E2E_DOCKER_IMAGE=jaiscloud-lambda-echo JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags lambda_e2e -timeout 10m ./tests/persistent_mode/aws/lambda/
make down-docker
```

| Test | What it verifies |
|---|---|
| `TestLambda_Docker_*` | Docker warm-pool: cold start, warm reuse, concurrent invocations, delete cleanup |
| `TestLambda_ColdStartAfterReset` | Container re-created after `/_jaiscloud/reset` |
| `TestLambda_DeleteAndReCreate` | Delete + immediate re-create without collision in warm pool |
| `TestLambda_HealthAfterOrphanCleanup` | Server healthy after startup orphan cleanup |

##### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `LAMBDA_E2E_DOCKER_IMAGE` | — | **(required)** Lambda container image URI. Tests are skipped if unset. |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `LAMBDA_E2E_INVOKE_TIMEOUT` | `2m` | Max time to wait for a single invocation |
| `LAMBDA_E2E_POLL_INTERVAL` | `3s` | Polling interval for async checks |

##### Troubleshooting Docker mode

**Tests skip with "LAMBDA_E2E_DOCKER_IMAGE not set"**  
Export the variable: `export LAMBDA_E2E_DOCKER_IMAGE=jaiscloud-lambda-test`

**Cold start times out**  
The container image pull can take 30–60 s on first run. Pre-pull the image: `docker pull jaiscloud-lambda-test`. Increase `LAMBDA_E2E_INVOKE_TIMEOUT=5m`.

**"connection refused" during warm invocation**  
JaisCloud's Docker executor exposes the container on a random host port. Make sure Docker's host networking is working: `docker run --rm alpine ping host.docker.internal`.

**Container not cleaned up after `DeleteFunction`**  
Container teardown is asynchronous. Run `docker ps | grep jc-lambda` to check. Orphaned containers are named `jc-lambda-<functionName>-<id>` and can be removed with `docker rm -f`.

---

#### Lambda Kubernetes mode

In K8s mode the Lambda executor creates **one warm Pod + ClusterIP Service per function** (matching the Docker executor pattern). Invocations POST to the RIE endpoint on the Service; the Pod is reused across calls and garbage-collected when idle.

##### Prerequisites

| Requirement | Notes |
|---|---|
| Kubernetes cluster | Docker Desktop, kind, minikube, or a remote cluster |
| `JAISCLOUD_EXECUTOR_MODE=k8s` | Controls executor selection at startup |
| In-cluster RBAC | Apply `deploy/k8s/rbac.yaml` — grants Jobs, Pods, Services permissions in the `jaiscloud` namespace |
| A Lambda container image pullable from the cluster | Use the echo image or your own |

##### RBAC

Apply the included manifest (grants all needed permissions in the `jaiscloud` namespace):

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/rbac.yaml
```

##### Running K8s mode tests

```bash
make test-e2e-lambda-k8s LAMBDA_IMAGE=jaiscloud-lambda-echo
# or manually:
make up-k8s
LAMBDA_E2E_K8S_IMAGE=jaiscloud-lambda-echo JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags lambda_e2e -timeout 15m ./tests/persistent_mode/aws/lambda/
make down-k8s
```

| Test | What it verifies |
|---|---|
| `TestLambda_K8s_*` | K8s warm-pod: cold start, warm reuse, concurrent invocations |
| `TestLambda_ColdStartAfterReset` | Pod re-created after `/_jaiscloud/reset` |
| `TestLambda_DeleteAndReCreate` | Delete + immediate re-create without K8s pod collision |
| `TestLambda_HealthAfterOrphanCleanup` | Server healthy after startup orphan pod cleanup |

##### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `LAMBDA_E2E_K8S_IMAGE` | — | **(required)** Lambda image URI pullable from the cluster. Tests skip if unset. |
| `JAISCLOUD_EXECUTOR_MODE` | `mock` | Must be `k8s` when the server starts |
| `JAISCLOUD_K8S_APISERVER` | `https://kubernetes.default.svc` | K8s API server URL |
| `JAISCLOUD_K8S_CA_FILE` | in-cluster CA | PEM CA cert; unset = system roots |
| `JAISCLOUD_K8S_NAMESPACE` | `jaiscloud` | Namespace for Lambda Pods + Services |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `LAMBDA_E2E_INVOKE_TIMEOUT` | `2m` | Max time to wait for cold start + invocation |
| `LAMBDA_E2E_POLL_INTERVAL` | `3s` | Polling interval |

##### Watching warm pods during tests

```bash
# Watch pods in real time
kubectl get pods -n jaiscloud -l app=jaiscloud-lambda -w

# Inspect a warm pod
kubectl describe pod -n jaiscloud -l app=jaiscloud-lambda

# Read logs
kubectl logs -n jaiscloud -l app=jaiscloud-lambda --tail=50
```

##### Troubleshooting K8s mode

**Tests skip with "LAMBDA_E2E_K8S_IMAGE not set"**  
Export `LAMBDA_E2E_K8S_IMAGE` before running.

**Pod stuck in `Pending`**  
Image may not be pullable. Check `kubectl describe pod -n jaiscloud` for `ImagePullBackOff`. For kind/minikube: `kind load docker-image jaiscloud-lambda-echo`.

**"Forbidden" creating Pods/Services**  
Apply `deploy/k8s/rbac.yaml` and verify `kubectl auth can-i create pods -n jaiscloud --as=system:serviceaccount:jaiscloud:jaiscloud`.

**Invocation timeout**  
K8s cold start includes image pull + pod scheduling. Increase `LAMBDA_E2E_INVOKE_TIMEOUT=10m` on first run. Subsequent invocations reuse the warm pod and are fast.

---

### KMS · SecretsManager · SSM e2e tests (`kms_fullmode` build tag)

These tests live under `tests/persistent_mode/aws/kms/` and verify cross-service integration between KMS, SecretsManager, and SSM Parameter Store. They do **not** require Docker or Kubernetes.

```bash
make test-e2e-kms
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=mock
JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags kms_fullmode -timeout 10m ./tests/persistent_mode/aws/kms/
make down-docker
```

| Test | What it verifies |
|---|---|
| `TestKMS_SecretsManager_Integration` | Create KMS CMK → create secret with that key → `GetSecretValue` decrypts correctly |
| `TestKMS_SecretsManager_BinarySecret` | Binary secret stored and retrieved as `SecretBinary` (not `SecretString`) |
| `TestKMS_SecretsManager_RotateSecret` | `PutSecretValue` creates a new version; latest version is returned |
| `TestKMS_DisabledKey_BlocksSecretCreate` | Creating a secret with a disabled KMS key returns an error |
| `TestSSM_SecureString_KMSEncryption` | `PutParameter` with `SecureString` type encrypts via KMS; `GetParameter` with `WithDecryption=true` decrypts |
| `TestSSM_PathHierarchy_RecursiveVsNonRecursive` | Non-recursive listing returns only direct children; recursive returns all descendants |
| `TestSSM_PathPrefix_NoFalseMatch` | `/app` does not match `/appname/x` (trailing-slash normalization fix) |
| `TestSSM_ParameterHistory` | Overwriting a parameter increments version and records history entry |

No environment variables beyond `JAISCLOUD_HOST` are required for this group.

---

### CloudFormation e2e tests (`cfn_fullmode` build tag)

These tests live under `tests/persistent_mode/aws/cloudformation/` and verify that CloudFormation stacks provision, update, and delete real downstream resources (SQS queues, Lambda functions, KMS keys, SecretsManager secrets). They do **not** require Docker or Kubernetes.

```bash
make test-e2e-cloudformation
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=mock
JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags cfn_fullmode -timeout 10m ./tests/persistent_mode/aws/cloudformation/
make down-docker
```

| Test | Template resources | What it verifies |
|---|---|---|
| `TestCFN_StackProvisionsSQSAndLambda` | `AWS::SQS::Queue` + `AWS::Lambda::Function` | Stack reaches `CREATE_COMPLETE`; both resources queryable via their service APIs; `Outputs` reference them |
| `TestCFN_StackWithKMSKey_SecretRef` | `AWS::KMS::Key` + `AWS::SecretsManager::Secret` (with `KmsKeyId: !Ref AppKey`) | KMS key is enabled; secret is decryptable using that key |
| `TestCFN_UpdateStack_ChangesResources` | `AWS::SQS::Queue` (parameterized name) | Stack reaches `UPDATE_COMPLETE`; parameter value persisted in stack metadata |
| `TestCFN_DeleteStack_CascadesChildren` | `AWS::SQS::Queue` + `AWS::Lambda::Function` | After `DeleteStack`, both child resources return not-found from their service APIs |
| `TestCFN_StackParameters_DefaultsApplied` | Same as above, no explicit parameters | Template default values are applied; Lambda function created under the default name |
| `TestCFNVPCSmokeTest` | VPC + Subnets + IGW + RouteTable + Route | VPC stack reaches `CREATE_COMPLETE`; outputs contain VpcId; DescribeStackResources lists all resource types |
| `TestCFNSAMTransformFunction` | `AWS::Serverless::Function` (SAM Transform) | SAM template with `Transform: AWS::Serverless-2016-10-31` creates a real Lambda function |
| `TestCFNChangeSetAddResource` | SQS → SQS + SNS via changeset | Changeset `CREATE_COMPLETE`; execute adds SNS; both resources visible |
| `TestCFNChangeSetDeleteResource` | SQS + SNS → SQS via changeset | Changeset removes SNS; after execute only SQS remains |

No environment variables beyond `JAISCLOUD_HOST` are required for this group.

##### Template reference

The tests use inline templates. Key patterns covered:

| Pattern | Template syntax | Test |
|---|---|---|
| `Ref` to a parameter | `{"Ref": "FunctionName"}` | `TestCFN_StackParameters_DefaultsApplied` |
| `Fn::GetAtt` for output | `{"Fn::GetAtt": ["ProcessorFunction", "Arn"]}` | `TestCFN_StackProvisionsSQSAndLambda` |
| Cross-resource `Ref` | `"KmsKeyId": {"Ref": "AppKey"}` | `TestCFN_StackWithKMSKey_SecretRef` |

---

### Kinesis e2e tests (`kinesis_fullmode` build tag)

These tests live under `tests/persistent_mode/aws/kinesis/` and verify record persistence across shard iterators. No Docker required.

```bash
./jaiscloud-aws start &
go test -v -tags kinesis_fullmode ./tests/persistent_mode/aws/kinesis/
```

| Test | What it verifies |
|---|---|
| `TestKinesisGetRecordsPersistence` | PutRecord → GetShardIterator (TRIM_HORIZON) → GetRecords; asserts payload round-trips correctly |

---

### Step Functions e2e tests (`sfn_e2e` build tag)

Integration tests for the real ASL engine live in `tests/integration/stepfunctions_test.go` and run against a standard memory-mode server (no docker-compose needed).

```bash
./jaiscloud-aws start &
go test -v -run TestSFN ./tests/integration/
```

| Test | State type exercised |
|---|---|
| `TestSFN_PassState_InputOutput` | Pass — InputPath, OutputPath, Result |
| `TestSFN_ChoiceState_StringEquals` | Choice — StringEquals comparison |
| `TestSFN_MapState_Items` | Map — parallel item iteration |
| `TestSFN_WaitState_Seconds` | Wait — Seconds |
| `TestSFN_ParallelState` | Parallel — branch merge |
| `TestSFN_TaskState_Lambda` | Task — Lambda invocation |
| `TestSFN_ErrorHandling_Retry` | Retry with MaxAttempts + IntervalSeconds |
| `TestSFN_ErrorHandling_Catch` | Catch fallback on Lambda error |

---

### Apache Iceberg e2e tests (`iceberg_e2e` build tag)

Iceberg tests run a real Spark SQL job via Docker, write Iceberg tables to JaisCloud's S3 (backed by Glue catalog and DynamoDB lock table), and verify the results via the AWS SDK. They are **excluded from normal CI** by the `//go:build iceberg_e2e` tag.

#### Prerequisites

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.26 | Test runner |
| Docker | 20+ | Runs Spark SQL container |
| curl | any | JAR download script |
| JaisCloud | latest | S3, Glue, DynamoDB store |

Start JaisCloud (any storage configuration works — Glue, S3, and DynamoDB are all in-memory by default):

```bash
# Default (memory + periodic saves)
./jaiscloud-aws start

# Or with PostgreSQL
./jaiscloud-aws start --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
```

#### 1. Download Iceberg JARs

The Spark container needs Iceberg and AWS connector JARs that are not included in the base image. Download them once from Maven Central:

```bash
cd tests/persistent_mode/aws/iceberg/spark-iceberg
bash download-jars.sh
```

This creates a `jars/` directory containing:

| JAR | Version |
|---|---|
| `iceberg-spark-runtime-3.5_2.12` | 1.5.2 |
| `iceberg-aws-bundle` | 1.5.2 |
| `hadoop-aws` | 3.3.4 |
| `aws-java-sdk-bundle` | 1.12.262 |

#### 2. Build the Spark Iceberg Docker image

```bash
# Still inside tests/persistent_mode/iceberg/spark-iceberg/
docker build -t spark-iceberg-test .
cd ../../../..    # back to repo root
```

The Dockerfile extends `apache/spark:3.5.0` and copies the downloaded JARs into `/opt/spark/jars/`.

#### 3. Run the Iceberg tests

```bash
export SPARK_E2E_ICEBERG_IMAGE=spark-iceberg-test

go test -v -tags iceberg_e2e \
  -timeout 30m \
  ./tests/persistent_mode/aws/iceberg/
```

`TestMain` runs first and creates the shared infrastructure once for the entire suite:

| Resource | Type | Purpose |
|---|---|---|
| `iceberg-warehouse` | S3 bucket | Iceberg data and metadata files |
| `iceberg_test_db` | Glue database | Table catalog |
| `iceberg_lock` | DynamoDB table | Iceberg `DynamoDbLockManager` commit lock |

After all tests complete, `TestMain` deletes the Glue database and DynamoDB table. S3 objects are left intact for debugging unless `ICEBERG_CLEAN_S3=true` is set.

#### 4. Run a single Iceberg test

```bash
go test -v -tags iceberg_e2e \
  -run TestIceberg_GlueCatalog_WriteAndRead \
  -timeout 15m \
  ./tests/persistent_mode/aws/iceberg/
```

#### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `SPARK_E2E_ICEBERG_IMAGE` | — | **(required)** Docker image built in step 2 |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `5s` | Polling interval for Spark job completion |
| `SPARK_E2E_JOB_TIMEOUT` | `10m` | Max time to wait for a Spark SQL job |
| `ICEBERG_CLEAN_S3` | `false` | Set to `true` to delete S3 objects on teardown |

#### How it works

Each test writes and reads Iceberg tables through the Glue catalog:

```
Test (Go SDK) ─────── PutRule / CreateQueue (setup)
Spark SQL job ────── docker run spark-iceberg-test spark-sql
                         │
                         ├─ Glue catalog:  reads/writes metadata_location pointer
                         ├─ S3 (JaisCloud): reads/writes Parquet data and metadata files
                         └─ DynamoDB:       acquires commit locks via DynamoDbLockManager
Test (Go SDK) ─────── DescribeTable / GetObject (verify)
```

Spark connects to JaisCloud via `host.docker.internal` so it can reach the host's port 4566. The Docker `--add-host=host.docker.internal:host-gateway` flag is set automatically by the test runner.

#### Troubleshooting

**`SPARK_E2E_ICEBERG_IMAGE not set — skipping`**  
Set `export SPARK_E2E_ICEBERG_IMAGE=spark-iceberg-test` before running.

**Spark job times out**  
Increase `SPARK_E2E_JOB_TIMEOUT=20m` and check Docker resource limits (Spark needs at least 2 GB RAM).

**`Connection refused` from inside the Spark container**  
JaisCloud must be reachable at `host.docker.internal:4566` from the container. On Linux, ensure Docker is started with `--add-host=host.docker.internal:host-gateway` (the test runner adds this automatically).

**JAR download fails**  
Rerun `bash download-jars.sh` — it skips already-downloaded files. Check your internet connection and Maven Central reachability.

### Race detector

Always run with `-race`. JaisCloud is heavily concurrent and the race detector catches real bugs:

```bash
go test -race ./...
```

---

## Platform Runtime Layer

The Platform Runtime Layer (`internal/platform/`) injects TLS certificate trust, extra volumes, and environment variables uniformly into **every** JaisCloud-managed container or pod — Lambda Docker/K8s executors and Spark Docker/K8s executors alike. Configuration is loaded once at startup via `platform.LoadFromEnv()` and passed by pointer to each executor constructor. It never embeds into any workload-specific config.

### TLS CA injection

Two materializers run in order whenever `JAISCLOUD_PLATFORM_TLS_ENABLED=true`:

| Materializer | What it does |
|---|---|
| **JVM truststore** | Copies the default JVM `cacerts` into an `emptyDir` and imports all CA certs via `keytool`. Sets `JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=...` on the main container. |
| **PEM bundle** | Concatenates all CA files into a single PEM bundle and sets `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `NODE_EXTRA_CA_CERTS`, `AWS_CA_BUNDLE`, `GIT_SSL_CAINFO`, `CURL_CA_BUNDLE` on the main container. |

CA sources are specified as a JSON or YAML array via `JAISCLOUD_PLATFORM_TLS_CA_SOURCES`:

```bash
# Single ConfigMap source (K8s mode — default if unset)
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"jaiscloud","source":{"kind":"configMap","name":"jaiscloud-ca-cert","key":"ca.crt"}}
]'

# Multiple sources (ConfigMap + Secret)
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"corp-ca","source":{"kind":"configMap","name":"corp-ca","key":"ca.pem"}},
  {"name":"internal","source":{"kind":"secret","name":"internal-ca","key":"tls.crt"}}
]'

# Local file (Docker mode — only "kind":"file" sources can be read by the host process)
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"my-ca","source":{"kind":"file","key":"/etc/ssl/certs/my-ca.pem"}}
]'
```

> **Docker mode note:** Only `kind="file"` CA sources can be materialised on the Docker host. `configMap` and `secret` sources are skipped with a warning — they are K8s-only. For Docker deployments, use `kind="file"` sources pointing to host-local PEM files.

Disable TLS injection entirely:

```bash
export JAISCLOUD_PLATFORM_TLS_ENABLED=false
```

### Extra volumes

Inject additional volumes into every pod/container via `JAISCLOUD_PLATFORM_VOLUMES` (JSON/YAML array, or `_FILE` variant for a file path). Both a short-form and a long-form spec are supported.

**Short form** — convenience for the common cases:

```bash
export JAISCLOUD_PLATFORM_VOLUMES='[
  {"name":"my-configmap", "configMap":"my-cm-name",   "mountPath":"/etc/conf"},
  {"name":"my-secret",    "secret":"my-secret-name",  "mountPath":"/etc/secret",  "readOnly":true},
  {"name":"my-pvc",       "pvc":"my-pvc-claim",       "mountPath":"/data"},
  {"name":"scratch",      "emptyDir":true,             "mountPath":"/tmp/scratch"}
]'
```

**Long form** — for projected volumes, CSI, hostPath, and per-mount subPath:

```bash
export JAISCLOUD_PLATFORM_VOLUMES='[
  {
    "name": "azure-workload-id",
    "source": {
      "kind": "projected",
      "projected": {
        "sources": [
          {"serviceAccountToken": {"audience":"api://AzureADTokenExchange","expirationSeconds":3600,"path":"token"}}
        ]
      }
    },
    "mounts": [{"mountPath":"/var/run/secrets/azure/identity"}]
  }
]'
```

> **Docker mode note:** Only `kind="hostPath"` volumes produce `-v` bind-mount args. `configMap`, `secret`, `pvc`, and `projected` volumes are K8s-only and are silently skipped in Docker mode.

### Extra environment variables

Inject extra env vars into every pod/container:

```bash
export JAISCLOUD_PLATFORM_ENV='{"CUSTOM_VAR":"value","ANOTHER":"value2"}'

# Or via file
export JAISCLOUD_PLATFORM_ENV_FILE=/etc/jaiscloud/extra-env.json
```

### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `JAISCLOUD_PLATFORM_TLS_ENABLED` | `true` | Enable CA injection |
| `JAISCLOUD_PLATFORM_TLS_CA_SOURCES` | JaisCloud default ConfigMap | JSON/YAML CA source array |
| `JAISCLOUD_PLATFORM_TLS_CA_SOURCES_FILE` | _(empty)_ | File-based variant; takes precedence over the inline var |
| `JAISCLOUD_PLATFORM_TLS_CLIENT_CERT` | _(empty)_ | Optional mTLS client cert source (same shape as one CA source element) |
| `JAISCLOUD_PLATFORM_TLS_PASSWORD` | `changeit` | JVM truststore password |
| `JAISCLOUD_PLATFORM_VOLUMES` | _(empty)_ | JSON/YAML volume spec array |
| `JAISCLOUD_PLATFORM_VOLUMES_FILE` | _(empty)_ | File-based variant |
| `JAISCLOUD_PLATFORM_ENV` | _(empty)_ | JSON/YAML `{"KEY":"VALUE"}` map |
| `JAISCLOUD_PLATFORM_ENV_FILE` | _(empty)_ | File-based variant |
| `JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST` | _(empty)_ | Comma-separated allowed `hostPath` prefixes; empty = hostPath disabled |

### Docker mode behaviour

`platform.ApplyDocker` is called inside `DockerExecutor.Submit` / `startContainer` and returns `-v` and `-e` args appended to the `docker run` command:

- TLS PEM bundle materialised to a host temp file → `-v /tmp/jaiscloud-ca-bundle-*.pem:/etc/ssl/certs/jaiscloud-ca.pem:ro`
- All six non-JVM env vars set to the container path → `-e SSL_CERT_FILE=/etc/ssl/certs/jaiscloud-ca.pem ...`
- `hostPath` volumes → `-v host/path:container/path:ro`
- Extra env → `-e KEY=VALUE`

### Kubernetes mode behaviour

`platform.ApplyK8s` is called inside `K8sExecutor.buildJobManifest` / `createPod` and mutates the `k8stypes.PodSpec` in place **after** the cloud-specific transform has already contributed its volumes and env:

1. CA source volumes added to `spec.volumes`
2. JVM materializer: `emptyDir` output volume + init container (keytool import) + main container env
3. PEM materializer: `emptyDir` output volume + init container (concatenate) + main container env vars
4. Platform extra volumes appended (conflict-checked against existing names)
5. Platform extra volume mounts added to the main container
6. Platform extra env merged into the main container env (first-wins deduplication)

---

## Multi-Cloud Spark Transforms

The `CloudSparkTransform` registry (`internal/executor/spark/cloud_transform.go`) decouples cloud-specific Spark contributions from the executor. Each cloud registers itself via `init()` and is selected at manifest build time from `SparkConfig.Cloud`, which is set to the cloud's name by the binary's `main.go` (e.g. `"aws"` for `jaiscloud-aws`, `"azure"` for `jaiscloud-azure`).

| Cloud | Transform | URI validation | Auth mechanism |
|---|---|---|---|
| `aws` | `awsTransform` | `s3://`, `s3a://` accepted; others rejected | S3 endpoint env + Hadoop S3A confs |
| `azure` | `azureTransform` | `abfss://` accepted; `s3a://` rejected | SharedKey or OAuth/Workload Identity |
| `gcp` | `gcpTransform` | `gs://` accepted; `s3a://` rejected | Service account key file or K8s Secret |

Each transform contributes:
- `ResolveCommand` — binary path + rewritten `spark-submit` args
- `SparkConfs` — `--conf` flags injected before the JAR path
- `PodEnv` — environment variables for the spark-submit container
- `PodVolumes` — cloud-specific volumes and mounts (credentials, identity tokens)

### Azure Spark transform

Use the `jaiscloud-azure` binary. Configure one of two authentication modes:

**Shared key** (simpler, for dev):

```bash
export JAISCLOUD_AZURE_STORAGE_ACCOUNT=mystorageacct
export JAISCLOUD_AZURE_STORAGE_KEY=base64encodedkey==
```

This injects `fs.azure.account.auth.type.*.dfs.core.windows.net=SharedKey` and the account key into Spark confs. URIs are rewritten `s3a://bucket/key` → `abfss://bucket@mystorageacct.dfs.core.windows.net/key`.

**OAuth / Workload Identity** (for production K8s):

```bash
export JAISCLOUD_AZURE_STORAGE_ACCOUNT=mystorageacct
export JAISCLOUD_AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export JAISCLOUD_AZURE_CLIENT_SECRET=my-client-secret
export JAISCLOUD_AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

This injects OAuth confs and mounts a `projected` volume (`serviceAccountToken` + `downwardAPI`) at `/var/run/secrets/azure/identity` for Workload Identity federation.

Optionally override the ADLS endpoint:

```bash
export JAISCLOUD_AZURE_STORAGE_ENDPOINT=https://mystorageacct.dfs.core.windows.net
```

### GCP Spark transform

Use the `jaiscloud-gcp` binary. Configure one of two service-account modes:

**Key file path** (the key is already present on the container filesystem):

```bash
export JAISCLOUD_GCP_PROJECT_ID=my-gcp-project
export JAISCLOUD_GCP_SA_KEY_PATH=/etc/gcp/key.json
```

**K8s Secret** (the key is stored in a Kubernetes Secret and mounted by the executor):

```bash
export JAISCLOUD_GCP_PROJECT_ID=my-gcp-project
export JAISCLOUD_GCP_SA_SECRET=my-gcp-sa-key-secret   # K8s Secret name in the executor namespace
```

The executor mounts the Secret at `/etc/gcp` and sets `GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/key.json`. URIs are rewritten `s3a://bucket/key` → `gs://bucket/key`. GCS Spark confs (`spark.hadoop.google.cloud.auth.service.account.enable=true`, `spark.hadoop.fs.gs.project.id`) are injected automatically.

Optionally override the GCS endpoint (for emulators):

```bash
export JAISCLOUD_GCP_STORAGE_ENDPOINT=http://fake-gcs-server:4443
```

---

## Executor Mode Configuration Examples

Complete environment variable sets for each executor mode. Copy the block that matches your setup and adjust paths / names.

### Mock mode (default — no containers, instant completion)

No extra configuration needed. This is the default when `JAISCLOUD_EXECUTOR_MODE` is unset.

```bash
# Minimal — everything defaults to in-memory mock
./jaiscloud-aws start

# Explicit mock with PostgreSQL backend
./jaiscloud-aws start \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud" \
  --executor-mode mock
```

All Lambda invocations return an echo of the request payload. All EMR steps and job runs complete immediately with `COMPLETED` state. No Docker daemon or Kubernetes cluster needed.

---

### Docker mode

Requires a running Docker daemon. Each Lambda function gets a warm Docker container; each Spark job runs `spark-submit` inside a Docker container.

```bash
# ── Core ──────────────────────────────────────────────────────────────────────
export JAISCLOUD_EXECUTOR_MODE=docker
export JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud

# ── Lambda ────────────────────────────────────────────────────────────────────
export JAISCLOUD_LAMBDA_IMAGE=public.ecr.aws/lambda/python:3.12  # default per-runtime; override globally here
export JAISCLOUD_LAMBDA_NETWORK=jaiscloud-net                     # Docker network containers join
export JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300                        # idle container TTL

# ── Spark (EMR / EMR on EKS) ──────────────────────────────────────────────────
export JAISCLOUD_K8S_SPARK_IMAGE=apache/spark:3.5.0   # Spark image for all executor modes

# Optional: inject JaisCloud's S3 endpoint into Spark containers
export JAISCLOUD_AWS_EMULATOR_ENDPOINT=http://host.docker.internal:4566
export JAISCLOUD_REGION=us-east-1

# ── Platform layer ─────────────────────────────────────────────────────────────
# TLS: disable if no custom CA is needed
export JAISCLOUD_PLATFORM_TLS_ENABLED=false

# TLS: inject a local CA PEM file into every container
# export JAISCLOUD_PLATFORM_TLS_ENABLED=true
# export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[{"name":"corp-ca","source":{"kind":"file","key":"/etc/ssl/certs/corp-ca.pem"}}]'

# Extra env passed to every managed container
export JAISCLOUD_PLATFORM_ENV='{"HTTP_PROXY":"http://proxy.corp:3128","NO_PROXY":"localhost"}'

# hostPath bind-mounts (must be in allowlist)
export JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST=/etc/ssl/certs,/run/secrets
export JAISCLOUD_PLATFORM_VOLUMES='[{"name":"corp-certs","mountPath":"/etc/corp/certs","hostPath":"/etc/ssl/certs","readOnly":true}]'

./jaiscloud-aws start
```

**Verify Docker executor is active:**
```bash
# After starting, you should see in the logs:
# INFO  executor  lambda=docker  spark=docker
# INFO  platform tls  enabled=false  ...

# Lambda cold start creates a container visible via:
docker ps --filter name=jc-lambda-
# Spark jobs create containers visible via:
docker ps --filter name=jc-spark-
```

---

### Kubernetes mode

Requires a reachable Kubernetes cluster. Each Lambda function gets a warm Pod + ClusterIP Service; each Spark job submits a `batch/v1` Job.

```bash
# ── Core ──────────────────────────────────────────────────────────────────────
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud

# ── Kubernetes API server ─────────────────────────────────────────────────────
# In-cluster (JaisCloud runs inside a pod): leave these unset — auto-detected
# Out-of-cluster (local dev / CI):
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443          # kubectl cluster-info
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n jaiscloud --duration=24h)
export JAISCLOUD_K8S_CA_FILE=$HOME/.kube/ca.crt                # unset = system roots
export JAISCLOUD_K8S_NAMESPACE=jaiscloud
export JAISCLOUD_K8S_SA=jaiscloud-sa

# ── Lambda ────────────────────────────────────────────────────────────────────
export JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300    # idle pod TTL before garbage collection

# ── Spark (EMR / EMR on EKS) ──────────────────────────────────────────────────
export JAISCLOUD_K8S_SPARK_IMAGE=apache/spark:3.5.0   # Spark image for K8s executor
export JAISCLOUD_K8S_SPARK_SA=spark-sa                # service account for the Spark driver

# Optional: inject JaisCloud's S3 endpoint into Spark pods
export JAISCLOUD_AWS_EMULATOR_ENDPOINT=http://jaiscloud.jaiscloud.svc.cluster.local:4566
export JAISCLOUD_REGION=us-east-1

# ── Platform layer ─────────────────────────────────────────────────────────────
# TLS: inject a CA from a ConfigMap that already exists in the cluster
export JAISCLOUD_PLATFORM_TLS_ENABLED=true
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"corp-ca","source":{"kind":"configMap","name":"corp-ca-bundle","key":"ca.crt"}}
]'
# Password for the JVM truststore init container
export JAISCLOUD_PLATFORM_TLS_PASSWORD=changeit

# Extra env injected into every pod's main container
export JAISCLOUD_PLATFORM_ENV='{"CUSTOM_ENDPOINT":"http://internal-service:8080"}'

# Extra volume from a Secret mounted at a specific path
export JAISCLOUD_PLATFORM_VOLUMES='[
  {"name":"corp-tls","secret":"corp-tls-secret","mountPath":"/etc/corp/tls","readOnly":true}
]'

./jaiscloud-aws start
```

**Verify K8s executor is active:**
```bash
# After starting you should see in the logs:
# INFO  executor  lambda=k8s  spark=k8s
# INFO  platform tls  enabled=true  ca-sources=[corp-ca]  ...

# Lambda warm pods:
kubectl get pods -n jaiscloud -l app=jaiscloud-lambda

# Lambda ClusterIP services (one per function):
kubectl get svc -n jaiscloud -l app=jaiscloud-lambda

# Spark batch jobs:
kubectl get jobs -n jaiscloud -l app=jaiscloud-spark
```

---

### Azure / GCP with K8s executor

Each cloud has its own binary. The Platform layer and K8s executor configuration apply identically regardless of which binary you run.

**Azure (K8s executor, OAuth auth):**
```bash
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n jaiscloud --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=jaiscloud

export JAISCLOUD_AZURE_STORAGE_ACCOUNT=mystorageacct
export JAISCLOUD_AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export JAISCLOUD_AZURE_CLIENT_SECRET=my-client-secret
export JAISCLOUD_AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

export JAISCLOUD_PLATFORM_TLS_ENABLED=true
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"azure-ca","source":{"kind":"configMap","name":"azure-ca-bundle","key":"ca.crt"}}
]'

./jaiscloud-azure start --dsn "postgres://..."
```

**GCP (K8s executor, Secret-based SA key):**
```bash
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n jaiscloud --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=jaiscloud

export JAISCLOUD_GCP_PROJECT_ID=my-gcp-project
export JAISCLOUD_GCP_SA_SECRET=my-gcp-sa-key-secret   # K8s Secret in the executor namespace

export JAISCLOUD_PLATFORM_TLS_ENABLED=false   # GCP root CAs already trusted by default JVM

./jaiscloud-gcp start --dsn "postgres://..."
```

---

## Platform Setup

### macOS

```bash
chmod +x scripts/setup-mac.sh
./scripts/setup-mac.sh
```

This installs Homebrew, Go, Docker, AWS CLI and configures a `localcloud` AWS CLI profile with test credentials.

Manual AWS CLI profile setup:
```bash
aws configure set aws_access_key_id test --profile localcloud
aws configure set aws_secret_access_key test --profile localcloud
aws configure set region us-east-1 --profile localcloud

# Use the profile
aws --profile localcloud --endpoint-url http://localhost:4566 sqs list-queues
```

### Windows

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
.\scripts\setup-windows.ps1
```

Build and run:
```powershell
go build -o jaiscloud-aws.exe ./cmd/jaiscloud-aws/
.\jaiscloud-aws.exe start
```

### AWS CLI shorthand

Add this to your shell profile to avoid repeating `--endpoint-url` and `--region`:

```bash
alias awslocal='aws --endpoint-url http://localhost:4566 --region us-east-1 --no-cli-pager'

# Then use:
awslocal sqs create-queue --queue-name my-queue
awslocal s3 ls
awslocal emr list-clusters
```

---

## Contributing Code

### Adding a new AWS service

All AWS service wiring flows from a single source of truth: the `awsServices` slice in [internal/aws/adapter/services.go](internal/aws/adapter/services.go). Adding one `ServiceDescriptor` entry here automatically updates service detection, SigV4 allow-list, Action routing, and gateway provider mapping.

**Step 1 — Register the service descriptor:**

```go
// internal/aws/adapter/services.go
{
    SigV4Name:      "my-service",       // matches Authorization scope
    TargetPrefix:   "MyService",        // for JSON/Target; "" for Query or REST
    ProviderPrefix: "MyService",        // prefix for Registry dispatch ("MyService.CreateFoo")
    QueryActions:   []string{},         // for Query-protocol services only
},
```

**Step 2 — Implement the codec:**

Create `internal/aws/adapter/services/myservice.go`. Choose the protocol that matches the real AWS SDK:

- JSON/Target (DynamoDB-style): embed `BaseCodec`, implement `Decode` to read `X-Amz-Target`, `Encode` to write `application/x-amz-json-1.1`.
- Query/XML (SQS-style): parse form body, `Action=` param.
- REST/JSON (Lambda-style): extract action from path + HTTP method.

Register in `buildAWSAdapter()` in `cmd/jaiscloud-aws/main.go`:
```go
"my-service": &services.MyServiceCodec{},
```

**Step 3 — Implement the provider:**

Create `internal/aws/provider/myservice/myservice.go`. Follow the struct pattern used by every other provider:

```go
type MyServiceProvider struct {
    resources store.ResourceStore
    bus       *events.EventBus
}

func New(resources store.ResourceStore, bus *events.EventBus) *MyServiceProvider { ... }

func (p *MyServiceProvider) Routes() map[string]provider.HandlerFunc {
    return map[string]provider.HandlerFunc{
        "MyService.CreateFoo": p.CreateFoo,
    }
}
```

**Step 4 — Wire in `main.go`:**

```go
myServiceProvider := myservice.New(resources, bus)
registry.RegisterAll(myServiceProvider.Routes())
```

**Step 5 — Add the ARN format** (if needed):

```go
// internal/config/config.go — awsARNFormatters map
"my-service-resource": func(region, accountID, name string) string {
    return fmt.Sprintf("arn:aws:my-service:%s:%s:resource/%s", region, accountID, name)
},
```

Providers must never call `fmt.Sprintf("arn:aws:...")` directly — always use `nr.ResourceID("my-service-resource", name)`.

---

### Adding a new operation to an existing provider

1. Add the handler method to the provider struct (matches `provider.HandlerFunc` signature).
2. Add the route in `Routes()`.
3. Parameters arrive pre-decoded in `nr.Params` as `map[string]any`. Type-assert with the two-value form: `name, _ := nr.Params["Name"].(string)`.
4. Return `provider.OK(map[string]any{...})` for success; return `nil, &model.ProviderError{Code: "...", Message: "...", HTTPStatus: 404}` for errors.

---

### Provider layout conventions

**Per-cloud layout:**
- All AWS provider packages live under `internal/aws/provider/`. The binary at `cmd/jaiscloud-aws/` imports only `internal/aws/` packages.
- Shared infrastructure (store interfaces, event bus, clock, config) lives under `internal/` and may be imported by any cloud binary, but must never import cloud-specific code.

**Struct fields:** Keep the struct small. Standard fields: `resources store.ResourceStore`, `bus *events.EventBus`. For providers with background goroutines: `ctx context.Context`, `cancel context.CancelFunc`, `wg sync.WaitGroup`.

**Options pattern:** Use `With*` option functions for optional dependencies (e.g. `WithIdentityMutator`). This keeps constructors stable and avoids nil checks scattered through handler code.

---

### EMR provider patterns

#### `handlerCtx` — preserve cloud provenance in goroutines

`NormalizedRequest` must not be read from goroutines after the handler returns. Capture the triplet at handler entry:

```go
h := newHandlerCtx(nr)  // {cloud, region, accountID}
```

Pass `h` into all goroutines and `emit*` helpers. The type is defined in `emr/events.go` and `emroneks/events.go`.

#### WaitGroup lifecycle

Every `go func` that does real work must bracket with `p.wg.Add(1)` + `defer p.wg.Done()`. `Shutdown()` calls `p.cancel()` then `p.wg.Wait()` — there is no timeout, so goroutines must be written to exit promptly on `ctx.Done()`.

```go
p.wg.Add(1)
go func() {
    defer p.wg.Done()
    // check ctx.Done() at blocking points
}()
```

#### State event ordering

State transitions for steps must be emitted in strict order: PENDING before RUNNING. The pattern in `AddJobFlowSteps` is four explicit phases:

1. Build in-memory records.
2. Persist the cluster (`saveCluster`).
3. Emit all PENDING events.
4. Launch goroutines (which will emit RUNNING).

Never launch goroutines before PENDING events are emitted — the goroutine's first action is to emit the next state, which would arrive out of order.

#### `emitStepStateChange` / `emitClusterStateChange`

These helpers in `emr/events.go` call `updateStepRecord` (returns `(string, bool)`) and then publish to the EventBus. If `updateStepRecord` returns `ok=false`, log at ERROR but still publish the event so downstream subscribers are not blocked.

---

### sparkhelpers / k8shelpers patterns

#### `SubmitClientMode` — the only Spark submission path

Do not implement your own Job creation. Call `sparkhelpers.SubmitClientMode(ctx, k8s, job)` with a `ClientModeJob`:

| Field | Required | Purpose |
|---|---|---|
| `Image` | Yes | spark-submit container image |
| `EntryPoint` | Yes | JAR or Python file URI |
| `SparkSubmitArgs` | Yes | `--conf` entries, executor counts |
| `JarArgs` | No | Arguments after the entry point |
| `IdentityMutator` | No | Cloud-specific pod identity (IRSA, MI, WI) |
| `PlatformOverlay` | No | TLS init containers, CA env vars, volume mounts |
| `OwnerHint` | No | Used by OwnershipPatcher to backfill executor ownerRefs |

`SubmitClientMode` creates a ConfigMap for the executor pod template, creates the `batch/v1 Job`, then patches the ConfigMap's `ownerReferences` to point at the Job. If Job creation fails the ConfigMap is cleaned up.

#### `BuildPodSpec` and `IdentityMutator`

`k8shelpers.BuildPodSpec` takes `ctx` and `k8s kubernetes.Interface` and passes them to the `IdentityMutator` callback. Pass `nil` for both only when `IdentityMutator` is `nil`.

```go
type IdentityMutator func(ctx context.Context, k8s kubernetes.Interface, tpl *corev1.PodTemplateSpec) error
```

The mutator is the extension point for cloud-specific identity injection — do not special-case cloud names inside `BuildPodSpec`.

#### `StartOwnershipPatcher`

Start it in the provider's `New()` when a k8s client is present, stop it in `Shutdown()`. It watches executor pods (label `spark-role=executor`) and patches `ownerReferences` back to the parent Job. Without it, executor pods are orphaned and never GC'd.

```go
if k8sClient != nil {
    stop := k8shelpers.StartOwnershipPatcher(ctx, k8sClient, namespace, resolver)
    p.patcherStop = stop
}
```

---

### EventBridge envelope conventions

When adding a new EMR-style state-change event:

1. Add an event type constant to `internal/events/events.go`.
2. Add the payload struct with `Cloud model.Cloud` field.
3. Add a `build*Envelope` function in `internal/provider/events/eventbridge.go` that:
   - Sets `source` as `string(ev.Cloud) + ".emr"` (never hardcoded `"aws.emr"`).
   - Puts `stateChangeReason` as a **nested object** `{"code": ..., "message": ...}` in `detail` — do not also emit `stateChangeCode` as a top-level key.
   - Only includes optional string fields (`arn`, `stateDetails`, `createdBy`) when non-empty.
4. Subscribe in `eventbridge.go`'s `subscribeToStateChanges()`.

**Severity mapping** (`"ERROR"` / `"WARN"` / `"INFO"`) is derived from the terminal state string — terminal failure states map to `"ERROR"`, cancellation to `"WARN"`, success to `"INFO"`.

---

### Common mistakes

| Mistake | Correct approach |
|---|---|
| `fmt.Sprintf("arn:aws:...")` in a provider | `nr.ResourceID("type", name)` |
| Reading `nr` from a goroutine after handler return | Capture `handlerCtx` at handler entry |
| Launching goroutines before emitting PENDING events | Four-phase pattern: build → save → emit → launch |
| Returning `"" , nil` from record loaders to signal not-found | Return `(string, bool)` so callers distinguish empty-name from load failure |
| Adding `stateChangeCode` as a top-level EventBridge detail key | Nest it inside `stateChangeReason: {code, message}` |
| Hardcoding `"aws.emr"` as EventBridge source | `string(ev.Cloud) + ".emr"` |
| Creating a ConfigMap without ownerReferences | Patch ownerReferences to the Job after `SubmitJob` succeeds |
| Skipping `wg.Add` for background goroutines | Every goroutine that does work must pair with `wg.Add(1)` + `defer wg.Done()` |
| A new service missing from the `buildAWSAdapter()` codec map | Add `"sigv4name": &services.MyCodec{}` — detection and routing silently fail without it |
| Calling `p.resources.Get/List/Create` without account+region | Always pass `nr.AccountID, nr.Region` — omitting them routes to the wrong scope or panics |
| Using `p.awsEmulator.AccountID` / `p.awsEmulator.Region` directly in a step goroutine | Copy the config with the step's `h.accountID`/`h.region` before passing to `sparkaws.DriverEnv` |
| Cross-service call (SQS, Lambda) with a bare name instead of full ARN | Pass the full ARN so the dispatcher can extract the target account and region |
