# JaisCloud

<p align="center">
  <img src="docs/images/jaiscloud-hero.png" alt="JaisCloud — AI Agent to multi-cloud (AWS, Azure, GCP) emulation" width="100%"/>
</p>

> **Early Development Notice**
> JaisCloud is under active development. While core services are functional and tested, some operations may have incomplete implementations, behavioural differences from AWS, or known bugs. If you encounter an issue, please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) with a minimal reproduction — your report directly shapes what gets fixed next.

**JaisCloud — Free AWS emulator for developers and CI. Single Go binary. Runs anywhere: laptop, CI, or Kubernetes.**

JaisCloud is a free, open-source AWS emulator that lets developers test AWS applications locally without touching real cloud infrastructure. It implements the exact AWS wire protocols — no SDK shims, no proxy rewrites — so your existing code works against JaisCloud unmodified. Use it as a drop-in LocalStack alternative with zero runtime dependencies.

**One binary per cloud.** Each binary is fully self-contained. There is no `--cloud` flag.

| Cloud | Binary | Status |
|---|---|---|
| AWS | `jaiscloud-aws` | Full implementation |
| Azure | `jaiscloud-azure` | In pipeline |
| GCP | `jaiscloud-gcp` | In pipeline — see the [GCP guide](README-GCP.md) |

---

## Key Features

### Portable State Snapshots

JaisCloud can export its complete state to a single gzip tarball and restore it on any other instance — a colleague's laptop, a CI runner, a staging environment — in milliseconds. Every account, every region, every resource type in one file.

**For development teams:**
- Capture a production-like baseline once and share the tarball across the team — no per-developer setup scripts
- Reproduce a bug exactly: export the state right before the failure, attach the file to the ticket, and anyone can reproduce it with one command
- Seed fresh environments from a known-good snapshot instead of running fixture scripts from scratch

**For CI/CD pipelines:**
- Import a pre-seeded snapshot at pipeline start instead of rebuilding state on every run — test setup that used to take minutes drops to under a second
- Use named snapshots to revert to a clean baseline between test suites without restarting the emulator
- Snapshot files travel with your repo or as CI artifacts — no shared state server required

```bash
# Capture full emulator state
jaiscloud-aws export -o baseline.tar.gz

# Restore on any machine in any environment
jaiscloud-aws import -i baseline.tar.gz

# Named snapshots for fine-grained test control
jaiscloud-aws snapshot create --name before-migration
# ... run tests that mutate state ...
jaiscloud-aws snapshot revert before-migration
```

The snapshot format is an open versioned JSON envelope inside a standard gzip tarball — no proprietary format, no lock-in. Works in every mode: memory, file-backed, and PostgreSQL. **No subscription, no license key, no usage limit.**

---

### Real Spark / EMR Execution

Most cloud emulators simulate compute: they accept the API call and immediately return "job completed." JaisCloud actually runs the Spark job.

In Kubernetes or Docker executor mode, submitting an EMR job causes JaisCloud to:

1. Build the Spark driver and executor spec using the same topology as AWS EMR on EKS
2. Submit a real `batch/v1 Job` to your local Kubernetes cluster (Docker Desktop, Minikube, or any cluster), or a Docker container in Docker mode
3. Stream logs and propagate real state transitions — `PENDING → RUNNING → COMPLETED / FAILED` — at the same granularity as AWS

**What this unlocks:**
- Run the exact PySpark or Scala job you'd submit to AWS EMR against a local cluster — catch serialization bugs, memory pressure, and dependency conflicts before they reach CI
- Write Functional Integration Tests that exercise the full Spark execution path, not a mock, and run them in any environment with a container runtime
- The JaisCloud endpoint is injected into the Spark driver pod at submission time, so jobs that read from S3 or write to DynamoDB work against the local emulator with zero code changes

```bash
# Start with Kubernetes executor
JAISCLOUD_EXECUTOR_MODE=k8s jaiscloud-aws start

# Submit an EMR on EKS job — JaisCloud submits a real K8s Job
aws emr-containers start-job-run \
  --virtual-cluster-id my-cluster \
  --execution-role-arn arn:aws:iam::000000000000:role/emr-role \
  --release-label emr-6.10.0-latest \
  --job-driver '{"sparkSubmitJobDriver":{"entryPoint":"s3://my-bucket/job.py","entryPointArguments":["--input","s3://my-bucket/data"]}}'

# Start with Docker executor (no Kubernetes required)
JAISCLOUD_EXECUTOR_MODE=docker jaiscloud-aws start
```

---

### Everything Free — No Subscription Required

JaisCloud has no tiers, no license keys, and no usage limits. Every feature in this table is Apache-2.0 open-source.

---

## Supported AWS Services

### Full implementations

These services implement real business logic and pass the AWS SDK integration test suite.

| Service |
|---------|
| Amazon S3 |
| Amazon SQS |
| Amazon DynamoDB + Streams |
| Amazon SNS |
| Amazon EventBridge |
| AWS IAM + STS |
| AWS Lambda |
| AWS Glue Data Catalog |
| Amazon Kinesis |
| Amazon EMR (on EC2) |
| Amazon EMR on EKS |
| AWS KMS |
| AWS Secrets Manager |
| AWS SSM Parameter Store |
| AWS API Gateway (REST) |
| AWS CloudFormation |
| Amazon CloudWatch + Logs |
| AWS Step Functions |

### Metadata-only

These services implement the full wire protocol and resource CRUD but have no execution engine.

EC2 · Route 53 · RDS · ElastiCache · ECS · EKS · ELBv2 · ECR · ACM · Kinesis Firehose · AWS Config · Resource Groups · Redshift · Athena

### Stub

SES · Cognito (User Pools + Identity Pools)

For per-operation coverage details see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## Quick Start

Three steps: install, start, connect.

### 1. Install

```bash
# macOS
brew tap jaisrajms/homebrew-tap && brew install --cask jaiscloud-aws

# Docker (any platform)
docker pull jaisraj/jaiscloud-aws:latest

# Or download a pre-built binary from the Releases page (no Go required)
```

Full installation options are in the [Install](#install) section below.

### 2. Start

```bash
jaiscloud-aws start
# Listening on http://localhost:4566
```

### 3. Connect

```bash
# Set once — every AWS CLI call routes to JaisCloud automatically
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws s3 mb s3://my-bucket
aws sqs create-queue --queue-name my-queue
aws dynamodb list-tables
```

---

## Install

### Homebrew (macOS)

```bash
brew tap jaisrajms/homebrew-tap
brew install --cask jaiscloud-aws
jaiscloud-aws start
```

### Scoop (Windows)

```powershell
scoop bucket add jaiscloud https://github.com/jaisrajms/scoop-jaiscloud
scoop install jaiscloud-aws
jaiscloud-aws start
```

### Download binary (all platforms — no Go required)

Pre-built binaries are published on the [GitHub Releases page](https://github.com/jaisrajms/jaiscloud/releases).

| Platform | Download |
|----------|----------|
| macOS arm64 (Apple Silicon) | `jaiscloud-aws_<version>_darwin_arm64.tar.gz` |
| macOS amd64 (Intel) | `jaiscloud-aws_<version>_darwin_amd64.tar.gz` |
| Linux amd64 | `jaiscloud-aws_<version>_linux_amd64.tar.gz` · `.deb` · `.rpm` |
| Linux arm64 | `jaiscloud-aws_<version>_linux_arm64.tar.gz` · `.deb` · `.rpm` |
| Windows amd64 | `jaiscloud-aws_<version>_windows_amd64.zip` |
| Windows arm64 | `jaiscloud-aws_<version>_windows_arm64.zip` |

```bash
# Replace VERSION with the release version (e.g. 0.1.0) from the Releases page above.

# macOS arm64 (Apple Silicon)
curl -LO https://github.com/jaisrajms/jaiscloud/releases/download/vVERSION/jaiscloud-aws_VERSION_darwin_arm64.tar.gz
tar -xzf jaiscloud-aws_VERSION_darwin_arm64.tar.gz
sudo mv jaiscloud-aws /usr/local/bin/
jaiscloud-aws start

# Linux amd64 (Debian/Ubuntu)
curl -LO https://github.com/jaisrajms/jaiscloud/releases/download/vVERSION/jaiscloud-aws_VERSION_linux_amd64.deb
sudo dpkg -i jaiscloud-aws_VERSION_linux_amd64.deb
jaiscloud-aws start
```

A `checksums.txt` is published alongside every release for verification.

### Docker

```bash
docker run -p 4566:4566 jaisraj/jaiscloud-aws:latest
```

### Docker Compose (with Postgres persistence)

```bash
make up-docker    # starts Postgres + JaisCloud
make down-docker
```

### Build from source (requires Go 1.26+)

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
./jaiscloud-aws start
```

---

## Connect your SDK

Point any SDK at `http://localhost:4566` with dummy credentials — no code changes required.

### AWS CLI

The simplest approach is to set `AWS_ENDPOINT_URL` in your environment. Every AWS CLI and SDK call then routes to JaisCloud automatically with no per-call flags.

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws s3 mb s3://my-bucket
aws sqs create-queue --queue-name my-queue
aws dynamodb list-tables
```

Or pass `--endpoint-url` per call:

```bash
aws --endpoint-url http://localhost:4566 s3 mb s3://my-bucket
```

### Go (aws-sdk-go-v2)

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
    o.UsePathStyle = true
})
```

### Python (boto3)

```python
import boto3
client = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
```

### Node.js (aws-sdk v3)

```javascript
import { S3Client } from "@aws-sdk/client-s3";
const client = new S3Client({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  forcePathStyle: true,
});
```

---

## Running in CI/CD

### GitHub Actions

Use the JaisCloud Docker image as a service container. The health check endpoint (`/_jaiscloud/health`) lets you wait for the emulator to be ready before running tests.

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      jaiscloud:
        image: jaisraj/jaiscloud-aws:latest
        ports:
          - 4566:4566
        env:
          JAISCLOUD_EPHEMERAL: "true"

    steps:
      - uses: actions/checkout@v4

      - name: Wait for JaisCloud
        run: |
          until curl -sf http://localhost:4566/_jaiscloud/health; do
            echo "waiting for JaisCloud..."; sleep 1
          done

      - name: Run tests
        env:
          AWS_ENDPOINT_URL: http://localhost:4566
          AWS_REGION: us-east-1
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
        run: go test ./...
```

### Docker Compose

Add JaisCloud as a dependency in your `docker-compose.yml`:

```yaml
services:
  jaiscloud:
    image: jaisraj/jaiscloud-aws:latest
    ports:
      - "4566:4566"
    environment:
      JAISCLOUD_EPHEMERAL: "true"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:4566/_jaiscloud/health"]
      interval: 2s
      timeout: 5s
      retries: 15

  your-app:
    build: .
    depends_on:
      jaiscloud:
        condition: service_healthy
    environment:
      AWS_ENDPOINT_URL: http://jaiscloud:4566
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
```

### Reset state between tests

Call the reset endpoint between test runs to start with a clean slate:

```bash
curl -X POST http://localhost:4566/_jaiscloud/reset
```

In Go:
```go
func resetState(t *testing.T) {
    t.Helper()
    resp, err := http.Post("http://localhost:4566/_jaiscloud/reset", "", nil)
    require.NoError(t, err)
    require.Equal(t, http.StatusOK, resp.StatusCode)
}
```

---

## Configuration

The most common flags — all have an equivalent `JAISCLOUD_*` env var.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `JAISCLOUD_PORT` | `4566` | Listen port |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN; when set all state is stored in PostgreSQL |
| `--ephemeral` | `JAISCLOUD_EPHEMERAL` | `false` | Disable all persistence — state is lost on exit (CI / unit tests) |
| `--data-dir` | `JAISCLOUD_DATA_DIR` | `~/.jaiscloud/jaiscloud-aws` | Directory for state.json saves and named snapshots |
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | Default AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |
| `--executor-mode` | `JAISCLOUD_EXECUTOR_MODE` | — | `mock` / `docker` / `k8s` for Lambda and EMR |

**Default (no flags)** — state lives in memory and is saved periodically to `state.json` in `--data-dir`. Survives restarts. No external dependencies.

**`--dsn`** — all state is stored in PostgreSQL. Required for multi-instance or production deployments. `--data-dir` is still used for named snapshots.

**`--ephemeral`** — purely in-memory with no disk writes. State is lost on exit. Intended for CI pipelines, unit tests, and throw-away runs. Mutually exclusive with `--dsn`.

```bash
# Default: state saved to ~/.jaiscloud/jaiscloud-aws/state.json, survives restarts
jaiscloud-aws start

# Explicit data directory for state.json saves and named snapshots
jaiscloud-aws start --data-dir /var/lib/jaiscloud

# Postgres-backed: all state in PostgreSQL
jaiscloud-aws start --dsn "postgres://user:pass@localhost:5433/jaiscloud"

# Ephemeral: no persistence, clean slate on every start (CI / tests)
jaiscloud-aws start --ephemeral
```

For the full configuration reference including Kubernetes, Spark, and platform options see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## CLI Reference

```bash
jaiscloud-aws start                             # start the emulator
jaiscloud-aws version                           # print version, commit, build date
jaiscloud-aws env                               # print effective config as env vars
jaiscloud-aws doctor                            # verify the emulator is reachable
jaiscloud-aws reset                             # wipe all state
jaiscloud-aws services                          # list service implementation levels
jaiscloud-aws export -o snapshot.tar.gz        # save full state to a snapshot tarball
jaiscloud-aws import -i snapshot.tar.gz        # restore state from a snapshot tarball
jaiscloud-aws snapshot create --name <name>    # create a named on-disk snapshot
jaiscloud-aws snapshot list                    # list all named snapshots
jaiscloud-aws snapshot revert <name>           # revert to a named snapshot
jaiscloud-aws snapshot delete <name> --yes     # delete a named snapshot
jaiscloud-aws snapshot inspect <name>          # show snapshot metadata
```

---

## Admin API

All endpoints are available at the emulator's base URL (default `http://localhost:4566`).

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | `{"status":"ok"}` — liveness check |
| `/_jaiscloud/doctor` | GET | Emulator diagnostics (version, mode, instance ID, uptime) |
| `/_jaiscloud/reset` | POST | Wipe all state |
| `/_jaiscloud/reset?account=X` | POST | Wipe all regions for account X |
| `/_jaiscloud/reset?account=X&region=Y` | POST | Wipe one (account, region) scope |
| `/_jaiscloud/export` | GET | Export full state as a gzip tarball (`Content-Type: application/gzip`) |
| `/_jaiscloud/import` | POST | Restore from a gzip tarball (`Content-Type: application/gzip`); `?dry_run=true` validates only |
| `/_jaiscloud/snapshot` | POST | Create a named snapshot (`{"name":"<n>","description":"<d>"}`) |
| `/_jaiscloud/snapshots` | GET | List all named snapshots |
| `/_jaiscloud/snapshot/{name}` | GET | Inspect snapshot metadata |
| `/_jaiscloud/snapshot/{name}/revert` | POST | Revert to a named snapshot |
| `/_jaiscloud/snapshot/{name}` | DELETE | Delete a named snapshot (`?yes=true` required) |
| `/_jaiscloud/clock` | GET | Return current clock state `{"mode","time"}` |
| `/_jaiscloud/clock` | POST | Set clock: `{"mode":"fixed","time":"..."}` / `{"mode":"offset","time":"..."}` / `{"mode":"real"}` |
| `/metrics` | GET | Prometheus metrics (requires `--metrics`) |

---

## UI Console

> **Coming Soon** — Under active development; shipping in an upcoming release. **Free and open-source.**

The JaisCloud UI Console is a browser-based interface that mirrors the AWS Management Console experience — without the AWS bill. It ships as part of the single binary: no separate server, no external service, no subscription.

**Planned capabilities:**

- **Resource browser** — list, inspect, create, update, and delete any emulated resource (queues, tables, buckets, Lambda functions, secrets, parameters, and more) across all accounts and regions from a single view
- **Multi-account switcher** — toggle between emulated AWS accounts and see per-account ARN scopes, exactly as you would in the AWS Console
- **State management** — trigger reset, export, import, and named snapshot operations from the UI instead of the CLI; share snapshots directly from the browser
- **CloudWatch dashboard** — visualise metric data, alarm states, and log groups without configuring a separate Prometheus or Grafana instance


Watch the [GitHub releases page](https://github.com/jaisrajms/jaiscloud/releases) for the announcement.

---

## Multi-Account Support

JaisCloud supports isolated state per AWS account. Each account gets its own queues, tables, buckets, keys, and secrets — no cross-contamination.

Account identity is derived from the **access key** you pass to the SDK. To use multiple accounts simultaneously, pass a 12-digit account ID as the access key:

```go
// Account A — access key is the literal account ID
cfgA, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("111111111111", "test", "")),
)

// Account B
cfgB, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("222222222222", "test", "")),
)

sqsA := sqs.NewFromConfig(cfgA, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://localhost:4566") })
sqsB := sqs.NewFromConfig(cfgB, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://localhost:4566") })

// Each account sees only its own resources
sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("my-queue")})
sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("my-queue")})
```

Any other access key (e.g. `"test"`, `"AKIAIOSFODNN7EXAMPLE"`) resolves to the server default account (`JAISCLOUD_ACCOUNT_ID`, default `000000000000`).

For cross-account ARN routing, STS AssumeRole, and LSIA encoding details see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## Star the Repo

If JaisCloud saves you time, please consider **[starring the repository](https://github.com/jaisrajms/jaiscloud)**. Stars help other developers discover the project and show the community it's worth maintaining.

[![GitHub Stars](https://img.shields.io/github/stars/jaisrajms/jaiscloud?style=social)](https://github.com/jaisrajms/jaiscloud)

---

## Contributing

Contributions welcome. Please open an issue before starting large changes.

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for build setup, test matrix, and architecture details.

---

## Author

[Raj Jaiswal](https://www.linkedin.com/in/jaisraj/)

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
