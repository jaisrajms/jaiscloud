# Spring Cloud GCP sample apps — emulator e2e

An **application-level** end-to-end for `jaiscloud-gcp`: the official
[Spring Cloud GCP](https://github.com/GoogleCloudPlatform/spring-cloud-gcp) sample
applications, deployed into the `jaiscloud` k3d namespace and pointed at the
in-cluster emulator. They exercise the emulator through the real Google Java
client libraries — not raw HTTP — and are the counterpart to the per-service
SDK suites under `tests/integration/gcp/`.

## What runs

Three samples, chosen because their starters document emulator support and need
**no source changes** (configuration only):

| Deployment | Upstream module | Emulated service | Fidelity |
| --- | --- | --- | --- |
| `gcp-sample-pubsub` | `spring-cloud-gcp-pubsub-sample` | Pub/Sub | 44/44 `ga`, Full data plane |
| `gcp-sample-firestore` | `spring-cloud-gcp-data-firestore-sample` | Firestore (Native) | 33/33 `ga`, Full data plane |
| `gcp-sample-datastore` | `spring-cloud-gcp-data-datastore-basic-sample` | Datastore mode | 8/8 `ga`, Full data plane |

Each runs as a Deployment + ClusterIP Service in `namespace: jaiscloud` (see
[`samples.yaml`](samples.yaml)). They are stateless web apps; the test drives
their HTTP endpoints and asserts the data round-trips through the emulator.

## Why these three

The emulator's highest-value surface is its **Full data-plane** services. The
Google demo apps (`Online Boutique`, `Bank of Anthos`) are Kubernetes-centric
and touch only a couple of GCP APIs, so they are a poor coverage choice. These
three Spring starters instead drive the emulator directly, and each
auto-configuration switches to `NoCredentials` when handed an emulator host —
so no credentials and no source edits are required.

## Usage

```bash
# 1. Build + push the sample images (pinned upstream tag, see GCP_SAMPLES_TAG)
make docker-gcp-samples

# 2. Deploy the emulator (if not already) and run the suite
kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml
make test-e2e-gcp-samples-k3d

# Reuse the already-deployed emulator instead of rebuilding it:
make test-e2e-gcp-samples-k3d SKIP_GCP_IMAGE_REBUILD=1
```

The suite skips cleanly when `kubectl` or `svc/jaiscloud-gcp` is absent, so it
is inert in plain `go test ./...`.

`make docker-gcp-samples` builds one image per sample from
[`deploy/docker/gcp-samples/Dockerfile`](../../docker/gcp-samples/Dockerfile),
which clones the pinned tag and builds a single Maven module. Override
`GCP_SAMPLES_TAG` (and the tag in `samples.yaml`) when bumping the release.

## Configuration

All wiring is environment-only (the shared `gcp-samples-env` ConfigMap plus a
few per-deployment variables):

| Variable | Value | Notes |
| --- | --- | --- |
| `SPRING_CLOUD_GCP_PROJECT_ID` | `jaiscloud-project` | Shared project ID (emulator maps project → account). |
| `SPRING_CLOUD_GCP_PUBSUB_EMULATOR_HOST` | `jaiscloud-gcp...:8081` | gRPC, plaintext. |
| `SPRING_CLOUD_GCP_FIRESTORE_EMULATOR_ENABLED` | `true` | Switches the starter to a plaintext emulator channel + `NoCredentials`. |
| `SPRING_CLOUD_GCP_FIRESTORE_HOST_PORT` | `jaiscloud-gcp...:8081` | gRPC endpoint. |
| `SPRING_CLOUD_GCP_FIRESTORE_PROJECT_ID` | `jaiscloud-project` | **Required.** The Firestore *emulator* auto-configuration ignores the shared `spring.cloud.gcp.project-id` and defaults to the literal `unused` (`GcpFirestoreEmulatorAutoConfiguration`), which desynchronises it from the REST API. |
| `SPRING_CLOUD_GCP_DATASTORE_HOST` | `jaiscloud-gcp...:8081` | Setting `host` (not `emulator.enabled`, which would spawn a local emulator) selects the external emulator and implies `NoCredentials`. |
| `SPRING_CLOUD_GCP_DATASTORE_NAMESPACE` | `""` | **Required override.** The sample's `application.properties` hardcodes `spring-demo`; the emulator rejects namespaced keys (see below). |
| `SPRING_CLOUD_GCP_METRICS_ENABLED` | `false` | Monitoring has no endpoint property in the starter, so the export is disabled. |

Emulator endpoints: REST `http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080`,
gRPC (h2c) `jaiscloud-gcp.jaiscloud.svc.cluster.local:8081`.

## Emulator gaps surfaced by this suite

1. **Datastore namespaces are not supported.** Any non-empty namespace makes the
   gRPC Datastore service fail with
   `INVALID_ARGUMENT: namespaced keys are not supported`
   ([`internal/gcp/grpc/datastore/service.go:927`](../../../internal/gcp/grpc/datastore/service.go)).
   The Datastore sample hardcodes `namespace=spring-demo`, so the manifest
   overrides it to empty. Supporting namespaces (a per-namespace partition of
   the entity space) would let the sample run unmodified.

## Not included (deliberately)

`Secret Manager`, `Cloud Logging` and `Cloud Monitoring` samples have **no
endpoint property** in their starters, so they cannot be pointed at the
emulator by configuration alone and are out of scope for this suite. `GCS` and
`KMS` expose endpoint properties but their plaintext/credentials behaviour
needs verification before adding.
