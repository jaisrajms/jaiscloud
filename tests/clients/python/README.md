# Python `google-cloud-*` client conformance suite

Drives the jaiscloud GCP emulator with the **official Python client
libraries** — a different language/codegen path from the Go SDKs and `gcloud`
— and reports, per service/operation, whether the call passed, failed, or is
unsupported. It is the Python analogue of `tests/gcpconformance/grpc` and
`tests/gcpconformance/gcloud`.

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `PROJECT_ID` | `jaiscloud-project` | GCP project all resources live under |
| `EMULATOR_REST` | `http://localhost:8080` | Emulator REST endpoint (Cloud Storage) |
| `EMULATOR_GRPC` | `localhost:8081` | Emulator gRPC endpoint (Pub/Sub, Secret Manager, KMS, Firestore) |
| `PY_CONFORMANCE_REPORT_DIR` | `./report` | Where `report.json` / `report.md` are written |

## Client wiring recipe (the crux)

Every client is pointed at the emulator without credentials. The ways differ
per client because only some libraries have an emulator hook:

| Service | Env / options | Auth | Transport |
| --- | --- | --- | --- |
| **storage** | `STORAGE_EMULATOR_HOST=http://localhost:8080` | auto `AnonymousCredentials` | REST |
| **pubsub** | `PUBSUB_EMULATOR_HOST=localhost:8081` | emulator creds (auto) | gRPC, insecure (auto) |
| **secretmanager** | *no hook* — inject `SecretManagerServiceGrpcTransport(channel=grpc.insecure_channel("localhost:8081"))` | none (channel supplied) | gRPC, insecure |
| **kms** | *no hook* — inject `KeyManagementServiceGrpcTransport(channel=grpc.insecure_channel(...))` | none (channel supplied) | gRPC, insecure |
| **firestore** | `FIRESTORE_EMULATOR_HOST=localhost:8081` | emulator creds (auto) | gRPC, insecure (auto) |

**Secret Manager / KMS gotcha.** The intuitive recipe
`SecretManagerServiceClient(credentials=AnonymousCredentials(),
client_options={"api_endpoint": "localhost:8081"})` does **not** work: the
generated client always builds a *TLS* channel
(`google.api_core.grpc_helpers.create_channel` → `grpc.secure_channel`), so the
handshake against the emulator's plaintext listener fails with:

```
StatusCode.UNAVAILABLE ... Ssl handshake failed (TSI_PROTOCOL_FAILURE):
SSL_ERROR_SSL ... WRONG_VERSION_NUMBER
```

The working recipe is to construct the generated gRPC transport over an
insecure channel and hand it to the client (`harness.secretmanager_client` /
`harness.kms_client`).

## Run

```sh
# 1. Build + start the emulator (REST :8080, gRPC :8081)
go build -o /tmp/jc-py ./cmd/jaiscloud-gcp/
/tmp/jc-py start --port 8080 --grpc-port 8081 --ephemeral &
until curl -sf http://localhost:8080/_jaiscloud/health; do sleep 1; done

# 2. Create a venv and install pinned deps
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 3. Run (from the repo root or this directory)
.venv/bin/python -m pytest -v tests/clients/python

# 4. Stop the emulator
kill $(lsof -ti tcp:8080)
```

Or from the repo root: `make test-gcp-python-conformance`.

`pytest` writes `report/report.json` and `report/report.md` (per-service
rollup) and prints a `python-sdk conformance: total=… pass=… …` summary line.
The suite skips cleanly when no emulator is reachable.

## Regression gate

A check marked `Expect.PASS` that does not pass is a regression and fails the
run. An unimplemented operation is recorded as `unsupported` and skipped —
never faked as a pass.
