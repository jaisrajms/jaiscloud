# Running JaisCloud in your CI

This is for **users** who run integration tests against the JaisCloud emulator in
their own pipeline. The emulator is a container (or a bare binary): a Kubernetes
cluster and the Istio front door from the [dual-substrate
deployment](../../deploy/k8s/istio/README.md) are **deployment options, not test
dependencies**. Nothing on the emulator's API path requires either.

Pick the smallest mode that covers your tests:

| Your tests need… | Use | Cluster? | Page |
|---|---|---|---|
| REST/gRPC API behaviour, SDK conformance | container on `:8080` / `:8081` | no | [Mode A](#mode-a--container-as-a-service-most-tests) |
| Persistence semantics (`--dsn`) | container + PostgreSQL | no | [Mode B](#mode-b--persistence) |
| Docker executors (Dataproc/Spark, Cloud Run, functions) | container + Docker socket | no | [Mode C](#mode-c--executor-features-need-pods) |
| k8s executors (Dataproc, Managed Kafka, Cloud Run k8s) | ephemeral k3d/kind + manifests | yes | [Mode C](#mode-c--executor-features-need-pods) |
| Real cloud hostnames over `https://` | TLS proxy or the Istio front door | optional | [Mode D](#mode-d--clients-that-insist-on-https) |

The one rule to keep in mind: **never make the mesh load-bearing for the
emulator**. Executors, function invocation and Cloud Run proxying talk to the
emulator over in-cluster Service DNS; the gateway only faces clients.

Ready-to-use files live in [`deploy/ci/`](../../deploy/ci/):
[docker-compose.ci.yml](../../deploy/ci/docker-compose.ci.yml),
[ci-tls/](../../deploy/ci/ci-tls/) (cert generator + nginx TLS proxy) and a
Postgres bootstrap script.

---

## Mode A — container as a service (most tests)

The image is distroless (the binary is the only file), so start it and wait from
the **host**: there is no shell, `curl` or `wget` inside for a healthcheck.

```bash
docker run -d --name jaiscloud-gcp \
  -p 8080:8080 -p 8081:8081 \
  jaisraj/jaiscloud-gcp:latest start --port 8080 --grpc-port 8081 --ephemeral
until curl -sf http://127.0.0.1:8080/_jaiscloud/health >/dev/null; do sleep 1; done
```

Point clients at it exactly as for a local run (see
[Connect your SDK](../../README-GCP.md#connect-your-sdk)):

```bash
export STORAGE_EMULATOR_HOST=http://127.0.0.1:8080     # GCS REST
export FIRESTORE_EMULATOR_HOST=127.0.0.1:8081          # Firestore (gRPC, h2c)
export PUBSUB_EMULATOR_HOST=127.0.0.1:8081             # Pub/Sub (gRPC)
export DATASTORE_EMULATOR_HOST=127.0.0.1:8081          # Datastore (gRPC)
```

**GitHub Actions** — a service container on the job network:

```yaml
jobs:
  it:
    runs-on: ubuntu-latest
    services:
      gcp:
        image: jaisraj/jaiscloud-gcp:latest
        ports: ["8080:8080", "8081:8081"]
    env:
      STORAGE_EMULATOR_HOST: http://gcp:8080
      FIRESTORE_EMULATOR_HOST: gcp:8081
      GOOGLE_CLOUD_PROJECT: ci-project
    steps:
      - uses: actions/checkout@v4
      - name: Wait for jaiscloud
        run: |
          for i in $(seq 1 60); do
            curl -sf http://127.0.0.1:8080/_jaiscloud/health && exit 0
            sleep 1
          done
          exit 1
      - run: <your test command>
```

**GitLab CI** — `services:` only reach ports the image `EXPOSE`s. The published
GCP image exposes `8080`; the gRPC port `8081` is not exposed, so either use
REST-only tests there or start the container yourself with both ports:

```yaml
it:
  image: golang:1.26
  services:
    - name: jaisraj/jaiscloud-gcp:latest
      alias: gcp
  variables:
    STORAGE_EMULATOR_HOST: "http://gcp:8080"
  script:
    - until curl -sf http://gcp:8080/_jaiscloud/health; do sleep 1; done
    - go test ./...
```

For gRPC in GitLab, run the emulator as a step instead:

```yaml
  script:
    - docker run -d --name gcp -p 8080:8080 -p 8081:8081 jaisraj/jaiscloud-gcp:latest
    - until curl -sf http://127.0.0.1:8080/_jaiscloud/health; do sleep 1; done
```

**Compose** (from this repo checkout):

```bash
docker compose -f deploy/ci/docker-compose.ci.yml up -d gcp
# ... tests ...
docker compose -f deploy/ci/docker-compose.ci.yml down -v
```

Start **one** compose service at a time (`gcp`, `gcp-postgres`, `aws`,
`aws-postgres`, or `gcp tls`): the variants share host ports by design.

### Isolation and determinism

- `--ephemeral` is in-memory only and is mutually exclusive with `--dsn`.
- Between test cases, wipe state instead of restarting:
  `curl -X POST http://127.0.0.1:8080/_jaiscloud/reset`.
- For reproducible timestamps: `--deterministic --time 2026-01-01T00:00:00Z --seed 1`,
  and move the clock at runtime with
  `POST /_jaiscloud/clock {"mode":"fixed","time":"..."}`.
- Run each job in its own container (or its own host port) rather than sharing
  one emulator across parallel jobs.

---

## Mode B — persistence

Add PostgreSQL and pass `--dsn`. The schema is per cloud (`search_path=aws|gcp`),
and one database serves one instance per cloud, so give each substrate its own
database — the compose file does this (`jaiscloud`, `jaiscloud_gcp`) and creates
`jaiscloud_gcp` on a fresh volume via `postgres-init/01-gcp-db.sql`.

```bash
docker compose -f deploy/ci/docker-compose.ci.yml up -d gcp-postgres
```

Or by hand:

```bash
docker run -d --name pg -e POSTGRES_USER=jaiscloud -e POSTGRES_PASSWORD=jaiscloud \
  -e POSTGRES_DB=jaiscloud -p 5432:5432 postgres:16-alpine
docker exec pg psql -U jaiscloud -d postgres -c 'CREATE DATABASE jaiscloud_gcp'
docker run -d -p 8080:8080 -p 8081:8081 \
  jaisraj/jaiscloud-gcp:latest start --port 8080 --grpc-port 8081 \
    --dsn postgres://jaiscloud:jaiscloud@host.docker.internal:5432/jaiscloud_gcp \
    --blob-dir /tmp/blobs
```

`--blob-dir` is where GCS object **bytes** live (metadata is in Postgres); point
it at a writable directory. `/tmp/blobs` uses the container's writable layer, so
it survives `docker restart`; to keep bytes across container recreation, bind
mount a host directory the non-root user can write (`mkdir -m 777 blobs`, then
`-v "$PWD/blobs:/data/blobs" --blob-dir /data/blobs`). (`--data-dir` is a
different knob: snapshot/export files, defaulting under `~/.jaiscloud`.)

---

## Mode C — executor features need pods

Dataproc Spark jobs, Managed Kafka brokers and Cloud Run/Functions k8s execution
create Pods, so those tests need a cluster. That is inherent to emulating the
service, not something the deployment added. An ephemeral k3d/kind job is enough:

```bash
k3d cluster create ci --wait
docker build --build-arg CLOUD=gcp -t jaiscloud-gcp:ci .
k3d image import -c ci jaiscloud-gcp:ci

kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/postgres.yaml
kubectl apply -f deploy/k8s/rbac.yaml
kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml
kubectl -n jaiscloud-gcp set image deployment/jaiscloud-gcp jaiscloud-gcp=jaiscloud-gcp:ci
kubectl -n jaiscloud-gcp patch deployment/jaiscloud-gcp --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
kubectl -n jaiscloud-gcp rollout status deployment/jaiscloud-gcp --timeout=180s
```

Then point tests at the in-cluster Service
(`jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8080` / `:8081`) and pass
`K8S_NAMESPACE=jaiscloud-gcp` to the repo's e2e helpers.

`postgres.yaml` creates `jaiscloud_gcp` on a **fresh** volume; if you reuse one,
create it once (`make postgres-gcp-db`, or
`kubectl -n jaiscloud-data exec deploy/postgres -- psql -U jaiscloud -d postgres -c 'CREATE DATABASE jaiscloud_gcp'`).
`rbac.yaml` is what lets the emulator create its per-cluster namespaces and
driver Pods; without it the emulator falls back to `mock`/the process namespace.

**Parallel jobs on a shared cluster**: one namespace and one
`JAISCLOUD_INSTANCE_ID` per job. The shipped manifests pin single values
(`gcp-substrate-1`, `jaiscloud-gcp`) because they describe one deployment; copy
them and change both for a multi-tenant cluster so no job's startup sweep reaps
another's Pods.

**Kafka data-plane clients must run where the broker's address resolves.** In
`k8s` broker mode the advertised `bootstrapAddress` is the broker's in-cluster
Service (`<svc>.<ns>.svc.cluster.local:9092`), and a Kafka client reconnects to
that advertised listener, so a test process on the runner cannot use it even
through a port-forward. Run the client as a Pod/Job in the cluster, or use the
`docker` (host mode) or `native` broker backend from the runner — see
[Where the client runs](../../README-GCP.md#managed-kafka-metadata-control-plane-optional-real-broker) in the emulator guide.

**You still do not need the mesh here** — executors and functions call the
emulator over Service DNS.

---

## Mode D — clients that insist on https

Three levels, cheapest first.

### D1. Native TLS on `:443` (no proxy)

The GCP binary also serves **REST** over TLS on a fixed `:443` (gRPC stays
plaintext h2c on `:8081`). It uses `/etc/tls/tls.crt` + `tls.key` when mounted,
otherwise a self-signed certificate generated and persisted in the state dir.
Modern Docker/k8s allow a non-root process to bind 443; if your runtime does not,
add `--sysctl net.ipv4.ip_unprivileged_port_start=0` (Docker) or a
`NET_BIND_SERVICE` capability (k8s).

```bash
deploy/ci/ci-tls/gen.sh                       # writes deploy/ci/ci-tls/certs
docker run -d -p 8080:8080 -p 8081:8081 -p 443:443 \
  -v "$PWD/deploy/ci/ci-tls/certs/tls.crt:/etc/tls/tls.crt:ro" \
  -v "$PWD/deploy/ci/ci-tls/certs/tls.key:/etc/tls/tls.key:ro" \
  jaisraj/jaiscloud-gcp:latest start --port 8080 --grpc-port 8081 --ephemeral
export SSL_CERT_FILE="$PWD/deploy/ci/ci-tls/certs/ca.crt"   # trust the dev CA
```

The generated cert covers `localhost` and the real host groups (including
`*.{location}.run.app`), so `https://localhost/...` and
`https://storage.googleapis.com/...` (with the name pointed at the emulator)
validate. IP literals are not covered. Clients that hardcode a real hostname
still need that name to resolve to the emulator (next step).

### D2. A thin TLS proxy for real hostnames (compose, REST)

```bash
deploy/ci/ci-tls/gen.sh
docker compose -f deploy/ci/docker-compose.ci.yml up -d gcp tls
# point the hostname(s) at the proxy, e.g. on the runner:
echo "127.0.0.1 storage.googleapis.com firestore.googleapis.com sts.amazonaws.com" | sudo tee -a /etc/hosts
export SSL_CERT_FILE="$PWD/deploy/ci/ci-tls/certs/ca.crt"
curl -sf https://storage.googleapis.com/storage/v1/b?project=ci
```

The proxy preserves `Host` (the emulator routes on the authority) and proxies
REST to `gcp:8080`. **It does not carry gRPC** — for gRPC over TLS use D3, or
point the gRPC client at the plaintext h2c port.

On Kubernetes, the same thing is a pod-level `hostAliases` entry instead of
`/etc/hosts`:

```yaml
spec:
  hostAliases:
    - ip: "10.43.x.y"          # the proxy Service's ClusterIP
      hostnames: ["storage.googleapis.com", "firestore.googleapis.com"]
```

### D3. The full Istio front door

Only needed when you want the real topology: SNI routing for many hosts, gRPC
over TLS (h2 → h2c upstream), and the exact routing rules the deployment ships
(GKE vs Managed Kafka authority, Cloud Run/Cloud Functions, S3 virtual-hosted).
Install order, CA generation and the smoke gate are in
[`deploy/k8s/istio/README.md`](../../deploy/k8s/istio/README.md); in short:

```bash
istioctl install -y --set profile=minimal \
  --set 'components.ingressGateways[0].enabled=true' \
  --set 'components.ingressGateways[0].name=istio-ingressgateway'
deploy/k8s/istio/certs/gen.sh
kubectl apply -f deploy/k8s/istio/gateway.yaml \
              -f deploy/k8s/istio/virtualservices.yaml \
              -f deploy/k8s/istio/destinationrule.yaml
scripts/istio-smoke.sh
```

Trust the CA from `gen.sh` (`SSL_CERT_FILE`, `AWS_CA_BUNDLE`, `NODE_EXTRA_CA_CERTS`,
`--cacert`), or put a publicly trusted certificate in the `jaiscloud-wildcard`
secret.

---

## Pitfalls

| Symptom | Cause |
|---|---|
| Container "unhealthy", healthcheck always fails | the image is distroless: no shell/curl inside. Wait from the host (`/_jaiscloud/health`). |
| `--ephemeral` and `--dsn` together | mutually exclusive by design; pick one. |
| `blobfs: mkdir : no such file` | `--dsn` without a usable blob dir; pass `--blob-dir <writable>`. |
| gRPC client hangs on `:443` | native TLS is REST-only; use the front door (D3) or plaintext `:8081` (h2c). |
| `x509: certificate is valid for …` | the client does not trust the dev CA, or the host is not covered (IP literal / unlisted depth). |
| `https://storage.googleapis.com` times out | the name does not resolve to the proxy/emulator yet (`/etc/hosts`, `hostAliases`, DNS). |
| Dataproc/Kafka/Cloud Run jobs stay `mock` | no k8s client: the pod needs `rbac.yaml` and a reachable API server, and the executor mode set to `k8s`. |
| Two jobs reap each other's Pods | shared cluster without a per-job `JAISCLOUD_INSTANCE_ID`/namespace. |

## See also

- [`README-GCP.md`](../../README-GCP.md) — the emulator guide and SDK endpoint list.
- [`docs/gcp/testability.md`](testability.md) — what a local run may be trusted to prove.
- [`deploy/k8s/istio/README.md`](../../deploy/k8s/istio/README.md) — the front door.
- [`deploy/ci/`](../../deploy/ci/) — compose file, TLS proxy and cert generator used above.
