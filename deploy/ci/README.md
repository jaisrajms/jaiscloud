# CI assets

Sample assets for running JaisCloud inside **your** integration-test pipeline.
The recipe is [`docs/gcp/ci.md`](../../docs/gcp/ci.md); this directory only holds
the files it references.

| file | what it is |
|---|---|
| `docker-compose.ci.yml` | GCP/AWS substrates (ephemeral or Postgres-backed) and an optional nginx TLS front door. Start one service at a time. |
| `postgres-init/01-gcp-db.sql` | creates `jaiscloud_gcp` on a fresh Postgres volume (the AWS database is `POSTGRES_DB`). |
| `ci-tls/gen.sh` | dev CA + wildcard leaf for the real cloud hostnames, reusing the front door's SAN policy (`deploy/k8s/istio/certs/gen.sh`). |
| `ci-tls/nginx.conf` | TLS termination for `*.googleapis.com`, `*.run.app`, `*.cloudfunctions.net` → REST `:8080`, preserving `Host`. REST only; gRPC-over-TLS needs the Istio front door. |

Nothing here is required to run the emulator: it remains a single container with
`mock` executors, no cluster and no mesh.
