# Dataproc-on-GKE lifecycle — Terraform module

Creates the infrastructure for the demo: a GKE cluster, a Dataproc-on-GKE
virtual cluster that targets it, the input/output/staging buckets and the
service account a job runs as. Nothing here submits a job or installs anything
on a cluster — those are separate legs of the demo.

| Variable | Default | Meaning |
| --- | --- | --- |
| `project` | `dpgke-demo` | Project the clusters live in |
| `region` | `us-central1` | Dataproc cluster region (always regional) |
| `gke_location` | `us-central1-a` | Zone (zonal cluster) or region (regional cluster) |
| `storage_project` | `null` → `project` | Project that owns the three buckets |
| `bucket_prefix` | `null` → `project` | Prefix for the derived bucket names |
| `gke_cluster_name` | `dpgke-gke` | GKE cluster name |
| `dataproc_cluster_name` | `dpgke-dataproc` | Virtual cluster name, and its node pool prefix |
| `namespace` | `dpgke-demo` | Kubernetes namespace Dataproc uses |
| `machine_type` | `e2-small` | Dataproc node pool machine type |
| `min_nodes` / `max_nodes` | `1` / `3` | Node pool autoscaler bounds |
| `spark_release` | `3.5-dataproc-28` | Pinned Spark component version |
| `endpoint` | `http://localhost:8080` | Emulator REST base URL |
| `container_endpoint` | `null` | GKE endpoint override; leave it unset |

## Running against the emulator

```sh
go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/     # from the repo root
./jaiscloud-gcp start --port 8080 --ephemeral &

export GOOGLE_OAUTH_ACCESS_TOKEN=emulator           # any non-empty token
cd demo/dpgke-lifecycle/terraform
terraform init
terraform validate
terraform plan -out demo.tfplan
terraform apply demo.tfplan
terraform plan                                      # must report no changes
terraform destroy
```

`terraform.tfvars.example` shows the overrides worth changing. The module was
written for the hashicorp/google provider (7.36+), so `terraform init` downloads
it; no other tooling is needed.

## The gate

`run.sh` is the module's own gate and does the sequence above in one command:
build/start an emulator if none is healthy (and stop it again), `init`,
`validate`, `plan`, `apply`, a second `plan` that must report **no changes**,
spot checks over the emulator's REST API (the zonal location, the virtual
cluster's target and state, the two reconciled node pools, the buckets and the
job service account), `destroy`, then "nothing is left" checks (empty state, no
clusters, no node pools, 404 buckets). It prints `SKIP` and exits 0 when no
`terraform` binary is on PATH, and it never installs tooling.

```sh
./demo/dpgke-lifecycle/terraform/run.sh                          # localhost:8080
./demo/dpgke-lifecycle/terraform/run.sh http://localhost:9090    # a different emulator
```

## The one caveat: GKE needs a `container.` host

The provider strips the path from a custom endpoint, so
`http://localhost:8080/container/` degenerates to `http://localhost:8080/`, whose
`/v1/projects/{p}/locations/{l}/clusters` collection is shared with Managed
Kafka. The emulator does disambiguate it (clusterId means Kafka, a GKE create
body means GKE, otherwise it probes which service holds a cluster there), but
that is a heuristic a copied module should not lean on — and real GKE has its own
host. So `provider.tf` derives a `container.<host>` endpoint from `endpoint`,
which resolves to `http://container.localhost:8080/container/` by default.

`*.localhost` resolves to a loopback address on Linux (`container.localhost` is
`::1` here); if it does not on your machine, set `container_endpoint` to a
`container.*` host that does resolve.

## Zonal vs regional

`gke_location` takes a zone or a region, and the same value is used for the GKE
cluster and the Dataproc `gkeClusterTarget`. A zone is the quickstart shape; the
Dataproc cluster stays regional either way, and the target must be in `region`.

## Cross-project buckets

`storage_project` moves all three buckets to a second project while the clusters
stay in `project`, and the buckets become that project's only footprint. On real
GCP the second project has to exist first; the emulator does not check. Bucket
IAM and requester-pays are not enforced by the emulator (DGD9), so this
demonstrates the shape, not an authorization outcome.

## Why the Spark release is pinned

Aliases such as `3.5` are dereferenced at cluster creation on real GCP, so a
config carrying one shows a permanent diff against the stored full release
`3.5-dataproc-28`. Keep the full string.
