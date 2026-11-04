# Dataproc-on-GKE lifecycle — Terraform module

Creates the infrastructure for the demo: a GKE cluster, a Dataproc-on-GKE
virtual cluster that targets it, the input/output/staging buckets, the service
account a job runs as, and a PySpark job submitted through
`google_dataproc_job`. This module's job is the "provision -> run" proof; the
Java submitter that runs the same lifecycle through the official clients lives
in [`../java/`](../java/), and the Helm chart that installs it as a Job lives in
[`../helm/`](../helm/).

The job's code is `../spark/wordcount.py`, uploaded into the input bucket as
`code/wordcount.py` and named by the job's `mainPythonFileUri`. It reads a
seeded input object and writes a word count under the output bucket.

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
| `job_input_object` | `input/words.txt` | Seed text object the job reads |
| `job_output_prefix` | `wordcount` | Prefix the job writes under |
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
cluster's target and state, the two reconciled node pools, the buckets, the job
service account, and the submitted job's provider read-back), a targeted
destroy that proves the virtual cluster goes before the GKE cluster, then a
full `destroy` and "nothing is left" checks (empty state, no clusters, no jobs,
no node pools, 404 buckets). It prints `SKIP` and exits 0 when no `terraform`
binary is on PATH, and it never installs tooling.

```sh
./demo/dpgke-lifecycle/terraform/run.sh                          # localhost:8080
./demo/dpgke-lifecycle/terraform/run.sh http://localhost:9090    # a different emulator
```

## The job: `google_dataproc_job`

The module uploads `../spark/wordcount.py` with
`google_storage_bucket_object` and submits it with `google_dataproc_job`:

- `placement.cluster_name` is the virtual cluster, so Terraform creates the
  virtual cluster first and destroys the job before it.
- `pyspark_config.main_python_file_uri` is the uploaded object's `gs://` URI,
  and `args` name the seeded input object and the output bucket prefix.
- `wait_for_completion = true` makes the resource's first read the terminal
  state. With the provider default (`false`) the apply would finish at
  `RUNNING`, and a DONE assertion would only hold after a later refresh.

The module exports `job_id`, `job_name`, `job_state`,
`job_placement_cluster_name`, `job_placement_cluster_uuid`,
`job_driver_output_resource_uri`, `job_main_python_file_uri`, `job_input_uri`
and `job_output_uri`, so a later leg can address the same job and objects
without re-deriving names.

### What the gate proves, and what it does not

The emulator's default Dataproc executor is **mock** (`cmd/jaiscloud-gcp` wires
it unless `JAISCLOUD_SPARK_EXECUTOR_MODE` asks for k8s/docker), and the gate
does not set that variable. In mock mode a submitted job walks
`PENDING -> SETUP_DONE -> RUNNING -> DONE` one hop per read, the driver output
URI is allocated at submit in the virtual cluster's staging bucket, and a small
synthetic driver log is written at the terminal transition. No Spark driver
starts and nothing is scheduled onto the reconciled node pools.

So the gate asserts, honestly:

- the provider's create/read path: state `DONE`, `done: true`, the placement
  cluster name and its captured `clusterUuid`, and a `driverOutputResourceUri`
  in the staging bucket whose synthetic object resolves;
- the job is the uploaded PySpark object, and its args are the seeded input URI
  and the output prefix;
- the **negative**: the output bucket is empty. If mock mode ever did write
  Spark output, this check would fail and the gate would need updating rather
  than quietly overclaiming.

It does **not** prove that Spark ran, that the driver executed
`wordcount.py`, or that Dataproc scheduled work onto the target cluster's node
pools. Those are DGD6's k3d gate, where the real Spark image and GCS connector
run the same script and the output bytes are compared.

## Destroy order: job, then virtual cluster, then GKE

Deleting a Dataproc-on-GKE virtual cluster leaves its reconciled node pools in
place: real Dataproc does not own them. The GKE cluster does, so deleting the
GKE cluster is what reclaims them. The module encodes the edges (the job names
the virtual cluster, the virtual cluster names the GKE cluster), and the gate
proves the middle step rather than asserting it in prose:

1. `terraform destroy -target=google_dataproc_cluster.gke` — Terraform destroys
   the job first (it depends on the cluster), then the virtual cluster.
2. Assert both `nodePools` still answer `200` through the GKE API, and the GKE
   cluster is still there.
3. Finish `terraform destroy` and assert both pools now answer `404`.

A destroy that ran in the wrong order would make step 2's pool reads `404` and
the gate fail at the summary; there is no wait loop that could hang on a pool
that never appears.

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

## Bucket access

All three buckets set `uniform_bucket_level_access = true` — on real GCP that is
what makes the bucket IAM grants below the only way in, and it disables the
per-object ACLs a job bucket should not rely on. The emulator stores the setting
and echoes it back from the bucket read (DGD10), so the re-plan in `run.sh`
converges; before that fix it always reported `false`, and setting it produced a
permanent diff.

## Why the Spark release is pinned

Aliases such as `3.5` are dereferenced at cluster creation on real GCP, so a
config carrying one shows a permanent diff against the stored full release
`3.5-dataproc-28`. Keep the full string.
