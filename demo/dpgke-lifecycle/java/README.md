# Dataproc-on-GKE lifecycle — Java SDK leg

The official Google **Java** clients running the demo's lifecycle against
`jaiscloud-gcp`: verify/create the GKE cluster, create the Dataproc-on-GKE
virtual cluster, list the reconciled node pools, submit the PySpark job, wait
for `DONE`, read the staged driver output, and delete in order.

This is the leg that proves the Java clients' **default transports** — Dataproc
and GKE over gRPC, Storage over HTTP/JSON — not a REST fallback.

```
ClusterManagerClient      GKE       gRPC  (client default)
ClusterControllerClient    Dataproc  gRPC  (client default, gax OperationFuture + LRO)
JobControllerClient        Dataproc  gRPC  (client default)
Storage                    GCS       REST  (the Java Storage client is HTTP/JSON)
```

Nothing here is emulator-specific: the only overrides are the endpoint
(`InstantiatingGrpcChannelProvider` with plaintext + `NoCredentials`) and, for
Storage, `StorageOptions.setHost` + `NoCredentials`. Against real GCP the same
code with ADC and no endpoint overrides is the normal client configuration.

## What it does

1. **Buckets.** Ensures the input/output/staging buckets exist and that the job
   code (`code/wordcount.py`) and seed input (`input/words.txt`) are in the
   input bucket. These are the Terraform module's names and objects; the module
   owns them on a full demo run and this leg only fills them in when it runs
   standalone.
2. **GKE.** Verifies `projects/{p}/locations/{loc}/clusters/{name}` or creates
   it with `initialNodeCount: 1` and
   `workloadIdentityConfig.workloadPool = {project}.svc.id.goog`, then polls
   `GetOperation` until `DONE` and asserts the cluster is `RUNNING` with Workload
   Identity set. Dataproc-on-GKE rejects a target without it.
3. **Virtual cluster.** Verifies the Dataproc cluster or creates it with
   `virtualClusterConfig`: staging bucket, namespace, the **zonal**
   `gkeClusterTarget` (`locations/us-central1-a`, not the region), two
   `nodePoolTarget`s (controller + spark, full resource names, autoscaling,
   `e2-small`) and a fully-qualified `SPARK` release. The create is an
   `OperationFuture`; the app polls it 2s × 60 (the medallion script's
   contract) and then asserts the cluster reaches `RUNNING` (or fails on
   `ERROR`).
4. **Node pools.** Lists them through `ClusterManagerClient` and asserts both
   reconciled pools exist with the module's machine type, autoscaler bound and
   `goog-dataproc-cluster-name` label.
5. **Job.** Submits the PySpark job (`mainPythonFileUri` = the uploaded object,
   args = the input object and the output prefix), polls `GetJob` until `DONE`,
   asserts `driverOutputResourceUri` is in the staging bucket, and reads
   `<uri>.000000000` through `Storage`.
6. **Teardown** in the order the resources require: jobs → virtual cluster →
   GKE cluster → (optionally) buckets. It also asserts the middle step real
   Dataproc has: deleting the virtual cluster leaves its reconciled node pools,
   and deleting the GKE cluster is what removes them.

## Mock mode: what the job actually does

The emulator's default Dataproc executor is the **mock** Spark executor. A job
walks `PENDING → SETUP_DONE → RUNNING → DONE` one state per read, the driver
output URI is allocated at submit, and a small **synthetic** terminal log is
written at `DONE`:

```
Dataproc job <id> finished with state DONE
```

No Spark driver starts, nothing is scheduled onto the reconciled pools, and the
**output bucket stays empty**. This app asserts exactly what is true (job
`DONE`, driver object present and readable, output prefix empty) and never
claims Spark ran or wrote records. Real Spark bytes and the actual word count
are DGD6's k3d gate.

## Configuration

All configuration comes from the environment so the Helm leg (DGD4) can inject
it. Defaults mirror `../terraform/variables.tf` and its outputs, so a Java run
against Terraform-provisioned infrastructure needs no overrides.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DGD3_PROJECT` | `dpgke-demo` | Project of the GKE + Dataproc clusters |
| `DGD3_REGION` | `us-central1` | Dataproc region |
| `DGD3_GKE_LOCATION` | `us-central1-a` | GKE location (zone → zonal cluster) |
| `DGD3_GKE_CLUSTER` | `dpgke-gke` | GKE cluster name |
| `DGD3_DATAPROC_CLUSTER` | `dpgke-dataproc` | Virtual cluster name; node pools are `<name>-controller` / `<name>-spark` |
| `DGD3_NAMESPACE` | `dpgke-demo` | `kubernetesNamespace` |
| `DGD3_STORAGE_PROJECT` | `DGD3_PROJECT` | Project that owns the buckets (cross-project shape) |
| `DGD3_INPUT_BUCKET` | `<project>-input` | Job code + seed input |
| `DGD3_OUTPUT_BUCKET` | `<project>-output` | Job output prefix |
| `DGD3_STAGING_BUCKET` | `<project>-staging` | `stagingBucket`, driver output |
| `DGD3_REST_ENDPOINT` | `http://localhost:8080` | Emulator REST base (Storage) |
| `DGD3_GRPC_ENDPOINT` | `localhost` | gRPC host (Dataproc + GKE) |
| `DGD3_GRPC_PORT` | `8081` | gRPC port (`--grpc-port`) |
| `DGD3_SPARK_RELEASE` | `3.5-dataproc-28` | Fully-qualified SPARK component version |
| `DGD3_MACHINE_TYPE` | `e2-small` | Reconciled node pool machine type |
| `DGD3_MIN_NODES` / `DGD3_MAX_NODES` | `1` / `3` | Autoscaler bounds |
| `DGD3_JOB_CODE_OBJECT` | `code/wordcount.py` | Object the job's `mainPythonFileUri` names |
| `DGD3_JOB_INPUT_OBJECT` | `input/words.txt` | Seed text the job reads |
| `DGD3_JOB_OUTPUT_PREFIX` | `wordcount` | Prefix the job writes under |
| `DGD3_SPARK_SCRIPT` | auto (`../spark/wordcount.py`) | Local file uploaded if the object is missing |
| `DGD3_POLL_SECONDS` / `DGD3_POLL_ATTEMPTS` | `2` / `60` | Poll cadence and bound for every wait |
| `DGD3_TEARDOWN` | `true` | Delete the clusters when the run finishes |
| `DGD3_DELETE_BUCKETS` | `false` | Also delete the three buckets |
| `DGD3_CLEANUP_ONLY` | `false` | Only delete what is there (failure-path safety net) |

`DGD3_DELETE_BUCKETS=true` is for the standalone gate, where this app created
the buckets. Leave it `false` when Terraform owns them, or the next
`terraform plan` will try to recreate them.

## Running

Prerequisites: JDK 21 and Maven. They are not needed at build time for the rest
of the repo; `run.sh` prints `SKIP` and exits 0 when they are missing. The
Maven build downloads `google-cloud-dataproc`, `google-cloud-container`,
`google-cloud-storage` and `libraries-bom` from Maven Central, so the first
build needs network.

```sh
# Build + unit test + the two-run gate against a local emulator.
./demo/dpgke-lifecycle/java/run.sh

# A different emulator: the gRPC port is REST port + 1.
./demo/dpgke-lifecycle/java/run.sh http://localhost:9090
```

By hand:

```sh
cd demo/dpgke-lifecycle/java
mvn -q -e -DskipTests package
java -jar target/dpgke-lifecycle-java.jar          # create + submit + teardown
```

### The two runs, and idempotent create

`run.sh` deliberately runs the app twice:

1. `DGD3_TEARDOWN=false` — creates the GKE cluster and virtual cluster,
   submits a job, asserts it, and **leaves everything in place**.
2. `DGD3_TEARDOWN=true` — sees the existing clusters, logs that it is adopting
   them (no duplicate create), asserts their configuration, submits a second
   job, then tears down and asserts nothing is left (clusters, jobs, node pools,
   buckets).

The second run is what proves the get-or-create path the DGD3 row asks for; the
first proves the create path. Both must be green.

### Against Terraform-created infrastructure

```sh
cd demo/dpgke-lifecycle/terraform && ./run.sh      # leaves nothing behind
# or apply the module yourself, then:
cd ../java && java -jar target/dpgke-lifecycle-java.jar
```

Point `DGD3_*` at the module's outputs if you changed its variables. With the
module's defaults the app finds the same buckets, virtual cluster and job
objects without any overrides.

## Two client details worth knowing

- **Node-pool names must be fully qualified.** `GkeNodePoolTarget.nodePool`
  takes `projects/{p}/locations/{l}/clusters/{c}/nodePools/{pool}`. Terraform
  expands short ids for you; the official Java client does not, and the service
  rejects a bare id (`GKE Node Pool name '...' must conform to pattern ...`).
- **`Operation.name` is the server-assigned ID**, while `GetOperation.name` is
  the canonical `projects/{p}/locations/{l}/operations/{id}` path (see the
  Container Discovery document). The app normalises the former into the latter
  before polling; the emulator matches real GCP here, so no workaround is
  needed.

The Terraform module's `container.*` host caveat does **not** apply: it exists
because the provider strips a custom HTTP endpoint's path, and this leg talks to
GKE over its native gRPC endpoint instead.

## What this gate does not prove

- That Spark ran, that the driver executed `wordcount.py`, or that output bytes
  were produced — the executor is mock. DGD6's k3d gate runs the same script on
  a real Spark image and compares the word count.
- Workload Identity's IAM bindings: the emulator stores the workload pool and
  uses it for Dataproc's create-time validation, but the roles
  `gcloud container clusters create --setup-workload-identity` would grant are a
  recorded no-fix on the emulator side. The app asserts the config field, not
  an authorization outcome.
- Bucket IAM enforcement (DGD9): grants are metadata-only on the emulator.
