# Dataproc-on-GKE lifecycle — Terraform, Helm and the Java SDK

One artefact a customer can copy: a Terraform module that stands up a zonal GKE
cluster, a Dataproc-on-GKE virtual cluster, the input/output/staging buckets and
a PySpark job the provider submits; a Helm chart that installs a one-shot
submitter; and a Java application built on the official Google clients that
adopts the cluster, submits a job, waits for `DONE`, reads the driver output from
GCS and proves a downscoped credential's boundary by asserting its denials.

This is the canonical Dataproc-on-GKE story in this repository. The
crypto-medallion demo has a Dataproc-on-GKE cluster too, but its GKE half is not
runnable as written; see [the note in its README](../crypto-medallion/README.md).

What is real: Terraform, the provider, both official client stacks (Terraform's
`google_*` resources and the Java clients), the STS token exchange, the GCS
bytes, and — in the k3d gate only — a real Spark driver and executor. What is
emulated: the GKE and Dataproc control planes, which are metadata and state
machines behind `jaiscloud-gcp`. There is no Kubernetes control plane behind the
GKE cluster the virtual cluster targets, and no Spark pod is ever scheduled onto
it. Every gate below asserts only what is true under that split.

| directory | what it is | its own gate | details |
|---|---|---|---|
| [`terraform/`](terraform/) | the module: GKE + virtual cluster + buckets + job | [`terraform/run.sh`](terraform/run.sh) | [`terraform/README.md`](terraform/README.md) |
| [`java/`](java/) | the submitter (`google-cloud-dataproc`, `-storage`, `-container`) | [`java/run.sh`](java/run.sh) | [`java/README.md`](java/README.md) |
| [`helm/`](helm/) | the chart that injects Terraform's outputs into the app | [`helm/render.sh`](helm/render.sh) | [`helm/README.md`](helm/README.md) |
| [`spark/`](spark/) | the word-count PySpark file Terraform uploads | exercised by the gates | — |
| [`scripts/`](scripts/) | the end-to-end k3d gate and the toolchain script | [`e2e-k3d.sh`](scripts/e2e-k3d.sh) | [below](#toolchain) |

## What runs where

| piece | runs on | reaches the emulator via | owns |
|---|---|---|---|
| Terraform | the **host** | a host REST endpoint: `localhost:8080` in its own gate, a `kubectl port-forward` in the k3d gate | the GKE cluster, the virtual cluster, the buckets, the GSA, the provider-submitted job |
| the submitter | a **pod in the cluster**, as a one-shot `Job` | the emulator's Service DNS (`jaiscloud-gcp.<ns>.svc.cluster.local`), REST `:8080` and gRPC `:8081` | the Java leg: adopt, submit, wait, read, assert |
| `jaiscloud-gcp` | the cluster | — | the GKE and Dataproc control planes, GCS, IAM, STS |
| Spark | the **emulator's** Dataproc namespace | in-cluster GCS endpoint | only when the gate selected the k8s executor; nothing runs locally |

Two clients, two network paths, on purpose. Terraform runs on the host, so the
k3d gate forwards the emulator's REST port to it and the module derives the GKE
host from that endpoint (see [the `container.` caveat](#the-container-host-caveat)).
The submitter never uses the port-forward: the chart points both of its
endpoints at the Service, and the gate greps the Job's log for both the gRPC and
the REST Service DNS, so a run that quietly went back to the host path fails.

The submitter image is built from [`java/Dockerfile`](java/Dockerfile) and pushed
to `GCP_REGISTRY` (`10.0.100.21:5050` by default) unless the gate runs in
`import` mode, where it is loaded with `k3d image import`. Either way the chart
pins the exact tag with `pullPolicy: IfNotPresent`; nothing published a tag, the
chart's default is an explicit placeholder.

### Which executor actually runs Spark

Two different answers, and they must not be blurred:

- **Mock executor (the default).** A local `jaiscloud-gcp` and every gate except
  the k3d one run with the emulator's default Dataproc executor. A job walks
  `PENDING → SETUP_DONE → RUNNING → DONE` one hop per read, the driver output URI
  is allocated at submit, and a small synthetic log is written at the terminal
  transition. No Spark driver starts, nothing is scheduled onto the reconciled
  node pools, and the output bucket stays empty. Those gates assert exactly that
  and never claim a word count.
- **Kubernetes executor (k3s, via k3d — the k3d gate only).**
  `make test-e2e-dpgke-demo-k3d` sets
  `JAISCLOUD_SPARK_EXECUTOR_MODE=k8s` on the deployed emulator (and pins it back
  if an earlier suite left it in mock mode), with the same `spark-gcs` image the
  other k3d Dataproc suites use. A real driver runs `spark/wordcount.py`, reads
  the input object and writes the counts. The gate asserts the bytes: the part
  file contains `dataproc 2`, `gke 2`, `on 2`, `the 4`, `count 1`, and Spark's
  `_SUCCESS` marker is there.

Even in k8s mode the driver and executors run in the **emulator's** Dataproc
namespace, not on the GKE cluster the virtual cluster names. The target cluster
is reconciled as metadata (node pools, labels, machine types) and the job runs on
the emulator's own executor. That is the documented difference, not a gap the
documentation hides.

## The spine: provision → process → serve/observe

The stage decomposition is borrowed from the crypto-medallion demo, which is
explicit about which piece owns which stage. This demo has no serving tier: the
last two stages are reading the result back and watching the assertions, and the
README says so instead of inventing a web app.

| stage | directory | command | what happens |
|---|---|---|---|
| **Provision** | [`terraform/`](terraform/) | `terraform init/plan/apply` (or `terraform/run.sh`) | GKE cluster (`dpgke-gke`), virtual cluster (`dpgke-dataproc`), three buckets in the bucket-owning project, the `dpgke-job` service account, the seed input and the PySpark code in the input bucket |
| **Process** | [`java/`](java/) + [`helm/`](helm/) | `helm install`, then `java -jar dpgke-lifecycle-java.jar` in the Job | the app adopts the clusters, exchanges two downscoped tokens, submits `wordcount.py`, waits for `DONE`, reads the driver output; Terraform has already submitted its own job through `google_dataproc_job` |
| **Serve** | GCS | the gate's object reads | the job's part file is read back by name and by content — that read is the demo's "serve" |
| **Observe** | [`scripts/e2e-k3d.sh`](scripts/e2e-k3d.sh) | `make test-e2e-dpgke-demo-k3d` | every `[PASS]` line, the submitter transcript, and the teardown assertions; the emulator's console shows the same clusters, jobs and buckets while they exist |

The legs can be run in that order by hand, or all at once by the k3d gate. The
Terraform module is the only piece that has to exist first: the chart creates no
infrastructure and fails loudly in the app if the clusters are missing.

## The `container.` host caveat

The `hashicorp/google` provider strips the path from a custom endpoint and keeps
only the scheme and host. `http://localhost:8080/container/` therefore degenerates
to `http://localhost:8080/`, whose
`/v1/projects/{p}/locations/{l}/clusters` collection is genuinely shared with
Managed Kafka.

The emulator does disambiguate that path — a request carrying `clusterId` is
Kafka's, a create body with a GKE `cluster` object and no `clusterId` is GKE's,
and otherwise it probes which service holds a cluster at that project and
location — but that is a heuristic over one origin, and a module a customer
copies should not lean on it. Real GKE has its own host.

So [`terraform/provider.tf`](terraform/provider.tf) derives a `container.<host>`
endpoint from `var.endpoint`, which is
`http://container.localhost:8080/container/` by default.
`*.localhost` resolves to a loopback address on Linux; if it does not on your
machine, set `container_endpoint` to a `container.*` host that resolves. The
module's own gate and the k3d gate both refuse to run when the derived host does
not resolve rather than falling back to the shared path.

Two rules that follow, both encoded in the module:

- **No `clusterId` on a GKE create.** It is exactly the signal that makes a
  request Managed Kafka's, and the provider sends none.
- **The Java leg does not need any of this.** It talks to GKE over the native
  gRPC `ClusterManager` transport, not a custom HTTP endpoint, so the caveat is
  a Terraform-only concern.

## Zonal vs regional

`gke_location` takes a zone or a region, and the same value feeds the GKE
cluster and the Dataproc `gkeClusterTarget`. The demo is zonal
(`us-central1-a`): that is the real-GCP quickstart shape, and it is the shape
that used to fail here — a Dataproc create targeting a zone was rejected before
the GKP1 fix, which is why it is the one the gates pin.

The Dataproc cluster itself is a regional resource either way; only the GKE
target moves. A regional GKE cluster is a supported shape (set `gke_location`
to `us-central1`), and the module derives node-pool locations for both, but
nothing in the demo asserts the regional variant end to end. The module's own
gate asserts the cluster's `location` and the exact
`projects/<p>/locations/<gke_location>/clusters/<name>` target string, so a
silently rewritten location fails instead of passing.

## Cross-project buckets, and what that does not prove

`storage_project` moves all three buckets into a second project while the GKE
cluster, the virtual cluster and the jobs stay in `project`. That is the shape
customers run when a platform team owns storage and a workload team owns the
cluster. The k3d gate runs it by default (`dpgke-demo` runner,
`dpgke-demo-storage` owner); `terraform/run-cross-project.sh` runs the
Terraform-only version.

What the gates assert:

- the owner's `buckets.list` contains all three buckets; the runner's listing is
  empty, so no shadow bucket was created (GCS bucket names are a global
  namespace, not a per-project one);
- `buckets.get` by name resolves from either project's context;
- the jobs' objects — driver output, part files, the Terraform-uploaded seed —
  land in the owner's bucket;
- after teardown, both listings are empty.

The module cannot create the second project: `google_project` reads Cloud
Billing, which the provider cannot reach here. On real GCP it has to exist
first (`gcloud projects create dpgke-demo-storage`); on the emulator the gates
create it through the Cloud Resource Manager v1 API (`POST /v1/projects`, the
call `gcloud projects create` makes) and assert it appears in `projects.list`,
because a merely synthesized project id would not. The project **record** is
left behind — real GCP keeps a deleted project restorable — while every resource
the module put in it is destroyed and asserted gone.

Two real-GCP behaviours are not modelled, so the demo shows the shape and never
an authorization outcome (DGD9):

- **bucket IAM is metadata only**: bindings are stored and echoed but never
  enforced, so a job in one project can never be *denied* a bucket in another;
- **requester-pays / `userProject` is not read at all**, which is why the
  module pins `user_project_override = false`.

## The downscoped credential

The buckets are reachable with any plain token, which proves nothing about the
way a customer hands a workload short-lived, scoped access. So the submitter
does what a workload does (DGD5):

1. it takes a source access token (the metadata-server / Workload Identity
   shape) and posts an RFC 8693 token-exchange to the emulator's STS surface
   (`POST /v1/token`, the same body real STS takes at
   `https://sts.googleapis.com/v1/token`);
2. it mints **two** downscoped tokens from Credential Access Boundaries — one
   that may read the input bucket at the input object's prefix
   (`inRole:roles/storage.legacyObjectReader`), one that may write the output
   bucket at the job's output prefix
   (`inRole:roles/storage.legacyBucketWriter`) — and builds a separate `Storage`
   client per token;
3. it does the work with each credential: read the input object, write a probe
   object under the output prefix;
4. it asserts the denials, because a boundary demo that only reports its 200s is
   theatre.

The denials and the witness, as the in-cluster transcript prints them:

- input read under the input boundary is `200`; output write under the output
  boundary is `200`;
- output read with the **plain** token is `200` — the witness that makes the
  next lines meaningful, so a `403` cannot be a wrong object name or a broken
  client;
- output read under the output boundary is `403` (it grants create/overwrite,
  not read);
- output read and output write under the **input** boundary are `403` (the
  boundary names the input bucket);
- input write under the **output** boundary is `403`;
- the denied write created no object, and the probe is deleted so the job's
  output prefix starts empty — real Spark's output committer refuses a prefix
  that already exists, and the k3d gate is what found that; the mock executor
  never writes, so the DGD5 gate alone could not.

Why the denials are the assertion: the emulator does **not** verify token
signatures. It trusts the `access_boundary` claim in the JWT payload and
enforces the rules on every GCS call. A boundary that is never enforced looks
exactly like a working one from the 200s, so the demonstration is the `403`s.

What this still does not prove: that the **Spark job** held the tokens. The mock
executor never fetches a credential, and in the k3d gate the exchange happens in
the submitter process — the same `Storage` + boundary code a driver would run,
but not inside the Dataproc pod. The boundary itself is proven; the job's use of
it is the same-code-shape argument, and the README says which one it is.

## The gates

Every gate is safe to run in a checkout without the tools: it prints `SKIP` and
exits 0, and none of them installs anything. A tool that is present and an
assertion that fails is a failure.

| command | what it asserts | when it skips |
|---|---|---|
| `make test-e2e-dpgke-demo-k3d` | the whole lifecycle: Terraform apply → submitter image → Helm → in-cluster Java → assertions → `helm uninstall` → `terraform destroy` → "nothing is left" | a missing terraform/helm/kubectl/java/mvn/curl/Docker daemon, no `jaiscloud` namespace or `svc/jaiscloud-gcp`, or a kubectl context with no `k3d-*` node |
| `make test-dpgke-demo-terraform` | the module alone: apply, a re-plan with no changes, the provider-submitted job, the destroy order, an empty state | no `terraform` on PATH |
| `./demo/dpgke-lifecycle/terraform/run.sh [endpoint]` | the same gate with a chosen emulator endpoint (it builds and starts one if none is healthy) | no `terraform` on PATH |
| `./demo/dpgke-lifecycle/terraform/run-cross-project.sh` | the same gate in the two-project shape, in its own Terraform workspace | no `terraform` (delegates to `run.sh`) |
| `./demo/dpgke-lifecycle/java/run.sh [endpoint]` | build + unit tests + two runs (create, then adopt/submit/tear down) and the DGD5 denials | no `java` or `mvn` (after sourcing SDKMAN) |
| `./demo/dpgke-lifecycle/helm/render.sh` | `helm lint`, two identical renders, and three goldens (defaults, overrides, tokens) | no `helm` |
| `./demo/dpgke-lifecycle/scripts/e2e-k3d.sh` | the script behind the k3d target; the target only supplies the Makefile's k3d conventions | as the k3d target |
| `./demo/dpgke-lifecycle/scripts/toolchain.sh --check` | reports which of JDK 21 / Maven 3 / Helm are usable, and what it would install; exits 0 only when complete | never — it reports instead of skipping |

```sh
make test-e2e-dpgke-demo-k3d                            # rebuild the emulator image first
make test-e2e-dpgke-demo-k3d SKIP_GCP_IMAGE_REBUILD=1   # reuse the deployed one
```

`make test-e2e-dpgke-demo-k3d` runs, in order:

1. preflight — terraform, helm, kubectl, java/mvn, docker, curl and a k3d
   cluster;
2. the emulator image from this tree (`_refresh-gcp-image`, unless
   `SKIP_GCP_IMAGE_REBUILD=1`), the per-cluster namespace RBAC
   (`deploy/k8s/rbac.yaml`) and `JAISCLOUD_SPARK_EXECUTOR_MODE=k8s`;
3. `terraform apply` **on the host**, through a `kubectl port-forward`;
4. `mvn package` + `docker build` + `docker push` (or `k3d image import`) for the
   submitter;
5. `helm install` into the Dataproc workload namespace — the submitter runs
   **inside the cluster**;
6. assertions on the Terraform job, the Java job, the bytes in GCS and the
   downscoped-credential denials;
7. `helm uninstall`, `terraform destroy`, and a listing of clusters, buckets,
   namespaces, pods and PVCs to prove nothing is left.

The gate uses fixed resource names, so it refuses to start when a previous run's
Terraform workspace still holds state or the Dataproc namespace exists: a
second lifecycle on top of a leftover one cannot converge, and adopting or
deleting someone else's clusters silently is worse than stopping.

### What the k3d gate asserts (one PASS line each)

- the emulator is in k8s mode with a real Spark image, and the derived
  `container.*` host resolves;
- Terraform's cluster is `RUNNING`, its job is `DONE`, and its part file is the
  real word count;
- the emulator provisioned the Dataproc namespace;
- the submitter Job completed; its log shows the in-cluster gRPC **and** REST
  Service DNS, the submitted job id, the job `DONE` line, the plain-token `200`
  witness and the `403` denials;
- the Java job is `DONE`, its `driverOutputResourceUri` is in the staging
  bucket and resolves to a non-empty object, the output prefix has a part file
  with the word count and `_SUCCESS`;
- `helm uninstall` removed the Job, ConfigMap and Secret and left no pods;
- `terraform destroy` emptied the state, both bucket listings, and removed the
  GKE cluster, the virtual cluster and the Terraform job; the Dataproc
  namespace, its pods and its PVCs are gone.

The Java job's *record* is deliberately not asserted gone: real Dataproc keeps
job history as a regional resource after its cluster is deleted, and the
emulator models that. The resources the demo owns — cluster, buckets,
Kubernetes namespace and pods — are what the teardown assertions cover.

## Toolchain

[`scripts/toolchain.sh`](scripts/toolchain.sh) installs what the demo needs,
user-local under `~/.local` and without sudo, and is idempotent:

```sh
./demo/dpgke-lifecycle/scripts/toolchain.sh --check   # report only; changes nothing
./demo/dpgke-lifecycle/scripts/toolchain.sh           # install what is missing
```

It puts Temurin JDK 21 in `~/.local/opt/jdk-21` (from the Adoptium API), Maven
in `~/.local/opt/maven` (from `archive.apache.org`, which keeps every release,
unlike `dlcdn`), and Helm in `~/.local/bin` (the upstream installer with
`USE_SUDO=false HELM_INSTALL_DIR=$HOME/.local/bin`). It detects a JDK 21, any
Maven 3 and any Helm 3+ already present and prints `nothing to do` instead of
reinstalling; a JDK of a different major is reported, never replaced. `--check`
downloads nothing and creates no directory; it exits 0 only when the toolchain
is complete, so a gate can use it as a precondition.

**A machine that already has `java` 21, `mvn` and `helm` needs none of this.**
The demo's gates source `~/.sdkman/bin/sdkman-init.sh` themselves when the tools
are not on PATH, and skip with a message when they are absent.

The script's comments also carry the two alternatives: SDKMAN (user-local, and
what this repository's environment happens to use — note the four-part Temurin
identifier `21.0.13.0+9-tem`, and never a bare `sdk install java`, which takes
the current default rather than the release this demo targets), and apt (needs
sudo, so it is documented and deliberately not implemented).

## Environment

Everything is overridable; the defaults are the module's defaults. The ones worth
knowing:

| variable | default | meaning |
|---|---|---|
| `K8S_NAMESPACE` | `jaiscloud` | namespace the emulator runs in |
| `DPGKE_NAMESPACE` | `dpgke-demo` | Dataproc workload **and** Helm release namespace |
| `PROJECT` / `STORAGE_PROJECT` | `dpgke-demo` / `dpgke-demo-storage` | runner / bucket owner |
| `REGION` / `GKE_LOCATION` | `us-central1` / `us-central1-a` | regional Dataproc, zonal GKE |
| `GCP_REGISTRY` | `10.0.100.21:5050` | registry the k3d node can pull from |
| `DPGKE_IMAGE_MODE` | `registry` | `import` uses `k3d image import` instead of a push |
| `DPGKE_K3D_CLUSTER` | from the `k3d-*` context | cluster name for `import` mode |
| `DPGKE_IMAGE_REPO` / `DPGKE_IMAGE_TAG` | registry path / unique per run | submitter image |
| `DPGKE_PORT` | `18080` | host port-forward for Terraform's REST path |
| `DPGKE_OUTPUT_PREFIX` / `DPGKE_TF_OUTPUT_PREFIX` | `wordcount` / `wordcount-tf` | the two jobs' prefixes |
| `DPGKE_JOB_TIMEOUT` | `900` | seconds to wait for the submitter Job |
| `SKIP_GCP_IMAGE_REBUILD` | unset | `1` reuses the deployed emulator |
| `DPGKE_TF_ENDPOINT` | `http://localhost:8080` | endpoint the `test-dpgke-demo-terraform` target passes to the module gate |

The Java app has its own `DGD3_*` / `DGD5_*` surface, documented in
[`java/README.md`](java/README.md) and mapped from the chart's values in
[`helm/README.md`](helm/README.md). The Terraform variables are in
[`terraform/README.md`](terraform/README.md).

## Limitations the demo states rather than asserts

- **Spark runs on the emulator's executor**, not on the target GKE cluster. The
  cluster and its reconciled node pools are metadata; no Spark pod is scheduled
  onto them.
- **Bucket IAM is metadata-only** (DGD9): a cross-project read is never denied,
  so the cross-project shape is demonstrated, not authorised.
- **Requester-pays / `userProject` is not modelled**, so `user_project_override`
  is pinned false in the module.
- **`--setup-workload-identity`'s real IAM bindings are a recorded no-fix**
  (DPG26): the demo checks the `workloadPool` field, not the bindings.
- **The downscoped token is held by the submitter, not by the Spark driver**:
  the boundary is enforced and asserted, the driver's use of it is the same code
  shape rather than a second execution.
- **Dataproc keeps job history after its cluster is deleted** (real GCP
  behaviour), so the gate does not assert the Java job record is gone.

Related: the module's `terraform/README.md` carries the provider-specific notes
(`container_custom_endpoint`, the destroy order, the pinned Spark release);
`README-GCP.md` has the emulator-side service notes this demo exercises.
