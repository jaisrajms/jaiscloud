# Dataproc-on-GKE lifecycle — Terraform, Helm and the Java SDK

One artefact a customer can copy: a Terraform module that stands up a zonal GKE
cluster, a Dataproc-on-GKE virtual cluster and the input/output/staging buckets;
a Helm chart that installs a one-shot submitter; and a Java application built on
the official Google clients that adopts the cluster, submits a PySpark job,
waits for `DONE`, reads the driver output from GCS and proves the downscoped
credential's boundary by asserting the denials.

| directory | what it is | its own gate |
|---|---|---|
| [`terraform/`](terraform/) | the module (GKE + virtual cluster + buckets + job) | `../terraform/run.sh` |
| [`java/`](java/) | the submitter (`google-cloud-dataproc`, `-storage`, `-container`) | `../java/run.sh` |
| [`helm/`](helm/) | the chart that injects Terraform's outputs into the app | `../helm/render.sh` |
| [`spark/`](spark/) | the word-count PySpark file Terraform uploads | exercised by the gates |
| [`scripts/`](scripts/) | the end-to-end k3d gate | `e2e-k3d.sh` |

Each directory has its own README with the details. This file is about the one
gate that runs the whole lifecycle (DGD6); DGD7 expands it into the narrated
demo guide and ships the toolchain script.

## `make test-e2e-dpgke-demo-k3d`

```bash
make test-e2e-dpgke-demo-k3d                 # rebuild the emulator image first
make test-e2e-dpgke-demo-k3d SKIP_GCP_IMAGE_REBUILD=1   # reuse the deployed one
```

It runs, in order:

1. preflight — terraform, helm, kubectl, java/mvn, docker and a k3d cluster;
2. the emulator image from this tree (`_refresh-gcp-image`), the per-cluster
   namespace RBAC (`deploy/k8s/rbac.yaml`) and `JAISCLOUD_SPARK_EXECUTOR_MODE=k8s`;
3. `terraform apply` **on the host**, through a `kubectl port-forward`;
4. `mvn package` + `docker build` + `docker push` for the submitter;
5. `helm install` into the Dataproc workload namespace — the submitter then runs
   **inside the cluster** and reaches the emulator over
   `jaiscloud-gcp.<namespace>.svc.cluster.local`;
6. assertions on the job, the bytes in GCS and the downscoped-credential denials;
7. `helm uninstall`, `terraform destroy`, and a listing of clusters, buckets,
   namespaces, pods and PVCs to prove nothing is left.

It skips with a message (exit 0) when a tool or the cluster is absent — the
`make test-gcp-terraform` precedent. A tool that *is* there and an assertion
that fails are a failure. Nothing is installed by the gate.

`make test-dpgke-demo-terraform` is the module's own gate (apply, re-plan,
destroy) for iterating without a cluster; the k3d target does not use it.

### Two clients, two paths, on purpose

* **Terraform on the host.** The hashicorp/google provider strips the path from
  a custom endpoint and keeps scheme+host, so the GKE API needs its own host.
  The module derives `container.<endpoint-host>` from `var.endpoint`; a host
  port-forward is enough because `container.localhost` resolves to loopback.
  The script refuses to run if it cannot resolve.
* **The Java app in a pod.** The chart points `endpoints.rest` and
  `endpoints.grpc` at the emulator's ClusterIP Service, never at the host
  port-forward. The transcript is checked for both targets, so a gate that
  accidentally went back to the host path fails.
* **The image.** Built from `java/Dockerfile` and pushed to `GCP_REGISTRY`
  (`10.0.100.21:5050` by default — the registry the emulator image also comes
  from), then pulled by the chart with `IfNotPresent`. `k3d image import` is an
  equivalent local-only route; the gate uses the registry so it also works when
  k3d is on a remote Docker host.

### Real Spark, and the one shape it does not fake

The k3d gate uses the emulator's Kubernetes Spark executor
(`JAISCLOUD_SPARK_EXECUTOR_MODE=k8s`) with the same `spark-gcs` image the other
k3d Dataproc suites use. The job's output prefix is asserted by content, not
existence: `dataproc 2`, `gke 2`, `on 2`, `the 4` and `count 1`, plus Spark's
`_SUCCESS`. The Terraform-submitted job runs under the same executor, so both
the provider path and the Java path produce real bytes.

What the emulator does not model: the Spark driver/executor pods run in the
emulator's cluster (in the Dataproc workload namespace), not on the *target*
GKE cluster the virtual cluster names. That is the documented difference — the
GKE target is reconciled as metadata, while the job executes on the emulator's
own executor.

### Cross-project buckets

The gate runs the shape customers actually use (DGD8): the GKE cluster, virtual
cluster and jobs live in `dpgke-demo`, while the input/output/staging buckets
are owned by `dpgke-demo-storage`. It asserts the owner's listing contains all
three buckets, the runner's listing is empty (no shadow bucket), the jobs'
objects land in the owner's bucket, and both listings are empty after `terraform
destroy`. The bucket-owning project record is left behind; the Terraform gate
does the same, because `google_project` needs Cloud Billing, which this provider
cannot reach.

### What the gate asserts (one PASS line each)

* the emulator is in k8s mode with a real Spark image, and the derived
  `container.*` host resolves;
* Terraform's cluster is `RUNNING`, its job is `DONE`, and its part file is the
  real word count;
* the emulator provisioned the Dataproc namespace;
* the submitter Job completed; its log shows the in-cluster gRPC **and** REST
  Service DNS, the submitted job id, the job `DONE` line, the plain-token `200`
  witness and the four `403` denials;
* the Java job is `DONE`, its `driverOutputResourceUri` is in the staging
  bucket and resolves to a non-empty object, the output prefix has a part file
  with the word count and `_SUCCESS`;
* `helm uninstall` removed the Job, ConfigMap and Secret and left no pods;
* `terraform destroy` emptied the state, both bucket listings, and removed the
  GKE cluster, virtual cluster and Terraform job; the Dataproc namespace, its
  pods and its PVCs are gone.

### Environment

Everything is overridable; the defaults are the module's defaults. The ones
worth knowing:

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

### Limitations the demo states rather than asserts

* Bucket IAM is metadata-only on the emulator (DGD9), so a cross-project read is
  never denied; the cross-project shape is demonstrated, authorization is not.
* `--setup-workload-identity`'s real IAM bindings are a recorded no-fix (DPG26);
  the demo checks the `workloadPool` value, not the bindings.
* Requester-pays/`userProject` is not modelled, so `user_project_override` is
  pinned false in the module.
* Dataproc keeps job history after its cluster is deleted (real GCP behaviour),
  so the gate does not assert the Java job record is gone — it asserts the
  cluster, buckets, namespace and pods are.
