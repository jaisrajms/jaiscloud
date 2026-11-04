# Dataproc-on-GKE lifecycle — Helm chart

Installs the demo's Java submitter ([`../java/`](../java/)) as a one-shot
Kubernetes `Job`, configured from the Terraform module's outputs
([`../terraform/`](../terraform/)). It is the "install" step of the reader's
path — `terraform apply` → `helm install` → read the logs — and it exists so
nobody has to hand-edit a manifest to point the app at the infrastructure
Terraform just created.

```
ConfigMap  the non-secret DGD3_* environment DemoConfig.java reads
Secret     GOOGLE_OAUTH_ACCESS_TOKEN, and nothing else (optional)
Job        java -jar dpgke-lifecycle-java.jar, once
```

There is no `Deployment`, `Service` or ingress: the app runs the lifecycle
(adopt the virtual cluster → submit the PySpark job → wait for `DONE` → read the
driver output) and exits. `helm install` is done when the Job is.

## Before you install

1. **Infrastructure exists.** `terraform apply` in `../terraform` — the chart
   creates no GKE cluster, no virtual cluster and no bucket. It adopts what the
   module made and fails loudly if it is missing.
2. **The emulator is reachable from a pod.** `jaiscloud-gcp` on REST `:8080`
   and gRPC `:8081` (see [Endpoints](#endpoints-the-one-caveat)). Against real
   GCP, endpoints are omitted and ADC/Workload Identity supplies the credential.
3. **An image exists.** The default `image.repository`/`image.tag` is an
   explicit placeholder — no tag is published for this demo. Build
   [`../java/Dockerfile`](../java/Dockerfile) and load it into the cluster, or
   point `image.*` at your own build. `helm template` and `helm lint` do not
   need the image; a real install does.

## Install

With the module applied unchanged, the chart's defaults already match it — only
the endpoints (and image) need overriding:

```sh
cd demo/dpgke-lifecycle/helm
helm install dpgke-lifecycle . \
  --namespace dpgke-demo --create-namespace \
  --set endpoints.rest=http://host.k3d.internal:8080 \
  --set endpoints.grpc=host.k3d.internal \
  --set image.repository=jaiscloud/dpgke-lifecycle-java \
  --set image.tag=dev
```

If you changed the module's variables, feed the outputs back rather than editing
values by hand (this is the whole point of the chart):

```sh
TF=../terraform
helm install dpgke-lifecycle . \
  --namespace "$(terraform -chdir="$TF" output -raw namespace)" --create-namespace \
  --set project="$(terraform -chdir="$TF" output -raw project)" \
  --set region="$(terraform -chdir="$TF" output -raw region)" \
  --set storageProject="$(terraform -chdir="$TF" output -raw storage_project)" \
  --set gkeLocation="$(terraform -chdir="$TF" output -raw gke_location)" \
  --set gkeCluster="$(terraform -chdir="$TF" output -raw gke_cluster_name)" \
  --set dataprocCluster="$(terraform -chdir="$TF" output -raw dataproc_cluster_name)" \
  --set sparkRelease="$(terraform -chdir="$TF" output -raw spark_release)" \
  --set machineType="$(terraform -chdir="$TF" output -raw machine_type)" \
  --set-json buckets="$(terraform -chdir="$TF" output -json buckets)" \
  --set endpoints.rest=http://host.k3d.internal:8080 \
  --set endpoints.grpc=host.k3d.internal
```

`--set-json` takes the module's `buckets` object whole (Helm 3.10+), so the
input/output/staging names never get transcribed by hand.

Watch it, then uninstall:

```sh
kubectl -n dpgke-demo logs -f job/dpgke-lifecycle
kubectl -n dpgke-demo wait --for=condition=complete job/dpgke-lifecycle --timeout=15m
helm uninstall dpgke-lifecycle --namespace dpgke-demo
```

A `Job` is immutable and is not re-created by `helm upgrade`, so re-running the
lifecycle is `helm uninstall` then `helm install` (delete the Job if you want to
keep the release). `backoffLimit: 0` means a failed run stays failed instead of
quietly submitting a second Dataproc job; read the logs, fix the cause, reinstall.

## `terraform output` → values

| `terraform output` | value | notes |
| --- | --- | --- |
| `project` | `project` | project owning the GKE + Dataproc clusters |
| `region` | `region` | Dataproc region (regional even for a zonal GKE cluster) |
| `storage_project` | `storageProject` | project owning the buckets (cross-project shape) |
| `gke_location` | `gkeLocation` | zone for a zonal cluster; also the `gkeClusterTarget` location |
| `gke_cluster_name` | `gkeCluster` | |
| `dataproc_cluster_name` | `dataprocCluster` | node pools are `<name>-controller` / `<name>-spark` |
| `namespace` | `namespace` | the app's `DGD3_NAMESPACE`; **not** where the chart installs (see below) |
| `buckets` | `buckets.{input,output,staging}` | `--set-json buckets="$(terraform output -json buckets)"` |
| `spark_release` | `sparkRelease` | fully qualified; the app asserts it on adopt |
| `machine_type` | `machineType` | asserted on the reconciled pools |
| `service_account_email` | — | the job GSA; bind it to `job.serviceAccountName` when using Workload Identity |
| `job_*` | — | the module's own `google_dataproc_job` (DGD2). This chart's Job is a different submission; nothing here reads those outputs |

`namespace` is the Kubernetes namespace Dataproc creates the Spark workload in.
The chart's objects go to the release namespace (`--namespace`), which the
examples above set to the same value; keeping them separate means the chart
never writes into a namespace the release does not own.

## Values → environment

The app reads exactly the names in
[`../java/src/main/java/com/jaiscloud/demo/dpgke/DemoConfig.java`](../java/src/main/java/com/jaiscloud/demo/dpgke/DemoConfig.java).
The chart must not rename them (DGD3 is merged); it only carries them.

| value | env | |
| --- | --- | --- |
| `project` | `DGD3_PROJECT` | |
| `region` | `DGD3_REGION` | |
| `storageProject` | `DGD3_STORAGE_PROJECT` | |
| `gkeLocation` | `DGD3_GKE_LOCATION` | |
| `gkeCluster` | `DGD3_GKE_CLUSTER` | |
| `dataprocCluster` | `DGD3_DATAPROC_CLUSTER` | |
| `namespace` | `DGD3_NAMESPACE` | |
| `buckets.input` / `.output` / `.staging` | `DGD3_INPUT_BUCKET` / `DGD3_OUTPUT_BUCKET` / `DGD3_STAGING_BUCKET` | |
| `endpoints.rest` | `DGD3_REST_ENDPOINT` | full URL |
| `endpoints.grpc` | `DGD3_GRPC_ENDPOINT` | **host only** |
| `endpoints.grpcPort` | `DGD3_GRPC_PORT` | the app joins host + port itself |
| `sparkRelease` | `DGD3_SPARK_RELEASE` | |
| `machineType` | `DGD3_MACHINE_TYPE` | |
| `job.codeObject` | `DGD3_JOB_CODE_OBJECT` | object Terraform uploaded |
| `job.inputObject` | `DGD3_JOB_INPUT_OBJECT` | |
| `job.outputPrefix` | `DGD3_JOB_OUTPUT_PREFIX` | |
| `job.sparkScript` | `DGD3_SPARK_SCRIPT` | empty: the object already exists in the bucket |
| `job.teardown` | `DGD3_TEARDOWN` | default `false` — see below |
| `job.extraEnv` | (appended) | escape hatch for anything the chart does not expose |

`endpoints.grpc` is a host, not `host:port`: `DemoConfig.grpcTarget()` appends
`DGD3_GRPC_PORT`, so `host:port` there would make the app dial
`host:port:port`. That is also why `endpoints.grpcPort` exists alongside the
`endpoints.{rest,grpc}` pair.

`DGD3_MIN_NODES` / `DGD3_MAX_NODES` / `DGD3_POLL_*` are deliberately not exposed:
they only matter when the app creates the cluster, and here Terraform does. Set
them through `job.extraEnv` if you want different poll bounds.

## What the chart does not do (and the teardown decision)

- **It does not create infrastructure.** GKE cluster, virtual cluster, buckets
  and node pools are `../terraform`'s. Running the chart against nothing fails
  in the app, with a message naming the missing resource.
- **It does not tear anything down.** `job.teardown` (`DGD3_TEARDOWN`) defaults
  to `false`, and `helm uninstall` removes only the Job, ConfigMap and Secret.
  Teardown is `terraform destroy`, because Terraform owns the resources.

  There is no `pre-delete`/`post-delete` Helm hook, deliberately. A Kubernetes
  `Job` cannot run on uninstall — it only exists as a resource of the release —
  so "delete on uninstall" would have to be a hook Job that runs `gcloud`
  deletes. That hook would (a) duplicate ownership with `terraform destroy` and
  race it, (b) leave the release deleting even when the hook fails (Helm's
  default `hook-delete-policy` deletes failed hooks), and (c) need the very
  credentials the chart is trying not to hold. A demo that says "`terraform
  destroy` is the teardown" is shorter, honest, and cannot orphan a cluster.

  If you *do* want this Job to create and delete the infrastructure (the
  standalone shape `java/run.sh` uses), set `job.teardown=true`; that is the
  app's own teardown path, not a Helm hook.
- **It does not publish an image.** `image.tag` is
  `0.0.0-dgd6-placeholder`. DGD6 builds [`../java/Dockerfile`](../java/Dockerfile)
  and loads it into k3d; nothing in this session built or pushed anything.

## Endpoints: the one caveat

A pod's `localhost` is the pod, not your laptop. The defaults therefore point at
the in-cluster Service that [`deploy/k8s/jaiscloud-gcp.yaml`](../../../deploy/k8s/jaiscloud-gcp.yaml)
creates:

```yaml
endpoints:
  rest: http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080
  grpc: jaiscloud-gcp.jaiscloud.svc.cluster.local
  grpcPort: 8081
```

For an emulator outside the cluster use `host.k3d.internal` (k3d) or
`host.docker.internal` (Docker Desktop), or `kubectl port-forward` the emulator
into the cluster's network. Whatever you choose must resolve from the Job's pod
in *both* transports: the Storage client speaks HTTP/JSON to `endpoints.rest`,
while the Dataproc and GKE clients use gRPC on `endpoints.grpc:endpoints.grpcPort`.

## Credentials

The Secret holds one key, `GOOGLE_OAUTH_ACCESS_TOKEN`, and never a generated
value. Two ways to use it:

```sh
# 1. Let the chart render the Secret from a value (fine for the emulator).
--set-string credentials.accessToken="$GOOGLE_OAUTH_ACCESS_TOKEN"

# 2. Create the Secret yourself and point the chart at it — no token in values.
kubectl -n dpgke-demo create secret generic dpgke-credentials \
  --from-literal=GOOGLE_OAUTH_ACCESS_TOKEN=emulator
helm install ... --set credentials.existingSecret=dpgke-credentials
```

With `credentials.existingSecret` set the chart renders **no** Secret; the Job
reads `GOOGLE_OAUTH_ACCESS_TOKEN` from yours.

Leaving both empty is the real-GCP shape: the pod would use Workload Identity
(annotate a Kubernetes ServiceAccount as `job.serviceAccountName` and bind it to
`terraform output service_account_email`) or ADC. The current clients are built
with `NoCredentials` because the emulator does not verify tokens
(`DpgkeLifecycle`), so treat the Secret as the real-GCP surface a reader copies:
the chart wires it correctly and never invents it, and no assertion in the demo
depends on the emulator enforcing it.

## The deterministic gate

`helm template` is the assertion — no cluster, no credentials:

```sh
cd demo/dpgke-lifecycle/helm
./render.sh
```

`render.sh` runs `helm lint`, renders the defaults **twice** and compares them
byte for byte, diffs against `golden/defaults.yaml`, then repeats with
`golden/override-values.yaml` (a different project, cross-project buckets, a
port-forwarded emulator, `existingSecret`, `extraArgs`/`extraEnv`, a service
account) against `golden/overrides.yaml`. A second values set is what keeps the
templates honest: the defaults alone would pass even if a conditional branch
were broken.

The chart is written to make that possible: no `randAlphaNum`, no `now`, no
`lookup`, no generated Secret value, and no `.Capabilities` branching. If a
change is intended, regenerate and commit the goldens:

```sh
UPDATE=1 ./render.sh
```

It exits 0 with `SKIP: 'helm' not installed` when Helm is absent, so the gate is
honest in a toolchain-less environment instead of silently green. Goldens were
produced with Helm v4.3.1; the chart is `apiVersion: v2`, so Helm 3 accepts it
too — a Helm-version rendering change shows up as drift, not as a surprise.

By hand:

```sh
helm lint .
helm template dpgke-lifecycle . --namespace dpgke-demo
helm template dpgke-lifecycle . --namespace dpgke-demo \
  --values golden/override-values.yaml
```
