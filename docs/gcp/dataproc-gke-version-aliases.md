# Dataproc on GKE — `dataproc-<major.minor>` image-line alias (real-GCP capture)

Recorded **2026-10-30** against real GCP (project `parity-diff-jaiscloud`, ADC,
`dataproc.googleapis.com`, region `us-central1`, GKE cluster `dpg36-probe` in
`us-central1-a`). This capture closes the open question in DPG36: the
`kubernetesSoftwareConfig.componentVersion.SPARK` alias form
`dataproc-<major.minor>` is documented by
[release versions](https://docs.cloud.google.com/managed-spark/docs/guides/dpgke/gke-versions)
("Most recent version of Spark engine that is compatible with Managed Service
for Apache Spark 2.2 images") and W5.2 refused to resolve it because neither
Discovery nor the release page carries an image-major → engine-release table.
Real GCP answers it directly: **only `dataproc-2.2` is accepted, and it resolves
to `3.5-dataproc-28`** — the same release `latest` resolves to.

## Method

1. **Acceptance probe (no infrastructure).** `POST
   /v1/projects/parity-diff-jaiscloud/regions/us-central1/clusters` with a
   `virtualClusterConfig` whose `gkeClusterTarget` deliberately does not exist
   and whose `nodePoolTarget` names a pool under it. Dataproc aggregates
   validation errors, so an accepted Spark version yields only the target error,
   while a rejected one adds
   `'<value>' is not a valid spark version for Virtual clusters.`
2. **Resolution probe.** One real GKE cluster (`dpg36-probe`, zonal, one
   `e2-medium` node, Workload Identity pool), then virtual clusters created with
   `gcloud dataproc clusters gke create … --spark-engine-version=<value>` and
   read back with `clusters.get` (`kubernetesSoftwareConfig.componentVersion`).

## Acceptance: which values real GCP takes

| `componentVersion.SPARK` | real GCP |
|---|---|
| `dataproc-2.2` | **accepted** |
| `dataproc-1.4`, `dataproc-1.5` | `'<value>' is not a valid spark version for Virtual clusters.` |
| `dataproc-2.0`, `dataproc-2.1`, `dataproc-2.3` | same |
| `dataproc-3.0`, `dataproc-4.0` | same |
| `dataproc-2.2.1` | same |
| `DATAPROC-2.2` | same (case-sensitive) |

So the image-line alias space is not "every Dataproc image line": the GKE engine
releases are built on the 2.2 image line, and `dataproc-2.2` is the only line the
API accepts. The other lines are rejected, not silently mapped — which is why
the emulator keeps them `InvalidArgument` instead of inferring a table from the
image-release pages (Spark 2.4/3.1/3.5 engine lines vs image lines 1.x/2.0/2.1/
2.2/2.3/3.0).

## Resolution: what the accepted alias dereferences to

| requested | `clusters.get` → `componentVersion.SPARK` |
|---|---|
| `latest` (control) | `3.5-dataproc-28` |
| `dataproc-2.2` | `3.5-dataproc-28` |

Both virtual clusters reported the effective `kubernetesNamespace` as the cluster
name (the emulator already does this) and echoed the resolved release on read.

## Side findings (not this item's behaviour)

* **`nodePoolTarget.nodePool` must be a full resource name.** Real GCP rejects a
  short pool name with
  `GKE Node Pool name '<name>', must conform to pattern 'projects/([^/]+)/(?:locations|zones)/([^/]+)/clusters/([^/]+)/nodePools/([^/]+)'`.
  The emulator accepted short names and its conformance transcripts used them;
  DPG44 made it reject a bare pool id with the same message and moved the
  transcripts to full names.
* **Validation errors are aggregated.** Real GCP returns
  `400 INVALID_ARGUMENT` / `"Multiple validation errors:\n - …\n - …"` listing
  every problem in one response; the emulator fails on the first. Recorded as
  DPG45.

## Cleanup

Every created resource was deleted in the same session: the two virtual
clusters, the GKE cluster `dpg36-probe` (with the pools Dataproc created under
it), and the staging bucket `dpg36-probe-staging-parity-diff-jaiscloud`. Verified
afterwards: `gcloud container clusters list` and `gcloud dataproc clusters list`
are both empty and the project has no bucket left. Total spend: one GKE
zonal cluster plus two small autoscaled pools for ~25 minutes.
