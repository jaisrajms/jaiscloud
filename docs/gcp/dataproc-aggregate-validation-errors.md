# Dataproc `clusters.create`: aggregated virtual-cluster validation (DPG45)

Captured **2026-10-31** against real GCP (project `parity-diff-jaiscloud`,
`dataproc.googleapis.com/v1`, region `us-central1`, ADC). These probes close
DPG45: the shape of the aggregated 400 a create returns when a
`virtualClusterConfig` has more than one problem. No infrastructure was created —
every request below was rejected.

## Method

`POST https://dataproc.googleapis.com/v1/projects/parity-diff-jaiscloud/regions/us-central1/clusters`
with `Authorization: Bearer $(gcloud auth print-access-token)` and the JSON body
under **Capture**. `gkeClusterTarget` deliberately names a cluster that does not
exist, so an accepted value contributes only the target error while a rejected
one adds its own line.

## Capture

### One problem: the bare message (no envelope)

```json
{"clusterName":"dpg45-p9","virtualClusterConfig":{"stagingBucket":"dpg45-probe-staging",
 "kubernetesClusterConfig":{"kubernetesNamespace":"dpg45-ns",
  "gkeClusterConfig":{"gkeClusterTarget":"projects/parity-diff-jaiscloud/locations/us-central1/clusters/does-not-exist",
   "nodePoolTarget":[{"nodePool":"projects/parity-diff-jaiscloud/locations/us-central1/clusters/does-not-exist/nodePools/pool-a","roles":["SPARK_DRIVER"]}]},
  "kubernetesSoftwareConfig":{"componentVersion":{"SPARK":"3.5-dataproc-28"}}}}}
```

`400 INVALID_ARGUMENT`:

```text
Exactly one GkeNodePoolTarget within the virtual cluster must have 'DEFAULT' role.
```

### Several problems: one message, one line per problem

```json
{"clusterName":"dpg45-probe3","virtualClusterConfig":{"stagingBucket":"dpg45-probe-staging",
 "kubernetesClusterConfig":{"kubernetesNamespace":"dpg45-ns",
  "gkeClusterConfig":{"gkeClusterTarget":"projects/parity-diff-jaiscloud/locations/us-central1/clusters/does-not-exist",
   "nodePoolTarget":[{"nodePool":"projects/parity-diff-jaiscloud/locations/us-central1/clusters/does-not-exist/nodePools/pool-a","roles":["DEFAULT"]}]},
  "kubernetesSoftwareConfig":{"componentVersion":{"SPARK":"dataproc-2.3"}}}}}
```

`400 INVALID_ARGUMENT`:

```text
Multiple validation errors:
 - 'dataproc-2.3' is not a valid spark version for Virtual clusters.
 - Target GKE cluster does not exist: 'projects/parity-diff-jaiscloud/locations/us-central1/clusters/does-not-exist'.
```

Duplicate roles and the missing-SPARK check are collected the same way:

```text
Multiple validation errors:
 - Duplicate role 'DEFAULT' detected.
 - GKE-based virtual clusters require 'virtualClusterConfig.kubernetesConfig.kubernetesSofwareConfig.componentMap[SPARK]' to be set. It was not set.
```

A bad `stagingBucket` is collected too when it accompanies a pool-role problem,
but real GCP's internal ordering is layered: a bad bucket alone can suppress the
version/target checks:

```text
Multiple validation errors:
 - Exactly one GkeNodePoolTarget within the virtual cluster must have 'DEFAULT' role.
 - Storage Bucket name 'gs://dpg45-probe-staging' must match pattern '[a-z0-9\-_.]{3,222}'
```

```text
Storage Bucket name 'gs://dpg45-probe-staging' must match pattern '[a-z0-9\-_.]{3,222}'
```

An invalid enum value is a proto-level failure and is *not* in the aggregate: it
keeps the `google.rpc.BadRequest` field-violation envelope.

## Emulator contract

* **Envelope.** `internal/gcp/service/dataproc/validation.go` renders more than
  one collected failure as
  `"Multiple validation errors:\n - <first>\n - <second>…"`; exactly one failure
  is returned with its original message, byte for byte, so every existing
  single-error response is unchanged.
* **What is collected.** The create path's `InvalidArgument` checks: the
  `virtualClusterConfig` structure, the software config (component and property
  rules), the staging-bucket form, the target/pool parsing, the cross-pool role
  rules and the placement rules (`projects.regions.clusters.create` in
  `clusters.go`, `gke.go`, `virtualcluster.go`). Store-dependent answers stay
  outside it because they are not validation failures of the request shape: a
  metastore reference that does not exist (`NotFound`), a namespace already
  owned on the same GKE cluster (`FailedPrecondition`) and a duplicate cluster
  name (`AlreadyExists`) are still answered on their own.
* **Order.** The checks run in a fixed order — software config, staging bucket,
  target and pool parsing, cross-pool rules, then placement (project, region,
  zones). Map-valued inputs are visited in sorted key order, so the joined
  message is deterministic. Real GCP's own ordering is layered (see the bucket
  note above); the emulator deliberately uses one collector rather than
  reproducing each internal stage.
* **Wording.** The *envelope* and the `- ` separator are the real-GCP capture
  above. The individual sentences are the emulator's existing messages, not
  re-recorded real-GCP prose: real GCP's text differs per check (`'<value>' is
  not a valid spark version for Virtual clusters.`, `Target GKE cluster does not
  exist: '<name>'.`, …). Matching each sentence is a separate, per-check
  capture; this item only aggregates them.
* **Out of scope.** Real GCP also reports a missing `componentVersion.SPARK`
  entry when the software config is absent, while this emulator accepts an
  absent `kubernetesSoftwareConfig` (the W5.2 decision), and real GCP reports a
  missing `DEFAULT` role when `nodePoolTarget` is absent, while this emulator
  synthesises the documented default target (DPG20 ambiguity #6). Neither
  behaviour changes here: DPG45 is about the aggregation, not about adding new
  requirements.
* Every create-path error transcript changed (the messages of requests with more
  than one problem); the re-recorded transcripts are in
  `tests/gcpconformance/testdata/transcripts.json`.
