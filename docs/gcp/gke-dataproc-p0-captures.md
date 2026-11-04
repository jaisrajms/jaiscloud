# GKE + Dataproc P0 — real-GCP captures (2026-11-03)

Recorded against project `parity-diff-jaiscloud` (ADC, `alexey.amerik@gmail.com`)
while fixing the P0 conformance rows in
[`plan_docs/gcp-gke-dataproc-conformance-wave-plan.md`](../plan_docs/gcp-gke-dataproc-conformance-wave-plan.md)
(GKP1–GKP6). Every resource created here was deleted in the same session; §7
records the verification.

Cost: one zonal GKE cluster (`p0-zonal`, 1 × `e2-small`, ~35 min) plus one
Dataproc-on-GKE virtual cluster and its pools. A regional cluster
(`p0-ver-probe`) was created by a probe and deleted immediately.

---

## 1. GKP2 — `initialClusterVersion` is honoured, and the two rejections differ

`POST /v1/projects/{p}/locations/us-central1/clusters` with
`{"cluster":{"name":…,"initialClusterVersion":…,"initialNodeCount":1}}`:

| requested | real GCP |
|---|---|
| `1.29.7-gke.1000` | `400` `Master version "1.29.7-gke.1000" is unsupported.` |
| `1.30` | `400` `No valid versions with the prefix "1.30" found.` |
| `latest` | **accepted**; `CREATE_CLUSTER` operation, cluster later reported `currentMasterVersion` `1.37.1-gke.1552000` |

So the alias forms are `latest` + a prefix that matches a served release, and the
two rejections are **different messages**: a full release string it does not serve
gets "Master version … is unsupported.", an unmatched prefix gets "No valid
versions with the prefix … found.". The emulator previously accepted every value
and rewrote it to its single supported release. GKP2 now resolves `latest` and
`1`/`1.X`/`1.X.Y` against the served release and answers these two messages.

Both errors carry a `google.rpc.RequestInfo` detail:
`{"@type":"type.googleapis.com/google.rpc.RequestInfo","requestId":"0x…"}`.

## 2. GKP3 — `locations/-` is the wildcard

`GET /v1/projects/{p}/locations/-/clusters` → `200` and, with one cluster present,
the full `{"clusters":[…]}` array (before any cluster existed it answered `{}`).
The emulator answered `{"clusters":[]}` for every project because it keyed the
store on the literal `-`.

## 3. GKP4 — `v1beta1` is published and served

| request | real GCP |
|---|---|
| `GET /v1beta1/projects/{p}/locations/-/clusters` | `200` |
| `GET /v1beta1/projects/{p}/locations` (`projects.locations.list`) | `200` — a method v1 does **not** declare |

`GET https://container.googleapis.com/$discovery/rest?version=v1beta2` → `404`
"Discovery document not found"; `v1alpha1` likewise. v1beta1 is a superset of v1
(72 methods vs 71; the only delta is `projects.locations.list`).

The emulator's failure was worse than a missing version: `/v1beta1/…` on the
container host fell through to the GCS raw-media fallback and answered an **XML
`NoSuchBucket`**, so `gcloud beta container clusters list` reported a storage
error. GKP4 routes both versions to the same core and codec.

## 4. GKP1 — a zonal `gkeClusterTarget` is accepted (the headline answer)

With a **zonal** cluster `p0-zonal` in `us-central1-a`
(`--workload-pool=parity-diff-jaiscloud.svc.id.goog`), a virtual cluster created
against it:

```
POST /v1/projects/parity-diff-jaiscloud/regions/us-central1/clusters
{"clusterName":"p0-dpz","virtualClusterConfig":{
  "stagingBucket":"p0-probe-staging-parity-diff-jaiscloud",
  "kubernetesClusterConfig":{
    "kubernetesNamespace":"p0-ns",
    "gkeClusterConfig":{
      "gkeClusterTarget":"projects/parity-diff-jaiscloud/locations/us-central1-a/clusters/p0-zonal",
      "nodePoolTarget":[{"nodePool":"…/clusters/p0-zonal/nodePools/p0-pool","roles":["DEFAULT"],
        "nodePoolConfig":{"config":{"machineType":"e2-small"},
                          "autoscaling":{"minNodeCount":1,"maxNodeCount":3}}}]},
    "kubernetesSoftwareConfig":{"componentVersion":{"SPARK":"latest"}}}}}
```

→ **accepted**, `200` with a `google.longrunning.Operation` whose metadata is
`ClusterOperationMetadata{operationType: "CREATE_VIRTUAL_GKE", status.state: "PENDING"}`.

Before the fix the emulator resolved the target under the Dataproc **region**
(`us-central1`), where no cluster exists, so the same request was
`404 cluster not found` and nothing was created. GKP1 passes the target's own
location to the GKE lookup.

Two further real-GCP answers recorded on the way:

* A non-existent target is **`400 INVALID_ARGUMENT`**, not `404`:
  `Target GKE cluster does not exist: 'projects/p/locations/<loc>/clusters/<c>'.`
* A `zones/{zone}` spelling is accepted and **normalised to `locations/{zone}`**
  in that message. Both spellings were accepted, so the emulator accepts both and
  reports the canonical form.

## 5. GKP5 — no real-GCP counterpart (emulator-only)

Real GKE and Managed Kafka are separate hosts, so GCP has no "bare origin"
ambiguity to compare against. This row is validated by the emulator's own
behaviour, not by a capture:

| request | before | after |
|---|---|---|
| bare-host `POST …/clusters` with a GKE `cluster` body and no `clusterId` | Managed Kafka `400 missing location or clusterId` | GKE `CREATE_CLUSTER` |
| bare-host `GET …/clusters` where the location holds a GKE cluster | Managed Kafka `{"clusters":[]}` | GKE's clusters |
| bare-host `POST …/clusters?clusterId=k1` | Managed Kafka | Managed Kafka (unchanged) |

Managed Kafka's `CreateCluster` requires `clusterId` — its own error names it — so
a create body without one cannot be Kafka's. That is the discriminator for the
*first* create in an empty project; the store probe covers everything after.

## 6. GKP6 — Managed Spark serverless is implemented on real GCP

Real GCP **implements** `BatchController`/`SessionController`/
`SessionTemplateController`, so the emulator's `501 UNIMPLEMENTED` is an
intentional non-ga surface, **not** a parity result. What was validated is that
the emulator now says so coherently on both transports:

```
POST /v1/projects/p/locations/us-central1/batches
→ 501 {"error":{"code":501,"message":"BatchController.CreateBatch is not supported by the emulator (Dataproc Serverless is not implemented)","status":"UNIMPLEMENTED"}}
```

gRPC `BatchController` / `SessionController` / `SessionTemplateController` are
registered and every RPC returns `UNIMPLEMENTED` (unit-gated in
`internal/gcp/transport/grpc/dataproc`). Before the fix REST answered an
unknown-service 404 (or a GCS XML error on a GET) while gRPC said "unknown
service".

## 7. Cleanup and verification

| resource | action |
|---|---|
| `p0-dpz` (Dataproc virtual cluster) | deleted |
| `p0-zonal` (GKE, `us-central1-a`) | deleted, with the pools Dataproc created under it |
| `p0-ver-probe` (regional GKE, created by a probe) | deleted |
| `gs://p0-probe-staging-parity-diff-jaiscloud` | deleted |

Verified empty afterwards: `gcloud container clusters list` (both
`--region us-central1` and `--zone us-central1-a`), `gcloud dataproc clusters
list --region us-central1`, and `gcloud storage ls` for the staging bucket.

## 8. Follow-ups this capture opened (recorded in the plan)

* **DPG-OP-TYPE** — the Dataproc cluster-create operation metadata now reports
  `CREATE_VIRTUAL_GKE` for a virtual cluster and a `PENDING` first status, as
  captured. `UPDATE`/`DELETE` variants were not captured.
* A real GKE cluster's failed-create operation can report
  `statusMessage: "Try a different location, or try again later: Google Compute
  Engine does not have enough resources…"` while the cluster record stays
  `PROVISIONING` — the emulator has no equivalent failure path (it cannot fail a
  create), which is fine for a mock but means clients that parse `statusMessage`
  see it only on real GCP.
