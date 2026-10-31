# GKE node-pool `config`: the defaults real GKE renders (DPG46)

Captured **2026-10-31** against real GCP (project `parity-diff-jaiscloud`,
`container.googleapis.com/v1`, location `us-central1`). This capture closes
DPG46: a cluster/pool read returned a fully populated `config` for the generated
`default-pool` even though the create named no `nodeConfig`. The probe cluster
was deleted in the same session.

## Method

`POST .../locations/us-central1/clusters` with

```json
{"cluster":{"name":"dpg46-probe","initialNodeCount":1}}
```

then polled `GET .../clusters/dpg46-probe` until `status` reached `RUNNING`
(about a minute) and read the `default-pool`.

## Capture — `clusters.get` → `nodePools[0].config`

```json
{
  "machineType": "e2-medium",
  "diskSizeGb": 100,
  "oauthScopes": [
    "https://www.googleapis.com/auth/devstorage.read_only",
    "https://www.googleapis.com/auth/logging.write",
    "https://www.googleapis.com/auth/monitoring",
    "https://www.googleapis.com/auth/service.management.readonly",
    "https://www.googleapis.com/auth/servicecontrol",
    "https://www.googleapis.com/auth/trace.append"
  ],
  "metadata": {"disable-legacy-endpoints": "true"},
  "imageType": "COS_CONTAINERD",
  "serviceAccount": "default",
  "diskType": "pd-balanced",
  "shieldedInstanceConfig": {"enableIntegrityMonitoring": true},
  "kubeletConfig": {"insecureKubeletReadonlyPortEnabled": false, "maxParallelImagePulls": 2},
  "resourceLabels": {"goog-gke-node-pool-provisioning-model": "on-demand"},
  "windowsNodeConfig": {},
  "effectiveCgroupMode": "EFFECTIVE_CGROUP_MODE_V2",
  "bootDisk": {"diskType": "pd-balanced", "sizeGb": "100"},
  "nodeImageConfig": {"image": "gke-1364-gke2046003-cos-129-19506-448-20-c-pre", "imageProject": "gke-node-images"}
}
```

The cluster-level `nodeConfig` echoed the same object (Discovery: "For
responses, this field will be populated with the node configuration of the first
node pool"). The cluster's `legacyAbac` was `{}` (present, ABAC disabled) — that
is a **cluster** field, already rendered by DPG37, not a member of `NodeConfig`.

The pool read also carried `management`/`maxPodsConstraint`/`locations`/
`networkConfig`/`upgradeSettings`/`version`/`instanceGroupUrls`, which the
emulator still does not model; this item is only the `config` message.

## Emulator contract

`internal/gcp/service/container/readshape.go` renders `NodeConfig` on every node
pool read (`GetNodePool`, `ListNodePools` and the pools embedded in a cluster
read) through `NodePoolWithReadDefaults`, and the cluster-level `nodeConfig` is
still derived from the first pool's config. The defaults are exactly the fields
this emulator models, taken from the capture:

| field | default |
|---|---|
| `machineType` | `e2-medium` |
| `imageType` | `COS_CONTAINERD` |
| `diskSizeGb` | `100` |
| `diskType` | `pd-balanced` |
| `serviceAccount` | `default` |
| `oauthScopes` | the six scopes above |

Fields the caller supplied are kept; only the unset ones are filled, and the
defaults are applied on read, never written into the stored record (the same
rule as the other read-shape defaults). `metadata`, `shieldedInstanceConfig`,
`kubeletConfig`, `effectiveCgroupMode`, `bootDisk` and `nodeImageConfig` from the
capture are **not** rendered: the emulated `NodeConfig` model has no fields for
them, and inventing partial shapes would be worse than the documented omission.

The `hashicorp/google` provider tolerates the absence today (its `node_config`
block is computed), so this must not change the provider-visible diff —
`make test-gcp-terraform` is the gate.

## Cleanup

`DELETE .../clusters/dpg46-probe`; the delete operation completed and
`clusters.list` was empty afterwards.
