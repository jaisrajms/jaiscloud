# GKE `clusters.create`: the default-pool fields conflict with `nodePools` (DPG41)

Captured **2026-10-31** against a live project (`parity-diff-jaiscloud`,
`container.googleapis.com/v1`, location `us-central1`) while closing DPG41. This
is the authority for the emulator's `clusters.create` validation: Discovery says
`Cluster.nodePools` "should not be set if `node_config` or `initial_node_count`
are specified", and `initialNodeCount`/`nodeConfig` say "Do not use this and a
node_pool at the same time", but the doc does not state the status or the message.

## Method

`gcloud auth print-access-token` for the bearer token, then `curl -s -X POST
https://container.googleapis.com/v1/projects/parity-diff-jaiscloud/locations/us-central1/clusters
-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" --data '<cluster>'`.
Every body below was POSTed to that endpoint.

## Capture

### `initialNodeCount` + `nodePools`

```json
{"cluster":{"name":"dpg41-probe","initialNodeCount":1,
            "nodePools":[{"name":"dpg41-pool","initialNodeCount":1}]}}
```

`400 INVALID_ARGUMENT`:

```json
{"error":{"code":400,
  "message":"It's invalid to specify both cluster.initial_node_count and a node pool. Please only provide a node pool.",
  "status":"INVALID_ARGUMENT"}}
```

Nothing was created.

### `nodeConfig` + `nodePools`

```json
{"cluster":{"name":"dpg41-probe2","nodeConfig":{"machineType":"e2-micro"},
            "nodePools":[{"name":"dpg41-pool","initialNodeCount":1}]}}
```

`400 INVALID_ARGUMENT`:

```json
{"error":{"code":400,
  "message":"It's invalid to specify both cluster.node_config and a node pool. Please only provide a node pool.",
  "status":"INVALID_ARGUMENT"}}
```

Nothing was created.

### Both set (precedence)

```json
{"cluster":{"name":"dpg41-probe3","initialNodeCount":1,"nodeConfig":{"machineType":"e2-micro"},
            "nodePools":[{"name":"dpg41-pool","initialNodeCount":1}]}}
```

Returns the `cluster.node_config` message, so `nodeConfig` is checked first:

```text
It's invalid to specify both cluster.node_config and a node pool. Please only provide a node pool.
```

### `initialNodeCount: 0` + `nodePools` (accepted)

```json
{"cluster":{"name":"dpg41-probe4","initialNodeCount":0,
            "nodePools":[{"name":"dpg41-pool","initialNodeCount":1}]}}
```

`200` with a `CREATE_CLUSTER` operation — a zero cluster-level
`initialNodeCount` means "no default node pool", not a conflict. The probe
cluster was deleted afterwards (its create/delete operations both completed and
the cluster list was empty again).

### Negative `initialNodeCount`

A negative count is a separate validation, and it is only applied when the
request carries no `nodePools`:

- `{"cluster":{"name":"dpg41-probe5","initialNodeCount":-1}}` → `400 INVALID_ARGUMENT`
  `"Cluster.initial_node_count must be zero or higher."`
- `{"cluster":{"name":"dpg41-probe6","initialNodeCount":-1,"nodePools":[{"name":"pool-a","initialNodeCount":1}]}}`
  → accepted (`CREATE_CLUSTER`), so the conflict rule is exactly
  `initialNodeCount > 0`, not "field present". The probe cluster was deleted
  afterwards. The negative-without-pools 400 is modelled by DPG49.

## Emulator contract

`internal/gcp/service/container.CreateCluster` enforces the two 400s with the
verbatim messages above — on both transports, since the rule lives in the
transport-neutral core — and persists nothing for a rejected create. The gate is
`initialNodeCount > 0` (not "present"), so a negative count combined with pools is
accepted exactly as real GKE accepts it; the negative-without-pools 400 is
recorded as DPG49.

A `nodeConfig` sent *without* `nodePools` is the request shape for the generated
`default-pool` (Discovery on `Cluster.nodeConfig`: "For requests, this field
should only be used in lieu of a 'node_pool' object, since this configuration
(along with the 'initial_node_count') will be used to create a 'NodePool' object
with an auto-generated name"). It is applied to that pool, which therefore
carries the same `selfLink`/`RUNNING` status/version/node count an inline pool
gets, and the cluster read renders `nodeConfig` from the first pool's `config`
(Discovery on the response: "this field will be populated with the node
configuration of the first node pool") — so DPG42's legacy shape round-trips on
both transports. The defaults GKE fills into an otherwise unset pool config
(machine type, image type, disk, scopes, service account) are rendered on every
cluster and node-pool read — DPG46, captured in
[`gke-node-pool-config-defaults.md`](gke-node-pool-config-defaults.md).

Note for those rows: the accepted probe's read echoed a **cluster-level
`nodeConfig`** (`machineType: e2-medium`, `diskSizeGb: 100`, the six default
OAuth scopes, `imageType: COS_CONTAINERD`, `serviceAccount: default`, …) as well
as each pool's own `config`, so real GKE renders both shapes.
