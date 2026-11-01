# Dataproc `clusters.get`: the default `networkUri` (DPG47)

Captured **2026-10-31** against real GCP (project `parity-diff-jaiscloud`,
`dataproc.googleapis.com/v1`, region `us-central1`). This capture closes DPG47:
a Compute-Engine cluster created with neither `networkUri` nor `subnetworkUri`
reads back the project's `default` network — and no `subnetworkUri`. It confirms
the older in-tree golden
(`tests/gcpdifferential/testdata/golden-tour/029-dataproc-tour_dataproc_cluster_get.json`,
which carries `projects/{p}/global/networks/default`) for a second
`internalIpOnly` value.

## Method

Two probes, both deleted afterwards:

1. A `dpg47-probe` cluster with no network fields and `internalIpOnly: false`
   (`configBucket` set to a temporary bucket because the project's Compute
   service account needed it, since removed).
2. The in-tree golden used image `2.2`, whose default is `internalIpOnly: true`.

`GET .../clusters/dpg47-probe` once the create operation had registered the
cluster (the `config.gceClusterConfig` block is populated before `RUNNING`).

## Capture

`internalIpOnly: false`:

```json
{"zoneUri":"https://www.googleapis.com/compute/v1/projects/parity-diff-jaiscloud/zones/us-central1-a",
 "networkUri":"https://www.googleapis.com/compute/v1/projects/parity-diff-jaiscloud/global/networks/default",
 "serviceAccountScopes":["https://www.googleapis.com/auth/cloud-platform"],
 "internalIpOnly":false,
 "shieldedInstanceConfig":{"enableSecureBoot":true,"enableVtpm":true,"enableIntegrityMonitoring":true},
 "resourceManagerTags":{"tagKeys/281484247092652":"tagValues/281483067396502"}}
```

`internalIpOnly: true` (golden 029, image `2.2`):

```json
{"internalIpOnly":true,
 "networkUri":"https://www.googleapis.com/compute/v1/projects/<project>/global/networks/default",
 "zoneUri":"https://www.googleapis.com/compute/v1/projects/<project>/zones/us-central1-a"}
```

Neither response carries `subnetworkUri`.

## Emulator contract

Discovery on `GceClusterConfig.networkUri`: "If neither network_uri nor
subnetwork_uri is specified, the 'default' network of the project is used."
`internal/gcp/service/dataproc/defaults.go` now renders exactly that:
`gceClusterConfig.networkUri` becomes
`https://www.googleapis.com/compute/v1/projects/{project}/global/networks/default`
when **both** fields are absent. A caller who named either field keeps what they
sent (canonicalised to the full link, DPG40), and **`subnetworkUri` is never
materialised** — the capture above shows the real get does not name a subnet
even when it uses the default network's implicit subnet. Because the default is
derived per read, the stored config stays exactly what the caller sent; a
`subnetworkUri`-only request is likewise left without a `networkUri`.

The consequence for the differential harness is deliberate: the per-location
ignore `config.gceClusterConfig.networkUri` in `tests/gcpdifferential/triage.go`
is deleted in the same change, so a regression that drops the rendered network
surfaces as an open divergence instead of being silently accepted.

## Cleanup

The probe cluster was deleted (`gcloud dataproc clusters list` empty afterwards),
the temporary staging bucket was removed, and the temporary `roles/storage.admin`
binding granted to the Compute service account was removed.
