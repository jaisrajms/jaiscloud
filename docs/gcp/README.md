# GCP documentation

The GCP-owned prose docs. The GCP guide itself is
[`README-GCP.md`](../../README-GCP.md) at the repo root; everything here is
reference material it links to.

| document | what it is |
|---|---|
| [`wire-contract.md`](wire-contract.md) | What the emulator's fidelity matrix promises: the per-operation/per-transport wire contract, the pinned API artifacts, the CI gates, and the surfaces that are explicitly non-`ga`. |
| [`testability.md`](testability.md) | The local-testability contract: per service, what a local run may be trusted to prove and what must be verified on real GCP, plus the behaviour classes (`p1`–`p5`) the depth gate requires. |
| [`bigquery-sql-engine.md`](bigquery-sql-engine.md) | Decision record (BQ0) freezing the BigQuery engine choice (pure-Go in-process SQLite, one engine in memory and `--dsn` modes) and the v1 dialect subset. |
| [`fidelity-reconciliation.md`](fidelity-reconciliation.md) | The AUD5 reconciliation of the fidelity matrix against wire evidence, code and the official Discovery documents, with the durable re-check. |
| [`dataproc-gke-version-aliases.md`](dataproc-gke-version-aliases.md) | Real-GCP capture (2026-10-30) of the Dataproc-on-GKE `dataproc-<major.minor>` Spark version alias: only `dataproc-2.2` is accepted and it resolves to `3.5-dataproc-28`. |
| [`gke-create-pool-conflict.md`](gke-create-pool-conflict.md) | Real-GCP capture (2026-10-31) of the GKE `clusters.create` default-pool conflict: `nodePools` with a positive `initialNodeCount` or a `nodeConfig` is a `400 INVALID_ARGUMENT` with the verbatim server message, while `initialNodeCount: 0` is accepted; a negative `initialNodeCount` is rejected only without pools. |
| [`gke-node-pool-config-defaults.md`](gke-node-pool-config-defaults.md) | Real-GCP capture (2026-10-31) of the `NodeConfig` GKE renders for a default-pool created without one: machine type, COS image, balanced 100 GB disk, default service account and the six OAuth scopes. |
| [`dataproc-gce-network-default.md`](dataproc-gce-network-default.md) | Real-GCP capture (2026-10-31) of the Dataproc Compute-Engine read: a create naming neither network field reads back the project's `default` network and no `subnetworkUri`. |
| [`dataproc-aggregate-validation-errors.md`](dataproc-aggregate-validation-errors.md) | Real-GCP capture (2026-10-31) of the aggregated Dataproc `clusters.create` validation: one `Multiple validation errors` 400 listing every problem, bare message when there is only one. |
| [`wave-plan-template.md`](wave-plan-template.md) | The template every GCP wave plan must follow (`tools/gcpstatus` parses its index/detail tables). |

Generated, machine-read artifacts stay outside this namespace on purpose:

- `docs/fidelity/fidelity-matrix.{md,json,csv}` — the generated matrix
  (`make gen-gcp-fidelity-matrix`, gated by `make check-gcp-fidelity-matrix`).
- `docs/fidelity-overrides.yaml`, `docs/gcp-coverage-floor.yaml`,
  `docs/gcp-mutation-floor.yaml`, `docs/gcpstatus-coverage-exemptions.yaml`,
  `docs/gcpstatus-resolved.yaml` — read by exact path from the Makefile and
  `tools/gcpstatus`.
- `docs/test-depth/` — the per-service behavioural-depth records.

`docs/` itself is also the GitHub Pages root (`index.html`, `CNAME`,
`robots.txt`, `sitemap.xml`, `images/`); those assets are unrelated to this
namespace.
