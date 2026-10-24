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
