# Uplifting from AWS to GCP — local-testability contract

This is the **migration contract** for teams moving a workload from AWS (where
`jaiscloud-aws` is the local target) to GCP (where `jaiscloud-gcp` is the local
target). It states, per service, **what you may trust a local run to prove** and
**what you must verify against real GCP**.

It is a companion to, not a replacement for:

- [`docs/GA.md`](GA.md) — the GA contract: what `ga`/`limited`/`preview`/`unsupported`
  mean, stability, CI gates, client compatibility.
- [`README-GCP.md`](../README-GCP.md) — the service overview and the authoritative
  [Known Limitations](../README-GCP.md#known-limitations) list.
- [`docs/fidelity/fidelity-matrix.md`](fidelity/fidelity-matrix.md) — the **source of
  truth** for per-operation state and reasons. This document derives every number from
  it; where they disagree, the matrix wins.

---

## 1. Purpose & audience

**Audience:** service teams uplifting a workload from AWS to GCP who want to use
`jaiscloud-gcp` as the local dev/CI target.

**What the emulator can prove locally**

- An official Google client (REST service client or native gRPC client) pointed at
  `jaiscloud-gcp` sees the same request/response *shapes*, status codes, error
  envelopes, field names, and long-running-operation envelopes as the real API, within
  the surface declared by the fidelity matrix.
- Control-plane / IaC / metadata workflows round-trip and persist.
- For a **Green, data-plane** service, the data-plane operations and most semantics are
  exercised locally and gated in CI against captured real-GCP responses.

**What it cannot prove locally**

- That your workload behaves correctly against the *real backend*. The emulator is not
  real GCP: several services are metadata-only, some long-running operations finish
  synchronously, authorization is not enforced, and there is no quota/throttling plane.
- Anything behind a `preview` service (no engine ships locally), or the data plane of a
  metadata-only service.

A local green run is evidence about the **wire contract**, not about real GCP
behaviour. Treat this document as the checklist for the gap.

---

## 2. Tier legend

Tiers classify each service from its fidelity-matrix cells, with two documented
overrides (`functions` → Yellow, `kms` → Green) so the tier reflects local **trust**
rather than a raw `ga` ratio.

| Tier | Matrix basis | Locally trustworthy? | What it means for an uplift team |
| --- | --- | --- | --- |
| 🟢 **Green** | `ga` cells, no `preview` | **Yes** — for data-plane services; shape only for metadata services | Build the GCP adapter and unit/CI-test it against the emulator. |
| 🟡 **Yellow** | only `limited` cells (or a large `limited` share) | **Shape only** | Control-plane/IaC metadata works; data-plane and authorization behaviour is not modelled. Gate on a real-GCP smoke test. |
| 🔴 **Red** | any `preview` cell | **No** | No engine/logic behind it (e.g. BigQuery SQL). Local green would be meaningless. |

> `ga` means *supported and wire-conformant*, not *behaves like real GCP*. Read §5
> (behavioural depth) alongside the colour — a Green service can still be metadata-only.

---

## 3. Coverage snapshot

Read from [`docs/fidelity/fidelity-matrix.md`](fidelity/fidelity-matrix.md) at the time
of writing:

| Layer | Cells | `ga` | `limited` | `preview` | `unsupported` | `ga` share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Overall** | 497 | 363 | 94 | 37 | 3 | 73% |
| **gRPC** (official clients) | 164 | 156 | 8 | 0 | 0 | 95% |
| **REST** (Discovery-backed) | 333 | 207 | 86 | 37 | 3 | 62% |

- gRPC-only services (no REST transport): **Datastore, Cloud Logging, Cloud Monitoring,
  Operations (long-running)**.
- gRPC split = 156 `ga` + 8 `limited` = 164. REST split = 207 + 86 + 37 + 3 = 333.
  Overall = 164 + 333 = 497.

**How to refresh.** The matrix is generated, not hand-edited. Run
`make gen-gcp-fidelity-matrix`, then re-read
[`docs/fidelity/fidelity-matrix.md`](fidelity/fidelity-matrix.md) (or the canonical
[`fidelity-matrix.json`](fidelity/fidelity-matrix.json)) and update the numbers here.
`make check-gcp-fidelity-matrix` fails CI if the committed matrix has drifted.

---

## 4. Per-service contract

`ga`/total is taken directly from the matrix. "Depth" is the behavioural-depth class
from §5. "Locally trustworthy?" answers the migration question, not the matrix state.

| Service | Transport(s) | `ga`/total | Tier | Depth | Locally trustworthy? | Note |
| --- | --- | ---: | --- | --- | --- | --- |
| `pubsub` | grpc, rest | 44/44 | 🟢 | Full | Yes | Topics, subscriptions, snapshots, seek, ordering, DLQ. |
| `storage` | grpc, rest | 51/52 | 🟢 | Full | Yes | Bucket/object/IAM/resumable; `BidiReadObject` unimplemented. |
| `kms` | grpc, rest | 55/64 | 🟢 | Full | Yes | Symmetric/asym/MAC/raw + delete/import-job; 4 hard crypto leftovers. |
| `secretmanager` | grpc, rest | 30/32 | 🟢 | Full | Yes | Rotation schedule tracked; managed rotation needs Cloud SQL. |
| `firestore` | grpc, rest | 33/33 | 🟢 | Full | Yes | Full incl. `Listen`/`Write`; pipeline is a read-only subset. |
| `datastore` | grpc | 8/8 | 🟢 | Full | Yes | Transactions are single entity-group with a read-set. |
| `monitoring` | grpc | 24/24 | 🟢 | Full | Yes | Metrics/alerts/channels; only `condition_threshold` is evaluated. |
| `logging` | grpc | 5/6 | 🟢 | Full | Yes | Write/List/Delete; `TailLogEntries` is a bounded poll. |
| `iam` | grpc, rest | 16/16 | 🟢 | Shape only | Shape only | Service accounts + policy: authz is **not enforced**. |
| `eventarc` | rest | 18/18 | 🟢 | Shape only | Shape only | Metadata only; no event-delivery engine. |
| `managedkafka` | rest | 16/16 | 🟢 | Shape only | Shape only | Metadata only; no real broker. |
| `metastore` | rest | 19/19 | 🟢 | Shape only | Shape only | Control plane only; no Hive Thrift table plane. |
| `dataproc` | rest | 15/15 | 🟢 | Shape only | Shape only | REST metadata + real Spark on Docker/K8s executors. |
| `operations` | grpc | 5/5 | 🟢 | Shape only | Shape only | Synchronous operation stub. |
| `workflows` | rest | 6/6 | 🟢 | Shape only | Shape only | Workflow definitions + executions; LROs complete synchronously. |
| `workflowexecutions` | rest | 4/4 | 🟢 | Shape only | Shape only | Executions are synchronous. |
| `functions` | rest | 14/17 | 🟡 | Shape only | Shape only | Metadata CRUD + mock/docker call; v2 deploy unsupported. |
| `compute` | rest | 0/33 | 🟡 | Metadata only | Metadata only | No VM/disk/network data plane. |
| `cloudsql` | rest | 0/24 | 🟡 | Metadata only | Metadata only | No SQL engine or data plane. |
| `clouddns` | rest | 0/16 | 🟡 | Metadata only | Metadata only | No authoritative DNS server. |
| `memorystore` | rest | 0/8 | 🟡 | Metadata only | Metadata only | No Redis data plane. |
| `bigquery` | rest | 0/23 | 🔴 | None | No | No SQL engine; `jobs.query` evaluates nothing. |
| `iceberg` | rest | 0/14 | 🔴 | None | No | BigLake Iceberg REST catalog; `preview`. |

Tier groups: **Green (16)** `dataproc`, `datastore`, `eventarc`, `firestore`, `iam`,
`kms`, `logging`, `managedkafka`, `metastore`, `monitoring`, `operations`, `pubsub`,
`secretmanager`, `storage`, `workflowexecutions`, `workflows`. **Yellow (5)** `clouddns`,
`cloudsql`, `compute`, `functions`, `memorystore`. **Red (2)** `bigquery`, `iceberg`.

---

## 5. Behavioural depth — the honesty check

`ga` ≠ "behaves like real GCP". These four classes tell you how much real behaviour sits
behind a wire-conformant API.

| Depth | Services | What you can actually rely on locally |
| --- | --- | --- |
| **Full** | `pubsub`, `storage`, `kms`, `secretmanager`, `firestore`, `datastore`, `monitoring`, `logging` | Data-plane operations and most semantics, gated against captured real-GCP responses. |
| **Shape only** (wire-conformant, thin behaviour) | `iam` (authz not enforced), `eventarc` (no delivery engine), `managedkafka` (no broker), `metastore` (no Hive plane), `operations` (LROs synchronous), `workflows` (LROs synchronous), `workflowexecutions` (LROs synchronous), `dataproc` (no real cluster locally unless an executor is wired), `functions` (no v2 deploy) | Control-plane shape and metadata. Real behaviour must be tested on real GCP. |
| **Metadata only** | `compute`, `cloudsql`, `clouddns`, `memorystore` | Resource records + `get`/`list`; nothing actually runs. |
| **None (preview)** | `bigquery` (no SQL engine), `iceberg` | Nothing local counts as evidence. |

> **Rule of thumb:** build against **Full** services locally; treat **Shape only**,
> **Metadata only**, and **None** as "the API shape is right, the behaviour is not
> proven" and gate those on a real-GCP smoke test.

---

## 6. AWS → GCP mapping

| AWS (today) | GCP target | Emulator tier | Local trust |
| --- | --- | --- | --- |
| SQS | **Pub/Sub** | 🟢 `ga` (44/44) | High — full surface. |
| S3 | **Cloud Storage** | 🟢 `ga` (51/52) | High — `BidiReadObject` unimplemented. |
| DynamoDB | **Firestore** / Datastore | 🟢 `ga` (Firestore 33/33, Datastore 8/8) | High — watch transaction/OCC caveats ([Known Limitations](../README-GCP.md#known-limitations)). |
| Lambda | **Cloud Functions** | 🟡 `limited` (14/17) | Control plane only; v2 deploy unsupported. |
| KMS | **Cloud KMS** | 🟢 `ga` (55/64) | High; 4 hard crypto leftovers (`ImportCryptoKeyVersion`, trusted-key wraps, `Decapsulate`). |
| Secrets Manager | **Secret Manager** | 🟢 `ga` (30/32) | High; managed rotation needs Cloud SQL. |
| IAM | **Cloud IAM** | 🟢 `ga` (16/16) | Shape only — authz not enforced. |
| CloudWatch Logs / Metrics | **Cloud Logging / Monitoring** | 🟢 `ga` (Logging 5/6, Monitoring 24/24) | High; `TailLogEntries` is a bounded poll, only `condition_threshold` evaluated. |
| EventBridge | **Eventarc** | 🟢 `ga` (18/18) | Metadata only — no delivery engine. |
| Step Functions | **Workflows / Workflow Executions** | 🟢 `ga` (Workflows 6/6, Executions 4/4) | LROs complete synchronously. |
| Athena / Redshift | **BigQuery** | 🔴 `preview` (0/23) | **None** — real GCP required. |
| RDS | **Cloud SQL** | 🟡 `limited` (0/24) | Metadata only. |
| EC2 | **Compute Engine** | 🟡 `limited` (0/33) | Metadata only. |
| ElastiCache | **Memorystore** | 🟡 `limited` (0/8) | Metadata only. |
| Route 53 | **Cloud DNS** | 🟡 `limited` (0/16) | Metadata only. |

*(The last five rows — BigQuery, Cloud SQL, Compute, Memorystore, Cloud DNS — are the
ones where AWS parity cannot be validated locally at all; budget real-GCP testing for
them up front.)*

---

## 7. Must test on real GCP

The local emulator deliberately does not model the following. A local green run is **not**
evidence for any of them.

- [ ] **Authz / IAM enforcement.** Permissions are not checked; Cloud IAM is shape-only
      across all services. Any permission-sensitive path must be smoke-tested on real GCP.
- [ ] **Async long-running-operation (LRO) timing.** The emulator completes operations
      synchronously (`operations`, `workflows`, `workflowexecutions`, `functions`,
      `kms`, `cloudsql`, `compute`). Code that assumes immediate readiness will pass
      locally and may fail against real, eventually-consistent GCP.
- [ ] **Metadata-only services.** `compute`, `cloudsql`, `clouddns`, `memorystore` have
      no control/data plane locally — only resource records. Test the real data plane.
- [ ] **BigQuery.** No SQL engine ships locally; `jobs.query` evaluates nothing. All
      query behaviour must be tested against real BigQuery.
- [ ] **Cloud Functions v2 deploy.** v2 request/response *shapes* are served (gcloud
      `list`/`describe` pass), but v2 `deploy` additionally needs `/v2/.../runtimes` and a
      resumable source upload, which are not modelled. Test deploy end-to-end on GCP.
- [ ] **Quotas, throttling, and retry/backoff.** No rate limits or quota plane is
      modelled, so backoff and quota-exhaustion paths are never exercised locally.
- [ ] **Frozen-clock OCC / TTL.** With the clock frozen (`POST /_jaiscloud/clock`),
      Firestore/Datastore optimistic-concurrency conflict detection and DynamoDB-style TTL
      edge cases can behave differently. Do not rely on frozen-clock results as production
      evidence.
- [ ] **Per-language SDK wire paths.** Different official SDKs exercise different wire
      paths. Add each migrating service's SDK/language to the conformance matrix rather
      than assuming one client's pass transfers.

Additional service-specific caveats live in
[`README-GCP.md` → Known Limitations](../README-GCP.md#known-limitations); read the
section for every service you touch.

---

## 8. Do not depend on emulator-only behaviour

Production code paths must never rely on affordances that exist only in the emulator.
These are fixed by policy, not by the fidelity matrix:

- **`/_jaiscloud/*` admin endpoints** — `health`, `doctor`, `reset`, `export`, `import`,
  `snapshot*`, `clock`, `ttl-sweep`, `eb-tick`, and `/metrics` are local control surfaces,
  not GCP APIs. Never call them from application code.
- **Reset / state wipe** — `POST /_jaiscloud/reset` and the `reset` CLI command exist for
  test isolation only; production code must not assume resettable state.
- **Lenient validation** — the emulator accepts and ignores more than real GCP (shallow
  create validation, dropped unmasked/unknown fields, synthesized placeholders). Do not
  encode emulator acceptance as a correctness assumption.
- **Absent authz** — requests succeed without credentials/permissions locally. Treat every
  IAM decision as unverified until tested on real GCP.
- **Frozen clock** — deterministic-time mode is a test affordance. Business logic must not
  depend on a frozen/offset clock.

A useful guard is a lint/test that rejects references to `/_jaiscloud/` and clock control
in production packages.
