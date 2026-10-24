# GCP fidelity-matrix reconciliation (AUD5)

This report records the AUD5 reconciliation of the emulator's fidelity matrix
(`docs/fidelity/fidelity-matrix.json`) against the wire evidence, the code, and
the official Discovery documents. The check below is the durable artifact.

## What the check proves

The matrix's `ga` grade is a *registration claim* (implemented + Discovery-mapped
+ no finding + persists) — the AUD1/AUD2 ledger already separates it from the
`verified` evidence signal. AUD5 closes the gap between the claim and the
**operation-level stub** evidence:

> an operation the wire proves hard-unimplemented (an operation-level
> `Unimplemented`, never a 2xx) must not be graded `ga`; and a cell graded
> `unsupported` must actually hard-fail (its method must never have served 2xx).

`tests/gcpconformance/stub_evidence.go` implements the join
(`TranscriptOpStatuses`, a sibling of `TranscriptCoverage` that keeps non-2xx
responses) and the comparison (`ReconcileStubs`).
`TestUnsupportedStubReconciliation` runs it offline over the committed transcript
and matrix, so it is wired into `make test-gcp-wire-conformance` (and therefore
`make ga-check` and CI's `check-gcp-fidelity-matrix`).

Two rejections are deliberately **not** stubs:

- a method that also answered 2xx — the operation works and only some inputs are
  unserved (a conditional rejection);
- a 501 that rejects the **request input** rather than naming the operation
  unimplemented. The emulator phrases these distinctly: the generic JSON codec
  says `unsupported operation`, the update-mask merges say `unsupported
  update_mask path: <field>`, while a real stub names the operation
  (`DiagnoseCluster is not supported by the emulator`). Those request rejections
  are the coverage ledger's business, not a fidelity over-claim.

Catch-all `Unimplemented` actions (`CloudSQL`/`CloudDNS`/`Compute`/`BigQuery`)
and gRPC methods carry no single REST Discovery method / no REST transcript, so
they are out of the join's scope (their declarations are asserted by the
overrides validation and the gRPC conformance report respectively).

## Findings

### 1. The first run's 10 "undeclared stubs" split 4 real gaps / 6 probe artifacts

The first run flagged ten `ga` operations whose resolved Discovery method recorded
a 501 and never a 2xx. Re-checking each against the code and the vendored
Discovery documents (the review caught this, and the code confirms it) split them:

**Four genuine partial gaps — re-graded `ga` → `limited`:**

| operation | writable field(s) the merge rejects |
| --- | --- |
| `Logging.BucketUpdate` | `restrictedFields` |
| `Logging.BucketUpdateAsync` | `restrictedFields` |
| `Logging.SinkPatch` | `bigqueryOptions`, `interceptChildren`, `outputVersionFormat` |
| `Logging.SinkUpdate` | `bigqueryOptions`, `interceptChildren`, `outputVersionFormat` |

`applyBucketMask` merges only description/retentionDays/analyticsEnabled/
indexConfigs/cmekSettings; `applySinkMask` only destination/filter/description/
disabled/exclusions/includeChildren. A mask naming one of the fields above —
which real GCP accepts — returns `501 UNIMPLEMENTED`. These four are the matrix's
honest `limited` grade; the override reasons cite the specific fields.

**Six probe artifacts — left `ga`:** `Logging.ExclusionPatch`,
`Logging.ViewUpdate`, `Logging.LogScopeUpdate`, `Monitoring.UpdateService`,
`Monitoring.UpdateServiceLevelObjective`, `Firestore.CreateDocument`.
`applyExclusionMask`/the view, log-scope and settings merges/`applyServiceMask`/
`applyServiceLevelObjectiveMask` each support **every writeable field** of their
resource, so these operations are fully implemented. The 501 was induced by the
probe: it sends `?updateMask=labels`, and `labels` is not a field of any of these
schemas (verified against the vendored Discovery `LogBucket`, `LogSink`,
`LogExclusion`, `LogView`, `LogScope`, `Settings`, `Service`,
`ServiceLevelObjective`); for `Firestore.CreateDocument` the probe's
`{+parent}` filling collapses, emitting a degenerate
`…/documents/<id>/<id>` path. Real GCP would reject a bad mask with `400`, and the
emulator's `501` here is a request rejection, not an unimplemented operation.

This also resolves an inconsistency the per-method join exposed:
`Logging.SettingsUpdate` was `ga` (its 200 and 501 hit the same wildcard method,
`logging.updateSettings`) while `Logging.ViewUpdate` was flagged, for identical
behaviour. With the probe artifacts excluded, the grades agree.

Rollup before → after: **ga 939 → 935, limited 101 → 105** (REST ga 497 → 493,
REST limited 100 → 104); `verified` is unchanged (500/623 REST, 442/463 gRPC).

**Follow-up (AUD2-1).** The four `limited` grades were a code gap, not a permanent
limitation. AUD2-1 models the fields (`LogBucket.restrictedFields`;
`LogSink.bigqueryOptions`/`interceptChildren`/`outputVersionFormat`), adds them to
the two merges, and maps them in both transports, so the four cells are `ga` again
and the rollup returns to **ga 939, limited 101** (REST ga 497, REST limited 100).
Unmappable mask paths in those handlers now answer `400 INVALID_ARGUMENT`
(AIP-134/AIP-161) instead of `501 UNIMPLEMENTED`.

### 2. No stale `unsupported` grade

Every REST `unsupported` cell either resolves to no single Discovery method (the
catch-all stub actions) or is backed by operation-level 501-only evidence: the
Dataproc `DiagnoseCluster` REST cell and the five Metastore metadata operations
all answer `<Operation> is not supported by the emulator` and never 2xx. Six
operation-level stubs, all graded `unsupported`.

### 3. `limited` reasons re-derived (no promotions)

The 105 `limited` cells are the five service-wide metadata-only overrides
(compute 32, cloudsql 23, bigquery 22, clouddns 15, memorystore 8), Logging
`TailLogEntries`, and the four AUD5 re-grades. Re-derived against the code and
the proto/Discovery:

- Cloud SQL / Compute / Cloud DNS / Memorystore providers are `ResourceStore`-only
  (no `blobfs`, no executor, no control/data plane) — the metadata-only reason
  holds.
- BigQuery's SQL-subset caveat (`docs/gcp/bigquery-sql-engine.md`) holds; the
  service-wide override is deliberate.
- Logging `TailLogEntries` has no deterministic conformance assertion — holds.
- The plan doc's "kms 4, secretmanager 2" limited cells are long gone (promoted);
  no cell was promotable now.

So no cell was promoted; the reasons were confirmed and the stale plan/wire-contract
counts corrected.

### 4. Transport classification confirmed (one gap recorded)

The REST-only set (BigQuery, Cloud DNS, Cloud SQL, Compute, Iceberg, Memorystore)
and the generic `operations` gRPC-only service match real GCP for the modelled
surfaces. **Firestore Admin** does not: real GCP serves its databases /
collection-group fields / backup schedules / backups / user creds over REST
(`firestore.googleapis.com/v1/...`) as well as gRPC, while the emulator serves
only the composite-index admin over REST (under the `firestore` service) and the
rest over gRPC. Recorded as the `AUD5-1` deferral rather than papered over.

## Reproduce

```bash
export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH
make gen-gcp-fidelity-matrix          # regenerate docs/fidelity/*
go test -count=1 -tags gcp_conformance -run TestUnsupportedStubReconciliation ./tests/gcpconformance/
```

`make test-gcp-wire-conformance` runs the gate as part of the normal suite.

## Intentional `gcp-matrix-diff` regressions

At AUD5 merge time `make gcp-matrix-diff REF=upstream/gcp` reported exactly **4
regressions** and **0 new gaps**: the four `ga → limited` re-grades above, and
nothing else. Each corrected a published grade to match a real, code-grounded
limitation. **Those four are closed by AUD2-1**, which implements the missing
fields and restores the cells to `ga`, so `gcp-matrix-diff` now reports **0
regressions**.

## Deferred

| ID | area | gap | impact | effort | verdict | prompt |
| --- | --- | --- | --- | --- | --- | --- |
| AUD5-1 | firestoreadmin | Firestore Admin is gRPC-only in the emulator, but real GCP serves databases/collection-group fields/backup schedules/backups/user creds over REST too | medium | L | fix | `feat/gcp-firestoreadmin-rest` |
| AUD5-2 | conformance | the probe synthesizer sends `updateMask=labels` (not a field on most resources) and collapses recursive `{+parent}` paths, producing 501s that look like stubs; the AUD2 coverage-exemption reasons for those ops say "non-`*` mask", which is inaccurate | medium | M | fix | `test/gcp-probe-synthesizer-valid-inputs` |
