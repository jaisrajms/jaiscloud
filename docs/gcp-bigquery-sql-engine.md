# BigQuery SQL engine — decision record (BQ0)

> **Status: IMPLEMENTED — Option A: pure-Go in-process SQLite
> (`modernc.org/sqlite`), one engine in both memory and `--dsn` modes.**
> This record freezes the engine choice and the v1 dialect subset. BQ1 (translator
> + SELECT) and BQ2/BQ8 (DDL/DML + catalog sync + result encoding) shipped
> (`internal/gcp/queryengine`, `internal/gcp/provider/bigquery`); BQ3 re-graded
> the fidelity cells to `limited`, and BQ4 proved memory/`--dsn` parity, snapshot/
> export safety and bounded scale (`plan_docs/gcp-bigquery-ga-wave-plan.md` §7).

Date: 2026-09-29 · Backlog ID: **BQ0** · Branch: `spike/gcp-bigquery-sql-engine`

## 1. Problem

At BQ0, BigQuery was the only `preview` GCP service in jaiscloud: all 23 cells were
"metadata + stored rows only, **no SQL engine**" (`docs/GA.md` §7,
`docs/fidelity/fidelity-matrix.md`), and `jobs.query` stored the query and returned
`jobComplete=true` with an empty result, so any client that evaluated SQL locally got a
successful-looking wrong answer. `docs/GCP-TESTABILITY.md` §4 graded `bigquery` 🔴
*"Nothing local counts as evidence."* **That gap is closed:** the engine below shipped in
BQ1/BQ2 and BQ3 re-graded the cells to `limited` (20) + `unsupported` (3).

Nothing else in the emulator consumes BigQuery (it is a leaf: no Logging sink
delivery, no Dataflow, Spark wires only GCS), so this is a **client-fidelity
product decision**, not a cross-service or AWS-parity gap (the AWS analogues —
Athena/RDS/Redshift — are metadata-only too).

## 2. Decision

**Option A: one pure-Go, in-process SQLite engine (`modernc.org/sqlite`) as the
sole BigQuery query engine, in both memory and `--dsn` modes.**

- SQLite is **disposable query scratch**, never persisted: rows are hydrated from
  the existing store (`jc_bq_rows` JSONB under `--dsn`, memory store otherwise),
  the query runs, results are encoded, the scratch is discarded.
- The **store stays the single source of truth**; snapshots/export/import/reset/
  `--dsn` durability are untouched and need no new migration.
- **One engine** preserves the memory-vs-`--dsn` parity guarantee: only the
  storage backend differs, never the SQL dialect.

## 3. Spike evidence (BQ0 feasibility gate)

Throwaway harness: a real SQLite scratch over hydrated tables exercising the
subset, plus the encoding to the Discovery result shape. Results:

- **Translation + execution (all green):** `SELECT *` over a backticked
  `` `project.dataset.table` ``, `WHERE col = literal`, `COUNT(*)`,
  `GROUP BY`/`HAVING`/`AVG`, `INNER JOIN`, `LEFT JOIN` (NULL-preserving), `WITH`
  CTE, `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)`.
- **Mandatory semantic rewrite proven:** BigQuery `/` is FLOAT64 division, SQLite
  `/` is integer division (`SELECT 1/2` → `0` vs `0.5`); the translator must cast
  operands. Unknown tables fail loud at resolution.
- **Discovery encoding:** `{kind:"bigquery#queryResponse", jobComplete:true,
  jobReference, schema:{fields:[{name,type,mode}]}, rows:[{f:[{v}]}],
  totalRows:"N"}` — scalars as strings, `INT64`→`INTEGER`, `BOOL`→`BOOLEAN`.
- **Cross-compile + size (`CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`):**

| target | with SQLite | stdlib-only | delta |
|---|---:|---:|---:|
| linux/amd64 | 6.7 MB | 1.2 MB | +5.5 MB |
| linux/arm64 | 6.5 MB | 1.2 MB | +5.3 MB |
| darwin/amd64 | 6.6 MB | 1.2 MB | +5.5 MB |
| darwin/arm64 | 6.5 MB | 1.2 MB | +5.4 MB |
| windows/amd64 | 6.6 MB | 1.2 MB | +5.5 MB |
| windows/arm64 | 6.3 MB | 1.1 MB | +5.3 MB |

  All six goreleaser targets build cleanly with `CGO_ENABLED=0`; the dependency
  adds **~5.5 MB** and no libc/libstdc++ requirement, so
  `gcr.io/distroless/static` keeps working. (`modernc.org/sqlite v1.60.1`.)

## 4. Rejected options

| Option | Why not |
|---|---|
| **B — Postgres as the query engine** | Memory mode has no Postgres, so two dialects would diverge (type affinity, int/float division, collation, NULL ordering, date functions). Violates the backend-parity rule. Postgres stays **storage-only**. |
| **C — DuckDB executor sidecar** | Kept as the designated *fallback* if the subset proves insufficient, not the default. Adds a Docker/k8s sidecar topology for a gain (arrays/structs/`UNNEST`) the v1 subset deliberately defers, and still needs a BigQuery→DuckDB translator. |
| **DuckDB in-process** (`github.com/duckdb/duckdb-go/v2`) | Measured: `CGO_ENABLED=0` **fails** (`build constraints exclude all Go files`); with cgo a trivial program is **57 MB** vs 1.2 MB, dynamically linked against `libstdc++`/`libc`, so `distroless/static` cannot host it; prebuilt libs exist for only 5 targets (**no `windows/arm64`**); cross-compiling needs per-target C toolchains. Disproportionate for a leaf service. |
| **LocalBQ** (localgcp's approach) | A third-party BigQuery emulator owning its own catalog/data → two sources of truth, duplicated control plane, and jaiscloud cannot honestly grade or fix it. Localgcp's model has no `--dsn`/snapshot parity contract. |

## 5. Frozen v1 dialect subset

**Supported** (translate + execute):

- `SELECT` / `WHERE` / `GROUP BY` / `HAVING` / `ORDER BY` / `LIMIT` / `OFFSET` /
  `DISTINCT`; `INNER`/`LEFT` joins; subqueries; `WITH` CTEs; `UNION ALL`.
- Aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) and window functions.
- A bounded string/math/null function set mapped to SQLite equivalents
  (`LENGTH`, `UPPER`, `LOWER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`,
  `INSTR`, `ABS`, `ROUND`, `COALESCE`, `NULLIF`, `IFNULL`) — **no date/format
  functions**.
- `COUNT(*)` and typed column references.

**Emulated** (translated, must not silently diverge):

- BigQuery `/` → `CAST(… AS REAL)/CAST(… AS REAL)` (FLOAT64 division).
- Backticked `` `project.dataset.table` `` / `` `dataset.table` `` → hydrated
  temp tables (`defaultDataset` supplies the project/dataset when unqualified).
- Bare-column and expression output types inferred from the query plan
  (`INT64`/`FLOAT64`/`BOOL`), accepting both the engine-canonical names and the
  Discovery/`TableFieldSchema` aliases (`INTEGER`/`FLOAT`/`BOOLEAN`).

**Fail loud (`InvalidArgument` / 400 `invalidQuery`)** — never return wrong rows:

- `UNNEST` / `ARRAY` / `STRUCT` (beyond storage round-trip), `GEOGRAPHY`.
- Wildcard tables / `TABLE_SUFFIX`, `INFORMATION_SCHEMA`.
- Scripting, stored procedures, `MERGE`, `QUALIFY`, `SAFE.` functions, date /
  format functions, query parameters and raw/bytes/triple-quoted string
  literals, DDL/DML beyond the accepted set, legacy SQL (recorded as BQ7).
- Any construct the translator cannot map.

## 6. Non-goals (recorded so they are not re-litigated)

- **gRPC CRUD for BigQuery** — real GCP's BigQuery control plane is REST-only
  (`plan_docs/gcp-dual-protocol-parity.md` §2); adding gRPC would invent a
  protocol no client speaks.
- **BigQuery Storage Read/Write API (gRPC)** — the Spark connector's data plane.
  A separate, larger scope (not in BQ1–BQ4); needed only if Spark/Storage-API
  support is prioritized.
- **Full GoogleSQL** — arrays/structs/geography/scripting are deferred by design.
- **`routines`/`models`/`rowAccessPolicies`** — remain documented `501`.
- **Real DB data planes for other services** (Cloud SQL/Memorystore) — a
  different problem (wire protocol + real server), not helped by this engine.
- **Authz, real LRO timing, quotas** — unchanged accepted risks (`docs/GA.md` §10).

## 7. Consequences / follow-ups

- **BQ1** implements the subset → SQLite translator + scratch hydration and
  SELECT execution. **BQ2** adds DDL/DML + catalog sync + result encoding.
  **BQ3** wires SDK/wire conformance and re-grades the cells `preview` →
  `limited` (not `ga`), updating `docs/GA.md` §1/§7, `docs/GCP-TESTABILITY.md` §4,
  README-GCP and the matrix overrides. **BQ4** proves memory/`--dsn` parity,
  snapshot/export safety and lakehouse scale.

### BQ4 outcome (2026-09-30) — parity, snapshots, scale

- **Per-query hydration retained; no incremental SQLite mirror.** The engine
  stays stateless scratch and the store stays the single source of truth, so
  snapshots/export/import/reset and `--dsn` durability need no invalidation
  logic. Hydration now loads each table inside **one SQLite transaction with a
  reused prepared statement** (was one implicit transaction per row).
- **Memory/`--dsn` parity is proven, not assumed.** The two store backends
  diverged in ways the engine could not see: Postgres bumped a table's
  `update_time` on streaming insert/`ReplaceRows` via SQL `now()` (bypassing the
  frozen clock), memory did not bump it at all, and memory did not default zero
  `createTime`/`updateTime`. Both now use `clock.Now().UTC()` and bump together,
  so `tables.get` `lastModifiedTime`/`etag`/`numRows` **agree across the two
  backends**. This is a cross-backend agreement guarantee, not a claim of exact
  real-GCP semantics: the emulator has no streaming buffer, so `insertAll`
  commits rows immediately (pre-existing `numRows` behavior) and `lastModifiedTime`
  advances on insert; real GCP excludes the streaming buffer from `numRows` and
  lags `lastModifiedTime` until flush. A cross-backend provider fingerprint
  (`TestSQLParityPostgres`, tag `gcp_persistence`) runs the same SQL workload
  against memory and Postgres and asserts the encoded
  `jobs.query`/`getQueryResults`/`tables.get`/`tabledata.list` bodies match after
  JSON canonicalization (random job IDs masked, clock frozen).
- **The store contract is JSON-value equality, not byte equality.** Postgres
  JSONB canonicalizes whitespace/key order, so the shared store matrix now
  compares configuration/schema/row JSON semantically (the memory/Postgres
  snapshot round-trip is shared too).
- **Scale is bounded and measured.** A table scan beyond `maxHydratedRows`
  (1,000,000) still fails loud (`invalidQuery`, never stale rows);
  `BenchmarkHydrateSelect` records hydration+aggregation over a 100k-row table.
  The `lakehouse` k3d E2E is a Spark/GCS pipeline and does not execute BigQuery
  SQL, so it is not this engine's scale evidence.
- **Remaining documented deviation:** a `dryRun` `CREATE SCHEMA` on an existing
  dataset is not validated (recorded as BQ12; the timestamp-parity half of that
  item is now fixed).

- **`go.mod` gains `modernc.org/sqlite` in BQ1**, not here; BQ0 ships no
  production code.
- BQ3 must not chase floci-gcp's `invalidQuery` assertions for constructs the
  frozen subset *does* support (e.g. `GROUP BY`/`ORDER BY`) — those pin floci's
  narrower engine, not real GCP.
