# BigQuery SQL differential corpus

An offline-first differential (record → replay → diff) corpus for the emulator's
BigQuery Standard SQL engine. [Real BigQuery is the spec](README.md): every
golden here is **recorded from real GCP**, never invented, and then replayed
against the emulator. The result is an explicit, evidence-backed frontier for the
engine's SQL subset, row/schema semantics and error envelopes.

Everything is under the `gcp_differential` build tag and reuses the curated REST
harness's target, normalizer, diff, triage and reporting plumbing. Only the
scenario list and the golden directory differ, exactly like the SDK-tour and
error-tour sets.

## Layout / targets

| Item | Value |
| --- | --- |
| Scenario builder | `BQSQLScenarios(project, suffix)` — `bqsql_scenarios.go` |
| Golden set | `testdata/golden-bigquery/` (74 query goldens + `manifest.json`) |
| Replay report | `testdata/report-bigquery/report.{json,md}` |
| Record | `make record-gcp-differential-bigquery` (needs ADC + a BigQuery-enabled project) |
| Replay | `make test-gcp-differential-bigquery` (ephemeral emulator, no credentials) |

The corpus is also replayed by the aggregate `make test-gcp-differential`.

### Fixtures

The set creates one run-scoped dataset (`bqsql_ds_<suffix>`) with three fixed
tables and fixed rows, as `Setup` scenarios that run in both record and replay
and are never diffed:

| table | columns |
| --- | --- |
| `nums` | `id INT64 REQUIRED`, `v FLOAT64`, `s STRING` → (1,1.5,a), (2,2.5,b), (3,NULL,NULL) |
| `employees` | `id INT64 REQUIRED`, `name STRING`, `dept_id INT64`, `salary INT64` → (1,ann,10,100), (2,bob,10,200), (3,cid,20,150) |
| `depts` | `id INT64 REQUIRED`, `dept STRING` → (10,eng), (20,ops) |

Every query is a `jobs.query` with `useLegacySql=false` and the fixture dataset
as `defaultDataset`. Multi-row queries carry a total `ORDER BY` so their row order
is stable across real BigQuery and SQLite.

### Normalization

The shared normalizer folds volatile job fields (query id, creation/start/end
times, slot ms) and the fixture dataset's run suffix. On top of that the corpus
canonicalizes **numeric result cells** at diff time only (`3.0` and `3` compare
equal; the schema field still records `FLOAT` vs `INTEGER`) — the committed
golden keeps real BigQuery's raw text. Additive job-statistics fields (cacheHit,
totalBytesBilled, …) and the advisory error `location`/`locationType` are
triaged as accepted, so they never mask a real rows/schema/status divergence.

## Expectations and the gate

Each corpus entry declares an `Expect`:

* **`match`** — the emulator must reproduce real BigQuery's result (rows, schema,
  statementType, error code/status) after normalization. A divergence is an
  unexpected regression and **fails** the gate.
* **`gap`** — the emulator does not implement a construct real BigQuery accepts
  (function / keyword / set operator / type outside the bounded subset). The
  emulator rejects the whole statement (400), so the divergence is expected and
  reported — never a silent wrong result.
* **`bug`** — the emulator attempts the query but returns a different result or
  status than real BigQuery. A genuine emulator divergence, expected to diverge
  until fixed and reported with evidence.

A `gap`/`bug` entry that starts matching is flagged as a **stale expectation**
(an improvement to fold in), not a failure. Only a `match` regression fails, so
`make test-gcp-differential-bigquery` exits non-zero only on unexpected
regressions. Run it with `-v` for the per-query match/mismatch table.

## Result (74 queries)

| class | match | gap | bug |
| --- | ---: | ---: | ---: |
| expression | 22 | 10 | 3 |
| aggregate | 7 | 1 | 1 |
| filter | 0 | 0 | 3 |
| join | 3 | 0 | 1 |
| subquery | 2 | 0 | 2 |
| setop | 0 | 3 | 1 |
| window | 1 | 1 | 4 |
| null | 2 | 0 | 3 |
| error | 3 | 0 | 1 |
| **total** | **40** | **15** | **19** |

### What matches (the supported subset)

Integer/float arithmetic (`+ - *`), `/`, `||`, `UPPER`/`LOWER`/`LENGTH`/`SUBSTR`/
`REPLACE`/`TRIM`/`INSTR`, `CAST` to/from STRING and INT64, `COALESCE`/`IFNULL`/
`NULLIF`, boolean logic, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `DISTINCT` with
`ORDER BY`, `WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`, `GROUP BY`/`HAVING`, `INNER`/
`LEFT` joins, `UNION ALL`, subqueries in `FROM`/`IN` and scalar subqueries, CTEs
(`WITH`), `SUM(...) OVER (PARTITION BY ...)`, and the error envelopes for a
missing table (404 NOT_FOUND), unknown column and syntax error (400
INVALID_ARGUMENT).

### Gaps — constructs real BigQuery accepts, the emulator rejects

The emulator returns `400 INVALID_ARGUMENT` ("… is not supported") where real
BigQuery returns `200` with rows. Grouped by construct:

| construct | queries | real GCP result |
| --- | --- | --- |
| scalar functions not in the allow-list | `CONCAT`, `IF`, `SAFE_CAST`, `MOD`, `DIV`, `STARTS_WITH`, `FORMAT`, `COUNTIF` | e.g. `CONCAT('a','b')` → `ab`; `COUNTIF(id>1)` → `2` |
| date functions | `DATE_ADD`, `CURRENT_DATE` | `DATE_ADD(DATE '2020-01-01', INTERVAL 1 DAY)` → `2020-01-02` |
| `DATE` literal | `DATE '2020-01-01'` | `2020-01-01` |
| set operators | `UNION DISTINCT`, `EXCEPT DISTINCT`, `INTERSECT DISTINCT` | deduped rows |
| `QUALIFY` | `QUALIFY rn = 1` | 1 row |

These are the documented **unsupported frontier**: the engine rejects them loudly
rather than returning a wrong result. `translate.go`'s `allowedFunctions` and
`rejectedKeywords` are the source of truth for the boundary.

### Bugs — the emulator attempts the query but diverges

Grouped by root cause; evidence is real GCP vs emulator.

**1. Query-result column mode is inherited from the source (11 queries).**
Real BigQuery reports projected scalar columns as `NULLABLE` even when the source
column is `REQUIRED`; the emulator propagates `REQUIRED`.

| op | real | emulator |
| --- | --- | --- |
| `filter_where_order`, `filter_order_desc_limit`, `filter_limit_offset`, `join_self_multi`, `setop_union_all`, `subq_from`, `subq_in`, `win_row_number`, `win_rank`, `win_lag`, `win_count_over` | `schema.fields[i].mode = "NULLABLE"` | `"REQUIRED"` |

Root cause: `queryengine/infer.go` copies the declared mode of a passthrough
column (`if col, ok := simpleColumnRef(expr); ok { f.Mode = sf.Mode }`).

**2. Unnamed output column numbering.** BigQuery numbers anonymous outputs among
the *unnamed* columns (`f0_`, `f1_`, …); the emulator numbers them by output
position.

| op | real | emulator |
| --- | --- | --- |
| `agg_group_by` (`SELECT dept_id, COUNT(*)`) | `f0_` | `f1_` |

Root cause: `queryengine/engine.go` names a non-identifier column
`fmt.Sprintf("f%d_", i)` with the positional index.

**3. Duplicate output column naming.** Real BigQuery disambiguates a repeated
column name; the emulator repeats it.

| op | real | emulator |
| --- | --- | --- |
| `join_self_multi` (`SELECT a.id, b.id`) | `id`, `id_1` | `id`, `id` |

**4. Literal `NULL` comparison is rejected by real BigQuery (2 queries).**
`SELECT NULL = NULL` / `SELECT 1 = NULL` → `400 INVALID_ARGUMENT`
`Operands of = cannot be literal NULL at [1:8]`; the emulator executes them and
returns `200` with a `NULL` row.

**5. `CASE` expression returns the wrong value.**
`SELECT CASE WHEN 1=1 THEN 'y' ELSE 'n' END` → real `"y"` (`STRING`); emulator
`"false"` (`BOOLEAN`).

**6. `CAST(FLOAT64 AS INT64)` truncates instead of rounding.**
`SELECT CAST(3.7 AS INT64)` → real `4`; emulator `3`.

**7. Division by zero is not an error.**
`SELECT 1/0` → real `400` `division by zero: 1 / 0`; emulator `200` with `NULL`.

**8. Implicit type coercion.**
`SELECT 1 + 'a'` → real `400` `Could not cast literal "a" to type DATE at
[1:12]`; emulator `200` with `1`.

**9. `NULL` literal type and name.**
`SELECT NULL` → real `f0_`/`INTEGER`; emulator `NULL`/`STRING`.

## Extending the corpus

Add a row to `BQSQLCases()` in `bqsql_scenarios.go` with a stable `ID`, a
`Class`, the `SQL` and an `Expect`. Keep multi-row queries deterministic (total
`ORDER BY` over unique keys). Then `make record-gcp-differential-bigquery` (the
one golden captured from real GCP per query) followed by
`make test-gcp-differential-bigquery`; set `Expect` from the observed result —
`match` only when the emulator reproduces real BigQuery, otherwise `gap` (a
rejected construct) or `bug` (a wrong result) with the evidence recorded in
`testdata/report-bigquery/report.md`.
