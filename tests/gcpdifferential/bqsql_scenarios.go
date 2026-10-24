//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// This file is the BigQuery SQL differential corpus (PLAN 5). It records the
// response of a curated corpus of Standard SQL `jobs.query` requests from REAL
// BigQuery into testdata/golden-bigquery, then replays the same corpus against
// the emulator and diffs rows, schema and error envelopes. The corpus makes the
// emulator's SQL subset and error semantics an explicit, evidence-backed
// frontier instead of an untested one.
//
// Real BigQuery is the spec: every golden is recorded from it, never invented.
// The replay classifies each query against a declared expectation (match /
// gap / bug) so a known subset gap or engine bug is reported with its
// real-vs-emulator evidence and does not fail the gate — only an unexpected
// regression to a `match` query does. See bqsql_test.go and README-bigquery.md.
//
// The corpus shares the curated REST harness's target, normalizer (with a
// BigQuery-specific numeric-cell canonicalization), diff and reporting
// plumbing; only the scenario list and the golden directory differ.

// BQSQLExpect is the declared expectation for one corpus query.
type BQSQLExpect string

const (
	// BQSQLMatch: the emulator must reproduce real BigQuery's result after
	// normalization (rows, schema, statementType, error code/status). A
	// divergence is an open regression and fails the gate.
	BQSQLMatch BQSQLExpect = "match"
	// BQSQLGap: the emulator does not implement the construct real BigQuery
	// accepts (function/keyword/set-operator/type outside the bounded subset).
	// The emulator rejects the whole statement, so the divergence is expected
	// and reported — never a silent wrong result.
	BQSQLGap BQSQLExpect = "gap"
	// BQSQLBug: the emulator attempts the query but returns a different
	// result or status than real BigQuery. This is a genuine emulator
	// divergence (not a subset gap); it is expected to diverge until fixed and
	// is reported with evidence.
	BQSQLBug BQSQLExpect = "bug"
)

// BQSQLCase is one corpus entry: a Standard SQL statement plus its declared
// expectation. Class groups entries for the summary doc. SQL is sent verbatim
// to jobs.query (useLegacySql=false) with the fixture dataset as defaultDataset.
type BQSQLCase struct {
	ID     string
	Class  string
	SQL    string
	Expect BQSQLExpect
}

// BQSQLDatasetName returns the corpus's run-scoped fixture dataset id. The run
// suffix is folded by the normalizer, so the committed goldens stay stable.
func BQSQLDatasetName(suffix string) string { return "bqsql_ds_" + suffix }

// BQSQLCases is the corpus. Every entry is a deterministic query: any
// multi-row result carries a total ORDER BY over unique keys so its row order
// is stable across real BigQuery and SQLite.
func BQSQLCases() []BQSQLCase {
	return []BQSQLCase{
		// ── Projection / expressions ──────────────────────────────────────
		{"expr_literal_int", "expression", "SELECT 1", BQSQLMatch},
		{"expr_arith_add", "expression", "SELECT 1+2", BQSQLMatch},
		{"expr_arith_sub", "expression", "SELECT 10-3", BQSQLMatch},
		{"expr_arith_mul", "expression", "SELECT 2*3", BQSQLMatch},
		{"expr_arith_float_mul", "expression", "SELECT 1.5*2", BQSQLMatch},
		{"expr_arith_div", "expression", "SELECT 7/2", BQSQLMatch},
		{"expr_arith_div_zero", "expression", "SELECT 1/0", BQSQLBug},
		{"expr_string_concat_op", "expression", "SELECT 'a' || 'b'", BQSQLMatch},
		{"expr_func_upper", "expression", "SELECT UPPER('abc')", BQSQLMatch},
		{"expr_func_lower", "expression", "SELECT LOWER('ABC')", BQSQLMatch},
		{"expr_func_length", "expression", "SELECT LENGTH('hello')", BQSQLMatch},
		{"expr_func_substr", "expression", "SELECT SUBSTR('abcdef', 2, 3)", BQSQLMatch},
		{"expr_func_concat", "expression", "SELECT CONCAT('a','b')", BQSQLGap},
		{"expr_func_replace", "expression", "SELECT REPLACE('aXbX','X','-')", BQSQLMatch},
		{"expr_func_trim", "expression", "SELECT TRIM('  x  ')", BQSQLMatch},
		{"expr_func_instr", "expression", "SELECT INSTR('hello','l')", BQSQLMatch},
		{"expr_coalesce", "expression", "SELECT COALESCE(NULL, 5)", BQSQLMatch},
		{"expr_coalesce_chain", "expression", "SELECT COALESCE(NULL, NULL, 3)", BQSQLMatch},
		{"expr_case", "expression", "SELECT CASE WHEN 1=1 THEN 'y' ELSE 'n' END", BQSQLBug},
		{"expr_if", "expression", "SELECT IF(1=1, 'y', 'n')", BQSQLGap},
		{"expr_cast_str_int", "expression", "SELECT CAST('42' AS INT64)", BQSQLMatch},
		{"expr_cast_float_int", "expression", "SELECT CAST(3.7 AS INT64)", BQSQLBug},
		{"expr_cast_int_str", "expression", "SELECT CAST(42 AS STRING)", BQSQLMatch},
		{"expr_safe_cast", "expression", "SELECT SAFE_CAST('x' AS INT64)", BQSQLGap},
		{"expr_func_abs", "expression", "SELECT ABS(-3)", BQSQLMatch},
		{"expr_func_round", "expression", "SELECT ROUND(2.567, 2)", BQSQLMatch},
		{"expr_func_mod", "expression", "SELECT MOD(7,3)", BQSQLGap},
		{"expr_func_div", "expression", "SELECT DIV(7,2)", BQSQLGap},
		{"expr_func_starts_with", "expression", "SELECT STARTS_WITH('abc','a')", BQSQLGap},
		{"expr_func_format", "expression", "SELECT FORMAT('%d', 5)", BQSQLGap},
		{"expr_date_literal", "expression", "SELECT DATE '2020-01-01'", BQSQLGap},
		{"expr_date_add", "expression", "SELECT DATE_ADD(DATE '2020-01-01', INTERVAL 1 DAY)", BQSQLGap},
		{"expr_current_date", "expression", "SELECT CURRENT_DATE()", BQSQLGap},
		{"expr_logic_not", "expression", "SELECT NOT TRUE", BQSQLMatch},
		{"expr_logic_and", "expression", "SELECT TRUE AND FALSE", BQSQLMatch},

		// ── NULL / three-valued logic ─────────────────────────────────────
		{"null_literal", "null", "SELECT NULL", BQSQLBug},
		{"null_eq_null", "null", "SELECT NULL = NULL", BQSQLBug},
		{"null_eq_int", "null", "SELECT 1 = NULL", BQSQLBug},
		{"null_func_nullif", "null", "SELECT NULLIF(1,1)", BQSQLMatch},
		{"null_func_ifnull", "null", "SELECT IFNULL(NULL, 9)", BQSQLMatch},

		// ── Filtering / aggregation ───────────────────────────────────────
		{"agg_count_star", "aggregate", "SELECT COUNT(*) FROM nums", BQSQLMatch},
		{"agg_count_col", "aggregate", "SELECT COUNT(v) FROM nums", BQSQLMatch},
		{"agg_sum", "aggregate", "SELECT SUM(id) FROM nums", BQSQLMatch},
		{"agg_avg", "aggregate", "SELECT AVG(id) FROM nums", BQSQLMatch},
		{"agg_min_max", "aggregate", "SELECT MIN(id), MAX(id) FROM nums", BQSQLMatch},
		{"agg_countif", "aggregate", "SELECT COUNTIF(id > 1) FROM nums", BQSQLGap},
		{"agg_distinct", "aggregate", "SELECT DISTINCT s FROM nums ORDER BY s", BQSQLMatch},
		{"filter_where_order", "filter", "SELECT id FROM nums WHERE id > 1 ORDER BY id", BQSQLBug},
		{"filter_order_desc_limit", "filter", "SELECT id FROM nums ORDER BY id DESC LIMIT 2", BQSQLBug},
		{"filter_limit_offset", "filter", "SELECT id FROM nums ORDER BY id LIMIT 1 OFFSET 1", BQSQLBug},
		{"agg_group_by", "aggregate", "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id ORDER BY dept_id", BQSQLBug},
		{"agg_having", "aggregate", "SELECT dept_id, SUM(salary) c FROM employees GROUP BY dept_id HAVING SUM(salary) > 250 ORDER BY dept_id", BQSQLMatch},

		// ── Joins ─────────────────────────────────────────────────────────
		{"join_inner_on", "join", "SELECT e.name, d.dept FROM employees e JOIN depts d ON e.dept_id = d.id ORDER BY e.id", BQSQLMatch},
		{"join_inner_kw", "join", "SELECT e.name, d.dept FROM employees e INNER JOIN depts d ON e.dept_id = d.id ORDER BY e.id", BQSQLMatch},
		{"join_left", "join", "SELECT e.name, d.dept FROM employees e LEFT JOIN depts d ON e.dept_id = d.id ORDER BY e.id", BQSQLMatch},
		{"join_self_multi", "join", "SELECT a.id, b.id FROM nums a JOIN nums b ON a.id = b.id ORDER BY a.id", BQSQLBug},

		// ── Subqueries / CTEs / set operators ─────────────────────────────
		{"subq_from", "subquery", "SELECT id FROM (SELECT id FROM nums WHERE id > 1) ORDER BY id", BQSQLBug},
		{"subq_cte", "subquery", "WITH t AS (SELECT id FROM nums WHERE id > 1) SELECT COUNT(*) FROM t", BQSQLMatch},
		{"subq_in", "subquery", "SELECT id FROM nums WHERE id IN (SELECT id FROM nums WHERE id > 1) ORDER BY id", BQSQLBug},
		{"subq_scalar", "subquery", "SELECT (SELECT 1)", BQSQLMatch},
		{"setop_union_all", "setop", "SELECT id FROM nums WHERE id > 1 UNION ALL SELECT id FROM nums WHERE id < 3 ORDER BY id", BQSQLBug},
		{"setop_union_distinct", "setop", "SELECT id FROM nums UNION DISTINCT SELECT id FROM nums ORDER BY id", BQSQLGap},
		{"setop_except", "setop", "SELECT id FROM nums EXCEPT DISTINCT SELECT id FROM nums WHERE id = 2", BQSQLGap},
		{"setop_intersect", "setop", "SELECT id FROM nums INTERSECT DISTINCT SELECT id FROM nums", BQSQLGap},

		// ── Window functions ──────────────────────────────────────────────
		{"win_row_number", "window", "SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM nums ORDER BY id", BQSQLBug},
		{"win_rank", "window", "SELECT id, RANK() OVER (ORDER BY id) rk FROM nums ORDER BY id", BQSQLBug},
		{"win_sum_partition", "window", "SELECT dept_id, SUM(salary) OVER (PARTITION BY dept_id) s FROM employees ORDER BY id", BQSQLMatch},
		{"win_lag", "window", "SELECT id, LAG(id) OVER (ORDER BY id) lg FROM nums ORDER BY id", BQSQLBug},
		{"win_count_over", "window", "SELECT id, COUNT(*) OVER () c FROM nums ORDER BY id", BQSQLBug},
		{"win_qualify", "window", "SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM nums QUALIFY rn = 1", BQSQLGap},

		// ── Errors ────────────────────────────────────────────────────────
		{"err_missing_table", "error", "SELECT * FROM missing_table", BQSQLMatch},
		{"err_unknown_column", "error", "SELECT missing_col FROM nums", BQSQLMatch},
		{"err_syntax", "error", "SELECT FROM", BQSQLMatch},
		{"err_type_mismatch", "error", "SELECT 1 + 'a'", BQSQLBug},
	}
}

// bqsqlCaseByOp indexes the corpus by its scenario Op so the replay can recover
// the declared expectation for a matched golden.
func bqsqlCaseByOp() map[string]BQSQLCase {
	m := map[string]BQSQLCase{}
	for _, c := range BQSQLCases() {
		m["bqsql_"+c.ID] = c
	}
	return m
}

// bqsqlQueryBody renders the jobs.query body for one case. useLegacySql is
// false (Standard SQL) and the fixture dataset is the default so unqualified
// table references resolve, exactly as a console query would.
func bqsqlQueryBody(project, dataset, sql string) string {
	return fmt.Sprintf(`{"query":%q,"useLegacySql":false,"defaultDataset":{"projectId":%q,"datasetId":%q}}`,
		sql, project, dataset)
}

// BQSQLScenarios returns the fixture setup plus the query corpus, in dependency
// order: the dataset, three tables and their fixed rows are Setup scenarios
// (run in both record and replay, never diffed), then one jobs.query golden per
// corpus entry. The fixture rows are fixed so a recorded golden is stable.
func BQSQLScenarios(project, suffix string) []Scenario {
	ds := BQSQLDatasetName(suffix)
	bqBase := "/bigquery/v2/projects/" + project

	var sc []Scenario

	// Fixtures. Every fixture is a Setup scenario: its own response (dataset /
	// table metadata, insertAll envelope) is not part of the query contract, so
	// only the queries are recorded and diffed.
	sc = append(sc,
		Scenario{Op: "bqsql_setup_dataset", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, project, ds)},
		Scenario{Op: "bqsql_setup_table_nums", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":"nums"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"v","type":"FLOAT"},{"name":"s","type":"STRING"}]}}`, project, ds)},
		Scenario{Op: "bqsql_setup_table_employees", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":"employees"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"dept_id","type":"INTEGER"},{"name":"salary","type":"INTEGER"}]}}`, project, ds)},
		Scenario{Op: "bqsql_setup_table_depts", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":"depts"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"dept","type":"STRING"}]}}`, project, ds)},
		Scenario{Op: "bqsql_setup_rows_nums", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets/" + ds + "/tables/nums/insertAll",
			Body: `{"rows":[{"insertId":"n1","json":{"id":"1","v":"1.5","s":"a"}},{"insertId":"n2","json":{"id":"2","v":"2.5","s":"b"}},{"insertId":"n3","json":{"id":"3","v":null,"s":null}}]}`},
		Scenario{Op: "bqsql_setup_rows_employees", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets/" + ds + "/tables/employees/insertAll",
			Body: `{"rows":[{"insertId":"e1","json":{"id":"1","name":"ann","dept_id":"10","salary":"100"}},{"insertId":"e2","json":{"id":"2","name":"bob","dept_id":"10","salary":"200"}},{"insertId":"e3","json":{"id":"3","name":"cid","dept_id":"20","salary":"150"}}]}`},
		Scenario{Op: "bqsql_setup_rows_depts", Service: "bigquery", Method: http.MethodPost, Setup: true,
			Path: bqBase + "/datasets/" + ds + "/tables/depts/insertAll",
			Body: `{"rows":[{"insertId":"d1","json":{"id":"10","dept":"eng"}},{"insertId":"d2","json":{"id":"20","dept":"ops"}}]}`},
	)

	for _, c := range BQSQLCases() {
		sc = append(sc, Scenario{
			Op:      "bqsql_" + c.ID,
			Service: "bigquery",
			Method:  http.MethodPost,
			Path:    bqBase + "/queries",
			Body:    bqsqlQueryBody(project, ds, c.SQL),
		})
	}
	return sc
}

// CleanupBQSQL deletes the corpus fixture dataset (and its tables/rows). It is
// best-effort and idempotent (404s are ignored).
func (t *Target) CleanupBQSQL() []string {
	ds := BQSQLDatasetName(t.Suffix)
	path := "/bigquery/v2/projects/" + t.Project + "/datasets/" + ds + "?deleteContents=true"
	status, _, err := t.request(http.MethodDelete, "bigquery", path, "", "application/json")
	if err != nil {
		return []string{fmt.Sprintf("cleanup bigquery-sql dataset: error: %v", err)}
	}
	return []string{fmt.Sprintf("cleanup bigquery-sql dataset: HTTP %d", status)}
}

// VerifyBQSQLAbsent reads back the corpus dataset; a fully cleaned target
// reports 404.
func (t *Target) VerifyBQSQLAbsent() []string {
	ds := BQSQLDatasetName(t.Suffix)
	path := "/bigquery/v2/projects/" + t.Project + "/datasets/" + ds
	status, _, err := t.request(http.MethodGet, "bigquery", path, "", "")
	if err != nil {
		return []string{fmt.Sprintf("verify bigquery-sql dataset: error: %v", err)}
	}
	return []string{fmt.Sprintf("verify bigquery-sql dataset: HTTP %d", status)}
}

// decimalCell matches a plain decimal (an integer part with a fractional part)
// as BigQuery renders a FLOAT64 cell value, e.g. "3.0", "2.50", "-0.0".
var decimalCell = regexp.MustCompile(`^-?\d+\.\d+$`)

// canonicalizeNumericCell trims the redundant fractional formatting BigQuery and
// the emulator differ on ("3.0" vs "3"): numeric equality is the contract, and
// the schema field still records INTEGER vs FLOAT. It is applied to both sides
// at diff time only, so the committed golden keeps real BigQuery's raw text.
func canonicalizeNumericCell(s string) string {
	if !decimalCell.MatchString(s) {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimRight(s, ".")
}

// canonicalizeBQSQLExchange returns a copy of ex whose result-cell strings have
// their redundant decimal formatting canonicalized. Non-numeric cells, schema,
// statementType and error envelopes are untouched, so a real row/schema/error
// divergence still surfaces.
func canonicalizeBQSQLExchange(ex Exchange) Exchange {
	if len(ex.Response) == 0 {
		return ex
	}
	var root map[string]any
	if err := json.Unmarshal(ex.Response, &root); err != nil {
		return ex
	}
	rows, _ := root["rows"].([]any)
	changed := false
	for _, r := range rows {
		rm, ok := r.(map[string]any)
		if !ok {
			continue
		}
		cells, _ := rm["f"].([]any)
		for _, c := range cells {
			cm, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if v, ok := cm["v"].(string); ok {
				if nv := canonicalizeNumericCell(v); nv != v {
					cm["v"] = nv
					changed = true
				}
			}
		}
	}
	if !changed {
		return ex
	}
	b, err := json.Marshal(root)
	if err != nil {
		return ex
	}
	ex.Response = b
	return ex
}
