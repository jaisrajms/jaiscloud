package queryengine

import "testing"

// TestInferOutputTypesFromPlan locks in BQ8: expression and empty-result column
// types come from the query shape, not the runtime SQLite values.
func TestInferOutputTypesFromPlan(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			"boolean expression",
			"SELECT age > 30 AS adult FROM `p.ds.people` ORDER BY id",
			[]string{"BOOL"},
		},
		{
			"empty result keeps planned types",
			"SELECT age, age > 30 AS adult, name FROM `p.ds.people` WHERE id = 999",
			[]string{"INT64", "BOOL", "STRING"},
		},
		{
			"empty count is INT64 not STRING",
			"SELECT COUNT(*) AS c FROM `p.ds.people` WHERE id = 999",
			[]string{"INT64"},
		},
		{
			"empty float division is FLOAT64",
			"SELECT 1/2 AS r FROM `p.ds.people` WHERE id = 999",
			[]string{"FLOAT64"},
		},
		{
			"cast, string fn, aggregate",
			"SELECT CAST(age AS STRING) AS s, LENGTH(name) AS n, SUM(score) AS t FROM `p.ds.people`",
			[]string{"STRING", "INT64", "FLOAT64"},
		},
		{
			"passthrough declared types",
			"SELECT id, score FROM `p.ds.people`",
			[]string{"INT64", "FLOAT64"},
		},
		{
			"unaliased bool expression",
			"SELECT age > 30 FROM `p.ds.people`",
			[]string{"BOOL"},
		},
		{
			"not expression",
			"SELECT NOT active AS n FROM `p.ds.people`",
			[]string{"BOOL"},
		},
		{
			"int arithmetic",
			"SELECT age + 1 AS n FROM `p.ds.people`",
			[]string{"INT64"},
		},
		{
			"float arithmetic",
			"SELECT age + 1.5 AS n FROM `p.ds.people`",
			[]string{"FLOAT64"},
		},
		{
			"string concat",
			"SELECT name || 'x' AS s FROM `p.ds.people`",
			[]string{"STRING"},
		},
		{
			"min max keep source type",
			"SELECT MIN(age) AS lo, MAX(score) AS hi FROM `p.ds.people`",
			[]string{"INT64", "FLOAT64"},
		},
		{
			"string fn and coalesce",
			"SELECT LOWER(name) AS l, COALESCE(score, 0.0) AS c FROM `p.ds.people`",
			[]string{"STRING", "FLOAT64"},
		},
		{
			"qualified column",
			"SELECT p.name FROM `p.ds.people` p",
			[]string{"STRING"},
		},
		{
			"parenthesized column",
			"SELECT (age) AS a FROM `p.ds.people`",
			[]string{"INT64"},
		},
		{
			// A depth-0 '*' in a select item is multiplication, not a wildcard.
			"empty multiplication stays INT64",
			"SELECT age * 2 AS n FROM `p.ds.people` WHERE id = 999",
			[]string{"INT64"},
		},
		{
			"empty aggregate multiplication stays INT64",
			"SELECT COUNT(*) * 2 AS n FROM `p.ds.people` WHERE id = 999",
			[]string{"INT64"},
		},
		{
			"wildcard still falls back",
			"SELECT * FROM `p.ds.people`",
			[]string{"INT64", "STRING", "INT64", "FLOAT64", "BOOL"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := exec(t, tc.query)
			if len(res.Fields) != len(tc.want) {
				t.Fatalf("fields = %+v, want %d", res.Fields, len(tc.want))
			}
			for i, w := range tc.want {
				if res.Fields[i].Type != w {
					t.Fatalf("field %d (%s) = %q, want %q", i, res.Fields[i].Name, res.Fields[i].Type, w)
				}
			}
		})
	}
}

// TestInferBoolExpressionValues verifies the inferred BOOL normalizes the
// runtime integer to a Go bool so the wire renders true/false.
func TestInferBoolExpressionValues(t *testing.T) {
	res := exec(t, "SELECT age > 30 AS adult FROM `p.ds.people` ORDER BY id")
	if res.Rows[0][0] != false || res.Rows[3][0] != true {
		t.Fatalf("bool expression rows = %v", res.Rows)
	}
}

// TestNumericExprTypeAcceptsDiscoveryTypeNames locks in the wire-name aliases:
// a table schema read back from the store uses the Discovery/TableFieldSchema
// names (FLOAT, INTEGER), so arithmetic over a FLOAT column must still promote
// to FLOAT64 rather than fall through to the INT64 default.
func TestNumericExprTypeAcceptsDiscoveryTypeNames(t *testing.T) {
	fields := map[string]Field{
		"score": {Name: "score", Type: "FLOAT"},
		"id":    {Name: "id", Type: "INTEGER"},
	}
	for _, tc := range []struct {
		expr string
		want string
	}{
		{"score * 2", "FLOAT64"},
		{"score + id", "FLOAT64"},
		{"id + 1", "INT64"},
		{"SUM(score)", "FLOAT64"},
		{"SUM(id)", "INT64"},
	} {
		toks, err := tokenize("SELECT " + tc.expr)
		if err != nil {
			t.Fatalf("tokenize %q: %v", tc.expr, err)
		}
		if got := inferExprType(toks[1:], fields); got != tc.want {
			t.Errorf("inferExprType(%q) = %q, want %q", tc.expr, got, tc.want)
		}
	}
}
