package queryengine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestToSQLite(t *testing.T) {
	cases := []struct {
		field Field
		in    any
		want  any
	}{
		{Field{Type: "INT64"}, nil, nil},
		{Field{Type: "INT64"}, true, int64(1)},
		{Field{Type: "INT64"}, false, int64(0)},
		{Field{Type: "INT64"}, int64(7), int64(7)},
		{Field{Type: "INT64"}, 7, int64(7)},
		{Field{Type: "INT64"}, float64(7.9), int64(7)},
		{Field{Type: "INT64"}, json.Number("7"), int64(7)},
		{Field{Type: "INT64"}, json.Number("7.9"), int64(7)},
		{Field{Type: "INT64"}, "7", int64(7)},
		{Field{Type: "INT64"}, "x", "x"},
		{Field{Type: "FLOAT64"}, true, float64(1)},
		{Field{Type: "FLOAT64"}, false, float64(0)},
		{Field{Type: "FLOAT64"}, int64(3), float64(3)},
		{Field{Type: "FLOAT64"}, float64(2.5), float64(2.5)},
		{Field{Type: "FLOAT64"}, json.Number("2.5"), float64(2.5)},
		{Field{Type: "FLOAT64"}, "2.5", float64(2.5)},
		{Field{Type: "BYTES"}, []byte("ab"), []byte("ab")},
		{Field{Type: "BYTES"}, "ab", []byte("ab")},
		{Field{Type: "STRING"}, "s", "s"},
		{Field{Type: "STRING"}, int64(5), "5"},
		{Field{Type: "STRING"}, true, "true"},
	}
	for _, c := range cases {
		got := toSQLite(c.field, c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("toSQLite(%+v, %#v) = %#v, want %#v", c.field, c.in, got, c.want)
		}
	}
}

func TestNormalizeValue(t *testing.T) {
	cases := []struct {
		field Field
		in    any
		want  any
	}{
		{Field{}, int64(1), int64(1)}, // untyped: pass through
		{Field{Type: "INT64"}, nil, nil},
		{Field{Type: "BOOL"}, int64(0), false},
		{Field{Type: "BOOL"}, float64(1), true},
		{Field{Type: "BOOL"}, true, true},
		{Field{Type: "BOOL"}, "1", true},
		{Field{Type: "BOOL"}, "false", false},
		{Field{Type: "BOOL"}, []byte("true"), true},
		{Field{Type: "INT64"}, float64(3.9), int64(3)},
		{Field{Type: "INT64"}, true, int64(1)},
		{Field{Type: "INT64"}, false, int64(0)},
		{Field{Type: "INT64"}, []byte("8"), int64(8)},
		{Field{Type: "INT64"}, "8", int64(8)},
		{Field{Type: "FLOAT64"}, int64(2), float64(2)},
		{Field{Type: "FLOAT64"}, []byte("2.5"), float64(2.5)},
		{Field{Type: "FLOAT64"}, "2.5", float64(2.5)},
		{Field{Type: "FLOAT64"}, true, float64(1)},
		{Field{Type: "FLOAT64"}, false, float64(0)},
		{Field{Type: "FLOAT64"}, float64(1.5), float64(1.5)},
		{Field{Type: "STRING"}, int64(9), "9"},
		{Field{Type: "STRING"}, 1.5, "1.5"},
		{Field{Type: "STRING"}, true, "true"},
		{Field{Type: "STRING"}, false, "false"},
		{Field{Type: "STRING"}, []byte("b"), []byte("b")},
		{Field{Type: "STRING"}, "s", "s"},
	}
	for _, c := range cases {
		got := normalizeValue(c.field, c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("normalizeValue(%+v, %#v) = %#v, want %#v", c.field, c.in, got, c.want)
		}
	}
}

func TestSQLiteCastType(t *testing.T) {
	ok := map[string]string{
		"INT64": "INTEGER", "INTEGER": "INTEGER", "FLOAT64": "REAL",
		"NUMERIC": "REAL", "BIGNUMERIC": "REAL", "STRING": "TEXT",
		"DATE": "TEXT", "TIMESTAMP": "TEXT", "BYTES": "BLOB", "BOOL": "INTEGER",
	}
	for in, want := range ok {
		got, err := sqliteCastType(in)
		if err != nil || got != want {
			t.Errorf("sqliteCastType(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := sqliteCastType("ARRAY"); err == nil {
		t.Fatal("expected ARRAY cast type to be rejected")
	}
}

func TestSmallHelpers(t *testing.T) {
	if inferType(true) != "BOOL" || inferType(int64(1)) != "INT64" ||
		inferType(float64(1)) != "FLOAT64" || inferType([]byte("x")) != "BYTES" ||
		inferType(nil) != "STRING" || inferType("x") != "STRING" {
		t.Fatal("inferType mismatch")
	}
	if modeOrDefault("") != "NULLABLE" || modeOrDefault("required") != "REQUIRED" {
		t.Fatal("modeOrDefault mismatch")
	}
	if !isNestedType("record") || !isNestedType("STRUCT") || !isNestedType("geography") {
		t.Fatal("isNestedType should accept nested types")
	}
	if isNestedType("STRING") || isNestedType("INT64") {
		t.Fatal("isNestedType should reject scalars")
	}
	if !numericTrue("TRUE") || !numericTrue(" 1 ") || numericTrue("no") {
		t.Fatal("numericTrue mismatch")
	}
}

func specialCatalog() memCatalog {
	c := seedCatalog()
	c.tables["p\x00ds\x00repeated"] = Table{
		Project: "p", Dataset: "ds", Table: "repeated",
		Fields: []Field{{Name: "tags", Type: "STRING", Mode: "REPEATED"}},
	}
	c.tables["p\x00ds\x00nested"] = Table{
		Project: "p", Dataset: "ds", Table: "nested",
		Fields: []Field{{Name: "addr", Type: "RECORD"}},
	}
	c.tables["p\x00ds\x00noschema"] = Table{Project: "p", Dataset: "ds", Table: "noschema"}
	c.tables["p\x00ds\x00typed"] = Table{
		Project: "p", Dataset: "ds", Table: "typed",
		Fields: []Field{
			{Name: "b", Type: "BYTES"},
			{Name: "n", Type: "NUMERIC"},
			{Name: "d", Type: "DATE"},
		},
		Rows: []map[string]any{
			{"b": "bytes", "n": 12.5, "d": "2026-01-02"},
		},
	}
	return c
}

func TestExecuteQualificationErrors(t *testing.T) {
	cases := []string{
		"SELECT * FROM `a.b.c.d`",
		"SELECT * FROM `p.ds.INFORMATION_SCHEMA.TABLES`",
		"SELECT * FROM foo",    // no defaultDataset
		"SELECT * FROM bar(1)", // table-valued function
	}
	for _, q := range cases {
		req := Request{Project: "p", Query: q}
		if q == "SELECT * FROM bar(1)" {
			req.DefaultDataset = "ds"
		}
		_, err := Execute(context.Background(), specialCatalog(), req)
		var ue *UnsupportedError
		if !errors.As(err, &ue) {
			t.Fatalf("%q: expected UnsupportedError, got %v", q, err)
		}
	}
}

func TestExecuteRejectsNestedAndSchemaLess(t *testing.T) {
	for _, q := range []string{
		"SELECT * FROM `p.ds.repeated`",
		"SELECT * FROM `p.ds.nested`",
		"SELECT * FROM `p.ds.noschema`",
	} {
		_, err := Execute(context.Background(), specialCatalog(), Request{Project: "p", Query: q, DefaultDataset: "ds"})
		var ue *UnsupportedError
		if !errors.As(err, &ue) {
			t.Fatalf("%q: expected UnsupportedError, got %v", q, err)
		}
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("%q: expected ErrUnsupported match", q)
		}
	}
}

func TestExecuteTypedRoundTrip(t *testing.T) {
	res, err := Execute(context.Background(), specialCatalog(), Request{
		Project: "p", Query: "SELECT b, n, d FROM `p.ds.typed`", DefaultDataset: "ds",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Rows[0][0]; !reflect.DeepEqual(got, []byte("bytes")) {
		t.Fatalf("bytes = %#v", got)
	}
	if res.Rows[0][1] != 12.5 {
		t.Fatalf("numeric = %#v", res.Rows[0][1])
	}
	if res.Rows[0][2] != "2026-01-02" {
		t.Fatalf("date = %#v", res.Rows[0][2])
	}
}

func TestExecuteCastAndDefaultProjectFallback(t *testing.T) {
	res := exec(t, "SELECT CAST(id AS FLOAT64) AS f, CAST(active AS STRING) AS s FROM `p.ds.people` ORDER BY id")
	if res.Rows[0][0] != float64(1) || res.Rows[0][1] != "1" {
		t.Fatalf("cast row = %v", res.Rows[0])
	}

	// DefaultProject empty falls back to Request.Project.
	r, err := Execute(context.Background(), seedCatalog(), Request{Project: "p", Query: "SELECT COUNT(*) FROM people", DefaultDataset: "ds"})
	if err != nil || r.Rows[0][0] != int64(4) {
		t.Fatalf("default-project fallback: %v, %v", r, err)
	}
}

func TestExecuteMultiCTE(t *testing.T) {
	// Reference the *second* CTE: collectCTENames must collect all of them.
	res := exec(t, "WITH a AS (SELECT * FROM `p.ds.people`), b AS (SELECT * FROM `p.ds.orders`) SELECT COUNT(*) FROM b")
	if res.Rows[0][0] != int64(3) {
		t.Fatalf("multi-cte = %v", res.Rows)
	}
	res = exec(t, "WITH a AS (SELECT * FROM `p.ds.people`), b AS (SELECT * FROM `p.ds.orders`) SELECT COUNT(*) FROM a, b")
	if res.Rows[0][0] != int64(12) {
		t.Fatalf("multi-cte cross = %v", res.Rows)
	}
}

func TestExecuteStringLiteralSafety(t *testing.T) {
	// A BigQuery backslash-escaped quote must be decoded and re-quoted for
	// SQLite, never allowed to terminate the literal early.
	res := exec(t, `SELECT 'a\'; DROP TABLE bq_t0; --' AS x FROM people LIMIT 1`)
	if len(res.Rows) != 1 || res.Rows[0][0] != "a'; DROP TABLE bq_t0; --" {
		t.Fatalf("decoded literal = %#v", res.Rows)
	}
	r := exec(t, "SELECT COUNT(*) FROM people")
	if r.Rows[0][0] != int64(4) {
		t.Fatalf("people damaged by injection: %v", r.Rows)
	}
}

func TestExecuteDoubleQuotedString(t *testing.T) {
	// BigQuery "..." is a string literal; it must not become a SQLite
	// identifier (which would make `name = "name"` match every row).
	res := exec(t, `SELECT name FROM people WHERE name = "name"`)
	if len(res.Rows) != 0 {
		t.Fatalf(`double-quoted "name" must be a string literal, got %v`, res.Rows)
	}
}

func TestExecuteLikeCaseSensitive(t *testing.T) {
	res := exec(t, `SELECT COUNT(*) FROM people WHERE name LIKE 'AL%'`)
	if res.Rows[0][0] != int64(0) {
		t.Fatalf("LIKE must be case-sensitive, got %v", res.Rows)
	}
	res = exec(t, `SELECT COUNT(*) FROM people WHERE name LIKE 'al%'`)
	if res.Rows[0][0] != int64(1) {
		t.Fatalf("LIKE lowercase count = %v", res.Rows)
	}
}

func TestExecuteEmptyAndNilCatalog(t *testing.T) {
	if _, err := Execute(context.Background(), seedCatalog(), Request{Query: "   "}); err == nil {
		t.Fatal("expected empty query error")
	}
	if _, err := Execute(context.Background(), nil, Request{Query: "SELECT 1"}); err == nil {
		t.Fatal("expected nil catalog error")
	}
}
