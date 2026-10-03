package queryengine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type memCatalog struct {
	tables map[string]Table
}

func (c memCatalog) Table(_ context.Context, project, dataset, table string) (Table, error) {
	if t, ok := c.tables[project+"\x00"+dataset+"\x00"+table]; ok {
		return t, nil
	}
	return Table{}, ErrTableNotFound
}

func seedCatalog() memCatalog {
	return memCatalog{tables: map[string]Table{
		"p\x00ds\x00people": {
			Project: "p", Dataset: "ds", Table: "people",
			Fields: []Field{
				{Name: "id", Type: "INT64"},
				{Name: "name", Type: "STRING"},
				{Name: "age", Type: "INT64"},
				{Name: "score", Type: "FLOAT64"},
				{Name: "active", Type: "BOOL"},
			},
			Rows: []map[string]any{
				{"id": int64(1), "name": "alice", "age": int64(30), "score": 9.5, "active": true},
				{"id": int64(2), "name": "bob", "age": int64(25), "score": 7.0, "active": false},
				{"id": int64(3), "name": "carol", "age": int64(30), "score": 8.25, "active": true},
				{"id": int64(4), "name": nil, "age": int64(40), "active": true},
			},
		},
		"p\x00ds\x00orders": {
			Project: "p", Dataset: "ds", Table: "orders",
			Fields: []Field{
				{Name: "person_id", Type: "INT64"},
				{Name: "amount", Type: "FLOAT64"},
			},
			Rows: []map[string]any{
				{"person_id": int64(1), "amount": 10.0},
				{"person_id": int64(1), "amount": 5.5},
				{"person_id": int64(3), "amount": 20.0},
			},
		},
	}}
}

func exec(t *testing.T, q string) Result {
	t.Helper()
	res, err := Execute(context.Background(), seedCatalog(), Request{
		Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
	})
	if err != nil {
		t.Fatalf("Execute(%q): %v", q, err)
	}
	return res
}

func TestExecuteSelectSubset(t *testing.T) {
	// Backquoted fully-qualified table, WHERE.
	res := exec(t, "SELECT name FROM `p.ds.people` WHERE age = 30 ORDER BY name")
	if len(res.Rows) != 2 || res.Rows[0][0] != "alice" || res.Rows[1][0] != "carol" {
		t.Fatalf("WHERE result = %v", res.Rows)
	}
	if res.Fields[0].Name != "name" || res.Fields[0].Type != "STRING" {
		t.Fatalf("field = %+v", res.Fields[0])
	}

	// COUNT(*) and expression-column naming.
	res = exec(t, "SELECT COUNT(*) FROM `p.ds.people`")
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(4) {
		t.Fatalf("COUNT = %v", res.Rows)
	}
	if res.Fields[0].Name != "f0_" || res.Fields[0].Type != "INT64" {
		t.Fatalf("count field = %+v", res.Fields[0])
	}

	// GROUP BY / HAVING / AVG.
	res = exec(t, "SELECT age, COUNT(*) AS c, AVG(score) AS a FROM `p.ds.people` GROUP BY age HAVING COUNT(*) > 1")
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(30) || res.Rows[0][1] != int64(2) {
		t.Fatalf("group = %v", res.Rows)
	}

	// INNER JOIN.
	res = exec(t, "SELECT p.name, o.amount FROM `p.ds.people` p JOIN `p.ds.orders` o ON p.id = o.person_id ORDER BY o.amount")
	if len(res.Rows) != 3 {
		t.Fatalf("inner join rows = %v", res.Rows)
	}

	// LEFT JOIN preserves the unmatched row as NULL.
	res = exec(t, "SELECT p.id, o.amount FROM `p.ds.people` p LEFT JOIN `p.ds.orders` o ON p.id = o.person_id ORDER BY p.id, o.amount")
	if len(res.Rows) != 5 || res.Rows[4][1] != nil {
		t.Fatalf("left join = %v", res.Rows)
	}

	// CTE.
	res = exec(t, "WITH adults AS (SELECT * FROM `p.ds.people` WHERE age >= 30) SELECT COUNT(*) FROM adults")
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(3) {
		t.Fatalf("cte = %v", res.Rows)
	}

	// Window function.
	res = exec(t, "SELECT id, ROW_NUMBER() OVER (PARTITION BY age ORDER BY id) AS rn FROM `p.ds.people` ORDER BY id")
	if len(res.Rows) != 4 || res.Rows[2][1] != int64(2) {
		t.Fatalf("window = %v", res.Rows)
	}

	// DISTINCT + LIMIT/OFFSET.
	res = exec(t, "SELECT DISTINCT age FROM `p.ds.people` ORDER BY age LIMIT 2 OFFSET 0")
	if len(res.Rows) != 2 || res.Rows[0][0] != int64(25) {
		t.Fatalf("distinct = %v", res.Rows)
	}

	// UNION ALL.
	res = exec(t, "SELECT id FROM `p.ds.people` UNION ALL SELECT person_id FROM `p.ds.orders`")
	if len(res.Rows) != 7 {
		t.Fatalf("union all = %v", res.Rows)
	}

	// Unqualified table resolved through defaultDataset.
	res = exec(t, "SELECT COUNT(*) FROM people")
	if res.Rows[0][0] != int64(4) {
		t.Fatalf("default dataset = %v", res.Rows)
	}

	// dataset.table form.
	res = exec(t, "SELECT COUNT(*) FROM ds.people")
	if res.Rows[0][0] != int64(4) {
		t.Fatalf("dataset.table = %v", res.Rows)
	}
}

func TestExecuteLiteralAndFloatDivision(t *testing.T) {
	res := exec(t, "SELECT 1")
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(1) {
		t.Fatalf("SELECT 1 = %v", res.Rows)
	}
	if res.Fields[0].Name != "f0_" || res.Fields[0].Type != "INT64" {
		t.Fatalf("SELECT 1 field = %+v", res.Fields[0])
	}

	// BigQuery '/' is FLOAT64 division; SQLite would return 0.
	res = exec(t, "SELECT 1/2 AS r")
	if len(res.Rows) != 1 || res.Rows[0][0] != 0.5 {
		t.Fatalf("float division = %v", res.Rows)
	}

	// '/'-containing string literals are untouched.
	res = exec(t, "SELECT 'a/b' AS s")
	if res.Rows[0][0] != "a/b" {
		t.Fatalf("string literal = %v", res.Rows)
	}
}

func TestExecuteBoolAndNullNormalization(t *testing.T) {
	res := exec(t, "SELECT active FROM `p.ds.people` ORDER BY id")
	if res.Fields[0].Type != "BOOL" {
		t.Fatalf("active type = %q", res.Fields[0].Type)
	}
	if res.Rows[0][0] != true || res.Rows[1][0] != false {
		t.Fatalf("bool values = %v", res.Rows)
	}
	res = exec(t, "SELECT name FROM `p.ds.people` ORDER BY id")
	if res.Rows[3][0] != nil {
		t.Fatalf("expected NULL name, got %v", res.Rows[3][0])
	}
}

func TestExecuteFailLoud(t *testing.T) {
	cases := map[string]string{
		"unknown table":  "SELECT * FROM `p.ds.nope`",
		"unnest":         "SELECT * FROM UNNEST([1,2,3])",
		"ddl":            "CREATE TABLE x (a INT64)",
		"qualify":        "SELECT id FROM `p.ds.people` QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1",
		"unknown func":   "SELECT FARM_FINGERPRINT(name) FROM `p.ds.people`",
		"legacy date":    "SELECT DATE '2020-01-01'",
		"right join":     "SELECT * FROM `p.ds.people` RIGHT JOIN `p.ds.orders` ON a = b",
		"select star":    "SELECT * FROM `p.ds.people` WHERE id = ",
		"wildcard table": "SELECT * FROM `p.ds.people*`",
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Execute(context.Background(), seedCatalog(), Request{Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds"})
			if err == nil {
				t.Fatalf("expected error for %q", q)
			}
			var ue *UnsupportedError
			var nf *TableNotFoundError
			if !errors.As(err, &ue) && !errors.As(err, &nf) {
				t.Fatalf("expected Unsupported/TableNotFound, got %T: %v", err, err)
			}
		})
	}
}

func TestExecuteTableNotFound(t *testing.T) {
	_, err := Execute(context.Background(), seedCatalog(), Request{Project: "p", Query: "SELECT * FROM `p.ds.gone`", DefaultProject: "p", DefaultDataset: "ds"})
	var nf *TableNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected TableNotFoundError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("expected ErrTableNotFound match")
	}
	if !strings.Contains(nf.Error(), "p.ds.gone") {
		t.Fatalf("error = %v", nf)
	}
}

func TestExecuteIdentifierInjectionIsInert(t *testing.T) {
	// A table name that looks like SQL must be treated as data (a missing
	// table), never executed.
	_, err := Execute(context.Background(), seedCatalog(), Request{
		Project: "p", Query: "SELECT * FROM `p.ds.t\"; DROP TABLE people; --`", DefaultProject: "p", DefaultDataset: "ds",
	})
	var nf *TableNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected TableNotFoundError, got %T: %v", err, err)
	}
	// people still exists.
	res := exec(t, "SELECT COUNT(*) FROM people")
	if res.Rows[0][0] != int64(4) {
		t.Fatalf("people damaged: %v", res.Rows)
	}
}
