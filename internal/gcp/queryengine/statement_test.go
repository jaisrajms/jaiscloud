package queryengine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// memMutator is an in-memory Mutator used to exercise DDL/DML without a store.
type memMutator struct {
	tables   map[string]Table
	datasets map[string]bool
}

func newMemMutator() *memMutator {
	return &memMutator{tables: map[string]Table{}, datasets: map[string]bool{}}
}

func scope(project, dataset, table string) string {
	return project + "\x00" + dataset + "\x00" + table
}

func (m *memMutator) Table(_ context.Context, project, dataset, table string) (Table, error) {
	if t, ok := m.tables[scope(project, dataset, table)]; ok {
		return t, nil
	}
	return Table{}, ErrTableNotFound
}

func (m *memMutator) ListTables(_ context.Context, project, dataset string) ([]string, error) {
	prefix := project + "\x00" + dataset + "\x00"
	var names []string
	for k := range m.tables {
		if strings.HasPrefix(k, prefix) {
			names = append(names, k[len(prefix):])
		}
	}
	return names, nil
}

func (m *memMutator) CreateTable(_ context.Context, t Table) error {
	key := scope(t.Project, t.Dataset, t.Table)
	if _, ok := m.tables[key]; ok {
		return ErrTableExists
	}
	m.tables[key] = t
	return nil
}

func (m *memMutator) DropTable(_ context.Context, project, dataset, table string) error {
	key := scope(project, dataset, table)
	if _, ok := m.tables[key]; !ok {
		return ErrTableNotFound
	}
	delete(m.tables, key)
	return nil
}

func (m *memMutator) CreateDataset(_ context.Context, project, dataset string) error {
	key := project + "\x00" + dataset
	if m.datasets[key] {
		return ErrDatasetExists
	}
	m.datasets[key] = true
	return nil
}

func (m *memMutator) DropDataset(_ context.Context, project, dataset string) error {
	key := project + "\x00" + dataset
	if !m.datasets[key] {
		return ErrDatasetNotFound
	}
	delete(m.datasets, key)
	prefix := project + "\x00" + dataset + "\x00"
	for k := range m.tables {
		if strings.HasPrefix(k, prefix) {
			delete(m.tables, k)
		}
	}
	return nil
}

func (m *memMutator) ReplaceRows(_ context.Context, project, dataset, table string, rows []map[string]any) error {
	key := scope(project, dataset, table)
	t, ok := m.tables[key]
	if !ok {
		return ErrTableNotFound
	}
	t.Rows = rows
	m.tables[key] = t
	return nil
}

func seededMutator() *memMutator {
	m := newMemMutator()
	m.datasets["p\x00ds"] = true
	m.tables[scope("p", "ds", "people")] = Table{
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
		},
	}
	m.tables[scope("p", "ds", "orders")] = Table{
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
	}
	return m
}

func execStmt(t *testing.T, m *memMutator, q string) Result {
	t.Helper()
	res, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
	})
	if err != nil {
		t.Fatalf("ExecuteStatement(%q): %v", q, err)
	}
	return res
}

func TestExecuteDMLInsertSelect(t *testing.T) {
	m := seededMutator()
	res := execStmt(t, m, "INSERT INTO `p.ds.orders` (person_id, amount) SELECT id, 99.5 FROM `p.ds.people` WHERE id = 1")
	if res.StatementType != StatementInsert || res.NumDMLAffectedRows != 1 {
		t.Fatalf("insert result = %+v", res)
	}
	if got := len(m.tables[scope("p", "ds", "orders")].Rows); got != 4 {
		t.Fatalf("orders rows = %d, want 4", got)
	}
}

func TestExecuteDMLUpdateDeleteTruncate(t *testing.T) {
	m := seededMutator()
	res := execStmt(t, m, "UPDATE `p.ds.orders` SET amount = amount + 1 WHERE person_id = 1")
	if res.StatementType != StatementUpdate || res.NumDMLAffectedRows != 2 {
		t.Fatalf("update result = %+v", res)
	}
	res = execStmt(t, m, "DELETE FROM `p.ds.orders` WHERE person_id = 1")
	if res.StatementType != StatementDelete || res.NumDMLAffectedRows != 2 {
		t.Fatalf("delete result = %+v", res)
	}
	if got := len(m.tables[scope("p", "ds", "orders")].Rows); got != 1 {
		t.Fatalf("orders after delete = %d, want 1", got)
	}
	res = execStmt(t, m, "TRUNCATE TABLE `p.ds.orders`")
	if res.StatementType != StatementTruncateTable || res.NumDMLAffectedRows != 1 {
		t.Fatalf("truncate result = %+v", res)
	}
	if got := len(m.tables[scope("p", "ds", "orders")].Rows); got != 0 {
		t.Fatalf("orders after truncate = %d, want 0", got)
	}
}

func TestExecuteDMLInsertValues(t *testing.T) {
	m := seededMutator()
	res := execStmt(t, m, "INSERT INTO `p.ds.orders` (person_id, amount) VALUES (9, 1.5), (10, 2.5)")
	if res.NumDMLAffectedRows != 2 {
		t.Fatalf("insert values result = %+v", res)
	}
	rows := m.tables[scope("p", "ds", "orders")].Rows
	if len(rows) != 5 {
		t.Fatalf("orders rows = %d, want 5", len(rows))
	}
	if rows[3]["person_id"] != int64(9) || rows[4]["amount"] != 2.5 {
		t.Fatalf("inserted rows = %v", rows[3:])
	}
}

func TestExecuteDDLCreateTableSchema(t *testing.T) {
	m := seededMutator()
	res := execStmt(t, m, "CREATE TABLE `p.ds.newt` (a INT64 NOT NULL, b STRING)")
	if res.StatementType != StatementCreateTable {
		t.Fatalf("result = %+v", res)
	}
	tbl := m.tables[scope("p", "ds", "newt")]
	want := []Field{
		{Name: "a", Type: "INT64", Mode: "REQUIRED"},
		{Name: "b", Type: "STRING", Mode: "NULLABLE"},
	}
	if !reflect.DeepEqual(tbl.Fields, want) {
		t.Fatalf("fields = %+v, want %+v", tbl.Fields, want)
	}
}

func TestExecuteDDLCreateTableAsSelect(t *testing.T) {
	m := seededMutator()
	res := execStmt(t, m, "CREATE TABLE `p.ds.summary` AS SELECT age, COUNT(*) AS c FROM `p.ds.people` GROUP BY age")
	if res.StatementType != StatementCreateTableAsSelect {
		t.Fatalf("result = %+v", res)
	}
	tbl := m.tables[scope("p", "ds", "summary")]
	if len(tbl.Fields) != 2 || tbl.Fields[0].Type != "INT64" || tbl.Fields[1].Type != "INT64" {
		t.Fatalf("ctas fields = %+v", tbl.Fields)
	}
	if len(tbl.Rows) != 2 {
		t.Fatalf("ctas rows = %v", tbl.Rows)
	}
}

func TestExecuteDDLDropAndExists(t *testing.T) {
	m := seededMutator()
	if res := execStmt(t, m, "DROP TABLE `p.ds.orders`"); res.StatementType != StatementDropTable {
		t.Fatalf("drop result = %+v", res)
	}
	// IF EXISTS on a missing table succeeds; a bare DROP is TableNotFound.
	execStmt(t, m, "DROP TABLE IF EXISTS `p.ds.orders`")
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "DROP TABLE `p.ds.orders`", DefaultProject: "p", DefaultDataset: "ds",
	})
	var nf *TableNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("drop missing: expected TableNotFoundError, got %v", err)
	}

	// Re-creating an existing table is AlreadyExists, unless IF NOT EXISTS.
	execStmt(t, m, "CREATE TABLE `p.ds.t2` (a INT64)")
	execStmt(t, m, "CREATE TABLE IF NOT EXISTS `p.ds.t2` (a INT64)")
	_, err = ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "CREATE TABLE `p.ds.t2` (a INT64)", DefaultProject: "p", DefaultDataset: "ds",
	})
	if !errors.Is(err, ErrTableExists) {
		t.Fatalf("duplicate create: expected ErrTableExists, got %v", err)
	}
}

func TestExecuteDDLCreateOrReplace(t *testing.T) {
	m := seededMutator()
	execStmt(t, m, "CREATE OR REPLACE TABLE `p.ds.people` AS SELECT id FROM `p.ds.people` WHERE id = 1")
	tbl := m.tables[scope("p", "ds", "people")]
	if len(tbl.Rows) != 1 || tbl.Rows[0]["id"] != int64(1) {
		t.Fatalf("or replace rows = %v", tbl.Rows)
	}
}

func TestExecuteDDLSchema(t *testing.T) {
	m := seededMutator()
	if res := execStmt(t, m, "CREATE SCHEMA `p.reporting`"); res.StatementType != StatementCreateSchema {
		t.Fatalf("create schema = %+v", res)
	}
	// Non-empty dataset without CASCADE fails loud.
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "DROP SCHEMA `p.ds`", DefaultProject: "p", DefaultDataset: "ds",
	})
	var ue *UnsupportedError
	if !errors.As(err, &ue) {
		t.Fatalf("drop non-empty schema: expected UnsupportedError, got %v", err)
	}
	if res := execStmt(t, m, "DROP SCHEMA `p.ds` CASCADE"); res.StatementType != StatementDropSchema {
		t.Fatalf("drop schema cascade = %+v", res)
	}
	// IF EXISTS on a missing schema succeeds.
	execStmt(t, m, "DROP SCHEMA IF EXISTS `p.missing`")
}

func TestExecuteDryRunDoesNotMutate(t *testing.T) {
	m := seededMutator()
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "CREATE TABLE `p.ds.t3` (a INT64)", DefaultProject: "p", DefaultDataset: "ds", DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry run create: %v", err)
	}
	if _, ok := m.tables[scope("p", "ds", "t3")]; ok {
		t.Fatal("dry run created a table")
	}
	_, err = ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "INSERT INTO `p.ds.orders` (person_id) VALUES (7)", DefaultProject: "p", DefaultDataset: "ds", DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry run insert: %v", err)
	}
	if got := len(m.tables[scope("p", "ds", "orders")].Rows); got != 3 {
		t.Fatalf("dry run inserted: rows = %d", got)
	}
}

func TestExecuteStatementNilMutator(t *testing.T) {
	if _, err := ExecuteStatement(context.Background(), nil, Request{Query: "SELECT 1"}); err == nil {
		t.Fatal("expected nil mutator error")
	}
}

func TestExecuteSchemaBadReference(t *testing.T) {
	m := seededMutator()
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "CREATE SCHEMA `p.a.b`", DefaultProject: "p", DefaultDataset: "ds",
	})
	var ue *UnsupportedError
	if !errors.As(err, &ue) {
		t.Fatalf("expected UnsupportedError, got %v", err)
	}
}

func TestExecuteDMLMissingTable(t *testing.T) {
	m := seededMutator()
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "DELETE FROM `p.ds.missing`", DefaultProject: "p", DefaultDataset: "ds",
	})
	var nf *TableNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected TableNotFoundError, got %v", err)
	}
}

func TestExecuteSchemaEdgeCases(t *testing.T) {
	m := seededMutator()
	execStmt(t, m, "CREATE SCHEMA `p.ns`")
	// Duplicate create without IF NOT EXISTS is ErrDatasetExists.
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "CREATE SCHEMA `p.ns`", DefaultProject: "p", DefaultDataset: "ds",
	})
	if !errors.Is(err, ErrDatasetExists) {
		t.Fatalf("duplicate schema: expected ErrDatasetExists, got %v", err)
	}
	// IF NOT EXISTS is a no-op.
	execStmt(t, m, "CREATE SCHEMA IF NOT EXISTS `p.ns`")
	// DROP SCHEMA of a missing dataset is DatasetNotFound unless IF EXISTS.
	_, err = ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "DROP SCHEMA `p.gone`", DefaultProject: "p", DefaultDataset: "ds",
	})
	var dn *DatasetNotFoundError
	if !errors.As(err, &dn) {
		t.Fatalf("drop missing schema: expected DatasetNotFoundError, got %v", err)
	}
	execStmt(t, m, "DROP SCHEMA IF EXISTS `p.gone`")
}

func TestExecuteDDLColumnDefErrors(t *testing.T) {
	m := seededMutator()
	for _, q := range []string{
		"CREATE TABLE `p.ds.bad1` (a)",
		"CREATE TABLE `p.ds.bad2` (a RECORD)",
		"CREATE TABLE `p.ds.bad3` ()",
	} {
		_, err := ExecuteStatement(context.Background(), m, Request{
			Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
		})
		var ue *UnsupportedError
		if !errors.As(err, &ue) {
			t.Fatalf("%q: expected UnsupportedError, got %v", q, err)
		}
	}
}

func TestExecuteCreateTablePositionalColumns(t *testing.T) {
	m := seededMutator()
	res := execStmt(t, m, "CREATE TABLE `p.ds.pos` (a INT64, b INT64) AS SELECT id, age FROM `p.ds.people`")
	if res.StatementType != StatementCreateTableAsSelect {
		t.Fatalf("result = %+v", res)
	}
	tbl := m.tables[scope("p", "ds", "pos")]
	if len(tbl.Fields) != 2 || tbl.Fields[0].Name != "a" || tbl.Fields[1].Name != "b" {
		t.Fatalf("fields = %+v", tbl.Fields)
	}
	if len(tbl.Rows) == 0 || tbl.Rows[0]["a"] != int64(1) || tbl.Rows[0]["b"] != int64(30) {
		t.Fatalf("rows = %+v", tbl.Rows)
	}
	// A column list that does not match the AS query arity fails loud.
	_, err := ExecuteStatement(context.Background(), m, Request{
		Project: "p", Query: "CREATE TABLE `p.ds.bad` (a INT64) AS SELECT id, age FROM `p.ds.people`",
		DefaultProject: "p", DefaultDataset: "ds",
	})
	var ue *UnsupportedError
	if !errors.As(err, &ue) {
		t.Fatalf("expected UnsupportedError, got %v", err)
	}
}

func TestExecuteUnsupportedStatements(t *testing.T) {
	m := seededMutator()
	for _, q := range []string{
		"MERGE `p.ds.orders` T USING `p.ds.people` S ON T.person_id = S.id WHEN MATCHED THEN UPDATE SET amount = 0",
		"ALTER TABLE `p.ds.orders` ADD COLUMN x INT64",
		"CREATE VIEW `p.ds.v` AS SELECT 1",
		"CREATE TABLE `p.ds.t` (a ARRAY<INT64>)",
		"CREATE TABLE `p.ds.bare`",
		"INSERT `p.ds.orders` (person_id) VALUES (1)",
		"CREATE TABLE `p.ds.part` (a INT64) PARTITION BY a",
		"CREATE TABLE `p.ds.opt` (a INT64) OPTIONS(description='x')",
		"CREATE SCHEMA `p.eu` OPTIONS(location='EU')",
		"DROP TABLE `p.ds.people` PURGE",
	} {
		_, err := ExecuteStatement(context.Background(), m, Request{
			Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
		})
		if err == nil {
			t.Fatalf("expected %q to be rejected", q)
		}
	}
}
