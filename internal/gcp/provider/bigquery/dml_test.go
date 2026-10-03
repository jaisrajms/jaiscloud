package bigquery

import (
	"context"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

func seedDMLProvider(t *testing.T) *Provider {
	t.Helper()
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "sales"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": "sales",
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": "sales", "tableId": "orders"},
			"schema": map[string]any{"fields": []any{
				map[string]any{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				map[string]any{"name": "amount", "type": "FLOAT64"},
			}},
		},
	})); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return p
}

func queryOK(t *testing.T, p *Provider, query string) map[string]any {
	t.Helper()
	resp, err := p.Query(context.Background(), newNR(map[string]any{"body": map[string]any{"query": query}}))
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return resp.Data
}

func selectInt(t *testing.T, p *Provider, query string) string {
	t.Helper()
	data := queryOK(t, p, query)
	rows, _ := data["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("query %q rows = %v", query, rows)
	}
	first, _ := rows[0].(map[string]any)
	cells, _ := first["f"].([]any)
	cell, _ := cells[0].(map[string]any)
	v, _ := cell["v"].(string)
	return v
}

func TestQueryDMLRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)

	ins := queryOK(t, p, "INSERT INTO `proj.sales.orders` (id, amount) VALUES (1, 10.5), (2, 4.25)")
	if ins["statementType"] != "INSERT" || ins["numDmlAffectedRows"] != "2" {
		t.Fatalf("insert response = %v", ins)
	}
	if _, ok := ins["schema"]; ok {
		t.Fatalf("DML response must not carry a schema: %v", ins)
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.orders`"); got != "2" {
		t.Fatalf("count after insert = %s", got)
	}

	upd := queryOK(t, p, "UPDATE `proj.sales.orders` SET amount = amount + 1 WHERE id = 2")
	if upd["statementType"] != "UPDATE" || upd["numDmlAffectedRows"] != "1" {
		t.Fatalf("update response = %v", upd)
	}

	del := queryOK(t, p, "DELETE FROM `proj.sales.orders` WHERE id = 1")
	if del["statementType"] != "DELETE" || del["numDmlAffectedRows"] != "1" {
		t.Fatalf("delete response = %v", del)
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.orders`"); got != "1" {
		t.Fatalf("count after delete = %s", got)
	}

	// The mutated rows survive a non-query read (tabledata.list).
	list, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "sales", "tableId": "orders"}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	if rows, _ := list.Data["rows"].([]any); len(rows) != 1 {
		t.Fatalf("tabledata rows = %v", list.Data["rows"])
	}
}

func TestGetQueryResultsDoesNotReapplyDML(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)

	ins := queryOK(t, p, "INSERT INTO `proj.sales.orders` (id, amount) VALUES (1, 10.5)")
	ref, _ := ins["jobReference"].(map[string]any)
	jobID, _ := ref["jobId"].(string)
	if jobID == "" {
		t.Fatalf("insert job reference = %v", ins["jobReference"])
	}

	resp, err := p.GetQueryResults(ctx, newNR(map[string]any{"jobId": jobID}))
	if err != nil {
		t.Fatalf("getQueryResults: %v", err)
	}
	if resp.Data["numDmlAffectedRows"] != "1" {
		t.Fatalf("getQueryResults response = %v", resp.Data)
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.orders`"); got != "1" {
		t.Fatalf("getQueryResults re-applied the INSERT: count = %s", got)
	}
}

// TestJobsInsertDMLDoesNotDoubleApply is the jobs.insert counterpart: the query
// job executes once at insert time and getQueryResults must not apply it again.
func TestJobsInsertDMLDoesNotDoubleApply(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)

	_, err := p.InsertJob(ctx, newNR(map[string]any{"body": map[string]any{
		"jobReference": map[string]any{"projectId": "proj", "jobId": "dm1"},
		"configuration": map[string]any{"query": map[string]any{
			"query": "INSERT INTO `proj.sales.orders` (id, amount) VALUES (1, 10.5)", "useLegacySql": false,
		}},
	}}))
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.orders`"); got != "1" {
		t.Fatalf("jobs.insert did not execute the DML: count = %s", got)
	}
	for i := 0; i < 3; i++ {
		if _, err := p.GetQueryResults(ctx, newNR(map[string]any{"jobId": "dm1"})); err != nil {
			t.Fatalf("getQueryResults %d: %v", i, err)
		}
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.orders`"); got != "1" {
		t.Fatalf("getQueryResults re-applied the jobs.insert DML: count = %s", got)
	}
}

func TestQueryDDLRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)

	create := queryOK(t, p, "CREATE TABLE `proj.sales.archive` (id INT64 NOT NULL, note STRING)")
	if create["statementType"] != "CREATE_TABLE" {
		t.Fatalf("create response = %v", create)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "sales", "tableId": "archive"}))
	if err != nil {
		t.Fatalf("get created table: %v", err)
	}
	schema, _ := tbl.Data["schema"].(map[string]any)
	fields, _ := schema["fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("created schema = %v", schema)
	}
	f0, _ := fields[0].(map[string]any)
	if f0["name"] != "id" || f0["type"] != "INTEGER" || f0["mode"] != "REQUIRED" {
		t.Fatalf("created field = %v", f0)
	}

	drop := queryOK(t, p, "DROP TABLE `proj.sales.archive`")
	if drop["statementType"] != "DROP_TABLE" {
		t.Fatalf("drop response = %v", drop)
	}
	if _, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "sales", "tableId": "archive"})); err == nil {
		t.Fatal("dropped table still present")
	}

	if res := queryOK(t, p, "CREATE SCHEMA `proj.reporting`"); res["statementType"] != "CREATE_SCHEMA" {
		t.Fatalf("create schema = %v", res)
	}
	if _, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "reporting"})); err != nil {
		t.Fatalf("get created dataset: %v", err)
	}
	if res := queryOK(t, p, "DROP SCHEMA `proj.reporting` CASCADE"); res["statementType"] != "DROP_SCHEMA" {
		t.Fatalf("drop schema = %v", res)
	}
	if _, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "reporting"})); err == nil {
		t.Fatal("dropped dataset still present")
	}
}

func TestQueryCreateTableAsSelect(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)
	queryOK(t, p, "INSERT INTO `proj.sales.orders` (id, amount) VALUES (1, 2.5), (2, 3.5)")

	res := queryOK(t, p, "CREATE TABLE `proj.sales.copy` AS SELECT id, amount * 2 AS doubled FROM `proj.sales.orders`")
	if res["statementType"] != "CREATE_TABLE_AS_SELECT" {
		t.Fatalf("ctas response = %v", res)
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.copy`"); got != "2" {
		t.Fatalf("ctas row count = %s", got)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "sales", "tableId": "copy"}))
	if err != nil {
		t.Fatalf("get ctas table: %v", err)
	}
	schema, _ := tbl.Data["schema"].(map[string]any)
	fields, _ := schema["fields"].([]any)
	f1, _ := fields[1].(map[string]any)
	if f1["name"] != "doubled" || f1["type"] != "FLOAT" {
		t.Fatalf("ctas field = %v", f1)
	}
}

func TestQueryDryRunNoMutation(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)

	if res := queryOK(t, p, "CREATE TABLE `proj.sales.dry` (a INT64)"); res == nil {
		t.Fatal("nil response")
	}
	// Dry run must not create.
	if _, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{
		"query": "CREATE TABLE `proj.sales.dryrun` (a INT64)", "dryRun": true,
	}})); err != nil {
		t.Fatalf("dry run create: %v", err)
	}
	if _, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "sales", "tableId": "dryrun"})); err == nil {
		t.Fatal("dry run created a table")
	}

	// Dry run must not insert.
	if _, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{
		"query": "INSERT INTO `proj.sales.orders` (id) VALUES (1)", "dryRun": true,
	}})); err != nil {
		t.Fatalf("dry run insert: %v", err)
	}
	if got := selectInt(t, p, "SELECT COUNT(*) AS c FROM `proj.sales.orders`"); got != "0" {
		t.Fatalf("dry run inserted: count = %s", got)
	}
}

func TestQueryDMLNotFound(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)
	for _, q := range []string{
		"INSERT INTO `proj.sales.missing` (id) VALUES (1)",
		"DELETE FROM `proj.sales.missing`",
		"DROP TABLE `proj.sales.missing`",
	} {
		_, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{"query": q}}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != 404 {
			t.Fatalf("%q: expected NotFound/404, got %v", q, err)
		}
	}
}

func TestQueryCreateDuplicateIsConflict(t *testing.T) {
	ctx := context.Background()
	p := seedDMLProvider(t)
	_, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{
		"query": "CREATE TABLE `proj.sales.orders` (id INT64)",
	}}))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 409 {
		t.Fatalf("expected AlreadyExists/409, got %v", err)
	}

	// IF NOT EXISTS is a no-op.
	if _, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{
		"query": "CREATE TABLE IF NOT EXISTS `proj.sales.orders` (id INT64)",
	}})); err != nil {
		t.Fatalf("create if not exists: %v", err)
	}
}
