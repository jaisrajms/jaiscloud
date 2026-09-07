package bigquery

import (
	"context"
	"math"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

func TestDatasetCRUD(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	// Missing → 404.
	if _, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "nope"})); err == nil {
		t.Fatalf("expected NotFound for missing dataset")
	}

	// Create.
	create := newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "sales"},
		"friendlyName":     "Sales",
		"location":         "US",
		"labels":           map[string]any{"env": "dev"},
	}})
	resp, err := p.CreateDataset(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.Data["kind"] != "bigquery#dataset" {
		t.Fatalf("unexpected kind: %v", resp.Data["kind"])
	}
	ref, _ := resp.Data["datasetReference"].(map[string]any)
	if ref["datasetId"] != "sales" || ref["projectId"] != "proj" {
		t.Fatalf("unexpected datasetReference: %v", ref)
	}
	if resp.Data["friendlyName"] != "Sales" {
		t.Fatalf("friendlyName lost: %v", resp.Data)
	}

	// Duplicate → 409.
	if _, err := p.CreateDataset(ctx, create); err == nil {
		t.Fatalf("expected AlreadyExists on duplicate create")
	}

	// Get.
	got, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "sales"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["id"] != "proj:sales" {
		t.Fatalf("unexpected id: %v", got.Data["id"])
	}

	// List.
	list, err := p.ListDatasets(ctx, newNR(map[string]any{}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	datasets, _ := list.Data["datasets"].([]any)
	if len(datasets) != 1 {
		t.Fatalf("expected 1 dataset, got %d", len(datasets))
	}

	// Patch.
	patched, err := p.UpdateDataset(ctx, newNR(map[string]any{
		"datasetId": "sales",
		"body":      map[string]any{"friendlyName": "Sales2", "description": "updated"},
	}))
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if patched.Data["friendlyName"] != "Sales2" || patched.Data["description"] != "updated" {
		t.Fatalf("patch not applied: %v", patched.Data)
	}

	// Delete + missing → 404.
	if _, err := p.DeleteDataset(ctx, newNR(map[string]any{"datasetId": "sales"})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "sales"})); err == nil {
		t.Fatalf("expected NotFound after delete")
	}
}

func TestTableAndRowRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "d"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create table with schema.
	createTable := newNR(map[string]any{
		"datasetId": "d",
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": "d", "tableId": "t"},
			"schema": map[string]any{
				"fields": []any{
					map[string]any{"name": "id", "type": "INTEGER"},
					map[string]any{"name": "name", "type": "STRING"},
				},
			},
		},
	})
	resp, err := p.CreateTable(ctx, createTable)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	ref, _ := resp.Data["tableReference"].(map[string]any)
	if ref["tableId"] != "t" {
		t.Fatalf("unexpected tableReference: %v", ref)
	}
	if resp.Data["type"] != "TABLE" {
		t.Fatalf("expected type TABLE, got %v", resp.Data["type"])
	}

	// Duplicate table → 409.
	if _, err := p.CreateTable(ctx, createTable); err == nil {
		t.Fatalf("expected AlreadyExists on duplicate table")
	}

	// insertAll two rows.
	insert := newNR(map[string]any{
		"datasetId": "d",
		"tableId":   "t",
		"body": map[string]any{
			"rows": []any{
				map[string]any{"insertId": "1", "json": map[string]any{"id": 1, "name": "alice"}},
				map[string]any{"insertId": "2", "json": map[string]any{"id": 2, "name": "bob"}},
			},
		},
	})
	insResp, err := p.InsertAll(ctx, insert)
	if err != nil {
		t.Fatalf("insertAll: %v", err)
	}
	errors_, _ := insResp.Data["insertErrors"].([]any)
	if len(errors_) != 0 {
		t.Fatalf("expected empty insertErrors, got %v", errors_)
	}

	// List rows → round-trip in schema order.
	listRows, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t"}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	rows, _ := listRows.Data["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	row0, _ := rows[0].(map[string]any)
	cells0, _ := row0["f"].([]any)
	cell0, _ := cells0[0].(map[string]any)
	cell1, _ := cells0[1].(map[string]any)
	if cell0["v"] != float64(1) || cell1["v"] != "alice" {
		t.Fatalf("row 0 cells not schema-ordered: %v", cells0)
	}
	if listRows.Data["totalRows"] != "2" {
		t.Fatalf("expected totalRows 2, got %v", listRows.Data["totalRows"])
	}

	// numRows reflected on get.
	got, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t"}))
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if got.Data["numRows"] != "2" {
		t.Fatalf("expected numRows 2, got %v", got.Data["numRows"])
	}

	// Missing table → 404.
	if _, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "nope"})); err == nil {
		t.Fatalf("expected NotFound for missing table")
	}
}

func TestJobAndQueryEmptyResults(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	// jobs.insert with explicit jobId.
	insert := newNR(map[string]any{"body": map[string]any{
		"jobReference": map[string]any{"projectId": "proj", "jobId": "j1"},
		"configuration": map[string]any{
			"query": map[string]any{"query": "SELECT 1", "useLegacySql": false},
		},
	}})
	resp, err := p.InsertJob(ctx, insert)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	status, _ := resp.Data["status"].(map[string]any)
	if status["state"] != "DONE" {
		t.Fatalf("expected DONE, got %v", status["state"])
	}
	ref, _ := resp.Data["jobReference"].(map[string]any)
	if ref["jobId"] != "j1" {
		t.Fatalf("unexpected jobReference: %v", ref)
	}

	// Get job.
	got, err := p.GetJob(ctx, newNR(map[string]any{"jobId": "j1"}))
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Data["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("get job state: %v", got.Data)
	}

	// jobs.query → empty result shape.
	q, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{
		"query":        "SELECT * FROM t",
		"useLegacySql": false,
	}}))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if q.Data["jobComplete"] != true {
		t.Fatalf("expected jobComplete=true, got %v", q.Data["jobComplete"])
	}
	if q.Data["totalRows"] != "0" {
		t.Fatalf("expected totalRows 0, got %v", q.Data["totalRows"])
	}
	qref, _ := q.Data["jobReference"].(map[string]any)
	jobID, _ := qref["jobId"].(string)
	if jobID == "" {
		t.Fatalf("expected generated jobId in query response")
	}
	rows, _ := q.Data["rows"].([]any)
	if len(rows) != 0 {
		t.Fatalf("expected empty rows, got %v", rows)
	}

	// The query job is stored: getJob + getQueryResults work.
	if _, err := p.GetJob(ctx, newNR(map[string]any{"jobId": jobID})); err != nil {
		t.Fatalf("get query job: %v", err)
	}
	qr, err := p.GetQueryResults(ctx, newNR(map[string]any{"jobId": jobID}))
	if err != nil {
		t.Fatalf("getQueryResults: %v", err)
	}
	if qr.Data["jobComplete"] != true {
		t.Fatalf("getQueryResults jobComplete: %v", qr.Data)
	}

	// List jobs.
	list, err := p.ListJobs(ctx, newNR(map[string]any{}))
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	jobs, _ := list.Data["jobs"].([]any)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}

	// Cancel + delete.
	cancel, err := p.CancelJob(ctx, newNR(map[string]any{"jobId": "j1"}))
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancel.Data["kind"] != "bigquery#jobCancelResponse" {
		t.Fatalf("unexpected cancel kind: %v", cancel.Data)
	}
	if _, err := p.DeleteJob(ctx, newNR(map[string]any{"jobId": "j1"})); err != nil {
		t.Fatalf("delete job: %v", err)
	}
	if _, err := p.GetJob(ctx, newNR(map[string]any{"jobId": "j1"})); err == nil {
		t.Fatalf("expected NotFound after job delete")
	}
}

// TestQuery_RejectsUnencodableConfig verifies that Query never reports
// jobComplete=true without actually storing the job. A json.Marshal failure
// while building the stored job config used to be silently swallowed (the
// store.CreateJob call sat inside "if data, err := json.Marshal(...); err ==
// nil { ... }", so a marshal failure just skipped job creation entirely) while
// the handler still returned a success response — a later GetJob/
// GetQueryResults for that jobId would then 404 despite Query having reported
// success. A real request body can't itself contain a value json.Marshal
// rejects (it's always already-decoded JSON), so this forces the failure
// directly with a math.Inf value to exercise the handler's own error path.
func TestQuery_RejectsUnencodableConfig(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	_, err := p.Query(ctx, newNR(map[string]any{"body": map[string]any{
		"query":        "SELECT * FROM t",
		"useLegacySql": false,
		"unencodable":  math.Inf(1),
	}}))
	if err == nil {
		t.Fatal("expected an error for an unencodable job configuration, got nil")
	}
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "Internal" || perr.HTTPStatus != 500 {
		t.Fatalf("expected Internal/500, got %v", err)
	}

	// No job should have been created.
	list, err := p.ListJobs(ctx, newNR(map[string]any{}))
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if jobs, _ := list.Data["jobs"].([]any); len(jobs) != 0 {
		t.Fatalf("expected no jobs stored, got %d: %+v", len(jobs), jobs)
	}
}

func TestListPagination(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	for _, id := range []string{"a", "b", "c", "d"} {
		if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
			"datasetReference": map[string]any{"datasetId": id},
		}})); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	// Page size 2.
	page1, err := p.ListDatasets(ctx, newNR(map[string]any{"maxResults": "2"}))
	if err != nil {
		t.Fatalf("list page1: %v", err)
	}
	datasets1, _ := page1.Data["datasets"].([]any)
	if len(datasets1) != 2 {
		t.Fatalf("expected 2 datasets on page1, got %d", len(datasets1))
	}
	token, _ := page1.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatalf("expected nextPageToken")
	}

	page2, err := p.ListDatasets(ctx, newNR(map[string]any{"maxResults": "2", "pageToken": token}))
	if err != nil {
		t.Fatalf("list page2: %v", err)
	}
	datasets2, _ := page2.Data["datasets"].([]any)
	if len(datasets2) != 2 {
		t.Fatalf("expected 2 datasets on page2, got %d", len(datasets2))
	}
	if _, has := page2.Data["nextPageToken"]; has {
		t.Fatalf("expected no nextPageToken on final page")
	}
}

func TestGetServiceAccount(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	resp, err := p.GetServiceAccount(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("getServiceAccount: %v", err)
	}
	if resp.Data["kind"] != "bigquery#getServiceAccountResponse" {
		t.Fatalf("unexpected kind: %v", resp.Data["kind"])
	}
	email, _ := resp.Data["email"].(string)
	if email == "" {
		t.Fatalf("expected email")
	}
}
