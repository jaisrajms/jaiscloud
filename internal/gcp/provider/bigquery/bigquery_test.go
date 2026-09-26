package bigquery

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/identity"
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

// TestDeleteStatusCodes pins the delete contract: real BigQuery returns
// 204 No Content with an empty body, not 200 with {}. The codec/framework
// writes no body for a 204, so the provider response carries an empty map.
func TestDeleteStatusCodes(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "d"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": "d",
		"body": map[string]any{
			"tableReference": map[string]any{"datasetId": "d", "tableId": "t"},
		},
	})); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tblResp, err := p.DeleteTable(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t"}))
	if err != nil {
		t.Fatalf("delete table: %v", err)
	}
	if tblResp.HTTPStatus != 204 {
		t.Errorf("table delete status = %d, want 204", tblResp.HTTPStatus)
	}
	if len(tblResp.Data) != 0 {
		t.Errorf("table delete body = %v, want empty", tblResp.Data)
	}

	dsResp, err := p.DeleteDataset(ctx, newNR(map[string]any{"datasetId": "d"}))
	if err != nil {
		t.Fatalf("delete dataset: %v", err)
	}
	if dsResp.HTTPStatus != 204 {
		t.Errorf("dataset delete status = %d, want 204", dsResp.HTTPStatus)
	}
	if len(dsResp.Data) != 0 {
		t.Errorf("dataset delete body = %v, want empty", dsResp.Data)
	}
}

// delayedGetDatasetStore wraps a bqstore.Store, delaying every GetDataset
// call to widen a TOCTOU race window in tests. It only affects the pre-fix
// code path (UpdateDataset calling a standalone GetDataset);
// UpdateDatasetAtomic does its own internal locked read and never reaches
// this override.
type delayedGetDatasetStore struct {
	bqstore.Store
	delay time.Duration
}

func (d *delayedGetDatasetStore) GetDataset(ctx context.Context, projectID, datasetID string) (bqstore.Dataset, error) {
	ds, err := d.Store.GetDataset(ctx, projectID, datasetID)
	time.Sleep(d.delay)
	return ds, err
}

// TestUpdateDatasetConcurrentDisjointFieldsNoLostUpdate proves UpdateDataset's
// get-merge-write cycle is atomic with respect to other concurrent PATCH
// requests. Without atomicity, a PATCH touching only "friendlyName" reads a
// stale full copy of the dataset (taken before a concurrent "description"-only
// PATCH committed), then writes that stale copy back — silently reverting the
// description change even though the friendlyName PATCH never touched it. 25
// goroutines each PATCH only "friendlyName" and 25 PATCH only "description";
// a delayed-Get store wrapper widens the TOCTOU window reliably (the real
// in-memory round trip otherwise completes in nanoseconds).
func TestUpdateDatasetConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	p := New(&delayedGetDatasetStore{Store: bqstore.NewMemoryStore(), delay: 5 * time.Millisecond})

	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "ds1"},
		"friendlyName":     "orig-name",
		"description":      "orig-desc",
	}})); err != nil {
		t.Fatalf("create: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.UpdateDataset(ctx, newNR(map[string]any{
				"datasetId": "ds1",
				"body":      map[string]any{"friendlyName": fmt.Sprintf("vName%d", i)},
			}))
		}(i)
		go func(i int) {
			defer wg.Done()
			_, errs[perField+i] = p.UpdateDataset(ctx, newNR(map[string]any{
				"datasetId": "ds1",
				"body":      map[string]any{"description": fmt.Sprintf("vDesc%d", i)},
			}))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "ds1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	friendlyName, _ := got.Data["friendlyName"].(string)
	description, _ := got.Data["description"].(string)
	if friendlyName == "orig-name" || !strings.HasPrefix(friendlyName, "vName") {
		t.Errorf("friendlyName reverted to a stale value instead of one of the 25 concurrent writers': got %q", friendlyName)
	}
	if description == "orig-desc" || !strings.HasPrefix(description, "vDesc") {
		t.Errorf("description reverted to a stale value instead of one of the 25 concurrent writers': got %q", description)
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
	// Primitive cells are strings on the wire (the SDK parses them that way).
	if cell0["v"] != "1" || cell1["v"] != "alice" {
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

func TestListRowsStartIndex(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "d"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": "d",
		"body": map[string]any{
			"tableReference": map[string]any{"tableId": "t"},
			"schema": map[string]any{
				"fields": []any{map[string]any{"name": "id", "type": "INTEGER"}},
			},
		},
	})); err != nil {
		t.Fatalf("create table: %v", err)
	}
	rawRows := make([]any, 0, 5)
	for i := 0; i < 5; i++ {
		rawRows = append(rawRows, map[string]any{"json": map[string]any{"id": i}})
	}
	if _, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d",
		"tableId":   "t",
		"body":      map[string]any{"rows": rawRows},
	})); err != nil {
		t.Fatalf("insertAll: %v", err)
	}

	firstID := func(row any) any {
		m, _ := row.(map[string]any)
		cells, _ := m["f"].([]any)
		if len(cells) == 0 {
			return nil
		}
		cell, _ := cells[0].(map[string]any)
		return cell["v"]
	}

	// startIndex=2 returns rows from index 2; startIndex is a request-only
	// parameter and is not echoed in the response.
	list, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t", "startIndex": "2"}))
	if err != nil {
		t.Fatalf("list rows startIndex=2: %v", err)
	}
	if _, ok := list.Data["startIndex"]; ok {
		t.Errorf("startIndex must not be echoed, got %v", list.Data["startIndex"])
	}
	if list.Data["totalRows"] != "5" {
		t.Errorf("totalRows = %v, want 5", list.Data["totalRows"])
	}
	rows, _ := list.Data["rows"].([]any)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows from startIndex 2, got %d", len(rows))
	}
	if got := firstID(rows[0]); got != "2" {
		t.Errorf("first row id = %v, want \"2\"", got)
	}

	// Out-of-range startIndex yields an empty page, not an error.
	empty, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t", "startIndex": "99"}))
	if err != nil {
		t.Fatalf("list rows with out-of-range startIndex: %v", err)
	}
	if rows, _ := empty.Data["rows"].([]any); len(rows) != 0 {
		t.Errorf("expected no rows for out-of-range startIndex, got %d", len(rows))
	}
	if _, ok := empty.Data["startIndex"]; ok {
		t.Errorf("startIndex must not be echoed, got %v", empty.Data["startIndex"])
	}

	// Invalid startIndex → 400 InvalidArgument.
	_, err = p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t", "startIndex": "abc"}))
	if err == nil {
		t.Fatalf("expected InvalidArgument for non-numeric startIndex")
	}
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("expected InvalidArgument/400, got %v", err)
	}
}

func TestDeferredResourceOps_Unimplemented(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	routes := p.Routes()
	for _, action := range []string{"BigQuery.Routines", "BigQuery.Models", "BigQuery.RowAccessPolicies"} {
		h, ok := routes[action]
		if !ok {
			t.Fatalf("missing route %q", action)
		}
		_, err := h(ctx, newNR(nil))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "Unimplemented" || perr.HTTPStatus != 501 {
			t.Errorf("%s: expected Unimplemented/501, got %v", action, err)
		}
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

// --- tabledata.insertAll semantics ---

func createDataset(t *testing.T, p *Provider, datasetID string) {
	t.Helper()
	if _, err := p.CreateDataset(context.Background(), newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": datasetID},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
}

func createTable(t *testing.T, p *Provider, datasetID, tableID string, schema any) {
	t.Helper()
	body := map[string]any{"tableReference": map[string]any{"tableId": tableID}}
	if schema != nil {
		body["schema"] = schema
	}
	if _, err := p.CreateTable(context.Background(), newNR(map[string]any{
		"datasetId": datasetID,
		"body":      body,
	})); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func schemaOf(fields ...map[string]any) map[string]any {
	arr := make([]any, 0, len(fields))
	for _, f := range fields {
		arr = append(arr, f)
	}
	return map[string]any{"fields": arr}
}

func insertErrorsOf(t *testing.T, resp *model.ProviderResponse) []any {
	t.Helper()
	errs, _ := resp.Data["insertErrors"].([]any)
	return errs
}

// soleRowError asserts exactly one row-error entry and returns its request
// index and first error object.
func soleRowError(t *testing.T, resp *model.ProviderResponse) (int, map[string]any) {
	t.Helper()
	errs := insertErrorsOf(t, resp)
	if len(errs) != 1 {
		t.Fatalf("expected exactly 1 row error, got %v", errs)
	}
	re, _ := errs[0].(map[string]any)
	idx, _ := re["index"].(int)
	list, _ := re["errors"].([]any)
	if len(list) == 0 {
		t.Fatalf("row error entry has no errors: %v", re)
	}
	e0, _ := list[0].(map[string]any)
	return idx, e0
}

func rowDataCount(t *testing.T, p *Provider, datasetID, tableID string) int {
	t.Helper()
	resp, err := p.ListRows(context.Background(), newNR(map[string]any{"datasetId": datasetID, "tableId": tableID}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	rows, _ := resp.Data["rows"].([]any)
	return len(rows)
}

// TestInsertAllInsertIdDedup verifies a repeated insertId is skipped (not
// double-inserted) and reported as reason "duplicate" at its request index.
func TestInsertAllInsertIdDedup(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	createDataset(t, p, "d")
	createTable(t, p, "d", "t", schemaOf(map[string]any{"name": "id", "type": "INTEGER"}))

	insert := func() *model.ProviderResponse {
		resp, err := p.InsertAll(ctx, newNR(map[string]any{
			"datasetId": "d", "tableId": "t",
			"body": map[string]any{"rows": []any{
				map[string]any{"insertId": "dup-1", "json": map[string]any{"id": 1}},
			}},
		}))
		if err != nil {
			t.Fatalf("insertAll: %v", err)
		}
		return resp
	}

	if errs := insertErrorsOf(t, insert()); len(errs) != 0 {
		t.Fatalf("first insert should have no errors, got %v", errs)
	}
	idx, e := soleRowError(t, insert())
	if idx != 0 || e["reason"] != "duplicate" {
		t.Fatalf("expected duplicate at index 0, got index=%d err=%v", idx, e)
	}
	if got := rowDataCount(t, p, "d", "t"); got != 1 {
		t.Fatalf("duplicate insertId must not double-insert: got %d rows", got)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t"}))
	if err != nil || tbl.Data["numRows"] != "1" {
		t.Fatalf("expected numRows 1, got %v (%v)", tbl.Data["numRows"], err)
	}
}

// TestInsertAllUnknownField verifies an unknown field with
// ignoreUnknownValues=false is reported per-row at HTTP 200 (never a top-level
// failure), while true accepts the row. Both default skipInvalidRows=false, so
// the rejected batch inserts nothing.
func TestInsertAllUnknownField(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	createDataset(t, p, "d")
	createTable(t, p, "d", "t", schemaOf(map[string]any{"name": "id", "type": "INTEGER"}))

	insert := func(ignore bool, insertID string) (*model.ProviderResponse, error) {
		return p.InsertAll(ctx, newNR(map[string]any{
			"datasetId": "d", "tableId": "t",
			"body": map[string]any{
				"ignoreUnknownValues": ignore,
				"rows": []any{
					map[string]any{"insertId": insertID, "json": map[string]any{"id": 1, "extra": "x"}},
				},
			},
		}))
	}

	resp, err := insert(false, "u1")
	if err != nil {
		t.Fatalf("insertAll must not fail the request on an unknown field: %v", err)
	}
	idx, e := soleRowError(t, resp)
	if idx != 0 || e["reason"] != "invalid" || e["location"] != "extra" {
		t.Fatalf("expected invalid unknown field at index 0, got index=%d err=%v", idx, e)
	}
	if got := rowDataCount(t, p, "d", "t"); got != 0 {
		t.Fatalf("skipInvalidRows=false must insert nothing, got %d rows", got)
	}

	resp, err = insert(true, "u1")
	if err != nil {
		t.Fatalf("insertAll with ignoreUnknownValues=true: %v", err)
	}
	if errs := insertErrorsOf(t, resp); len(errs) != 0 {
		t.Fatalf("expected no insertErrors, got %v", errs)
	}
	if got := rowDataCount(t, p, "d", "t"); got != 1 {
		t.Fatalf("expected 1 stored row, got %d", got)
	}
}

// TestInsertAllRequiredFieldAndSkipInvalidRows covers both skipInvalidRows
// modes at HTTP 200: false reports the invalid row plus a "stopped" entry for
// the otherwise-valid one and writes nothing; true reports only the invalid row
// while inserting the valid ones.
func TestInsertAllRequiredFieldAndSkipInvalidRows(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	createDataset(t, p, "d")
	createTable(t, p, "d", "t", schemaOf(
		map[string]any{"name": "id", "type": "STRING", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	))

	resp, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d", "tableId": "t",
		"body": map[string]any{"rows": []any{
			map[string]any{"insertId": "r1", "json": map[string]any{"id": "a"}},
			map[string]any{"insertId": "r2", "json": map[string]any{"name": "missing-id"}},
		}},
	}))
	if err != nil {
		t.Fatalf("insertAll must not fail the request when a required field is missing: %v", err)
	}
	reasonAt := func(resp *model.ProviderResponse) map[int]map[string]any {
		first := map[int]map[string]any{}
		for _, e := range insertErrorsOf(t, resp) {
			m, _ := e.(map[string]any)
			idx, _ := m["index"].(int)
			list, _ := m["errors"].([]any)
			if len(list) == 0 {
				continue
			}
			first[idx], _ = list[0].(map[string]any)
		}
		return first
	}
	byIndex := reasonAt(resp)
	if len(byIndex) != 2 {
		t.Fatalf("expected invalid + stopped entries, got %v", byIndex)
	}
	if byIndex[0]["reason"] != "stopped" {
		t.Fatalf("valid row must be reported stopped, got %v", byIndex[0])
	}
	if byIndex[1]["reason"] != "invalid" || byIndex[1]["location"] != "id" {
		t.Fatalf("invalid row must be reported invalid, got %v", byIndex[1])
	}
	if got := rowDataCount(t, p, "d", "t"); got != 0 {
		t.Fatalf("failed request must write nothing, got %d rows", got)
	}

	resp, err = p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d", "tableId": "t",
		"body": map[string]any{
			"skipInvalidRows": true,
			"rows": []any{
				map[string]any{"insertId": "r1", "json": map[string]any{"id": "a"}},
				map[string]any{"insertId": "r2", "json": map[string]any{"name": "missing-id"}},
				map[string]any{"insertId": "r3", "json": map[string]any{"id": "c"}},
			},
		},
	}))
	if err != nil {
		t.Fatalf("insertAll: %v", err)
	}
	idx, e := soleRowError(t, resp)
	if idx != 1 || e["reason"] != "invalid" || e["location"] != "id" {
		t.Fatalf("unexpected row error: index=%d err=%v", idx, e)
	}
	if got := rowDataCount(t, p, "d", "t"); got != 2 {
		t.Fatalf("valid rows must be inserted alongside the invalid one: got %d rows", got)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t"}))
	if err != nil || tbl.Data["numRows"] != "2" {
		t.Fatalf("expected numRows 2, got %v (%v)", tbl.Data["numRows"], err)
	}
}

// TestInsertAllNoSchemaAcceptsAnyRow locks in that a table without a schema
// performs no validation (the pre-existing behavior).
func TestInsertAllNoSchemaAcceptsAnyRow(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	createDataset(t, p, "d")
	createTable(t, p, "d", "t", nil)

	resp, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d", "tableId": "t",
		"body": map[string]any{"rows": []any{
			map[string]any{"insertId": "n1", "json": map[string]any{"whatever": 1, "extra": true}},
		}},
	}))
	if err != nil {
		t.Fatalf("insertAll: %v", err)
	}
	if errs := insertErrorsOf(t, resp); len(errs) != 0 {
		t.Fatalf("schema-less table should accept any row, got %v", errs)
	}
	if got := rowDataCount(t, p, "d", "t"); got != 1 {
		t.Fatalf("expected 1 stored row, got %d", got)
	}
}

// TestDatasetDefaultAccess verifies that a dataset created without an explicit
// access list renders the ACL real GCP applies by default, and that an explicit
// access list is preserved.
func TestDatasetDefaultAccess(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	create := newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "acl"},
	}})
	resp, err := p.CreateDataset(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	assertAccess := func(where string, data map[string]any) {
		t.Helper()
		access, ok := data["access"].([]any)
		if !ok || len(access) != 4 {
			t.Fatalf("%s: access = %#v, want 4 entries", where, data["access"])
		}
		want := []map[string]any{
			{"role": "WRITER", "specialGroup": "projectWriters"},
			{"role": "OWNER", "specialGroup": "projectOwners"},
			{"role": "OWNER", "userByEmail": identity.DefaultServiceAccount},
			{"role": "READER", "specialGroup": "projectReaders"},
		}
		for i, w := range want {
			got, _ := access[i].(map[string]any)
			for k, v := range w {
				if got[k] != v {
					t.Errorf("%s: access[%d][%s] = %v, want %v", where, i, k, got[k], v)
				}
			}
		}
	}
	assertAccess("create", resp.Data)

	got, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "acl"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertAccess("get", got.Data)

	// datasets.list returns the summary subset and must omit access.
	list, err := p.ListDatasets(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["datasets"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 dataset, got %d", len(items))
	}
	first, _ := items[0].(map[string]any)
	if _, ok := first["access"]; ok {
		t.Fatalf("datasets.list must omit access, got %#v", first["access"])
	}

	// An explicit access list must be preserved verbatim.
	explicit := newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "acl2"},
		"access":           []any{map[string]any{"role": "READER", "userByEmail": "reader@example.com"}},
	}})
	if _, err := p.CreateDataset(ctx, explicit); err != nil {
		t.Fatalf("create explicit: %v", err)
	}
	got2, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "acl2"}))
	if err != nil {
		t.Fatalf("get explicit: %v", err)
	}
	access2, _ := got2.Data["access"].([]any)
	if len(access2) != 1 {
		t.Fatalf("explicit access overwritten: %#v", got2.Data["access"])
	}
}

// TestDatasetMaxTimeTravelHours verifies datasets.get reports the default
// 7-day maxTimeTravelHours while datasets.insert omits it (matching real GCP).
func TestDatasetMaxTimeTravelHours(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	created, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "tt"},
	}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, ok := created.Data["maxTimeTravelHours"]; ok {
		t.Fatalf("create must omit maxTimeTravelHours, got %#v", created.Data["maxTimeTravelHours"])
	}

	got, err := p.GetDataset(ctx, newNR(map[string]any{"datasetId": "tt"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["maxTimeTravelHours"] != "168" {
		t.Fatalf("maxTimeTravelHours = %#v, want 168", got.Data["maxTimeTravelHours"])
	}
}

// TestListSummariesOmitFullResourceFields pins the Discovery summary subsets:
// datasets.list and tables.list must carry only the DatasetList/TableList item
// fields and must not leak full-resource fields.
func TestListSummariesOmitFullResourceFields(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "d"},
		"friendlyName":     "D",
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": "d",
		"body": map[string]any{
			"tableReference": map[string]any{"tableId": "t"},
			"friendlyName":   "T",
			"timePartitioning": map[string]any{
				"type": "DAY",
			},
			"schema": map[string]any{
				"fields": []any{map[string]any{"name": "id", "type": "INTEGER"}},
			},
		},
	})); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ds, err := p.ListDatasets(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list datasets: %v", err)
	}
	datasets, _ := ds.Data["datasets"].([]any)
	if len(datasets) != 1 {
		t.Fatalf("expected 1 dataset, got %d", len(datasets))
	}
	dm, _ := datasets[0].(map[string]any)
	for _, k := range []string{"creationTime", "etag", "lastModifiedTime", "access"} {
		if _, ok := dm[k]; ok {
			t.Errorf("datasets.list must omit %q, got %#v", k, dm[k])
		}
	}
	for _, k := range []string{"kind", "id", "datasetReference", "labels", "location"} {
		if _, ok := dm[k]; !ok {
			t.Errorf("datasets.list missing summary field %q", k)
		}
	}
	if dm["friendlyName"] != "D" {
		t.Errorf("datasets.list must retain allowed friendlyName, got %#v", dm["friendlyName"])
	}

	tbls, err := p.ListTables(ctx, newNR(map[string]any{"datasetId": "d"}))
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	tables, _ := tbls.Data["tables"].([]any)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	tm, _ := tables[0].(map[string]any)
	for _, k := range []string{"schema", "numRows", "etag", "lastModifiedTime"} {
		if _, ok := tm[k]; ok {
			t.Errorf("tables.list must omit %q, got %#v", k, tm[k])
		}
	}
	for _, k := range []string{"kind", "tableReference", "id", "creationTime", "type", "labels"} {
		if _, ok := tm[k]; !ok {
			t.Errorf("tables.list missing summary field %q", k)
		}
	}
	if tm["friendlyName"] != "T" {
		t.Errorf("tables.list must retain allowed friendlyName, got %#v", tm["friendlyName"])
	}
	if _, ok := tm["timePartitioning"]; !ok {
		t.Errorf("tables.list must retain allowed timePartitioning, got %#v", tm)
	}
}

// TestListJobsOmitsEtag pins the ListFormatJob projection: jobs.list items
// omit etag, while the full Job resource returned by jobs.get still has it.
func TestListJobsOmitsEtag(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	if _, err := p.InsertJob(ctx, newNR(map[string]any{"body": map[string]any{
		"jobReference": map[string]any{"jobId": "j1"},
	}})); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	list, err := p.ListJobs(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	jobs, _ := list.Data["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	jm, _ := jobs[0].(map[string]any)
	if _, ok := jm["etag"]; ok {
		t.Errorf("jobs.list must omit etag (ListFormatJob), got %#v", jm["etag"])
	}
	if jm["kind"] != "bigquery#job" || jm["id"] != "proj:j1" {
		t.Errorf("jobs.list item malformed: %#v", jm)
	}

	got, err := p.GetJob(ctx, newNR(map[string]any{"jobId": "j1"}))
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if _, ok := got.Data["etag"]; !ok {
		t.Errorf("jobs.get must include etag, got %#v", got.Data)
	}
}

// TestListRowsCellEncoding locks in the BigQuery TableCell wire contract: every
// primitive is a string, REPEATED wraps each element in its own cell,
// RECORD/STRUCT is {"v": {"f": [...]}}, and a NULL is {"v": null}. The official
// client SDKs reject raw numbers/booleans and objects with neither f nor v.
func TestListRowsCellEncoding(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	createDataset(t, p, "d")
	createTable(t, p, "d", "t", schemaOf(
		map[string]any{"name": "id", "type": "INTEGER"},
		map[string]any{"name": "score", "type": "FLOAT64"},
		map[string]any{"name": "active", "type": "BOOL"},
		map[string]any{"name": "tags", "type": "STRING", "mode": "REPEATED"},
		map[string]any{"name": "addr", "type": "RECORD", "fields": []any{
			map[string]any{"name": "city", "type": "STRING"},
			map[string]any{"name": "zip", "type": "INTEGER"},
		}},
	))

	if _, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d", "tableId": "t",
		"body": map[string]any{"rows": []any{
			map[string]any{"json": map[string]any{
				"id": 7, "score": 9.5, "active": true,
				"tags": []any{"admin", "dev"},
				"addr": map[string]any{"city": "NYC", "zip": 10001},
			}},
		}},
	})); err != nil {
		t.Fatalf("insertAll: %v", err)
	}

	resp, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "t"}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	rows, _ := resp.Data["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	cells, _ := rows[0].(map[string]any)["f"].([]any)
	want := []map[string]any{
		{"v": "7"},
		{"v": "9.5"},
		{"v": "true"},
		{"v": []any{map[string]any{"v": "admin"}, map[string]any{"v": "dev"}}},
		{"v": map[string]any{"f": []any{
			map[string]any{"v": "NYC"},
			map[string]any{"v": "10001"},
		}}},
	}
	if len(cells) != len(want) {
		t.Fatalf("expected %d cells, got %d: %#v", len(want), len(cells), cells)
	}
	for i := range want {
		if !reflect.DeepEqual(cells[i], want[i]) {
			t.Errorf("cell %d = %#v, want %#v", i, cells[i], want[i])
		}
	}

	// A schema-less table still stringifies scalar values and emits nulls as
	// {"v": null}.
	createTable(t, p, "d", "raw", nil)
	if _, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d", "tableId": "raw",
		"body": map[string]any{"rows": []any{
			map[string]any{"json": map[string]any{"flag": false, "n": 3}},
		}},
	})); err != nil {
		t.Fatalf("insertAll raw: %v", err)
	}
	rawResp, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "raw"}))
	if err != nil {
		t.Fatalf("list raw: %v", err)
	}
	rawRows, _ := rawResp.Data["rows"].([]any)
	rawCells, _ := rawRows[0].(map[string]any)["f"].([]any)
	// Keys are sorted: flag, n.
	if !reflect.DeepEqual(rawCells[0], map[string]any{"v": "false"}) ||
		!reflect.DeepEqual(rawCells[1], map[string]any{"v": "3"}) {
		t.Fatalf("schema-less cells not stringified: %#v", rawCells)
	}

	// A missing value in a schema'd table is a JSON-null cell, not an omitted
	// one (the SDK enforces the row/field count match).
	createTable(t, p, "d", "sparse", schemaOf(
		map[string]any{"name": "id", "type": "INTEGER"},
		map[string]any{"name": "name", "type": "STRING"},
	))
	if _, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": "d", "tableId": "sparse",
		"body": map[string]any{"rows": []any{
			map[string]any{"json": map[string]any{"id": 1}},
		}},
	})); err != nil {
		t.Fatalf("insertAll sparse: %v", err)
	}
	sparseResp, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "d", "tableId": "sparse"}))
	if err != nil {
		t.Fatalf("list sparse: %v", err)
	}
	sparseRows, _ := sparseResp.Data["rows"].([]any)
	sparseCells, _ := sparseRows[0].(map[string]any)["f"].([]any)
	if !reflect.DeepEqual(sparseCells[1], map[string]any{"v": nil}) {
		t.Fatalf("missing value must be {\"v\": null}, got %#v", sparseCells[1])
	}
}
