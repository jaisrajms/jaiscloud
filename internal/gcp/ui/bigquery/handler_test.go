package bigqueryui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// mockProvider records the last NormalizedRequest and returns a canned response.
type mockProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockProvider) reply(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.resp != nil {
		return m.resp, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockProvider) ListDatasets(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetDataset(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateDataset(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteDataset(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListTables(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListRows(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) InsertAll(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Query(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListJobs(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetJob(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteJob(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CancelJob(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      8080,
		UIPort:    4567,
		Region:    "global",
		AccountID: "test-project",
		ProjectID: "test-project",
		Clock:     clock.RealClock{},
	}
}

func do(t *testing.T, mock *mockProvider, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func datasetItem(id, location string) map[string]any {
	return map[string]any{
		"kind": "bigquery#dataset",
		"id":   "test-project:" + id,
		"datasetReference": map[string]any{
			"projectId": "test-project",
			"datasetId": id,
		},
		"location": location,
		"labels":   map[string]string{"env": "test"},
	}
}

func tableItem(dataset, id string) map[string]any {
	return map[string]any{
		"kind": "bigquery#table",
		"id":   "test-project:" + dataset + "." + id,
		"tableReference": map[string]any{
			"projectId": "test-project",
			"datasetId": dataset,
			"tableId":   id,
		},
		"type":         "TABLE",
		"creationTime": "1700000000000",
	}
}

func TestListDatasets_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"datasets":      []any{datasetItem("analytics", "US"), datasetItem("raw", "EU")},
			"nextPageToken": "tok",
		},
	}}

	w := do(t, mock, http.MethodGet, "/datasets", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListDatasetsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Datasets) != 2 || resp.NextPageToken != "tok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Datasets[0].DatasetID != "analytics" || resp.Datasets[0].Location != "US" {
		t.Fatalf("dataset not flattened: %+v", resp.Datasets[0])
	}
	if resp.Datasets[0].Labels["env"] != "test" {
		t.Fatalf("labels not flattened: %+v", resp.Datasets[0])
	}
	if mock.lastNR.Cloud != model.CloudGCP || mock.lastNR.AccountID != "test-project" {
		t.Fatalf("NR not GCP-scoped: %+v", mock.lastNR)
	}
	if mock.lastNR.Action != "BigQuery.ListDatasets" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
}

func TestCreateDataset_BuildsBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: datasetItem("analytics", "US")}}

	w := do(t, mock, http.MethodPost, "/datasets",
		`{"datasetId":"analytics","location":"US","friendlyName":"Analytics","description":"d","labels":{"env":"prod"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.CreateDataset" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	ref, _ := body["datasetReference"].(map[string]any)
	if ref["datasetId"] != "analytics" || ref["projectId"] != "test-project" {
		t.Fatalf("datasetReference = %+v", ref)
	}
	if body["location"] != "US" || body["friendlyName"] != "Analytics" {
		t.Fatalf("body = %+v", body)
	}
	// Labels must reach the provider as map[string]any: its body readers
	// type-assert on that shape and would otherwise silently drop them.
	labels, ok := body["labels"].(map[string]any)
	if !ok || labels["env"] != "prod" {
		t.Fatalf("labels not converted to map[string]any: %#v", body["labels"])
	}
}

func TestCreateDataset_RequiresID(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/datasets", `{"location":"US"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if mock.lastNR != nil {
		t.Fatalf("provider called on invalid request")
	}
}

func TestGetDataset_PassesThrough(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"kind": "bigquery#dataset", "maxTimeTravelHours": "168"},
	}}
	w := do(t, mock, http.MethodGet, "/datasets/analytics", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.GetDataset" || mock.lastNR.Params["datasetId"] != "analytics" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
}

func TestDeleteDataset_NoContent(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}}
	w := do(t, mock, http.MethodDelete, "/datasets/analytics", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.DeleteDataset" || mock.lastNR.Params["datasetId"] != "analytics" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
}

func TestListTables_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"tables": []any{tableItem("analytics", "events"), tableItem("analytics", "users")},
		},
	}}
	w := do(t, mock, http.MethodGet, "/datasets/analytics/tables", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListTablesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || resp.Tables[0].TableID != "events" || resp.Tables[0].DatasetID != "analytics" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if mock.lastNR.Action != "BigQuery.ListTables" || mock.lastNR.Params["datasetId"] != "analytics" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
}

func TestCreateTable_BuildsBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: tableItem("analytics", "events")}}
	w := do(t, mock, http.MethodPost, "/datasets/analytics/tables",
		`{"tableId":"events","schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"}]}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.CreateTable" || mock.lastNR.Params["datasetId"] != "analytics" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	ref, _ := body["tableReference"].(map[string]any)
	if ref["datasetId"] != "analytics" || ref["tableId"] != "events" {
		t.Fatalf("tableReference = %+v", ref)
	}
	if _, ok := body["schema"].(map[string]any); !ok {
		t.Fatalf("schema missing from body: %+v", body)
	}
}

func TestGetTable_AndDelete(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: tableItem("analytics", "events")}}
	w := do(t, mock, http.MethodGet, "/datasets/analytics/tables/events", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if mock.lastNR.Action != "BigQuery.GetTable" ||
		mock.lastNR.Params["datasetId"] != "analytics" || mock.lastNR.Params["tableId"] != "events" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}

	mock2 := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}}
	if w := do(t, mock2, http.MethodDelete, "/datasets/analytics/tables/events", ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", w.Code)
	}
	if mock2.lastNR.Action != "BigQuery.DeleteTable" {
		t.Fatalf("action = %q", mock2.lastNR.Action)
	}
}

func TestListRows(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"rows": []any{
				map[string]any{"f": []any{map[string]any{"v": "1"}}},
			},
			"totalRows": "1",
			"pageToken": "next",
		},
	}}
	w := do(t, mock, http.MethodGet, "/datasets/analytics/tables/events/rows", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListRowsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalRows != "1" || len(resp.Rows) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	// The provider names the rows cursor pageToken, not nextPageToken.
	if resp.NextPageToken != "next" {
		t.Fatalf("pageToken not remapped: %+v", resp)
	}
	if mock.lastNR.Action != "BigQuery.ListRows" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
}

func TestListJobs_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"jobs": []any{
				map[string]any{
					"id":            "test-project:job1",
					"jobReference":  map[string]any{"jobId": "job1"},
					"status":        map[string]any{"state": "DONE"},
					"configuration": map[string]any{"query": map[string]any{"query": "SELECT 1"}},
					"statistics":    map[string]any{"query": map[string]any{"statementType": "SELECT"}},
				},
			},
		},
	}}
	w := do(t, mock, http.MethodGet, "/jobs", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListJobsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.Jobs[0].JobID != "job1" || resp.Jobs[0].State != "DONE" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Jobs[0].StatementType != "SELECT" || resp.Jobs[0].Query != "SELECT 1" {
		t.Fatalf("job query fields not flattened: %+v", resp.Jobs[0])
	}
}

func TestCancelJob(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"kind": "bigquery#jobCancelResponse"},
	}}
	w := do(t, mock, http.MethodPost, "/jobs/job1/cancel", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.CancelJob" || mock.lastNR.Params["jobId"] != "job1" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
}

func TestGetJob_PropagatesNotFound(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "job not found", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/jobs/missing", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["code"] != "NotFound" {
		t.Fatalf("code = %q, want NotFound", body["code"])
	}
}

func TestDatasetSegmentRejectsSlash(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodGet, "/datasets/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if mock.lastNR != nil {
		t.Fatalf("provider called for a malformed dataset id")
	}
}

func TestListDatasets_ForwardsPaging(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	do(t, mock, http.MethodGet, "/datasets?pageToken=abc&maxResults=5", "")
	if mock.lastNR.Params["pageToken"] != "abc" || mock.lastNR.Params["maxResults"] != "5" {
		t.Fatalf("paging params not forwarded: %v", mock.lastNR.Params)
	}
}

func TestDeleteJob_NoContent(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodDelete, "/jobs/job1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.DeleteJob" || mock.lastNR.Params["jobId"] != "job1" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
}

func TestRunQuery_ForwardsBodyAndScopesDefaultDataset(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"kind":         "bigquery#queryResponse",
			"jobComplete":  true,
			"jobReference": map[string]any{"projectId": "test-project", "jobId": "job1"},
			"schema":       map[string]any{"fields": []any{map[string]any{"name": "n", "type": "INTEGER"}}},
			"rows":         []any{map[string]any{"f": []any{map[string]any{"v": "1"}}}},
			"totalRows":    "1",
		},
	}}
	w := do(t, mock, http.MethodPost, "/query",
		`{"query":"SELECT 1","defaultDataset":{"datasetId":"analytics"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	// The provider's Discovery-shaped body is passed through unchanged.
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["jobReference"].(map[string]any)["jobId"] != "job1" {
		t.Fatalf("query response not passed through: %+v", resp)
	}
	if mock.lastNR.Action != "BigQuery.Query" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["query"] != "SELECT 1" {
		t.Fatalf("query not forwarded: %+v", body)
	}
	dd, _ := body["defaultDataset"].(map[string]any)
	if dd["datasetId"] != "analytics" || dd["projectId"] != "test-project" {
		t.Fatalf("defaultDataset project not scoped to the account: %+v", dd)
	}
}

func TestRunQuery_ForwardsDryRunLegacyAndLocation(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodPost, "/query",
		`{"query":"SELECT 1","dryRun":true,"useLegacySql":true,"location":"US"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["dryRun"] != true || body["useLegacySql"] != true || body["location"] != "US" {
		t.Fatalf("query flags not forwarded: %+v", body)
	}
}

func TestRunQuery_RequiresQuery(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/query", `{"defaultDataset":{"datasetId":"analytics"}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if mock.lastNR != nil {
		t.Fatalf("provider called on an empty query")
	}
}

func TestRunQuery_PropagatesInvalidQuery(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("InvalidQuery", "Syntax error", http.StatusBadRequest)}
	w := do(t, mock, http.MethodPost, "/query", `{"query":"SELEC 1"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["code"] != "InvalidQuery" {
		t.Fatalf("code = %q, want InvalidQuery", body["code"])
	}
}

func TestInsertRows_BuildsBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"kind": "bigquery#tableDataInsertAllResponse", "insertErrors": []any{}},
	}}
	w := do(t, mock, http.MethodPost, "/datasets/analytics/tables/events/rows",
		`{"rows":[{"insertId":"a","json":{"id":"1"}}],"skipInvalidRows":true,"ignoreUnknownValues":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "BigQuery.InsertAll" ||
		mock.lastNR.Params["datasetId"] != "analytics" || mock.lastNR.Params["tableId"] != "events" {
		t.Fatalf("unexpected NR: action=%q params=%v", mock.lastNR.Action, mock.lastNR.Params)
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	rows, _ := body["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows not forwarded: %+v", body)
	}
	row, _ := rows[0].(map[string]any)
	if row["insertId"] != "a" {
		t.Fatalf("insertId not forwarded: %+v", row)
	}
	jsonRow, _ := row["json"].(map[string]any)
	if jsonRow["id"] != "1" {
		t.Fatalf("row json not forwarded: %+v", jsonRow)
	}
	if body["skipInvalidRows"] != true || body["ignoreUnknownValues"] != true {
		t.Fatalf("insert flags not forwarded: %+v", body)
	}
}

func TestInsertRows_RequiresRows(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/datasets/analytics/tables/events/rows", `{"rows":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if mock.lastNR != nil {
		t.Fatalf("provider called with no rows")
	}
}

func TestInsertRows_PassesThroughRowErrors(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"kind": "bigquery#tableDataInsertAllResponse",
			"insertErrors": []any{
				map[string]any{
					"index": 0,
					"errors": []any{map[string]any{
						"reason":   "invalid",
						"location": "id",
						"message":  `missing required field "id"`,
					}},
				},
			},
		},
	}}
	w := do(t, mock, http.MethodPost, "/datasets/analytics/tables/events/rows",
		`{"rows":[{"json":{"name":"x"}}]}`)
	// The real insertAll contract is HTTP 200 with row-level errors.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errs, _ := resp["insertErrors"].([]any)
	if len(errs) != 1 {
		t.Fatalf("insertErrors not passed through: %+v", resp)
	}
}

func TestRunQuery_KeepsExplicitProject(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodPost, "/query",
		`{"query":"SELECT 1","defaultDataset":{"projectId":"other-project","datasetId":"analytics"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	dd, _ := body["defaultDataset"].(map[string]any)
	if dd["projectId"] != "other-project" {
		t.Fatalf("explicit project overwritten: %+v", dd)
	}
}
