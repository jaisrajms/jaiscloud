package glueui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

type mockGlueProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockGlueProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockGlueProvider) GetDatabases(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"DatabaseList": []any{}}}, nil
}

func (m *mockGlueProvider) CreateDatabase(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) DeleteDatabase(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) GetTables(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"TableList": []any{}}}, nil
}

func (m *mockGlueProvider) CreateTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) DeleteTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) GetJobs(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Jobs": []any{}}}, nil
}

func (m *mockGlueProvider) CreateJob(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Name": nr.Params["Name"]}}, m.err
}

func (m *mockGlueProvider) DeleteJob(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) StartJobRun(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"JobRunId": "run-1"}}, m.err
}

func (m *mockGlueProvider) GetJobRuns(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"JobRuns": []any{}}}, nil
}

func (m *mockGlueProvider) GetCrawlers(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Crawlers": []any{}}}, nil
}

func (m *mockGlueProvider) CreateCrawler(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) DeleteCrawler(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockGlueProvider) StartCrawler(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testGlueCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newGlueRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestGlueListDatabases_Returns200(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodGet, "/databases", nil)
	w := httptest.NewRecorder()
	h.ListDatabases(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListDatabasesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestGlueCreateDatabase_Returns201(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	body, _ := json.Marshal(map[string]any{"name": "mydb", "description": "test"})
	req := httptest.NewRequest(http.MethodPost, "/databases", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateDatabase(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	dbInput, _ := mock.lastNR.Params["DatabaseInput"].(map[string]any)
	if dbInput["Name"] != "mydb" {
		t.Fatalf("expected DatabaseInput.Name=mydb, got %v", dbInput)
	}
}

func TestGlueCreateDatabase_RequiresName(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	body, _ := json.Marshal(map[string]any{"description": "no name"})
	req := httptest.NewRequest(http.MethodPost, "/databases", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateDatabase(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestGlueDeleteDatabase_Returns204(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodDelete, "/databases/mydb", nil)
	req = req.WithContext(newGlueRouteCtx(map[string]string{"name": "mydb"}))
	w := httptest.NewRecorder()
	h.DeleteDatabase(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "mydb" {
		t.Fatalf("expected Name=mydb, got %v", mock.lastNR.Params["Name"])
	}
}

func TestGlueListTables_SetsDatabaseName(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodGet, "/databases/mydb/tables", nil)
	req = req.WithContext(newGlueRouteCtx(map[string]string{"db": "mydb"}))
	w := httptest.NewRecorder()
	h.ListTables(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["DatabaseName"] != "mydb" {
		t.Fatalf("expected DatabaseName=mydb, got %v", mock.lastNR.Params["DatabaseName"])
	}
}

func TestGlueCreateJob_Returns201(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	body, _ := json.Marshal(map[string]any{"name": "etl-job", "role": "GlueRole"})
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateJob(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "etl-job" {
		t.Fatalf("expected Name=etl-job, got %v", mock.lastNR.Params["Name"])
	}
}

func TestGlueStartJobRun_Returns201(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodPost, "/jobs/etl-job/runs", nil)
	req = req.WithContext(newGlueRouteCtx(map[string]string{"name": "etl-job"}))
	w := httptest.NewRecorder()
	h.StartJobRun(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["JobName"] != "etl-job" {
		t.Fatalf("expected JobName=etl-job, got %v", mock.lastNR.Params["JobName"])
	}
}

func TestGlueDeleteJob_UsesJobName(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodDelete, "/jobs/etl-job", nil)
	req = req.WithContext(newGlueRouteCtx(map[string]string{"name": "etl-job"}))
	w := httptest.NewRecorder()
	h.DeleteJob(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["JobName"] != "etl-job" {
		t.Fatalf("expected JobName=etl-job (not Name), got %v", mock.lastNR.Params["JobName"])
	}
}

func TestGlueCreateCrawler_SetsS3Targets(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	body, _ := json.Marshal(map[string]any{
		"name":         "my-crawler",
		"role":         "GlueRole",
		"databaseName": "mydb",
		"s3Targets":    []string{"s3://bucket/prefix"},
	})
	req := httptest.NewRequest(http.MethodPost, "/crawlers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateCrawler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	targets, ok := mock.lastNR.Params["Targets"].(map[string]any)
	if !ok {
		t.Fatalf("expected Targets map, got %v", mock.lastNR.Params["Targets"])
	}
	s3t, _ := targets["S3Targets"].([]any)
	if len(s3t) != 1 {
		t.Fatalf("expected 1 S3 target, got %d", len(s3t))
	}
}

func TestGlueStartCrawler_Returns204(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodPost, "/crawlers/my-crawler/start", nil)
	req = req.WithContext(newGlueRouteCtx(map[string]string{"name": "my-crawler"}))
	w := httptest.NewRecorder()
	h.StartCrawler(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "my-crawler" {
		t.Fatalf("expected Name=my-crawler, got %v", mock.lastNR.Params["Name"])
	}
}

func TestGlueNR_ClockIsNonNil(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodGet, "/databases", nil)
	w := httptest.NewRecorder()
	h.ListDatabases(w, req)

	if mock.lastNR.Clock == nil {
		t.Fatal("nr.Clock must not be nil")
	}
}

func TestGlueNR_ServiceIsGlue(t *testing.T) {
	mock := &mockGlueProvider{}
	h := NewHandler(mock, testGlueCfg())

	req := httptest.NewRequest(http.MethodGet, "/databases", nil)
	w := httptest.NewRecorder()
	h.ListDatabases(w, req)

	if mock.lastNR.Service != "glue" {
		t.Fatalf("expected service=glue, got %s", mock.lastNR.Service)
	}
}
