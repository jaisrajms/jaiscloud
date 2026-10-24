package loggingui

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

func (m *mockProvider) EntryList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) LogList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) LogDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) MetricList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) MetricGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) MetricCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) MetricUpdate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) MetricDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SinkList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SinkGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SinkCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SinkPatch(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SinkDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ExclusionList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ExclusionGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ExclusionCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ExclusionPatch(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ExclusionDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func TestListEntries_ScopesToProjectAndFilter(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"entries": []any{map[string]any{"logName": "projects/test-project/logs/sys"}}},
	}}
	w := do(t, mock, http.MethodGet, "/entries?filter=severity%3E%3DERROR&orderBy=timestamp+desc", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body == nil {
		t.Fatalf("body not set: %+v", mock.lastNR.Params)
	}
	resNames, _ := body["resourceNames"].([]any)
	if len(resNames) != 1 || resNames[0] != "projects/test-project" {
		t.Fatalf("resourceNames = %v", body["resourceNames"])
	}
	if body["filter"] != "severity>=ERROR" || body["orderBy"] != "timestamp desc" {
		t.Fatalf("filter/orderBy = %v/%v", body["filter"], body["orderBy"])
	}
	if mock.lastNR.Action != "Logging.EntryList" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
}

func TestListMetrics_SetsParent(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"metrics": []any{map[string]any{"name": "m1"}}},
	}}
	w := do(t, mock, http.MethodGet, "/metrics", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["parent"] != "projects/test-project" {
		t.Fatalf("parent = %v", mock.lastNR.Params["parent"])
	}
}

func TestGetMetric_EncodesSlashID(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "nginx/requests"}}}
	w := do(t, mock, http.MethodGet, "/metrics/nginx%2Frequests", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["metricName"] != "projects/test-project/metrics/nginx%2Frequests" {
		t.Fatalf("metricName = %v", mock.lastNR.Params["metricName"])
	}
	if mock.lastNR.Action != "Logging.MetricGet" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
}

func TestCreateMetric_SetsParentAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "errors"}}}
	body := `{"name":"errors","filter":"severity=ERROR","metricDescriptor":{"metricKind":"DELTA","valueType":"INT64"}}`
	w := do(t, mock, http.MethodPost, "/metrics", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["parent"] != "projects/test-project" {
		t.Fatalf("parent = %v", mock.lastNR.Params["parent"])
	}
	b, _ := mock.lastNR.Params["body"].(map[string]any)
	if b["name"] != "errors" || b["filter"] != "severity=ERROR" {
		t.Fatalf("body = %+v", b)
	}
	if b["disabled"] != false {
		t.Fatalf("disabled = %v, want false", b["disabled"])
	}
}

func TestDeleteLog_EncodesName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodDelete, "/logs/nginx%2Frequests", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["logName"] != "projects/test-project/logs/nginx%2Frequests" {
		t.Fatalf("logName = %v", mock.lastNR.Params["logName"])
	}
}

func TestSinkCRUD_NamesAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	_ = do(t, mock, http.MethodGet, "/sinks", "")
	if mock.lastNR.Params["parent"] != "projects/test-project" {
		t.Fatalf("parent = %v", mock.lastNR.Params["parent"])
	}

	_ = do(t, mock, http.MethodGet, "/sinks/my-sink", "")
	if mock.lastNR.Params["sinkName"] != "projects/test-project/sinks/my-sink" {
		t.Fatalf("sinkName = %v", mock.lastNR.Params["sinkName"])
	}

	w := do(t, mock, http.MethodPost, "/sinks", `{"name":"my-sink","destination":"storage.googleapis.com/b"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201: %s", w.Code, w.Body.String())
	}
	b, _ := mock.lastNR.Params["body"].(map[string]any)
	if b["destination"] != "storage.googleapis.com/b" {
		t.Fatalf("body = %+v", b)
	}
}

func TestExclusionCRUD_Names(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	_ = do(t, mock, http.MethodGet, "/exclusions", "")
	if mock.lastNR.Params["parent"] != "projects/test-project" {
		t.Fatalf("parent = %v", mock.lastNR.Params["parent"])
	}
	w := do(t, mock, http.MethodDelete, "/exclusions/noisy", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "projects/test-project/exclusions/noisy" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestGetMetric_PropagatesProviderError(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "metric not found", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/metrics/missing", "")
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
