package monitoringui

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

func (m *mockProvider) ListMetricDescriptors(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListTimeSeries(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListAlertPolicies(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetAlertPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateAlertPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) UpdateAlertPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteAlertPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListNotificationChannels(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetNotificationChannel(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateNotificationChannel(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) UpdateNotificationChannel(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteNotificationChannel(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListNotificationChannelDescriptors(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func TestListMetricDescriptors_ForwardsFilter(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"metricDescriptors": []any{map[string]any{"type": "custom.googleapis.com/x"}}},
	}}
	w := do(t, mock, http.MethodGet, "/metricDescriptors?filter=metric.type%3D%22x%22&pageSize=50", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["filter"] != `metric.type="x"` {
		t.Fatalf("filter = %v", mock.lastNR.Params["filter"])
	}
	if mock.lastNR.Params["pageSize"] != "50" {
		t.Fatalf("pageSize = %v", mock.lastNR.Params["pageSize"])
	}
	if mock.lastNR.Action != "Monitoring.ListMetricDescriptors" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
}

func TestListTimeSeries_ForwardsInterval(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	_ = do(t, mock, http.MethodGet, "/timeSeries?filter=metric.type%3D%22x%22&startTime=2026-01-01T00:00:00Z&endTime=2026-01-01T01:00:00Z", "")
	if mock.lastNR.Params["filter"] != `metric.type="x"` {
		t.Fatalf("filter = %v", mock.lastNR.Params["filter"])
	}
	if mock.lastNR.Params["interval.startTime"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("startTime = %v", mock.lastNR.Params["interval.startTime"])
	}
	if mock.lastNR.Params["interval.endTime"] != "2026-01-01T01:00:00Z" {
		t.Fatalf("endTime = %v", mock.lastNR.Params["interval.endTime"])
	}
}

func TestCreateAlertPolicy_SetsBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "projects/test-project/alertPolicies/1"}}}
	w := do(t, mock, http.MethodPost, "/alertPolicies", `{"displayName":"High errors","combiner":"OR"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	b, _ := mock.lastNR.Params["body"].(map[string]any)
	if b["displayName"] != "High errors" || b["combiner"] != "OR" {
		t.Fatalf("body = %+v", b)
	}
	if _, ok := b["conditions"]; ok {
		t.Fatalf("unexpected conditions in body: %+v", b)
	}
}

func TestDeleteAlertPolicy_ResourceName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodDelete, "/alertPolicies/abc", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "projects/test-project/alertPolicies/abc" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestNotificationChannel_CRUD(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	_ = do(t, mock, http.MethodGet, "/notificationChannels", "")
	if mock.lastNR.Action != "Monitoring.ListNotificationChannels" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	w := do(t, mock, http.MethodPost, "/notificationChannels", `{"type":"email","displayName":"Ops","labels":{"email_address":"ops@example.com"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201: %s", w.Code, w.Body.String())
	}
	b, _ := mock.lastNR.Params["body"].(map[string]any)
	if b["type"] != "email" {
		t.Fatalf("body = %+v", b)
	}

	w = do(t, mock, http.MethodDelete, "/notificationChannels/ch-1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "projects/test-project/notificationChannels/ch-1" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestGetAlertPolicy_PropagatesProviderError(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "policy not found", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/alertPolicies/missing", "")
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
