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
func (m *mockProvider) ListServices(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetService(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateService(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) UpdateService(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteService(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListServiceLevelObjectives(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetServiceLevelObjective(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateServiceLevelObjective(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) UpdateServiceLevelObjective(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DeleteServiceLevelObjective(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func TestListServices_ForwardsProjectFilterAndPaging(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/services?filter=display_name%3D%22checkout%22&pageSize=25&pageToken=abc", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "Monitoring.ListServices" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	if mock.lastNR.Params["project"] != "test-project" {
		t.Fatalf("project = %v", mock.lastNR.Params["project"])
	}
	if mock.lastNR.Params["filter"] != `display_name="checkout"` {
		t.Fatalf("filter = %v", mock.lastNR.Params["filter"])
	}
	if mock.lastNR.Params["pageSize"] != "25" || mock.lastNR.Params["pageToken"] != "abc" {
		t.Fatalf("paging = %v", mock.lastNR.Params)
	}
}

func TestGetService_ResourceName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/services/checkout", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "Monitoring.GetService" || mock.lastNR.Params["name"] != "projects/test-project/services/checkout" {
		t.Fatalf("action/name = %q/%v", mock.lastNR.Action, mock.lastNR.Params["name"])
	}
}

func TestCreateService_SetsIDAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodPost, "/services?serviceId=checkout",
		`{"displayName":"Checkout","custom":{"displayName":"Checkout"},"userLabels":{"team":"payments"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["project"] != "test-project" || mock.lastNR.Params["serviceId"] != "checkout" {
		t.Fatalf("project/serviceId = %v/%v", mock.lastNR.Params["project"], mock.lastNR.Params["serviceId"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["displayName"] != "Checkout" {
		t.Fatalf("body = %+v", body)
	}
	if _, ok := body["custom"]; !ok {
		t.Fatalf("custom identifier missing from body: %+v", body)
	}
}

func TestUpdateService_ResourceNameBodyAndMask(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodPatch, "/services/checkout?updateMask=displayName",
		`{"displayName":"Checkout v2"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "Monitoring.UpdateService" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	if mock.lastNR.Params["name"] != "projects/test-project/services/checkout" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["updateMask"] != "displayName" {
		t.Fatalf("updateMask = %v", mock.lastNR.Params["updateMask"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["displayName"] != "Checkout v2" {
		t.Fatalf("body = %+v", body)
	}
}

func TestDeleteService_ResourceName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodDelete, "/services/checkout", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "projects/test-project/services/checkout" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestListServiceLevelObjectives_ForwardsServiceFilterAndPaging(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/services/checkout/serviceLevelObjectives?filter=display_name%3D%22availability%22&pageSize=10&pageToken=x", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "Monitoring.ListServiceLevelObjectives" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	if mock.lastNR.Params["project"] != "test-project" || mock.lastNR.Params["service"] != "checkout" {
		t.Fatalf("project/service = %v/%v", mock.lastNR.Params["project"], mock.lastNR.Params["service"])
	}
	if mock.lastNR.Params["filter"] != `display_name="availability"` {
		t.Fatalf("filter = %v", mock.lastNR.Params["filter"])
	}
	if mock.lastNR.Params["pageSize"] != "10" || mock.lastNR.Params["pageToken"] != "x" {
		t.Fatalf("paging = %v", mock.lastNR.Params)
	}
}

func TestGetServiceLevelObjective_ResourceName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/services/checkout/serviceLevelObjectives/availability", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	want := "projects/test-project/services/checkout/serviceLevelObjectives/availability"
	if mock.lastNR.Action != "Monitoring.GetServiceLevelObjective" || mock.lastNR.Params["name"] != want {
		t.Fatalf("action/name = %q/%v", mock.lastNR.Action, mock.lastNR.Params["name"])
	}
}

func TestCreateServiceLevelObjective_SetsIDAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodPost, "/services/checkout/serviceLevelObjectives?serviceLevelObjectiveId=availability",
		`{"displayName":"99.9% availability","goal":0.999,"rollingPeriod":"86400s","serviceLevelIndicator":{"basicSli":{"availability":{}}}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "Monitoring.CreateServiceLevelObjective" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	if mock.lastNR.Params["project"] != "test-project" || mock.lastNR.Params["service"] != "checkout" {
		t.Fatalf("project/service = %v/%v", mock.lastNR.Params["project"], mock.lastNR.Params["service"])
	}
	if mock.lastNR.Params["serviceLevelObjectiveId"] != "availability" {
		t.Fatalf("serviceLevelObjectiveId = %v", mock.lastNR.Params["serviceLevelObjectiveId"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["goal"] != 0.999 || body["rollingPeriod"] != "86400s" {
		t.Fatalf("body = %+v", body)
	}
	if _, ok := body["serviceLevelIndicator"].(map[string]any); !ok {
		t.Fatalf("serviceLevelIndicator missing from body: %+v", body)
	}
}

func TestUpdateServiceLevelObjective_ResourceNameBodyAndMask(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodPatch, "/services/checkout/serviceLevelObjectives/availability?updateMask=goal",
		`{"goal":0.995,"serviceLevelIndicator":{"basicSli":{"availability":{}}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "Monitoring.UpdateServiceLevelObjective" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	want := "projects/test-project/services/checkout/serviceLevelObjectives/availability"
	if mock.lastNR.Params["name"] != want {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["updateMask"] != "goal" {
		t.Fatalf("updateMask = %v", mock.lastNR.Params["updateMask"])
	}
}

func TestDeleteServiceLevelObjective_ResourceName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodDelete, "/services/checkout/serviceLevelObjectives/availability", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	want := "projects/test-project/services/checkout/serviceLevelObjectives/availability"
	if mock.lastNR.Params["name"] != want {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestServiceLevelObjective_PropagatesProviderError(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "slo not found", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/services/checkout/serviceLevelObjectives/missing", "")
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
