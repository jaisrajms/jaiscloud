package logsui

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

type mockLogsProvider struct {
	describeGroupsResp *model.ProviderResponse
	describeGroupsErr  error
	createGroupErr     error
	deleteGroupErr     error
	retentionErr       error
	describeStreamsResp *model.ProviderResponse
	describeStreamsErr  error
	getEventsResp      *model.ProviderResponse
	getEventsErr       error
	filterEventsResp   *model.ProviderResponse
	filterEventsErr    error

	lastNR *model.NormalizedRequest
}

func (m *mockLogsProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockLogsProvider) DescribeLogGroups(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.describeGroupsResp, m.describeGroupsErr
}
func (m *mockLogsProvider) CreateLogGroup(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.createGroupErr
}
func (m *mockLogsProvider) DeleteLogGroup(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteGroupErr
}
func (m *mockLogsProvider) PutRetentionPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.retentionErr
}
func (m *mockLogsProvider) DescribeLogStreams(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.describeStreamsResp, m.describeStreamsErr
}
func (m *mockLogsProvider) GetLogEvents(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.getEventsResp, m.getEventsErr
}
func (m *mockLogsProvider) FilterLogEvents(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.filterEventsResp, m.filterEventsErr
}

func testCfg() *config.Config {
	return &config.Config{
		Region:    "us-east-1",
		AccountID: "000000000000",
		Port:      4566,
		UIPort:    4567,
		Clock:     clock.RealClock{},
	}
}

func buildHandlerWithRouter(p ProviderInterface, cfg *config.Config) http.Handler {
	r := BuildRouter(p, cfg)
	return r
}

// ── ListLogGroups ──────────────────────────────────────────────────────────

func TestListLogGroups_Returns200WithItems(t *testing.T) {
	mock := &mockLogsProvider{
		describeGroupsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data: map[string]any{
				"logGroups": []any{
					map[string]any{
						"logGroupName": "/aws/lambda/my-func",
						"arn":          "arn:aws:logs:us-east-1:000000000000:log-group:/aws/lambda/my-func",
						"storedBytes":  float64(1024),
						"creationTime": float64(1700000000000),
					},
				},
			},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	items, _ := resp["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func TestListLogGroups_EmptyReturns200(t *testing.T) {
	mock := &mockLogsProvider{
		describeGroupsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"logGroups": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// MANDATORY params key test — lowercase camelCase, not "MaxResults"
func TestListLogGroups_NrParamsLimitKey(t *testing.T) {
	mock := &mockLogsProvider{
		describeGroupsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"logGroups": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups?pageSize=25", nil)
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	if _, ok := mock.lastNR.Params["limit"]; !ok {
		t.Fatal(`expected nr.Params["limit"] to be set, not "MaxResults" or another key`)
	}
	if _, bad := mock.lastNR.Params["MaxResults"]; bad {
		t.Fatal(`nr.Params["MaxResults"] must not be set — use lowercase "limit"`)
	}
}

// ── CreateLogGroup ─────────────────────────────────────────────────────────

func TestCreateLogGroup_Returns201(t *testing.T) {
	mock := &mockLogsProvider{}

	body, _ := json.Marshal(map[string]string{"logGroupName": "/aws/lambda/my-func"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/groups", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
}

// MANDATORY: logGroupName key is lowercase camelCase
func TestCreateLogGroup_NrParamsLogGroupNameKey(t *testing.T) {
	mock := &mockLogsProvider{}

	body, _ := json.Marshal(map[string]string{"logGroupName": "/aws/lambda/my-func"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/groups", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	v, ok := mock.lastNR.Params["logGroupName"]
	if !ok {
		t.Fatal(`expected nr.Params["logGroupName"] to be set`)
	}
	if v != "/aws/lambda/my-func" {
		t.Fatalf(`expected logGroupName "/aws/lambda/my-func", got %v`, v)
	}
}

func TestCreateLogGroup_MissingNameReturns400(t *testing.T) {
	mock := &mockLogsProvider{}

	body, _ := json.Marshal(map[string]string{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/groups", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// ── DeleteLogGroup ─────────────────────────────────────────────────────────

func TestDeleteLogGroup_Returns204(t *testing.T) {
	mock := &mockLogsProvider{}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/groups/my-group", nil)
	buildHandlerWithRouterWithURLParam(mock, testCfg(), "name", "my-group").ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

// MANDATORY: logGroupName key is lowercase camelCase
func TestDeleteLogGroup_NrParamsLogGroupNameKey(t *testing.T) {
	mock := &mockLogsProvider{}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/groups/my-group", nil)
	buildHandlerWithRouterWithURLParam(mock, testCfg(), "name", "my-group").ServeHTTP(rec, req)

	if _, ok := mock.lastNR.Params["logGroupName"]; !ok {
		t.Fatal(`expected nr.Params["logGroupName"] to be set`)
	}
}

// ── GetLogEvents ───────────────────────────────────────────────────────────

func TestGetLogEvents_Returns200(t *testing.T) {
	mock := &mockLogsProvider{
		getEventsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"events": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups/my-group/streams/my-stream/events", nil)
	buildHandlerWithRouterWithMultiParam(mock, testCfg(), map[string]string{"name": "my-group", "stream": "my-stream"}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// MANDATORY: params use lowercase camelCase
func TestGetLogEvents_NrParamsGroupNameKey(t *testing.T) {
	mock := &mockLogsProvider{
		getEventsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"events": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups/my-group/streams/my-stream/events", nil)
	buildHandlerWithRouterWithMultiParam(mock, testCfg(), map[string]string{"name": "my-group", "stream": "my-stream"}).ServeHTTP(rec, req)

	if _, ok := mock.lastNR.Params["logGroupName"]; !ok {
		t.Fatal(`expected nr.Params["logGroupName"] to be set (lowercase camelCase)`)
	}
}

func TestGetLogEvents_NrParamsStreamNameKey(t *testing.T) {
	mock := &mockLogsProvider{
		getEventsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"events": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups/my-group/streams/my-stream/events", nil)
	buildHandlerWithRouterWithMultiParam(mock, testCfg(), map[string]string{"name": "my-group", "stream": "my-stream"}).ServeHTTP(rec, req)

	if _, ok := mock.lastNR.Params["logStreamName"]; !ok {
		t.Fatal(`expected nr.Params["logStreamName"] to be set (lowercase camelCase)`)
	}
}

// MANDATORY: default time range is set (start/end when query params absent)
func TestGetLogEvents_DefaultTimeRangeIsSet(t *testing.T) {
	mock := &mockLogsProvider{
		getEventsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"events": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups/my-group/streams/my-stream/events", nil)
	buildHandlerWithRouterWithMultiParam(mock, testCfg(), map[string]string{"name": "my-group", "stream": "my-stream"}).ServeHTTP(rec, req)

	if _, ok := mock.lastNR.Params["startTime"]; !ok {
		t.Fatal(`expected nr.Params["startTime"] to be set when no query param provided`)
	}
	if _, ok := mock.lastNR.Params["endTime"]; !ok {
		t.Fatal(`expected nr.Params["endTime"] to be set when no query param provided`)
	}
}

// ── uiNR invariants (via NR helper) ───────────────────────────────────────

func TestUiNR_PortIsWirePortNotUIPort(t *testing.T) {
	mock := &mockLogsProvider{
		describeGroupsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"logGroups": []any{}},
		},
	}
	cfg := testCfg()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	buildHandlerWithRouter(mock, cfg).ServeHTTP(rec, req)

	if mock.lastNR.Port != cfg.Port {
		t.Fatalf("expected nr.Port == cfg.Port (%d), got %d (UI port is %d)", cfg.Port, mock.lastNR.Port, cfg.UIPort)
	}
}

func TestUiNR_ClockIsNotNil(t *testing.T) {
	mock := &mockLogsProvider{
		describeGroupsResp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"logGroups": []any{}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	buildHandlerWithRouter(mock, testCfg()).ServeHTTP(rec, req)

	if mock.lastNR.Clock == nil {
		t.Fatal("expected nr.Clock != nil — providers panic on nil clock")
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

// buildHandlerWithRouterWithURLParam wraps a single chi URL param for tests.
func buildHandlerWithRouterWithURLParam(p ProviderInterface, cfg *config.Config, key, val string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chiCtx := chi.NewRouteContext()
		chiCtx.URLParams.Add(key, val)
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, chiCtx))
		NewHandler(p, cfg).DeleteLogGroup(w, r)
	})
}

// buildHandlerWithRouterWithMultiParam wraps multiple chi URL params for tests.
func buildHandlerWithRouterWithMultiParam(p ProviderInterface, cfg *config.Config, params map[string]string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chiCtx := chi.NewRouteContext()
		for k, v := range params {
			chiCtx.URLParams.Add(k, v)
		}
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, chiCtx))
		NewHandler(p, cfg).GetLogEvents(w, r)
	})
}
