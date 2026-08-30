package eventbridgeui

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

type mockEBProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockEBProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockEBProvider) ListRules(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Rules": []any{}}}, nil
}

func (m *mockEBProvider) PutRule(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"RuleArn": "arn:aws:events:::rule/test"}}, nil
}

func (m *mockEBProvider) DeleteRule(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEBProvider) EnableRule(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEBProvider) DisableRule(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEBProvider) ListTargetsByRule(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Targets": []any{}}}, nil
}

func (m *mockEBProvider) PutTargets(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"FailedEntryCount": 0, "FailedEntries": []any{}}}, m.err
}

func (m *mockEBProvider) RemoveTargets(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEBProvider) PutEvents(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"FailedEntryCount": 0, "Entries": []any{}}}, m.err
}

func (m *mockEBProvider) ListEventBuses(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"EventBuses": []any{}}}, nil
}

func (m *mockEBProvider) CreateEventBus(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"EventBusArn": "arn:aws:events:::event-bus/test"}}, m.err
}

func (m *mockEBProvider) DeleteEventBus(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testEBCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newEBRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestEBListRules_Returns200(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	w := httptest.NewRecorder()
	h.ListRules(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListRulesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestEBListRules_BusFilter(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodGet, "/rules?bus=my-bus", nil)
	w := httptest.NewRecorder()
	h.ListRules(w, req)

	if mock.lastNR.Params["EventBusName"] != "my-bus" {
		t.Fatalf("expected EventBusName=my-bus, got %v", mock.lastNR.Params["EventBusName"])
	}
}

func TestEBPutRule_Returns201(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	body, _ := json.Marshal(map[string]any{
		"name":         "my-rule",
		"eventPattern": `{"source":["aws.ec2"]}`,
		"state":        "ENABLED",
	})
	req := httptest.NewRequest(http.MethodPost, "/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.PutRule(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "my-rule" {
		t.Fatalf("expected Name=my-rule, got %v", mock.lastNR.Params["Name"])
	}
}

func TestEBPutRule_RequiresName(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	body, _ := json.Marshal(map[string]any{"eventPattern": "{}"})
	req := httptest.NewRequest(http.MethodPost, "/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.PutRule(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestEBDeleteRule_Returns204(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodDelete, "/rules/my-rule", nil)
	req = req.WithContext(newEBRouteCtx(map[string]string{"name": "my-rule"}))
	w := httptest.NewRecorder()
	h.DeleteRule(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "my-rule" {
		t.Fatalf("expected Name=my-rule, got %v", mock.lastNR.Params["Name"])
	}
}

func TestEBEnableRule_Returns204(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodPost, "/rules/my-rule/enable", nil)
	req = req.WithContext(newEBRouteCtx(map[string]string{"name": "my-rule"}))
	w := httptest.NewRecorder()
	h.EnableRule(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestEBPutTargets_SetsRuleAndTargets(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	body, _ := json.Marshal(map[string]any{
		"targets": []any{map[string]any{"id": "t1", "arn": "arn:aws:sqs:::my-queue"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/rules/my-rule/targets", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(newEBRouteCtx(map[string]string{"name": "my-rule"}))
	w := httptest.NewRecorder()
	h.PutTargets(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["Rule"] != "my-rule" {
		t.Fatalf("expected Rule=my-rule, got %v", mock.lastNR.Params["Rule"])
	}
	targets, _ := mock.lastNR.Params["Targets"].([]any)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
}

func TestEBRemoveTarget_Returns204(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodDelete, "/rules/my-rule/targets/t1", nil)
	req = req.WithContext(newEBRouteCtx(map[string]string{"name": "my-rule", "targetId": "t1"}))
	w := httptest.NewRecorder()
	h.RemoveTarget(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	ids, _ := mock.lastNR.Params["Ids"].([]any)
	if len(ids) != 1 || ids[0] != "t1" {
		t.Fatalf("expected Ids=[t1], got %v", mock.lastNR.Params["Ids"])
	}
}

func TestEBPutEvents_SetsEntries(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	body, _ := json.Marshal(map[string]any{
		"entries": []any{map[string]any{"source": "my.app", "detailType": "StateChange", "detail": "{}"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.PutEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	entries, _ := mock.lastNR.Params["Entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestEBCreateEventBus_Returns201(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-bus"})
	req := httptest.NewRequest(http.MethodPost, "/buses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateEventBus(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "my-bus" {
		t.Fatalf("expected Name=my-bus, got %v", mock.lastNR.Params["Name"])
	}
}

func TestEBNR_ClockIsNonNil(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	w := httptest.NewRecorder()
	h.ListRules(w, req)

	if mock.lastNR.Clock == nil {
		t.Fatal("nr.Clock must not be nil")
	}
}

func TestEBNR_ServiceIsEvents(t *testing.T) {
	mock := &mockEBProvider{}
	h := NewHandler(mock, testEBCfg())

	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	w := httptest.NewRecorder()
	h.ListRules(w, req)

	if mock.lastNR.Service != "events" {
		t.Fatalf("expected service=events, got %s", mock.lastNR.Service)
	}
}
