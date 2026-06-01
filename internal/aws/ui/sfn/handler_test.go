package sfnui

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

type mockSFNProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockSFNProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockSFNProvider) ListStateMachines(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"stateMachines": []any{}}}, nil
}

func (m *mockSFNProvider) CreateStateMachine(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"stateMachineArn": "arn:aws:states:::stateMachine/test"}}, nil
}

func (m *mockSFNProvider) DeleteStateMachine(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockSFNProvider) StartExecution(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"executionArn": "arn:aws:states:::execution/test/exec1"}}, nil
}

func (m *mockSFNProvider) ListExecutions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"executions": []any{}}}, nil
}

func (m *mockSFNProvider) StopExecution(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockSFNProvider) GetExecutionHistory(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"events": []any{}}}, nil
}

func testSFNCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newSFNRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestSFNListStateMachines_Returns200(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	req := httptest.NewRequest(http.MethodGet, "/state-machines", nil)
	w := httptest.NewRecorder()
	h.ListStateMachines(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListStateMachinesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestSFNCreateStateMachine_Returns201(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	body, _ := json.Marshal(map[string]any{
		"name":       "my-sm",
		"definition": `{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}`,
		"roleArn":    "arn:aws:iam:::role/test",
	})
	req := httptest.NewRequest(http.MethodPost, "/state-machines", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStateMachine(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["name"] != "my-sm" {
		t.Fatalf("expected name=my-sm, got %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["roleArn"] != "arn:aws:iam:::role/test" {
		t.Fatalf("expected roleArn set, got %v", mock.lastNR.Params["roleArn"])
	}
}

func TestSFNCreateStateMachine_RequiresName(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	body, _ := json.Marshal(map[string]any{"definition": "{}"})
	req := httptest.NewRequest(http.MethodPost, "/state-machines", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStateMachine(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestSFNCreateStateMachine_DefaultsDefinition(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-sm"})
	req := httptest.NewRequest(http.MethodPost, "/state-machines", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStateMachine(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if def, ok := mock.lastNR.Params["definition"].(string); !ok || def == "" {
		t.Fatal("expected non-empty definition default")
	}
}

func TestSFNDeleteStateMachine_Returns204(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	req := httptest.NewRequest(http.MethodDelete, "/state-machines/arn:aws:states:::stateMachine/test", nil)
	req = req.WithContext(newSFNRouteCtx(map[string]string{"arn": "arn:aws:states:::stateMachine/test"}))
	w := httptest.NewRecorder()
	h.DeleteStateMachine(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["stateMachineArn"] != "arn:aws:states:::stateMachine/test" {
		t.Fatalf("expected stateMachineArn set, got %v", mock.lastNR.Params["stateMachineArn"])
	}
}

func TestSFNStartExecution_Returns201(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	body, _ := json.Marshal(map[string]any{"name": "run1", "input": `{"key":"val"}`})
	req := httptest.NewRequest(http.MethodPost, "/state-machines/arn:aws:states:::stateMachine/test/executions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(newSFNRouteCtx(map[string]string{"arn": "arn:aws:states:::stateMachine/test"}))
	w := httptest.NewRecorder()
	h.StartExecution(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["stateMachineArn"] != "arn:aws:states:::stateMachine/test" {
		t.Fatalf("expected stateMachineArn set, got %v", mock.lastNR.Params["stateMachineArn"])
	}
	if mock.lastNR.Params["name"] != "run1" {
		t.Fatalf("expected name=run1, got %v", mock.lastNR.Params["name"])
	}
}

func TestSFNListExecutions_SetsStateMachineArn(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	req := httptest.NewRequest(http.MethodGet, "/state-machines/arn:aws:states:::stateMachine/test/executions", nil)
	req = req.WithContext(newSFNRouteCtx(map[string]string{"arn": "arn:aws:states:::stateMachine/test"}))
	w := httptest.NewRecorder()
	h.ListExecutions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["stateMachineArn"] != "arn:aws:states:::stateMachine/test" {
		t.Fatalf("expected stateMachineArn set, got %v", mock.lastNR.Params["stateMachineArn"])
	}
}

func TestSFNStopExecution_Returns204(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	req := httptest.NewRequest(http.MethodPost, "/executions/arn:aws:states:::execution/test/exec1/stop", nil)
	req = req.WithContext(newSFNRouteCtx(map[string]string{"arn": "arn:aws:states:::execution/test/exec1"}))
	w := httptest.NewRecorder()
	h.StopExecution(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["executionArn"] != "arn:aws:states:::execution/test/exec1" {
		t.Fatalf("expected executionArn set, got %v", mock.lastNR.Params["executionArn"])
	}
}

func TestSFNGetExecutionHistory_Returns200(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	req := httptest.NewRequest(http.MethodGet, "/executions/arn:aws:states:::execution/test/exec1/history", nil)
	req = req.WithContext(newSFNRouteCtx(map[string]string{"arn": "arn:aws:states:::execution/test/exec1"}))
	w := httptest.NewRecorder()
	h.GetExecutionHistory(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ExecutionHistoryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Events == nil {
		t.Fatal("expected non-nil events")
	}
}

func TestSFNNR_ServiceIsStates(t *testing.T) {
	mock := &mockSFNProvider{}
	h := NewHandler(mock, testSFNCfg())

	req := httptest.NewRequest(http.MethodGet, "/state-machines", nil)
	w := httptest.NewRecorder()
	h.ListStateMachines(w, req)

	if mock.lastNR.Service != "states" {
		t.Fatalf("expected service=states, got %s", mock.lastNR.Service)
	}
}
