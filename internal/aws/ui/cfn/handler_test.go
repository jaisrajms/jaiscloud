package cfnui

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

type mockCFNProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockCFNProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockCFNProvider) ListStacks(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"StackSummaries": []any{}}}, nil
}

func (m *mockCFNProvider) CreateStack(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"StackId": "arn:aws:cloudformation:::stack/my-stack/abc123"}}, nil
}

func (m *mockCFNProvider) DescribeStacks(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Stacks": []any{}}}, m.err
}

func (m *mockCFNProvider) DeleteStack(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testCFNCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newCFNRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestCFNListStacks_Returns200(t *testing.T) {
	mock := &mockCFNProvider{}
	h := NewHandler(mock, testCFNCfg())

	req := httptest.NewRequest(http.MethodGet, "/stacks", nil)
	w := httptest.NewRecorder()
	h.ListStacks(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListStacksResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestCFNCreateStack_Returns201(t *testing.T) {
	mock := &mockCFNProvider{}
	h := NewHandler(mock, testCFNCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-stack", "templateBody": `{"Resources":{}}`})
	req := httptest.NewRequest(http.MethodPost, "/stacks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStack(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["StackName"] != "my-stack" {
		t.Fatalf("expected StackName=my-stack, got %v", mock.lastNR.Params["StackName"])
	}
	if mock.lastNR.Params["TemplateBody"] != `{"Resources":{}}` {
		t.Fatalf("expected TemplateBody set, got %v", mock.lastNR.Params["TemplateBody"])
	}
}

func TestCFNCreateStack_RequiresName(t *testing.T) {
	mock := &mockCFNProvider{}
	h := NewHandler(mock, testCFNCfg())

	body, _ := json.Marshal(map[string]any{"templateBody": "{}"})
	req := httptest.NewRequest(http.MethodPost, "/stacks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStack(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCFNDeleteStack_Returns204(t *testing.T) {
	mock := &mockCFNProvider{}
	h := NewHandler(mock, testCFNCfg())

	req := httptest.NewRequest(http.MethodDelete, "/stacks/my-stack", nil)
	req = req.WithContext(newCFNRouteCtx(map[string]string{"name": "my-stack"}))
	w := httptest.NewRecorder()
	h.DeleteStack(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["StackName"] != "my-stack" {
		t.Fatalf("expected StackName=my-stack, got %v", mock.lastNR.Params["StackName"])
	}
}

func TestCFNNR_ServiceIsCloudFormation(t *testing.T) {
	mock := &mockCFNProvider{}
	h := NewHandler(mock, testCFNCfg())

	req := httptest.NewRequest(http.MethodGet, "/stacks", nil)
	w := httptest.NewRecorder()
	h.ListStacks(w, req)

	if mock.lastNR.Service != "cloudformation" {
		t.Fatalf("expected service=cloudformation, got %s", mock.lastNR.Service)
	}
}
