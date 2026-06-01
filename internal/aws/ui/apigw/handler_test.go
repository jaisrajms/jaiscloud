package apigwui

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

type mockAPGWProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockAPGWProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockAPGWProvider) GetRestApis(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"item": []any{}}}, nil
}

func (m *mockAPGWProvider) CreateRestApi(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"id": "abc123", "name": nr.Params["name"]}}, nil
}

func (m *mockAPGWProvider) DeleteRestApi(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, m.err
}

func (m *mockAPGWProvider) GetResources(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"item": []any{}}}, nil
}

func (m *mockAPGWProvider) GetStages(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"item": []any{}}}, nil
}

func (m *mockAPGWProvider) GetDeployments(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"item": []any{}}}, nil
}

func (m *mockAPGWProvider) CreateDeployment(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"id": "dep1"}}, nil
}

func testAPGWCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newAPGWRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestAPGWListRestAPIs_Returns200(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	req := httptest.NewRequest(http.MethodGet, "/apis", nil)
	w := httptest.NewRecorder()
	h.ListRestAPIs(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListRestAPIsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestAPGWCreateRestAPI_Returns201(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-api", "description": "test api"})
	req := httptest.NewRequest(http.MethodPost, "/apis", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateRestAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["name"] != "my-api" {
		t.Fatalf("expected name=my-api, got %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["description"] != "test api" {
		t.Fatalf("expected description=test api, got %v", mock.lastNR.Params["description"])
	}
}

func TestAPGWCreateRestAPI_RequiresName(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	body, _ := json.Marshal(map[string]any{"description": "no name"})
	req := httptest.NewRequest(http.MethodPost, "/apis", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateRestAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAPGWDeleteRestAPI_Returns204(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	req := httptest.NewRequest(http.MethodDelete, "/apis/abc123", nil)
	req = req.WithContext(newAPGWRouteCtx(map[string]string{"id": "abc123"}))
	w := httptest.NewRecorder()
	h.DeleteRestAPI(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["restApiId"] != "abc123" {
		t.Fatalf("expected restApiId=abc123, got %v", mock.lastNR.Params["restApiId"])
	}
}

func TestAPGWListResources_SetsRestApiId(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	req := httptest.NewRequest(http.MethodGet, "/apis/abc123/resources", nil)
	req = req.WithContext(newAPGWRouteCtx(map[string]string{"id": "abc123"}))
	w := httptest.NewRecorder()
	h.ListResources(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["restApiId"] != "abc123" {
		t.Fatalf("expected restApiId=abc123, got %v", mock.lastNR.Params["restApiId"])
	}
}

func TestAPGWListStages_SetsRestApiId(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	req := httptest.NewRequest(http.MethodGet, "/apis/abc123/stages", nil)
	req = req.WithContext(newAPGWRouteCtx(map[string]string{"id": "abc123"}))
	w := httptest.NewRecorder()
	h.ListStages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["restApiId"] != "abc123" {
		t.Fatalf("expected restApiId=abc123, got %v", mock.lastNR.Params["restApiId"])
	}
}

func TestAPGWCreateDeployment_Returns201(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	body, _ := json.Marshal(map[string]any{"stageName": "prod", "description": "v1"})
	req := httptest.NewRequest(http.MethodPost, "/apis/abc123/deployments", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(newAPGWRouteCtx(map[string]string{"id": "abc123"}))
	w := httptest.NewRecorder()
	h.CreateDeployment(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["restApiId"] != "abc123" {
		t.Fatalf("expected restApiId=abc123, got %v", mock.lastNR.Params["restApiId"])
	}
	if mock.lastNR.Params["stageName"] != "prod" {
		t.Fatalf("expected stageName=prod, got %v", mock.lastNR.Params["stageName"])
	}
}

func TestAPGWListDeployments_SetsRestApiId(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	req := httptest.NewRequest(http.MethodGet, "/apis/abc123/deployments", nil)
	req = req.WithContext(newAPGWRouteCtx(map[string]string{"id": "abc123"}))
	w := httptest.NewRecorder()
	h.ListDeployments(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["restApiId"] != "abc123" {
		t.Fatalf("expected restApiId=abc123, got %v", mock.lastNR.Params["restApiId"])
	}
}

func TestAPGWNR_ServiceIsApigateway(t *testing.T) {
	mock := &mockAPGWProvider{}
	h := NewHandler(mock, testAPGWCfg())

	req := httptest.NewRequest(http.MethodGet, "/apis", nil)
	w := httptest.NewRecorder()
	h.ListRestAPIs(w, req)

	if mock.lastNR.Service != "apigateway" {
		t.Fatalf("expected service=apigateway, got %s", mock.lastNR.Service)
	}
}
