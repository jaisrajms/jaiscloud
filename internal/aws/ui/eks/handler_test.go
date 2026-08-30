package eksui

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

type mockEKSProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockEKSProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockEKSProvider) ListClusters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"clusters": []any{}}}, nil
}

func (m *mockEKSProvider) CreateCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"cluster": map[string]any{"name": nr.Params["name"]}}}, nil
}

func (m *mockEKSProvider) DescribeCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"cluster": map[string]any{}}}, m.err
}

func (m *mockEKSProvider) DeleteCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testEKSCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newEKSRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestEKSListClusters_Returns200(t *testing.T) {
	mock := &mockEKSProvider{}
	h := NewHandler(mock, testEKSCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListClustersResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestEKSCreateCluster_Returns201(t *testing.T) {
	mock := &mockEKSProvider{}
	h := NewHandler(mock, testEKSCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-cluster"})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateCluster(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["name"] != "my-cluster" {
		t.Fatalf("expected name=my-cluster, got %v", mock.lastNR.Params["name"])
	}
}

func TestEKSCreateCluster_RequiresName(t *testing.T) {
	mock := &mockEKSProvider{}
	h := NewHandler(mock, testEKSCfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateCluster(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestEKSDeleteCluster_Returns204(t *testing.T) {
	mock := &mockEKSProvider{}
	h := NewHandler(mock, testEKSCfg())

	req := httptest.NewRequest(http.MethodDelete, "/clusters/my-cluster", nil)
	req = req.WithContext(newEKSRouteCtx(map[string]string{"name": "my-cluster"}))
	w := httptest.NewRecorder()
	h.DeleteCluster(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["name"] != "my-cluster" {
		t.Fatalf("expected name=my-cluster, got %v", mock.lastNR.Params["name"])
	}
}

func TestEKSNR_ServiceIsEks(t *testing.T) {
	mock := &mockEKSProvider{}
	h := NewHandler(mock, testEKSCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if mock.lastNR.Service != "eks" {
		t.Fatalf("expected service=eks, got %s", mock.lastNR.Service)
	}
}
