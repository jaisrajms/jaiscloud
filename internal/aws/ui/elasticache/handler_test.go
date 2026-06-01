package elasticacheui

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

type mockElastiCacheProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockElastiCacheProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockElastiCacheProvider) DescribeCacheClusters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"CacheClusters": []any{}}}, nil
}

func (m *mockElastiCacheProvider) CreateCacheCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"CacheCluster": map[string]any{"CacheClusterId": nr.Params["CacheClusterId"]}}}, nil
}

func (m *mockElastiCacheProvider) DeleteCacheCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testElastiCacheCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newElastiCacheRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestElastiCacheListClusters_Returns200(t *testing.T) {
	mock := &mockElastiCacheProvider{}
	h := NewHandler(mock, testElastiCacheCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListCacheClusters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListCacheClustersResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestElastiCacheCreateCluster_Returns201(t *testing.T) {
	mock := &mockElastiCacheProvider{}
	h := NewHandler(mock, testElastiCacheCfg())

	body, _ := json.Marshal(map[string]any{"id": "my-cluster", "engine": "redis"})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateCacheCluster(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["CacheClusterId"] != "my-cluster" {
		t.Fatalf("expected CacheClusterId=my-cluster, got %v", mock.lastNR.Params["CacheClusterId"])
	}
	if mock.lastNR.Params["Engine"] != "redis" {
		t.Fatalf("expected Engine=redis, got %v", mock.lastNR.Params["Engine"])
	}
}

func TestElastiCacheCreateCluster_RequiresID(t *testing.T) {
	mock := &mockElastiCacheProvider{}
	h := NewHandler(mock, testElastiCacheCfg())

	body, _ := json.Marshal(map[string]any{"engine": "redis"})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateCacheCluster(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestElastiCacheDeleteCluster_Returns204(t *testing.T) {
	mock := &mockElastiCacheProvider{}
	h := NewHandler(mock, testElastiCacheCfg())

	req := httptest.NewRequest(http.MethodDelete, "/clusters/my-cluster", nil)
	req = req.WithContext(newElastiCacheRouteCtx(map[string]string{"id": "my-cluster"}))
	w := httptest.NewRecorder()
	h.DeleteCacheCluster(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["CacheClusterId"] != "my-cluster" {
		t.Fatalf("expected CacheClusterId=my-cluster, got %v", mock.lastNR.Params["CacheClusterId"])
	}
}

func TestElastiCacheNR_ServiceIsElastiCache(t *testing.T) {
	mock := &mockElastiCacheProvider{}
	h := NewHandler(mock, testElastiCacheCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListCacheClusters(w, req)

	if mock.lastNR.Service != "elasticache" {
		t.Fatalf("expected service=elasticache, got %s", mock.lastNR.Service)
	}
}
