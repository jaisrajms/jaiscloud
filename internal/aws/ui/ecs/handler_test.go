package ecsui

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

type mockECSProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockECSProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockECSProvider) ListClusters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"clusterArns": []any{}}}, nil
}

func (m *mockECSProvider) DescribeClusters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"clusters": []any{}}}, m.err
}

func (m *mockECSProvider) CreateCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"cluster": map[string]any{"clusterName": nr.Params["clusterName"]}}}, nil
}

func (m *mockECSProvider) DeleteCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockECSProvider) ListTasks(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"taskArns": []any{}}}, nil
}

func (m *mockECSProvider) RunTask(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"tasks": []any{}}}, nil
}

func (m *mockECSProvider) StopTask(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockECSProvider) ListServices(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"serviceArns": []any{}}}, nil
}

func testECSCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newECSRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestECSListClusters_Returns200(t *testing.T) {
	mock := &mockECSProvider{}
	h := NewHandler(mock, testECSCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListECSClustersResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestECSCreateCluster_Returns201(t *testing.T) {
	mock := &mockECSProvider{}
	h := NewHandler(mock, testECSCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-cluster"})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateCluster(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["clusterName"] != "my-cluster" {
		t.Fatalf("expected clusterName=my-cluster, got %v", mock.lastNR.Params["clusterName"])
	}
}

func TestECSDeleteCluster_Returns204(t *testing.T) {
	mock := &mockECSProvider{}
	h := NewHandler(mock, testECSCfg())

	req := httptest.NewRequest(http.MethodDelete, "/clusters/my-cluster", nil)
	req = req.WithContext(newECSRouteCtx(map[string]string{"name": "my-cluster"}))
	w := httptest.NewRecorder()
	h.DeleteCluster(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["cluster"] != "my-cluster" {
		t.Fatalf("expected cluster=my-cluster, got %v", mock.lastNR.Params["cluster"])
	}
}

func TestECSRunTask_Returns201(t *testing.T) {
	mock := &mockECSProvider{}
	h := NewHandler(mock, testECSCfg())

	body, _ := json.Marshal(map[string]any{"taskDefinition": "my-task:1"})
	req := httptest.NewRequest(http.MethodPost, "/clusters/my-cluster/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(newECSRouteCtx(map[string]string{"name": "my-cluster"}))
	w := httptest.NewRecorder()
	h.RunTask(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["taskDefinition"] != "my-task:1" {
		t.Fatalf("expected taskDefinition=my-task:1, got %v", mock.lastNR.Params["taskDefinition"])
	}
}

func TestECSRunTask_RequiresTaskDefinition(t *testing.T) {
	mock := &mockECSProvider{}
	h := NewHandler(mock, testECSCfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/clusters/my-cluster/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(newECSRouteCtx(map[string]string{"name": "my-cluster"}))
	w := httptest.NewRecorder()
	h.RunTask(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestECSNR_ServiceIsEcs(t *testing.T) {
	mock := &mockECSProvider{}
	h := NewHandler(mock, testECSCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if mock.lastNR.Service != "ecs" {
		t.Fatalf("expected service=ecs, got %s", mock.lastNR.Service)
	}
}
