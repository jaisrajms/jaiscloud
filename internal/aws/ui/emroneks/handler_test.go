package emroneksui

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

type mockEMRCProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockEMRCProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockEMRCProvider) ListVirtualClusters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"virtualClusters": []any{}}}, nil
}

func (m *mockEMRCProvider) DescribeVirtualCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"virtualCluster": map[string]any{"id": "vc-1", "name": "test-vc"}}}, nil
}

func (m *mockEMRCProvider) CreateVirtualCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"id": "vc-new"}}, nil
}

func (m *mockEMRCProvider) DeleteVirtualCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEMRCProvider) ListJobRuns(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"jobRuns": []any{}}}, nil
}

func (m *mockEMRCProvider) DescribeJobRun(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"jobRun": map[string]any{"id": "jr-1"}}}, nil
}

func (m *mockEMRCProvider) StartJobRun(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"id": "jr-new"}}, nil
}

func (m *mockEMRCProvider) CancelJobRun(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testEMRCCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newEMRCRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestEMRCListVirtualClusters_Returns200(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodGet, "/virtual-clusters", nil)
	w := httptest.NewRecorder()
	h.ListVirtualClusters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListVirtualClustersResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestEMRCListVirtualClusters_StateFilter(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodGet, "/virtual-clusters?state=RUNNING", nil)
	w := httptest.NewRecorder()
	h.ListVirtualClusters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["states"] != "RUNNING" {
		t.Fatalf("expected states=RUNNING, got %v", mock.lastNR.Params["states"])
	}
}

func TestEMRCCreateVirtualCluster_Returns201(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	body, _ := json.Marshal(map[string]any{
		"name":         "my-vc",
		"eksClusterId": "my-eks",
		"namespace":    "spark",
	})
	req := httptest.NewRequest(http.MethodPost, "/virtual-clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateVirtualCluster(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["name"] != "my-vc" {
		t.Fatalf("expected name=my-vc, got %v", mock.lastNR.Params["name"])
	}
	cp, ok := mock.lastNR.Params["containerProvider"].(map[string]any)
	if !ok || cp["id"] != "my-eks" {
		t.Fatalf("expected containerProvider.id=my-eks, got %v", mock.lastNR.Params["containerProvider"])
	}
}

func TestEMRCCreateVirtualCluster_DefaultsNamespace(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-vc", "eksClusterId": "eks-1"})
	req := httptest.NewRequest(http.MethodPost, "/virtual-clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateVirtualCluster(w, req)

	cp, _ := mock.lastNR.Params["containerProvider"].(map[string]any)
	info, _ := cp["info"].(map[string]any)
	eksInfo, _ := info["eksInfo"].(map[string]any)
	if eksInfo["namespace"] != "default" {
		t.Fatalf("expected namespace=default, got %v", eksInfo["namespace"])
	}
}

func TestEMRCDeleteVirtualCluster_Returns204(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodDelete, "/virtual-clusters/vc-1", nil)
	req = req.WithContext(newEMRCRouteCtx(map[string]string{"id": "vc-1"}))
	w := httptest.NewRecorder()
	h.DeleteVirtualCluster(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["virtualClusterId"] != "vc-1" {
		t.Fatalf("expected virtualClusterId=vc-1, got %v", mock.lastNR.Params["virtualClusterId"])
	}
}

func TestEMRCStartJobRun_Returns201(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	body, _ := json.Marshal(map[string]any{
		"name":             "my-job",
		"releaseLabel":     "emr-6.10.0-latest",
		"executionRoleArn": "arn:aws:iam::000000000000:role/EMRRole",
	})
	req := httptest.NewRequest(http.MethodPost, "/virtual-clusters/vc-1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(newEMRCRouteCtx(map[string]string{"vcId": "vc-1"}))
	w := httptest.NewRecorder()
	h.StartJobRun(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["virtualClusterId"] != "vc-1" {
		t.Fatalf("expected virtualClusterId=vc-1")
	}
	if mock.lastNR.Params["name"] != "my-job" {
		t.Fatalf("expected name=my-job, got %v", mock.lastNR.Params["name"])
	}
}

func TestEMRCCancelJobRun_Returns204(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodDelete, "/virtual-clusters/vc-1/jobs/jr-1", nil)
	req = req.WithContext(newEMRCRouteCtx(map[string]string{"vcId": "vc-1", "jobId": "jr-1"}))
	w := httptest.NewRecorder()
	h.CancelJobRun(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["id"] != "jr-1" {
		t.Fatalf("expected id=jr-1, got %v", mock.lastNR.Params["id"])
	}
}

func TestEMRCDescribeJobRun_UsesIdParam(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodGet, "/virtual-clusters/vc-1/jobs/jr-1", nil)
	req = req.WithContext(newEMRCRouteCtx(map[string]string{"vcId": "vc-1", "jobId": "jr-1"}))
	w := httptest.NewRecorder()
	h.DescribeJobRun(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["id"] != "jr-1" {
		t.Fatalf("expected id=jr-1 (not jobId), got %v", mock.lastNR.Params["id"])
	}
}

func TestEMRCNR_ClockIsNonNil(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodGet, "/virtual-clusters", nil)
	w := httptest.NewRecorder()
	h.ListVirtualClusters(w, req)

	if mock.lastNR.Clock == nil {
		t.Fatal("nr.Clock must not be nil")
	}
}

func TestEMRCNR_ServiceIsEMRContainers(t *testing.T) {
	mock := &mockEMRCProvider{}
	h := NewHandler(mock, testEMRCCfg())

	req := httptest.NewRequest(http.MethodGet, "/virtual-clusters", nil)
	w := httptest.NewRecorder()
	h.ListVirtualClusters(w, req)

	if mock.lastNR.Service != "emr-containers" {
		t.Fatalf("expected service=emr-containers, got %s", mock.lastNR.Service)
	}
}
