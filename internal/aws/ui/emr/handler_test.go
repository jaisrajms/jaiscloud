package emrui

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

type mockEMRProvider struct {
	resp    *model.ProviderResponse
	err     error
	lastNR  *model.NormalizedRequest
}

func (m *mockEMRProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockEMRProvider) ListClusters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Clusters": []any{}}}, nil
}

func (m *mockEMRProvider) DescribeCluster(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Cluster": map[string]any{"Id": "j-123", "Name": "test-cluster"}}}, nil
}

func (m *mockEMRProvider) RunJobFlow(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"JobFlowId": "j-456"}}, nil
}

func (m *mockEMRProvider) TerminateJobFlows(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEMRProvider) ListSteps(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Steps": []any{}}}, nil
}

func (m *mockEMRProvider) DescribeStep(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Step": map[string]any{"Id": "s-1"}}}, nil
}

func (m *mockEMRProvider) AddJobFlowSteps(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"StepIds": []any{"s-1"}}}, nil
}

func (m *mockEMRProvider) CancelSteps(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testEMRCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func TestEMRListClusters_Returns200(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

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

func TestEMRListClusters_StateFilter(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters?state=RUNNING", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	states, ok := mock.lastNR.Params["ClusterStates"].([]any)
	if !ok || len(states) != 1 || states[0] != "RUNNING" {
		t.Fatalf("expected ClusterStates=[RUNNING], got %v", mock.lastNR.Params["ClusterStates"])
	}
}

func TestEMRRunJobFlow_Returns201(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	body, _ := json.Marshal(map[string]any{
		"name":         "my-cluster",
		"releaseLabel": "emr-6.10.0",
	})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.RunJobFlow(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "my-cluster" {
		t.Fatalf("expected Name=my-cluster, got %v", mock.lastNR.Params["Name"])
	}
	if mock.lastNR.Params["ReleaseLabel"] != "emr-6.10.0" {
		t.Fatalf("expected ReleaseLabel=emr-6.10.0, got %v", mock.lastNR.Params["ReleaseLabel"])
	}
}

func TestEMRRunJobFlow_RequiresName(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	body, _ := json.Marshal(map[string]any{"releaseLabel": "emr-6.10.0"})
	req := httptest.NewRequest(http.MethodPost, "/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.RunJobFlow(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestEMRTerminateCluster_Returns204(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	req := httptest.NewRequest(http.MethodDelete, "/clusters/j-123", nil)
	rctx := newRouteCtx(map[string]string{"id": "j-123"})
	req = req.WithContext(rctx)
	w := httptest.NewRecorder()
	h.TerminateCluster(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	jobFlowIds, ok := mock.lastNR.Params["JobFlowIds"].([]any)
	if !ok || len(jobFlowIds) != 1 || jobFlowIds[0] != "j-123" {
		t.Fatalf("expected JobFlowIds=[j-123], got %v", mock.lastNR.Params["JobFlowIds"])
	}
}

func TestEMRAddSteps_UsesJobFlowId(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	body, _ := json.Marshal(map[string]any{"steps": []any{map[string]any{"Name": "step1"}}})
	req := httptest.NewRequest(http.MethodPost, "/clusters/j-123/steps", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rctx := newRouteCtx(map[string]string{"id": "j-123"})
	req = req.WithContext(rctx)
	w := httptest.NewRecorder()
	h.AddSteps(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["JobFlowId"] != "j-123" {
		t.Fatalf("expected JobFlowId=j-123, got %v", mock.lastNR.Params["JobFlowId"])
	}
}

func TestEMRCancelStep_Returns204(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	req := httptest.NewRequest(http.MethodPost, "/clusters/j-123/steps/s-1/cancel", nil)
	rctx := newRouteCtx(map[string]string{"id": "j-123", "stepId": "s-1"})
	req = req.WithContext(rctx)
	w := httptest.NewRecorder()
	h.CancelStep(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	stepIds, ok := mock.lastNR.Params["StepIds"].([]any)
	if !ok || len(stepIds) != 1 || stepIds[0] != "s-1" {
		t.Fatalf("expected StepIds=[s-1], got %v", mock.lastNR.Params["StepIds"])
	}
}

func TestEMRNR_ClockIsNonNil(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if mock.lastNR.Clock == nil {
		t.Fatal("nr.Clock must not be nil")
	}
}

func newRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestEMRNR_ServiceIsEMR(t *testing.T) {
	mock := &mockEMRProvider{}
	h := NewHandler(mock, testEMRCfg())

	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	w := httptest.NewRecorder()
	h.ListClusters(w, req)

	if mock.lastNR.Service != "emr" {
		t.Fatalf("expected service=emr, got %s", mock.lastNR.Service)
	}
}
