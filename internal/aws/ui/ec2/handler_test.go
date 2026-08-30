package ec2ui

import (
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

type mockEC2Provider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockEC2Provider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockEC2Provider) DescribeInstances(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Reservations": []any{}}}, nil
}

func (m *mockEC2Provider) TerminateInstances(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEC2Provider) StartInstances(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockEC2Provider) StopInstances(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testEC2Cfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newEC2RouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestEC2ListInstances_Returns200(t *testing.T) {
	mock := &mockEC2Provider{}
	h := NewHandler(mock, testEC2Cfg())

	req := httptest.NewRequest(http.MethodGet, "/instances", nil)
	w := httptest.NewRecorder()
	h.ListInstances(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListInstancesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestEC2ListInstances_FlattensReservations(t *testing.T) {
	mock := &mockEC2Provider{
		resp: &model.ProviderResponse{
			HTTPStatus: 200,
			Data: map[string]any{
				"Reservations": []any{
					map[string]any{
						"Instances": []any{
							map[string]any{"InstanceId": "i-001", "State": map[string]any{"Name": "running"}},
							map[string]any{"InstanceId": "i-002", "State": map[string]any{"Name": "stopped"}},
						},
					},
				},
			},
		},
	}
	h := NewHandler(mock, testEC2Cfg())

	req := httptest.NewRequest(http.MethodGet, "/instances", nil)
	w := httptest.NewRecorder()
	h.ListInstances(w, req)

	var resp ListInstancesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "i-001" {
		t.Fatalf("expected i-001, got %s", resp.Items[0].ID)
	}
}

func TestEC2TerminateInstance_Returns204(t *testing.T) {
	mock := &mockEC2Provider{}
	h := NewHandler(mock, testEC2Cfg())

	req := httptest.NewRequest(http.MethodDelete, "/instances/i-001", nil)
	req = req.WithContext(newEC2RouteCtx(map[string]string{"id": "i-001"}))
	w := httptest.NewRecorder()
	h.TerminateInstance(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["InstanceId.1"] != "i-001" {
		t.Fatalf("expected InstanceId.1=i-001, got %v", mock.lastNR.Params["InstanceId.1"])
	}
}

func TestEC2StartInstance_Returns204(t *testing.T) {
	mock := &mockEC2Provider{}
	h := NewHandler(mock, testEC2Cfg())

	req := httptest.NewRequest(http.MethodPost, "/instances/i-001/start", nil)
	req = req.WithContext(newEC2RouteCtx(map[string]string{"id": "i-001"}))
	w := httptest.NewRecorder()
	h.StartInstance(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestEC2NR_ServiceIsEc2(t *testing.T) {
	mock := &mockEC2Provider{}
	h := NewHandler(mock, testEC2Cfg())

	req := httptest.NewRequest(http.MethodGet, "/instances", nil)
	w := httptest.NewRecorder()
	h.ListInstances(w, req)

	if mock.lastNR.Service != "ec2" {
		t.Fatalf("expected service=ec2, got %s", mock.lastNR.Service)
	}
}
