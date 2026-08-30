package rdsui

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

type mockRDSProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockRDSProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockRDSProvider) DescribeDBInstances(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"DBInstances": []any{}}}, nil
}

func (m *mockRDSProvider) CreateDBInstance(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"DBInstance": map[string]any{"DBInstanceIdentifier": nr.Params["DBInstanceIdentifier"]}}}, nil
}

func (m *mockRDSProvider) DeleteDBInstance(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockRDSProvider) StartDBInstance(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockRDSProvider) StopDBInstance(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testRDSCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newRDSRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestRDSListInstances_Returns200(t *testing.T) {
	mock := &mockRDSProvider{}
	h := NewHandler(mock, testRDSCfg())

	req := httptest.NewRequest(http.MethodGet, "/instances", nil)
	w := httptest.NewRecorder()
	h.ListDBInstances(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListDBInstancesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestRDSCreateInstance_Returns201(t *testing.T) {
	mock := &mockRDSProvider{}
	h := NewHandler(mock, testRDSCfg())

	body, _ := json.Marshal(map[string]any{"id": "my-db", "engine": "mysql", "class": "db.t3.micro"})
	req := httptest.NewRequest(http.MethodPost, "/instances", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateDBInstance(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["DBInstanceIdentifier"] != "my-db" {
		t.Fatalf("expected DBInstanceIdentifier=my-db, got %v", mock.lastNR.Params["DBInstanceIdentifier"])
	}
	if mock.lastNR.Params["Engine"] != "mysql" {
		t.Fatalf("expected Engine=mysql, got %v", mock.lastNR.Params["Engine"])
	}
}

func TestRDSCreateInstance_RequiresID(t *testing.T) {
	mock := &mockRDSProvider{}
	h := NewHandler(mock, testRDSCfg())

	body, _ := json.Marshal(map[string]any{"engine": "mysql"})
	req := httptest.NewRequest(http.MethodPost, "/instances", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateDBInstance(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRDSDeleteInstance_Returns204(t *testing.T) {
	mock := &mockRDSProvider{}
	h := NewHandler(mock, testRDSCfg())

	req := httptest.NewRequest(http.MethodDelete, "/instances/my-db", nil)
	req = req.WithContext(newRDSRouteCtx(map[string]string{"id": "my-db"}))
	w := httptest.NewRecorder()
	h.DeleteDBInstance(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["DBInstanceIdentifier"] != "my-db" {
		t.Fatalf("expected DBInstanceIdentifier=my-db, got %v", mock.lastNR.Params["DBInstanceIdentifier"])
	}
}

func TestRDSStartInstance_Returns204(t *testing.T) {
	mock := &mockRDSProvider{}
	h := NewHandler(mock, testRDSCfg())

	req := httptest.NewRequest(http.MethodPost, "/instances/my-db/start", nil)
	req = req.WithContext(newRDSRouteCtx(map[string]string{"id": "my-db"}))
	w := httptest.NewRecorder()
	h.StartDBInstance(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestRDSNR_ServiceIsRds(t *testing.T) {
	mock := &mockRDSProvider{}
	h := NewHandler(mock, testRDSCfg())

	req := httptest.NewRequest(http.MethodGet, "/instances", nil)
	w := httptest.NewRecorder()
	h.ListDBInstances(w, req)

	if mock.lastNR.Service != "rds" {
		t.Fatalf("expected service=rds, got %s", mock.lastNR.Service)
	}
}
