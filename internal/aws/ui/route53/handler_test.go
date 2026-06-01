package route53ui

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

type mockRoute53Provider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockRoute53Provider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockRoute53Provider) ListHostedZones(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"HostedZones": []any{}}}, nil
}

func (m *mockRoute53Provider) CreateHostedZone(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"HostedZone": map[string]any{"Id": "/hostedzone/Z123"}}}, nil
}

func (m *mockRoute53Provider) DeleteHostedZone(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockRoute53Provider) ListResourceRecordSets(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"ResourceRecordSets": []any{}}}, nil
}

func testRoute53Cfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newRoute53RouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestRoute53ListZones_Returns200(t *testing.T) {
	mock := &mockRoute53Provider{}
	h := NewHandler(mock, testRoute53Cfg())

	req := httptest.NewRequest(http.MethodGet, "/zones", nil)
	w := httptest.NewRecorder()
	h.ListHostedZones(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListHostedZonesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestRoute53CreateZone_Returns201(t *testing.T) {
	mock := &mockRoute53Provider{}
	h := NewHandler(mock, testRoute53Cfg())

	body, _ := json.Marshal(map[string]any{"name": "example.com"})
	req := httptest.NewRequest(http.MethodPost, "/zones", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateHostedZone(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "example.com" {
		t.Fatalf("expected Name=example.com, got %v", mock.lastNR.Params["Name"])
	}
}

func TestRoute53CreateZone_RequiresName(t *testing.T) {
	mock := &mockRoute53Provider{}
	h := NewHandler(mock, testRoute53Cfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/zones", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateHostedZone(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRoute53DeleteZone_Returns204(t *testing.T) {
	mock := &mockRoute53Provider{}
	h := NewHandler(mock, testRoute53Cfg())

	req := httptest.NewRequest(http.MethodDelete, "/zones/Z123", nil)
	req = req.WithContext(newRoute53RouteCtx(map[string]string{"id": "Z123"}))
	w := httptest.NewRecorder()
	h.DeleteHostedZone(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["Id"] != "Z123" {
		t.Fatalf("expected Id=Z123, got %v", mock.lastNR.Params["Id"])
	}
}

func TestRoute53ListRecords_SetsZoneId(t *testing.T) {
	mock := &mockRoute53Provider{}
	h := NewHandler(mock, testRoute53Cfg())

	req := httptest.NewRequest(http.MethodGet, "/zones/Z123/records", nil)
	req = req.WithContext(newRoute53RouteCtx(map[string]string{"id": "Z123"}))
	w := httptest.NewRecorder()
	h.ListResourceRecordSets(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if mock.lastNR.Params["HostedZoneId"] != "Z123" {
		t.Fatalf("expected HostedZoneId=Z123, got %v", mock.lastNR.Params["HostedZoneId"])
	}
}

func TestRoute53NR_ServiceIsRoute53(t *testing.T) {
	mock := &mockRoute53Provider{}
	h := NewHandler(mock, testRoute53Cfg())

	req := httptest.NewRequest(http.MethodGet, "/zones", nil)
	w := httptest.NewRecorder()
	h.ListHostedZones(w, req)

	if mock.lastNR.Service != "route53" {
		t.Fatalf("expected service=route53, got %s", mock.lastNR.Service)
	}
}
