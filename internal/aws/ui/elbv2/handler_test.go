package elbv2ui

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

type mockELBv2Provider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockELBv2Provider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockELBv2Provider) DescribeLoadBalancers(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"LoadBalancers": []any{}}}, nil
}

func (m *mockELBv2Provider) CreateLoadBalancer(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"LoadBalancers": []any{map[string]any{"LoadBalancerName": nr.Params["Name"]}},
	}}, nil
}

func (m *mockELBv2Provider) DeleteLoadBalancer(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockELBv2Provider) DescribeTargetGroups(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"TargetGroups": []any{}}}, m.err
}

func testELBv2Cfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newELBv2RouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestELBv2ListLoadBalancers_Returns200(t *testing.T) {
	mock := &mockELBv2Provider{}
	h := NewHandler(mock, testELBv2Cfg())

	req := httptest.NewRequest(http.MethodGet, "/load-balancers", nil)
	w := httptest.NewRecorder()
	h.ListLoadBalancers(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListLoadBalancersResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestELBv2CreateLoadBalancer_Returns201(t *testing.T) {
	mock := &mockELBv2Provider{}
	h := NewHandler(mock, testELBv2Cfg())

	body, _ := json.Marshal(map[string]any{"name": "my-lb", "type": "application"})
	req := httptest.NewRequest(http.MethodPost, "/load-balancers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateLoadBalancer(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["Name"] != "my-lb" {
		t.Fatalf("expected Name=my-lb, got %v", mock.lastNR.Params["Name"])
	}
}

func TestELBv2CreateLoadBalancer_RequiresName(t *testing.T) {
	mock := &mockELBv2Provider{}
	h := NewHandler(mock, testELBv2Cfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/load-balancers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateLoadBalancer(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestELBv2DeleteLoadBalancer_Returns204(t *testing.T) {
	mock := &mockELBv2Provider{}
	h := NewHandler(mock, testELBv2Cfg())

	testARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/1234"
	req := httptest.NewRequest(http.MethodDelete, "/load-balancers/"+testARN, nil)
	req = req.WithContext(newELBv2RouteCtx(map[string]string{"arn": testARN}))
	w := httptest.NewRecorder()
	h.DeleteLoadBalancer(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["LoadBalancerArn"] != testARN {
		t.Fatalf("expected LoadBalancerArn=%s, got %v", testARN, mock.lastNR.Params["LoadBalancerArn"])
	}
}

func TestELBv2NR_ServiceIsELB(t *testing.T) {
	mock := &mockELBv2Provider{}
	h := NewHandler(mock, testELBv2Cfg())

	req := httptest.NewRequest(http.MethodGet, "/load-balancers", nil)
	w := httptest.NewRecorder()
	h.ListLoadBalancers(w, req)

	if mock.lastNR.Service != "elasticloadbalancing" {
		t.Fatalf("expected service=elasticloadbalancing, got %s", mock.lastNR.Service)
	}
}
