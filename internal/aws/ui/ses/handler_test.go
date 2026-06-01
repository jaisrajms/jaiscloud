package sesui

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

type mockSESProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockSESProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockSESProvider) ListIdentities(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Identities": []string{}}}, nil
}

func (m *mockSESProvider) VerifyEmailIdentity(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (m *mockSESProvider) DeleteIdentity(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockSESProvider) GetSendQuota(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testSESCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newSESRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestSESListIdentities_Returns200(t *testing.T) {
	mock := &mockSESProvider{}
	h := NewHandler(mock, testSESCfg())

	req := httptest.NewRequest(http.MethodGet, "/identities", nil)
	w := httptest.NewRecorder()
	h.ListIdentities(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListIdentitiesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestSESVerifyEmailIdentity_Returns201(t *testing.T) {
	mock := &mockSESProvider{}
	h := NewHandler(mock, testSESCfg())

	body, _ := json.Marshal(map[string]any{"identity": "user@example.com"})
	req := httptest.NewRequest(http.MethodPost, "/identities", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.VerifyEmailIdentity(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["EmailAddress"] != "user@example.com" {
		t.Fatalf("expected EmailAddress=user@example.com, got %v", mock.lastNR.Params["EmailAddress"])
	}
}

func TestSESVerifyEmailIdentity_RequiresIdentity(t *testing.T) {
	mock := &mockSESProvider{}
	h := NewHandler(mock, testSESCfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/identities", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.VerifyEmailIdentity(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestSESDeleteIdentity_Returns204(t *testing.T) {
	mock := &mockSESProvider{}
	h := NewHandler(mock, testSESCfg())

	req := httptest.NewRequest(http.MethodDelete, "/identities/user@example.com", nil)
	req = req.WithContext(newSESRouteCtx(map[string]string{"identity": "user@example.com"}))
	w := httptest.NewRecorder()
	h.DeleteIdentity(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["Identity"] != "user@example.com" {
		t.Fatalf("expected Identity=user@example.com, got %v", mock.lastNR.Params["Identity"])
	}
}

func TestSESNR_ServiceIsEmail(t *testing.T) {
	mock := &mockSESProvider{}
	h := NewHandler(mock, testSESCfg())

	req := httptest.NewRequest(http.MethodGet, "/identities", nil)
	w := httptest.NewRecorder()
	h.ListIdentities(w, req)

	if mock.lastNR.Service != "email" {
		t.Fatalf("expected service=email, got %s", mock.lastNR.Service)
	}
}
