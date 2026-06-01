package kinesisui

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

type mockKinesisProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockKinesisProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockKinesisProvider) ListStreams(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"StreamNames":     []any{},
		"StreamSummaries": []map[string]any{},
		"HasMoreStreams":   false,
	}}, nil
}

func (m *mockKinesisProvider) CreateStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (m *mockKinesisProvider) DeleteStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockKinesisProvider) DescribeStreamSummary(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testKinesisCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newKinesisRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestKinesisListStreams_Returns200(t *testing.T) {
	mock := &mockKinesisProvider{}
	h := NewHandler(mock, testKinesisCfg())

	req := httptest.NewRequest(http.MethodGet, "/streams", nil)
	w := httptest.NewRecorder()
	h.ListStreams(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListStreamsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestKinesisCreateStream_Returns201(t *testing.T) {
	mock := &mockKinesisProvider{}
	h := NewHandler(mock, testKinesisCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-stream", "shardCount": 1})
	req := httptest.NewRequest(http.MethodPost, "/streams", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStream(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["StreamName"] != "my-stream" {
		t.Fatalf("expected StreamName=my-stream, got %v", mock.lastNR.Params["StreamName"])
	}
}

func TestKinesisCreateStream_RequiresName(t *testing.T) {
	mock := &mockKinesisProvider{}
	h := NewHandler(mock, testKinesisCfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/streams", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateStream(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestKinesisDeleteStream_Returns204(t *testing.T) {
	mock := &mockKinesisProvider{}
	h := NewHandler(mock, testKinesisCfg())

	req := httptest.NewRequest(http.MethodDelete, "/streams/my-stream", nil)
	req = req.WithContext(newKinesisRouteCtx(map[string]string{"name": "my-stream"}))
	w := httptest.NewRecorder()
	h.DeleteStream(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["StreamName"] != "my-stream" {
		t.Fatalf("expected StreamName=my-stream, got %v", mock.lastNR.Params["StreamName"])
	}
}

func TestKinesisNR_ServiceIsKinesis(t *testing.T) {
	mock := &mockKinesisProvider{}
	h := NewHandler(mock, testKinesisCfg())

	req := httptest.NewRequest(http.MethodGet, "/streams", nil)
	w := httptest.NewRecorder()
	h.ListStreams(w, req)

	if mock.lastNR.Service != "kinesis" {
		t.Fatalf("expected service=kinesis, got %s", mock.lastNR.Service)
	}
}
