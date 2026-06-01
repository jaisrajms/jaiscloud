package firehoseui

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

type mockFirehoseProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockFirehoseProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockFirehoseProvider) ListDeliveryStreams(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	if m.resp != nil {
		return m.resp, nil
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"DeliveryStreamNames":    []string{},
		"HasMoreDeliveryStreams": false,
	}}, nil
}

func (m *mockFirehoseProvider) CreateDeliveryStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	if m.err != nil {
		return nil, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"DeliveryStreamARN": "arn:aws:firehose:us-east-1:000000000000:deliverystream/test"}}, nil
}

func (m *mockFirehoseProvider) DeleteDeliveryStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockFirehoseProvider) DescribeDeliveryStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func testFirehoseCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func newFirehoseRouteCtx(params map[string]string) context.Context {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return context.WithValue(context.Background(), chi.RouteCtxKey, rctx)
}

func TestFirehoseListStreams_Returns200(t *testing.T) {
	mock := &mockFirehoseProvider{}
	h := NewHandler(mock, testFirehoseCfg())

	req := httptest.NewRequest(http.MethodGet, "/streams", nil)
	w := httptest.NewRecorder()
	h.ListDeliveryStreams(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListDeliveryStreamsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Items == nil {
		t.Fatal("expected non-nil items")
	}
}

func TestFirehoseCreateStream_Returns201(t *testing.T) {
	mock := &mockFirehoseProvider{}
	h := NewHandler(mock, testFirehoseCfg())

	body, _ := json.Marshal(map[string]any{"name": "my-stream"})
	req := httptest.NewRequest(http.MethodPost, "/streams", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateDeliveryStream(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if mock.lastNR.Params["DeliveryStreamName"] != "my-stream" {
		t.Fatalf("expected DeliveryStreamName=my-stream, got %v", mock.lastNR.Params["DeliveryStreamName"])
	}
}

func TestFirehoseCreateStream_RequiresName(t *testing.T) {
	mock := &mockFirehoseProvider{}
	h := NewHandler(mock, testFirehoseCfg())

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/streams", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateDeliveryStream(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestFirehoseDeleteStream_Returns204(t *testing.T) {
	mock := &mockFirehoseProvider{}
	h := NewHandler(mock, testFirehoseCfg())

	req := httptest.NewRequest(http.MethodDelete, "/streams/my-stream", nil)
	req = req.WithContext(newFirehoseRouteCtx(map[string]string{"name": "my-stream"}))
	w := httptest.NewRecorder()
	h.DeleteDeliveryStream(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if mock.lastNR.Params["DeliveryStreamName"] != "my-stream" {
		t.Fatalf("expected DeliveryStreamName=my-stream, got %v", mock.lastNR.Params["DeliveryStreamName"])
	}
}

func TestFirehoseNR_ServiceIsFirehose(t *testing.T) {
	mock := &mockFirehoseProvider{}
	h := NewHandler(mock, testFirehoseCfg())

	req := httptest.NewRequest(http.MethodGet, "/streams", nil)
	w := httptest.NewRecorder()
	h.ListDeliveryStreams(w, req)

	if mock.lastNR.Service != "firehose" {
		t.Fatalf("expected service=firehose, got %s", mock.lastNR.Service)
	}
}
