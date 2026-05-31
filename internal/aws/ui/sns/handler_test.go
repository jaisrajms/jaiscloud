package snsui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

type mockSNSProvider struct {
	listTopicsResp              *model.ProviderResponse
	listTopicsErr               error
	createTopicResp             *model.ProviderResponse
	createTopicErr              error
	deleteTopicErr              error
	getTopicAttrsResp           *model.ProviderResponse
	getTopicAttrsErr            error
	subscribeResp               *model.ProviderResponse
	subscribeErr                error
	unsubscribeErr              error
	listSubsByTopicResp         *model.ProviderResponse
	listSubsByTopicErr          error
	publishResp                 *model.ProviderResponse
	publishErr                  error

	lastNR          *model.NormalizedRequest
	listTopicsNR    *model.NormalizedRequest
	createTopicNR   *model.NormalizedRequest
	publishNR       *model.NormalizedRequest
}

func (m *mockSNSProvider) ListTopics(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.listTopicsNR = nr
	m.lastNR = nr
	if m.listTopicsResp != nil {
		return m.listTopicsResp, m.listTopicsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Topics": []map[string]any{}}}, m.listTopicsErr
}
func (m *mockSNSProvider) CreateTopic(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createTopicNR = nr
	m.lastNR = nr
	if m.createTopicResp != nil {
		return m.createTopicResp, m.createTopicErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:test-topic",
	}}, m.createTopicErr
}
func (m *mockSNSProvider) DeleteTopic(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteTopicErr
}
func (m *mockSNSProvider) GetTopicAttributes(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getTopicAttrsResp != nil {
		return m.getTopicAttrsResp, m.getTopicAttrsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"Attributes": map[string]string{},
	}}, m.getTopicAttrsErr
}
func (m *mockSNSProvider) Subscribe(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.subscribeResp != nil {
		return m.subscribeResp, m.subscribeErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"SubscriptionArn": "arn:aws:sns:us-east-1:000000000000:test-topic:sub-123",
	}}, m.subscribeErr
}
func (m *mockSNSProvider) Unsubscribe(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.unsubscribeErr
}
func (m *mockSNSProvider) ListSubscriptionsByTopic(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listSubsByTopicResp != nil {
		return m.listSubsByTopicResp, m.listSubsByTopicErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"Subscriptions": []map[string]any{},
	}}, m.listSubsByTopicErr
}
func (m *mockSNSProvider) ListSubscriptions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Subscriptions": []map[string]any{}}}, nil
}
func (m *mockSNSProvider) Publish(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.publishNR = nr
	m.lastNR = nr
	if m.publishResp != nil {
		return m.publishResp, m.publishErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"MessageId": "msg-123"}}, m.publishErr
}
func (m *mockSNSProvider) ListTagsForResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Tags": []map[string]any{}}}, nil
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

// withChiParam sets a chi URL param on the request context.
func withChiParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		rctx = chi.NewRouteContext()
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	}
	rctx.URLParams.Add(key, value)
	return r
}

const testARN = "arn:aws:sns:us-east-1:000000000000:my-topic"

// ── ListTopics ────────────────────────────────────────────────────────────────

func TestSNSListTopics_Returns200WithItems(t *testing.T) {
	mock := &mockSNSProvider{
		listTopicsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Topics": []map[string]any{
				{"TopicArn": testARN},
			},
		}},
		getTopicAttrsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Attributes": map[string]string{"DisplayName": "My Topic"},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rr := httptest.NewRecorder()
	h.ListTopics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListTopicsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("want 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ARN != testARN {
		t.Errorf("want ARN=%q, got %q", testARN, resp.Items[0].ARN)
	}
}

func TestSNSListTopics_EmptyReturns200(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rr := httptest.NewRecorder()
	h.ListTopics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListTopicsResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Items == nil {
		t.Error("items should be non-nil empty slice")
	}
}

func TestSNSListTopics_ProviderErrorReturnsErrorShape(t *testing.T) {
	mock := &mockSNSProvider{
		listTopicsErr: model.NewProviderError("AccessDenied", "not allowed", 403),
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rr := httptest.NewRecorder()
	h.ListTopics(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d", rr.Code)
	}
}

// ── CreateTopic ───────────────────────────────────────────────────────────────

func TestSNSCreateTopic_ValidBodyReturns201(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	body := `{"name":"my-topic"}`
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateTopic(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestSNSCreateTopic_MissingNameReturns400(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewBufferString(`{"name":""}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateTopic(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestSNSCreateTopic_FIFOAppendsSuffix(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	body := `{"name":"my-topic","fifo":true}`
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.CreateTopic(httptest.NewRecorder(), req)

	if mock.createTopicNR == nil {
		t.Fatal("createTopicNR was not captured")
	}
	name, _ := mock.createTopicNR.Params["Name"].(string)
	if name != "my-topic.fifo" {
		t.Errorf("want name=my-topic.fifo for FIFO topic, got %q", name)
	}
}

// ── DeleteTopic ───────────────────────────────────────────────────────────────

func TestSNSDeleteTopic_MissingArnReturns400(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodDelete, "/topics", nil)
	rr := httptest.NewRecorder()
	h.DeleteTopic(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestSNSDeleteTopic_Returns204(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	encodedARN := url.QueryEscape(testARN)
	req := httptest.NewRequest(http.MethodDelete, "/topics?arn="+encodedARN, nil)
	rr := httptest.NewRecorder()
	h.DeleteTopic(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

// ── Publish ───────────────────────────────────────────────────────────────────

func TestSNSPublish_Returns200WithMessageId(t *testing.T) {
	mock := &mockSNSProvider{
		publishResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"MessageId": "msg-abc",
		}},
	}
	h := NewHandler(mock, testCfg())
	encodedARN := url.QueryEscape(testARN)
	body := `{"message":"hello world"}`
	req := httptest.NewRequest(http.MethodPost, "/topics/publish?arn="+encodedARN, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Publish(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestSNSPublish_MissingMessageReturns400(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	encodedARN := url.QueryEscape(testARN)
	body := `{"message":""}`
	req := httptest.NewRequest(http.MethodPost, "/topics/publish?arn="+encodedARN, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Publish(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestSNSPublish_MissingArnReturns400(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	body := `{"message":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/topics/publish", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Publish(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

// ── Subscribe ─────────────────────────────────────────────────────────────────

func TestSNSSubscribe_Returns201(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	encodedARN := url.QueryEscape(testARN)
	body := `{"protocol":"sqs","endpoint":"arn:aws:sqs:us-east-1:000000000000:q1"}`
	req := httptest.NewRequest(http.MethodPost, "/topics/subscribe?arn="+encodedARN, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Subscribe(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestSNSSubscribe_MissingProtocolReturns400(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	encodedARN := url.QueryEscape(testARN)
	body := `{"endpoint":"arn:aws:sqs:..."}`
	req := httptest.NewRequest(http.MethodPost, "/topics/subscribe?arn="+encodedARN, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Subscribe(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

// ── NR invariant tests ────────────────────────────────────────────────────────

func TestSNSNR_PortIsWirePortNotUIPort(t *testing.T) {
	cfg := testCfg()
	mock := &mockSNSProvider{}
	h := NewHandler(mock, cfg)
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	h.ListTopics(httptest.NewRecorder(), req)

	if mock.listTopicsNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listTopicsNR.Port != cfg.Port {
		t.Errorf("nr.Port = %d, want wire port %d (not UI port %d)", mock.listTopicsNR.Port, cfg.Port, cfg.UIPort)
	}
}

func TestSNSNR_ClockIsNotNil(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	h.ListTopics(httptest.NewRecorder(), req)

	if mock.listTopicsNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listTopicsNR.Clock == nil {
		t.Error("nr.Clock must not be nil")
	}
}

func TestSNSPublish_NrParamsTopicArnKey(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	encodedARN := url.QueryEscape(testARN)
	body := `{"message":"hi"}`
	req := httptest.NewRequest(http.MethodPost, "/topics/publish?arn="+encodedARN, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.Publish(httptest.NewRecorder(), req)

	if mock.publishNR == nil {
		t.Fatal("publishNR was not captured")
	}
	if v, ok := mock.publishNR.Params["TopicArn"]; !ok || v != testARN {
		t.Errorf("nr.Params[TopicArn] = %v, want %q", mock.publishNR.Params["TopicArn"], testARN)
	}
}

func TestSNSCreateTopic_NrParamsNameKey(t *testing.T) {
	mock := &mockSNSProvider{}
	h := NewHandler(mock, testCfg())
	body := `{"name":"my-queue"}`
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.CreateTopic(httptest.NewRecorder(), req)

	if mock.createTopicNR == nil {
		t.Fatal("createTopicNR was not captured")
	}
	if v, ok := mock.createTopicNR.Params["Name"]; !ok || v == "" {
		t.Error("nr.Params must contain key Name (capital N)")
	}
	if _, ok := mock.createTopicNR.Params["name"]; ok {
		t.Error("nr.Params must NOT contain lowercase name key")
	}
}
