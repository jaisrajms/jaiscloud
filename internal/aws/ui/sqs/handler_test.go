package sqsui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// mockQueueProvider is a hand-written mock of ProviderInterface.
type mockQueueProvider struct {
	listQueuesResp  *model.ProviderResponse
	listQueuesErr   error
	createQueueResp *model.ProviderResponse
	createQueueErr  error
	deleteQueueErr  error
	getAttrsResp    *model.ProviderResponse
	getAttrsErr     error
	purgeQueueErr   error
	sendMsgResp     *model.ProviderResponse
	sendMsgErr      error
	recvMsgsResp    *model.ProviderResponse
	recvMsgsErr     error
	deleteMsgErr    error
	dlqSourcesResp  *model.ProviderResponse
	dlqSourcesErr   error
	tagsResp        *model.ProviderResponse
	tagsErr         error

	// Per-method NR captures for param key assertions.
	listQueuesNR  *model.NormalizedRequest
	createQueueNR *model.NormalizedRequest
	lastNR        *model.NormalizedRequest // last call of any method
}

func (m *mockQueueProvider) ListQueues(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.listQueuesNR = nr
	m.lastNR = nr
	return m.listQueuesResp, m.listQueuesErr
}
func (m *mockQueueProvider) CreateQueue(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createQueueNR = nr
	m.lastNR = nr
	return m.createQueueResp, m.createQueueErr
}
func (m *mockQueueProvider) DeleteQueue(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteQueueErr
}
func (m *mockQueueProvider) GetQueueUrl(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
func (m *mockQueueProvider) GetQueueAttributes(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getAttrsResp != nil {
		return m.getAttrsResp, m.getAttrsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"Attributes": map[string]string{},
	}}, m.getAttrsErr
}
func (m *mockQueueProvider) PurgeQueue(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.purgeQueueErr
}
func (m *mockQueueProvider) SendMessage(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.sendMsgResp, m.sendMsgErr
}
func (m *mockQueueProvider) ReceiveMessage(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.recvMsgsResp, m.recvMsgsErr
}
func (m *mockQueueProvider) DeleteMessage(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteMsgErr
}
func (m *mockQueueProvider) ListDeadLetterSourceQueues(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.dlqSourcesResp, m.dlqSourcesErr
}
func (m *mockQueueProvider) ListQueueTags(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.tagsResp, m.tagsErr
}
func (m *mockQueueProvider) PeekMessages(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Messages": []any{}, "Total": 0}}, nil
}

// testCfg returns a Config with Port != UIPort to verify the wire-port invariant.
func testCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func TestListQueues_Returns200WithItems(t *testing.T) {
	mock := &mockQueueProvider{
		listQueuesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrls": []string{"http://localhost:4566/000000000000/my-queue"},
		}},
		getAttrsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Attributes": map[string]string{
				"QueueArn":                    "arn:aws:sqs:us-east-1:000000000000:my-queue",
				"ApproximateNumberOfMessages": "5",
			},
		}},
	}
	h := NewHandler(mock, testCfg())

	req := httptest.NewRequest(http.MethodGet, "/queues", nil)
	rr := httptest.NewRecorder()
	h.ListQueues(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListQueuesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("want 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].MessagesAvailable != 5 {
		t.Errorf("want 5 messages, got %d", resp.Items[0].MessagesAvailable)
	}
}

func TestListQueues_EmptyReturns200WithEmptyArray(t *testing.T) {
	mock := &mockQueueProvider{
		listQueuesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrls": []string{},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/queues", nil)
	rr := httptest.NewRecorder()
	h.ListQueues(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListQueuesResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Items == nil {
		t.Error("items should be non-nil empty slice")
	}
	if len(resp.Items) != 0 {
		t.Errorf("want 0 items, got %d", len(resp.Items))
	}
}

func TestListQueues_ProviderErrorReturnsErrorShape(t *testing.T) {
	mock := &mockQueueProvider{
		listQueuesErr: model.NewProviderError("AccessDenied", "not allowed", 403),
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/queues", nil)
	rr := httptest.NewRecorder()
	h.ListQueues(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d", rr.Code)
	}
	var body map[string]string
	json.NewDecoder(rr.Body).Decode(&body)
	if body["code"] != "AccessDenied" {
		t.Errorf("want code=AccessDenied, got %q", body["code"])
	}
}

func TestCreateQueue_ValidBodyReturns201(t *testing.T) {
	qURL := "http://localhost:4566/000000000000/test-queue"
	mock := &mockQueueProvider{
		createQueueResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrl": qURL,
		}},
		getAttrsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Attributes": map[string]string{"QueueArn": "arn:aws:sqs:us-east-1:000000000000:test-queue"},
		}},
	}
	h := NewHandler(mock, testCfg())

	body := `{"name":"test-queue","type":"Standard"}`
	req := httptest.NewRequest(http.MethodPost, "/queues", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateQueue(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestCreateQueue_InvalidBodyReturns400(t *testing.T) {
	mock := &mockQueueProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/queues", bytes.NewBufferString(`{"name":""}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateQueue(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestDeleteQueue_Returns204(t *testing.T) {
	mock := &mockQueueProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodDelete, "/queues?url=http%3A%2F%2Flocalhost%3A4566%2F000000000000%2Fmy-queue", nil)
	rr := httptest.NewRecorder()
	h.DeleteQueue(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

func TestPurgeQueue_Returns204(t *testing.T) {
	mock := &mockQueueProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/queues/purge?url=http%3A%2F%2Flocalhost%3A4566%2F000000000000%2Fmy-queue", nil)
	rr := httptest.NewRecorder()
	h.PurgeQueue(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

func TestSendMessage_Returns200WithMessageId(t *testing.T) {
	mock := &mockQueueProvider{
		sendMsgResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"MessageId": "msg-123",
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"body":"hello world"}`
	req := httptest.NewRequest(http.MethodPost, "/queues/messages?url=http%3A%2F%2Flocalhost%3A4566%2Ftest", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.SendMessage(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestReceiveMessages_Returns200WithMessages(t *testing.T) {
	mock := &mockQueueProvider{
		recvMsgsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Messages": []map[string]any{
				{"MessageId": "m1", "Body": "hello", "ReceiptHandle": "rh1"},
			},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/queues/messages?url=http%3A%2F%2Flocalhost%3A4566%2Ftest", nil)
	rr := httptest.NewRecorder()
	h.ReceiveMessages(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ReceiveMessagesResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if len(resp.Messages) != 1 {
		t.Errorf("want 1 message, got %d", len(resp.Messages))
	}
}

// ── Params key invariant tests ─────────────────────────────────────────────

func TestUiNR_PortIsWirePortNotUIPort(t *testing.T) {
	cfg := testCfg()
	mock := &mockQueueProvider{
		listQueuesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrls": []string{},
		}},
	}
	h := NewHandler(mock, cfg)
	req := httptest.NewRequest(http.MethodGet, "/queues", nil)
	h.ListQueues(httptest.NewRecorder(), req)

	if mock.listQueuesNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listQueuesNR.Port != cfg.Port {
		t.Errorf("nr.Port = %d, want wire port %d (not UI port %d)", mock.listQueuesNR.Port, cfg.Port, cfg.UIPort)
	}
}

func TestUiNR_ClockIsNotNil(t *testing.T) {
	cfg := testCfg()
	mock := &mockQueueProvider{
		listQueuesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrls": []string{},
		}},
	}
	h := NewHandler(mock, cfg)
	req := httptest.NewRequest(http.MethodGet, "/queues", nil)
	h.ListQueues(httptest.NewRecorder(), req)

	if mock.listQueuesNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listQueuesNR.Clock == nil {
		t.Error("nr.Clock must not be nil")
	}
}

func TestListQueues_NrParamsMaxResultsKey(t *testing.T) {
	mock := &mockQueueProvider{
		listQueuesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrls": []string{},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/queues?pageSize=25", nil)
	h.ListQueues(httptest.NewRecorder(), req)

	if _, ok := mock.listQueuesNR.Params["MaxResults"]; !ok {
		t.Error("nr.Params must contain key MaxResults (not Limit or maxResults)")
	}
}

func TestCreateQueue_NrParamsQueueNameKey(t *testing.T) {
	mock := &mockQueueProvider{
		createQueueResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrl": "http://localhost:4566/000000000000/q",
		}},
		getAttrsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Attributes": map[string]string{},
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"name":"q"}`
	req := httptest.NewRequest(http.MethodPost, "/queues", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.CreateQueue(httptest.NewRecorder(), req)

	if mock.createQueueNR == nil {
		t.Fatal("createQueueNR was not captured")
	}
	if v, ok := mock.createQueueNR.Params["QueueName"]; !ok || v == "" {
		t.Error("nr.Params must contain key QueueName (not queueName or name)")
	}
}

func TestCreateQueue_NrParamsTagsKeyCase(t *testing.T) {
	mock := &mockQueueProvider{
		createQueueResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"QueueUrl": "http://localhost:4566/000000000000/q",
		}},
		getAttrsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Attributes": map[string]string{},
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"name":"q","tags":{"Env":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/queues", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.CreateQueue(httptest.NewRecorder(), req)

	if mock.createQueueNR == nil {
		t.Fatal("createQueueNR was not captured")
	}
	if _, ok := mock.createQueueNR.Params["Tags"]; !ok {
		t.Error("nr.Params must contain key Tags (capital T, not lowercase tags)")
	}
	if _, ok := mock.createQueueNR.Params["tags"]; ok {
		t.Error("nr.Params must NOT contain lowercase tags key")
	}
}
