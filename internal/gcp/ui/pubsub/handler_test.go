package pubsubui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// mockProvider records the last NormalizedRequest and returns a canned response.
type mockProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockProvider) reply(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.resp != nil {
		return m.resp, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockProvider) TopicList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TopicCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TopicGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TopicDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TopicPublish(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionUpdate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TopicGetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TopicSetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionGetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SubscriptionSetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      8080,
		UIPort:    4567,
		Region:    "global",
		AccountID: "test-project",
		ProjectID: "test-project",
		Clock:     clock.RealClock{},
	}
}

func do(t *testing.T, mock *mockProvider, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestListTopics_MapsItems(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"topics": []any{
				map[string]any{
					"name":                     "projects/test-project/topics/events",
					"messageRetentionDuration": "604800s",
					"labels":                   map[string]any{"env": "dev"},
				},
			},
			"nextPageToken": "abc",
		},
	}}

	w := do(t, mock, http.MethodGet, "/topics?pageSize=5", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListTopicsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || len(resp.Topics) != 1 {
		t.Fatalf("got %+v, want 1 topic", resp)
	}
	if resp.Topics[0].Name != "events" || resp.Topics[0].Labels["env"] != "dev" {
		t.Fatalf("unexpected topic: %+v", resp.Topics[0])
	}
	if resp.NextPageToken != "abc" {
		t.Fatalf("nextPageToken = %q", resp.NextPageToken)
	}
	if mock.lastNR.Cloud != model.CloudGCP || mock.lastNR.AccountID != "test-project" {
		t.Fatalf("NR not GCP-scoped: %+v", mock.lastNR)
	}
	if mock.lastNR.Params["pageSize"] != "5" {
		t.Fatalf("pageSize = %v", mock.lastNR.Params["pageSize"])
	}
}

func TestCreateTopic_RequiresName(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/topics", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateTopic_PassesBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/topics/logs"},
	}}
	w := do(t, mock, http.MethodPost, "/topics", `{"name":"logs","messageRetentionDuration":"3600s","labels":{"team":"core"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "topics/logs" {
		t.Fatalf("name param = %v", mock.lastNR.Params["name"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["messageRetentionDuration"] != "3600s" {
		t.Fatalf("body = %+v", body)
	}
}

func TestPublishTopic_RequiresMessages(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/topics/events/publish", `{"messages":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestPublishTopic_PassesMessages(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"messageIds": []any{"1"}},
	}}
	w := do(t, mock, http.MethodPost, "/topics/events/publish", `{"messages":[{"data":"aGk=","attributes":{"k":"v"}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "topics/events" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages = %#v", body["messages"])
	}
	msg, _ := msgs[0].(map[string]any)
	if msg["data"] != "aGk=" {
		t.Fatalf("message = %#v", msg)
	}
}

func TestDeleteTopic_PassesName(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/topics/events", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if mock.lastNR.Params["name"] != "topics/events" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestGetTopic_MapsFullName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/topics/events", "kmsKeyName": "k"},
	}}
	w := do(t, mock, http.MethodGet, "/topics/events", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var top Topic
	if err := json.Unmarshal(w.Body.Bytes(), &top); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if top.Name != "events" || top.FullName != "projects/test-project/topics/events" || top.KmsKeyName != "k" {
		t.Fatalf("unexpected topic: %+v", top)
	}
}

func TestListSubscriptions_MapsItems(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"subscriptions": []any{
				map[string]any{
					"name":               "projects/test-project/subscriptions/sub1",
					"topic":              "projects/test-project/topics/events",
					"ackDeadlineSeconds": float64(30),
					"pushConfig":         map[string]any{"pushEndpoint": "https://example.com/push"},
					"deadLetterPolicy": map[string]any{
						"deadLetterTopic":     "projects/test-project/topics/dlq",
						"maxDeliveryAttempts": float64(5),
					},
				},
			},
		},
	}}

	w := do(t, mock, http.MethodGet, "/subscriptions", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListSubscriptionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 {
		t.Fatalf("total = %d, want 1", resp.Total)
	}
	sub := resp.Subscriptions[0]
	if sub.Name != "sub1" || sub.Topic != "events" || sub.AckDeadlineSeconds != 30 {
		t.Fatalf("unexpected subscription: %+v", sub)
	}
	if sub.PushEndpoint != "https://example.com/push" {
		t.Fatalf("pushEndpoint = %q", sub.PushEndpoint)
	}
	if sub.DeadLetterTopic != "dlq" || sub.MaxDeliveryAttempts != 5 {
		t.Fatalf("deadLetter = %+v", sub)
	}
}

func TestCreateSubscription_RequiresNameAndTopic(t *testing.T) {
	if w := do(t, &mockProvider{}, http.MethodPost, "/subscriptions", `{"name":"s"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("missing topic: status = %d, want 400", w.Code)
	}
	if w := do(t, &mockProvider{}, http.MethodPost, "/subscriptions", `{"topic":"t"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("missing name: status = %d, want 400", w.Code)
	}
}

func TestCreateSubscription_ExpandsTopicAndPush(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/subscriptions/sub1", "topic": "projects/test-project/topics/events"},
	}}
	w := do(t, mock, http.MethodPost, "/subscriptions",
		`{"name":"sub1","topic":"events","ackDeadlineSeconds":30,"pushEndpoint":"https://x/push"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["topic"] != "projects/test-project/topics/events" {
		t.Fatalf("topic = %v", body["topic"])
	}
	pc, _ := body["pushConfig"].(map[string]any)
	if pc["pushEndpoint"] != "https://x/push" {
		t.Fatalf("pushConfig = %#v", body["pushConfig"])
	}
	if v, ok := body["ackDeadlineSeconds"].(float64); !ok || v != 30 {
		t.Fatalf("ackDeadlineSeconds = %#v, want float64(30)", body["ackDeadlineSeconds"])
	}
}

func TestUpdateSubscription_DerivesMaskFromKeys(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/subscriptions/sub1"},
	}}
	w := do(t, mock, http.MethodPatch, "/subscriptions/sub1",
		`{"subscription":{"ackDeadlineSeconds":45,"labels":{"a":"b"}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "subscriptions/sub1" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["updateMask"] != "ackDeadlineSeconds,labels" {
		t.Fatalf("updateMask = %v", body["updateMask"])
	}
}

func TestUpdateSubscription_HonorsExplicitMask(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPatch, "/subscriptions/sub1",
		`{"updateMask":"retryPolicy","subscription":{"retryPolicy":{"minimumBackoff":"5s"}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["updateMask"] != "retryPolicy" {
		t.Fatalf("updateMask = %v", body["updateMask"])
	}
}

func TestDeleteSubscription_PassesName(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/subscriptions/sub1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if mock.lastNR.Params["name"] != "subscriptions/sub1" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestSetTopicIam_PassesBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "x"}}}
	w := do(t, mock, http.MethodPut, "/topics/events/iam",
		`{"bindings":[{"role":"roles/pubsub.publisher","members":["allUsers"]}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "topics/events" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if _, ok := body["bindings"].([]any); !ok {
		t.Fatalf("bindings not passed through: %#v", body["bindings"])
	}
}

func TestGetSubscriptionIam_PassesName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/subscriptions/sub1/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if mock.lastNR.Params["name"] != "subscriptions/sub1" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

// TestPathParams_DecodePercentEncoding covers UI17: the console client
// percent-encodes resource ids (encodeURIComponent), and chi leaves characters
// Go treats as path-safe ('+', '&', ':') escaped unless the handler decodes via
// uihelper.Segment. Routes taking a topic/subscription id must see the decoded
// value, not the raw escape.
func TestPathParams_DecodePercentEncoding(t *testing.T) {
	cases := []struct {
		name string
		verb string
		path string
		want string
	}{
		{"topic +", http.MethodGet, "/topics/a%2Bb", "topics/a+b"},
		{"topic &", http.MethodDelete, "/topics/a%26b", "topics/a&b"},
		{"topic :", http.MethodGet, "/topics/a%3Ab/iam", "topics/a:b"},
		{"topic publish", http.MethodPost, "/topics/a%2Bb/publish", "topics/a+b"},
		{"subscription +", http.MethodGet, "/subscriptions/a%2Bb", "subscriptions/a+b"},
		{"subscription &", http.MethodDelete, "/subscriptions/a%26b", "subscriptions/a&b"},
		{"subscription :", http.MethodGet, "/subscriptions/a%3Ab/iam", "subscriptions/a:b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
			body := ""
			if tc.verb == http.MethodPost {
				body = `{"messages":[{"data":"aGk="}]}`
			}
			w := do(t, mock, tc.verb, tc.path, body)
			if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 200/204: %s", w.Code, w.Body.String())
			}
			if got := mock.lastNR.Params["name"]; got != tc.want {
				t.Fatalf("name = %v, want %q", got, tc.want)
			}
		})
	}
}

// TestPathParams_RejectsDecodedSlash: a decoded '/' would smuggle a second path
// segment into the resource name, so the handler rejects it with 400 before
// calling the provider.
func TestPathParams_RejectsDecodedSlash(t *testing.T) {
	for _, path := range []string{"/topics/a%2Fb", "/topics/a%2Fb/iam", "/subscriptions/a%2Fb"} {
		mock := &mockProvider{}
		w := do(t, mock, http.MethodGet, path, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", path, w.Code)
		}
		if mock.lastNR != nil {
			t.Fatalf("%s: provider called with %+v", path, mock.lastNR)
		}
	}
}
