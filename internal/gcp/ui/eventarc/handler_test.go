package eventarcui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/policy"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	"jaiscloud/internal/model"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	triggers []eventarcstore.Trigger
	trigger  eventarcstore.Trigger
	channels []eventarcstore.Channel
	channel  eventarcstore.Channel
	pol      policy.Policy
	err      error

	gotProject  string
	gotLocation string
	gotID       string
	gotTrigger  TriggerInput
	gotChannel  ChannelInput
	gotIamBody  map[string]any
	deleted     bool
}

func (m *mockProvider) ListTriggersByProject(_ context.Context, project string) ([]eventarcstore.Trigger, error) {
	m.gotProject = project
	return m.triggers, m.err
}

func (m *mockProvider) GetTrigger(_ context.Context, project, location, id string) (eventarcstore.Trigger, error) {
	m.gotProject, m.gotLocation, m.gotID = project, location, id
	return m.trigger, m.err
}

func (m *mockProvider) CreateTrigger(_ context.Context, project, location string, in TriggerInput) (eventarcstore.Trigger, error) {
	m.gotProject, m.gotLocation, m.gotTrigger = project, location, in
	if m.err != nil {
		return eventarcstore.Trigger{}, m.err
	}
	t := m.trigger
	t.Location, t.Name = location, in.Name
	return t, nil
}

func (m *mockProvider) UpdateTrigger(_ context.Context, project, location, id string, in TriggerInput) (eventarcstore.Trigger, error) {
	m.gotProject, m.gotLocation, m.gotID, m.gotTrigger = project, location, id, in
	return m.trigger, m.err
}

func (m *mockProvider) DeleteTrigger(_ context.Context, project, location, id string) error {
	m.gotProject, m.gotLocation, m.gotID, m.deleted = project, location, id, true
	return m.err
}

func (m *mockProvider) TriggerGetIamPolicy(_ context.Context, project, location, id string) (policy.Policy, error) {
	m.gotProject, m.gotLocation, m.gotID = project, location, id
	return m.pol, m.err
}

func (m *mockProvider) TriggerSetIamPolicy(_ context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	m.gotProject, m.gotLocation, m.gotID, m.gotIamBody = project, location, id, body
	return m.pol, m.err
}

func (m *mockProvider) ListChannelsByProject(_ context.Context, project string) ([]eventarcstore.Channel, error) {
	m.gotProject = project
	return m.channels, m.err
}

func (m *mockProvider) GetChannel(_ context.Context, project, location, id string) (eventarcstore.Channel, error) {
	m.gotProject, m.gotLocation, m.gotID = project, location, id
	return m.channel, m.err
}

func (m *mockProvider) CreateChannel(_ context.Context, project, location string, in ChannelInput) (eventarcstore.Channel, error) {
	m.gotProject, m.gotLocation, m.gotChannel = project, location, in
	if m.err != nil {
		return eventarcstore.Channel{}, m.err
	}
	c := m.channel
	c.Location, c.Name = location, in.Name
	return c, nil
}

func (m *mockProvider) UpdateChannel(_ context.Context, project, location, id string, in ChannelInput) (eventarcstore.Channel, error) {
	m.gotProject, m.gotLocation, m.gotID, m.gotChannel = project, location, id, in
	return m.channel, m.err
}

func (m *mockProvider) DeleteChannel(_ context.Context, project, location, id string) error {
	m.gotProject, m.gotLocation, m.gotID, m.deleted = project, location, id, true
	return m.err
}

func (m *mockProvider) ChannelGetIamPolicy(_ context.Context, project, location, id string) (policy.Policy, error) {
	m.gotProject, m.gotLocation, m.gotID = project, location, id
	return m.pol, m.err
}

func (m *mockProvider) ChannelSetIamPolicy(_ context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	m.gotProject, m.gotLocation, m.gotID, m.gotIamBody = project, location, id, body
	return m.pol, m.err
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
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func trigger(name, location string) eventarcstore.Trigger {
	return eventarcstore.Trigger{
		ProjectID:  "test-project",
		Location:   location,
		Name:       name,
		UID:        "uid-1",
		Etag:       "etag-1",
		CreateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Labels:     map[string]string{"env": "dev"},
		Config: []byte(`{"destination":{"cloudFunction":"projects/test-project/locations/us-central1/functions/fn1"},` +
			`"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],` +
			`"serviceAccount":"sa@test-project.iam.gserviceaccount.com",` +
			`"transport":{"pubsub":{"topic":"projects/test-project/topics/t1"}}}`),
	}
}

func channel(name, location string) eventarcstore.Channel {
	return eventarcstore.Channel{
		ProjectID:       "test-project",
		Location:        location,
		Name:            name,
		UID:             "uid-c",
		Etag:            "etag-c",
		ActivationToken: "tok-c",
		CreateTime:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdateTime:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Config:          []byte(`{"provider":"projects/test-project/locations/us-central1/providers/some.saas","cryptoKeyName":"projects/test-project/locations/us-central1/keyRings/kr/cryptoKeys/k"}`),
	}
}

func TestListTriggers_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{triggers: []eventarcstore.Trigger{
		trigger("alpha", "europe-west1"),
		trigger("beta", "us-central1"),
	}}
	w := do(t, mock, http.MethodGet, "/triggers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListTriggersResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Triggers) != 2 {
		t.Fatalf("got %+v, want 2 triggers", resp)
	}
	first := resp.Triggers[0]
	if first.Name != "alpha" || first.Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.DestinationType != "cloudFunction" || !strings.HasSuffix(first.Destination, "/functions/fn1") {
		t.Fatalf("destination not flattened: %+v", first)
	}
	if len(first.EventFilters) != 1 || first.EventFilters[0].Attribute != "type" {
		t.Fatalf("event filters not flattened: %+v", first)
	}
	if first.ServiceAccount != "sa@test-project.iam.gserviceaccount.com" {
		t.Fatalf("service account not rendered: %+v", first)
	}
	if !strings.HasSuffix(first.TransportPubsubTopic, "/topics/t1") {
		t.Fatalf("transport topic not rendered: %+v", first)
	}
	if first.UID != "uid-1" || first.Etag != "etag-1" || first.Labels["env"] != "dev" {
		t.Fatalf("output-only fields not rendered: %+v", first)
	}
	if len(first.Config) == 0 {
		t.Fatalf("config not round-tripped: %+v", first)
	}
}

func TestGetTrigger_PassesPathIDs(t *testing.T) {
	mock := &mockProvider{trigger: trigger("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/triggers/us-central1/alpha", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotProject != "test-project" || mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("resolved = %q/%q/%q", mock.gotProject, mock.gotLocation, mock.gotID)
	}
	var body Trigger
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "alpha" || body.DestinationType != "cloudFunction" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestGetTrigger_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/triggers/us-central1/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateTrigger_RequiresLocation(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/triggers", `{"name":"t1","destinationType":"workflow","destination":"w1"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateTrigger_CreatedAndPassesStructuredInput(t *testing.T) {
	mock := &mockProvider{trigger: trigger("t1", "us-central1")}
	body := `{"name":"t1","location":"us-central1","destinationType":"cloudFunction",` +
		`"destination":"projects/test-project/locations/us-central1/functions/fn1",` +
		`"serviceAccount":"sa@test-project.iam.gserviceaccount.com",` +
		`"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],` +
		`"transportPubsubTopic":"projects/test-project/topics/t1"}`
	w := do(t, mock, http.MethodPost, "/triggers", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotTrigger.Name != "t1" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	cfg, err := buildTriggerConfig(mock.gotTrigger)
	if err != nil {
		t.Fatalf("build config: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(cfg, &m); err != nil {
		t.Fatalf("config not JSON: %v", err)
	}
	if dest, _ := m["destination"].(map[string]any); dest["cloudFunction"] != "projects/test-project/locations/us-central1/functions/fn1" {
		t.Fatalf("destination not built: %+v", m)
	}
	if filters, _ := m["eventFilters"].([]any); len(filters) != 1 {
		t.Fatalf("filters not built: %+v", m)
	}
	if tr, _ := m["transport"].(map[string]any); tr == nil {
		t.Fatalf("transport not built: %+v", m)
	}
}

func TestBuildTriggerConfig_CloudRunDestination(t *testing.T) {
	cfg, err := buildTriggerConfig(TriggerInput{
		DestinationType: "cloudRun",
		Destination:     "projects/test-project/locations/us-central1/services/svc",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(cfg, &m)
	cr, _ := m["destination"].(map[string]any)
	inner, _ := cr["cloudRun"].(map[string]any)
	if inner["service"] != "projects/test-project/locations/us-central1/services/svc" {
		t.Fatalf("cloudRun destination wrong: %+v", m)
	}
}

func TestBuildTriggerConfig_JSONEscapeHatchWins(t *testing.T) {
	raw := `{"destination":{"gke":{"cluster":"projects/p/locations/l/clusters/c","location":"l"}},"eventFilters":[{"attribute":"type","value":"x"}]}`
	cfg, err := buildTriggerConfig(TriggerInput{
		DestinationType: "cloudFunction",
		Destination:     "ignored",
		Config:          json.RawMessage(raw),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if string(cfg) != raw {
		t.Fatalf("escape hatch not used verbatim: %s", cfg)
	}
}

func TestBuildTriggerConfig_RejectsUnsupportedStructuredDestination(t *testing.T) {
	if _, err := buildTriggerConfig(TriggerInput{DestinationType: "gke", Destination: "x"}); err == nil {
		t.Fatal("expected error for gke without config body")
	}
}

func TestBuildTriggerConfig_RejectsBadJSONBody(t *testing.T) {
	if _, err := buildTriggerConfig(TriggerInput{Config: json.RawMessage(`not json`)}); err == nil {
		t.Fatal("expected error for malformed config")
	}
}

func TestUpdateTrigger_UsesPathIdentity(t *testing.T) {
	mock := &mockProvider{trigger: trigger("alpha", "us-central1")}
	body := `{"name":"ignored","location":"ignored","destinationType":"workflow","destination":"projects/p/locations/us-central1/workflows/w1"}`
	w := do(t, mock, http.MethodPut, "/triggers/us-central1/alpha", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotID != "alpha" || mock.gotTrigger.Name != "alpha" {
		t.Fatalf("path identity not applied: %+v", mock)
	}
}

func TestDeleteTrigger_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/triggers/us-central1/alpha", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestTriggerIam_RoundTrip(t *testing.T) {
	mock := &mockProvider{pol: policy.Policy{
		Version:  1,
		Etag:     "abc",
		Bindings: []any{map[string]any{"role": "roles/eventarc.admin", "members": []any{"user:a@b.com"}}},
	}}
	w := do(t, mock, http.MethodGet, "/triggers/us-central1/alpha/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "roles/eventarc.admin") {
		t.Fatalf("policy not rendered: %s", w.Body.String())
	}

	put := do(t, mock, http.MethodPut, "/triggers/us-central1/alpha/iam",
		`{"bindings":[{"role":"roles/editor","members":["user:x@y.com"]}],"etag":"abc","version":1}`)
	if put.Code != http.StatusOK {
		t.Fatalf("set status = %d: %s", put.Code, put.Body.String())
	}
	bindings, ok := mock.gotIamBody["bindings"].([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("bindings not mapped: %+v", mock.gotIamBody)
	}
}

func TestListTriggers_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "nope", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/triggers", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "nope") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestListChannels_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{channels: []eventarcstore.Channel{
		channel("alpha", "europe-west1"),
		channel("beta", "us-central1"),
	}}
	w := do(t, mock, http.MethodGet, "/channels", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListChannelsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Channels) != 2 {
		t.Fatalf("got %+v, want 2 channels", resp)
	}
	first := resp.Channels[0]
	if first.Name != "alpha" || first.Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if !strings.HasSuffix(first.PubsubTopic, "/topics/jc-eventarc-channel-alpha") {
		t.Fatalf("pubsub topic not synthesized: %+v", first)
	}
	if first.State != "PENDING" || first.ActivationToken != "tok-c" {
		t.Fatalf("output-only fields not rendered: %+v", first)
	}
	if !strings.HasSuffix(first.Provider, "/providers/some.saas") || !strings.HasSuffix(first.CryptoKeyName, "/cryptoKeys/k") {
		t.Fatalf("config fields not flattened: %+v", first)
	}
}

func TestGetChannel_PassesPathIDs(t *testing.T) {
	mock := &mockProvider{channel: channel("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/channels/us-central1/alpha", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotProject != "test-project" || mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("resolved = %q/%q/%q", mock.gotProject, mock.gotLocation, mock.gotID)
	}
}

func TestCreateChannel_RequiresLocation(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/channels", `{"name":"c1"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateChannel_MapsStructuredInput(t *testing.T) {
	mock := &mockProvider{channel: channel("c1", "us-central1")}
	body := `{"name":"c1","location":"us-central1","provider":"projects/p/locations/us-central1/providers/some.saas","cryptoKeyName":"projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"}`
	w := do(t, mock, http.MethodPost, "/channels", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	cfg, err := buildChannelConfig(mock.gotChannel)
	if err != nil {
		t.Fatalf("build config: %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(cfg, &m)
	if !strings.HasSuffix(m["provider"].(string), "/providers/some.saas") {
		t.Fatalf("provider not mapped: %+v", m)
	}
	if !strings.HasSuffix(m["cryptoKeyName"].(string), "/cryptoKeys/k") {
		t.Fatalf("cryptoKeyName not mapped: %+v", m)
	}
}

func TestUpdateChannel_UsesPathIdentity(t *testing.T) {
	mock := &mockProvider{channel: channel("alpha", "us-central1")}
	body := `{"name":"ignored","location":"ignored","provider":"projects/p/locations/l/providers/x"}`
	w := do(t, mock, http.MethodPut, "/channels/us-central1/alpha", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotID != "alpha" || mock.gotChannel.Name != "alpha" {
		t.Fatalf("path identity not applied: %+v", mock)
	}
}

func TestDeleteChannel_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/channels/us-central1/alpha", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestChannelIam_RoundTrip(t *testing.T) {
	mock := &mockProvider{pol: policy.Policy{
		Version:  1,
		Etag:     "abc",
		Bindings: []any{map[string]any{"role": "roles/eventarc.channelAdmin", "members": []any{"user:a@b.com"}}},
	}}
	w := do(t, mock, http.MethodGet, "/channels/us-central1/alpha/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "roles/eventarc.channelAdmin") {
		t.Fatalf("policy not rendered: %s", w.Body.String())
	}
	put := do(t, mock, http.MethodPut, "/channels/us-central1/alpha/iam", `{"bindings":[]}`)
	if put.Code != http.StatusOK {
		t.Fatalf("set status = %d: %s", put.Code, put.Body.String())
	}
	if mock.gotIamBody == nil {
		t.Fatal("iam body not forwarded")
	}
}
