package functionsui

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
	functionscore "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	functions []functionsstore.Function
	function  functionsstore.Function
	records   []functionsstore.Delivery
	pol       policy.Policy
	err       error

	callExecutionID string
	callResult      string
	callErr         string

	gotProject  string
	gotLocation string
	gotID       string
	gotInput    CreateInput
	gotData     string
	gotIamBody  map[string]any
	gotFunction string
	gotObject   string
	gotArchive  []byte
	deleted     bool
}

func (m *mockProvider) ListFunctions(_ context.Context, project string) ([]functionsstore.Function, error) {
	m.gotProject = project
	return m.functions, m.err
}

func (m *mockProvider) GetFunction(_ context.Context, project, location, id string) (functionsstore.Function, error) {
	m.gotProject, m.gotLocation, m.gotID = project, location, id
	return m.function, m.err
}

func (m *mockProvider) CreateFunction(_ context.Context, project, location string, in CreateInput) (functionsstore.Function, error) {
	m.gotProject, m.gotLocation, m.gotInput = project, location, in
	if m.err != nil {
		return functionsstore.Function{}, m.err
	}
	f := m.function
	f.ID, f.Location = in.ID, location
	return f, nil
}

func (m *mockProvider) DeleteFunction(_ context.Context, project, location, id string) error {
	m.gotProject, m.gotLocation, m.gotID, m.deleted = project, location, id, true
	return m.err
}

func (m *mockProvider) CallFunction(_ context.Context, project, location, id, data string) (string, string, string, error) {
	m.gotProject, m.gotLocation, m.gotID, m.gotData = project, location, id, data
	return m.callExecutionID, m.callResult, m.callErr, m.err
}

func (m *mockProvider) GetIamPolicy(_ context.Context, project, location, id string) (policy.Policy, error) {
	m.gotProject, m.gotLocation, m.gotID = project, location, id
	return m.pol, m.err
}

func (m *mockProvider) SetIamPolicy(_ context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	m.gotProject, m.gotLocation, m.gotID, m.gotIamBody = project, location, id, body
	return m.pol, m.err
}

func (m *mockProvider) ListDeliveries(_ context.Context, project, location, id string) ([]functionsstore.Delivery, error) {
	m.gotProject, m.gotLocation, m.gotFunction = project, location, id
	return m.records, m.err
}

func (m *mockProvider) GenerateUploadURL(_ context.Context, project, location string) (functionscore.UploadTarget, error) {
	m.gotProject, m.gotLocation = project, location
	if m.err != nil {
		return functionscore.UploadTarget{}, m.err
	}
	bucket := functionscore.SourceBucket(project, location)
	return functionscore.UploadTarget{Bucket: bucket, Object: "source-1.zip"}, nil
}

func (m *mockProvider) PutSourceArchive(_ context.Context, project, location, object string, data []byte) error {
	m.gotProject, m.gotLocation, m.gotObject, m.gotArchive = project, location, object, data
	return m.err
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

func function(name, location string) functionsstore.Function {
	return functionsstore.Function{
		ID:                name,
		Location:          location,
		Runtime:           "nodejs20",
		EntryPoint:        "handler",
		Status:            "ACTIVE",
		HttpsTriggerURL:   "https://" + location + "-test-project.cloudfunctions.net/" + name,
		AvailableMemoryMB: 256,
		Timeout:           "60s",
		Revision:          1,
		CreateTime:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdateTime:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Labels:            map[string]string{"env": "dev"},
	}
}

func TestListFunctions_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{functions: []functionsstore.Function{
		function("alpha", "europe-west1"),
		function("beta", "us-central1"),
	}}
	w := do(t, mock, http.MethodGet, "/functions", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListFunctionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Functions) != 2 {
		t.Fatalf("got %+v, want 2 functions", resp)
	}
	first := resp.Functions[0]
	if first.ID != "alpha" || first.Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.Name != "projects/test-project/locations/europe-west1/functions/alpha" {
		t.Fatalf("name not rendered: %+v", first)
	}
	if first.TriggerType != "http" || first.URL == "" {
		t.Fatalf("http trigger not rendered: %+v", first)
	}
	if first.Runtime != "nodejs20" || first.AvailableMemoryMB != 256 || first.Revision != 1 {
		t.Fatalf("config not rendered: %+v", first)
	}
}

func TestListFunctions_RendersEventTrigger(t *testing.T) {
	f := function("evt", "us-central1")
	f.HttpsTriggerURL = ""
	f.EventTrigger = &functionsstore.EventTrigger{
		EventType:   "google.cloud.pubsub.topic.v1.messagePublished",
		Resource:    "projects/test-project/topics/t1",
		RetryPolicy: "RETRY_POLICY_RETRY",
	}
	mock := &mockProvider{functions: []functionsstore.Function{f}}
	w := do(t, mock, http.MethodGet, "/functions", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListFunctionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := resp.Functions[0]
	if got.TriggerType != "event" || got.EventTrigger == nil || !got.EventTrigger.Retry {
		t.Fatalf("event trigger not rendered: %+v", got)
	}
	if got.EventTrigger.Resource != "projects/test-project/topics/t1" {
		t.Fatalf("event resource = %q", got.EventTrigger.Resource)
	}
}

func TestGetFunction_PassesPathIDs(t *testing.T) {
	mock := &mockProvider{function: function("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/functions/us-central1/alpha", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotProject != "test-project" || mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("resolved = %q/%q/%q", mock.gotProject, mock.gotLocation, mock.gotID)
	}
	var body Function
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ID != "alpha" || body.TriggerType != "http" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestGetFunction_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/functions/us-central1/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateFunction_RequiresFields(t *testing.T) {
	for _, body := range []string{
		`{"id":"f1","runtime":"nodejs20"}`,                // missing location
		`{"location":"us-central1","runtime":"nodejs20"}`, // missing id
		`{"id":"f1","location":"us-central1"}`,            // missing runtime
	} {
		w := do(t, &mockProvider{}, http.MethodPost, "/functions", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 for %s", w.Code, body)
		}
	}
}

func TestCreateFunction_CreatedAndPassesInput(t *testing.T) {
	mock := &mockProvider{function: function("f1", "us-central1")}
	body := `{"id":"f1","location":"us-central1","runtime":"nodejs20","entryPoint":"handler",` +
		`"triggerType":"event","eventType":"google.cloud.pubsub.topic.v1.messagePublished",` +
		`"eventResource":"projects/test-project/topics/t1","retry":true,"availableMemoryMB":512,` +
		`"sourceInline":"exports.handler = () => 1"}`
	w := do(t, mock, http.MethodPost, "/functions", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotInput.ID != "f1" {
		t.Fatalf("resolved input = %+v", mock.gotInput)
	}
	if mock.gotInput.TriggerType != "event" || !mock.gotInput.Retry {
		t.Fatalf("event fields not passed: %+v", mock.gotInput)
	}
	if mock.gotInput.SourceInline == "" {
		t.Fatalf("inline source not passed: %+v", mock.gotInput)
	}
}

func TestCreateFunction_PassesStorageSource(t *testing.T) {
	mock := &mockProvider{function: function("f1", "us-central1")}
	body := `{"id":"f1","location":"us-central1","runtime":"nodejs20","entryPoint":"handler",` +
		`"sourceBucket":"gcf-v2-sources-p-us-central1","sourceObject":"source-1.zip"}`
	w := do(t, mock, http.MethodPost, "/functions", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotInput.SourceBucket != "gcf-v2-sources-p-us-central1" || mock.gotInput.SourceObject != "source-1.zip" {
		t.Fatalf("storageSource not passed: %+v", mock.gotInput)
	}
}

func TestGenerateUploadURL_ReturnsIssuedTarget(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/functions/upload", `{"location":"us-central1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotProject != "test-project" {
		t.Fatalf("resolved target = %q/%q", mock.gotProject, mock.gotLocation)
	}
	var got SourceUploadTarget
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Bucket != functionscore.SourceBucket("test-project", "us-central1") || got.Object != "source-1.zip" {
		t.Fatalf("target = %+v", got)
	}
}

func TestGenerateUploadURL_RequiresLocation(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/functions/upload", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUploadSource_StoresArchive(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPut, "/functions/upload?location=us-central1&object=source-1.zip", "PK\x03\x04zip")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotObject != "source-1.zip" {
		t.Fatalf("resolved target = %q/%q", mock.gotLocation, mock.gotObject)
	}
	if string(mock.gotArchive) != "PK\x03\x04zip" {
		t.Fatalf("archive = %q", mock.gotArchive)
	}
}

func TestUploadSource_TooLarge(t *testing.T) {
	orig := maxSourceUploadBytes
	maxSourceUploadBytes = 4
	defer func() { maxSourceUploadBytes = orig }()

	w := do(t, &mockProvider{}, http.MethodPut, "/functions/upload?location=us-central1&object=source-1.zip", "12345")
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %s", w.Code, w.Body.String())
	}
}

func TestUploadSource_RequiresTargetAndBody(t *testing.T) {
	if w := do(t, &mockProvider{}, http.MethodPut, "/functions/upload?object=source-1.zip", "zip"); w.Code != http.StatusBadRequest {
		t.Fatalf("missing location: status = %d, want 400", w.Code)
	}
	if w := do(t, &mockProvider{}, http.MethodPut, "/functions/upload?location=us-central1&object=source-1.zip", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("empty body: status = %d, want 400", w.Code)
	}
}

func TestDeleteFunction_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/functions/us-central1/f1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if !mock.deleted || mock.gotID != "f1" {
		t.Fatalf("delete not recorded: %+v", mock)
	}
}

func TestCallFunction_ReturnsResult(t *testing.T) {
	mock := &mockProvider{callExecutionID: "exec-1", callResult: `{"ok":true}`}
	w := do(t, mock, http.MethodPost, "/functions/us-central1/f1/call", `{"data":"{\"x\":1}"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp CallResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ExecutionID != "exec-1" || resp.Result != `{"ok":true}` || resp.Error != "" {
		t.Fatalf("unexpected call response: %+v", resp)
	}
	if mock.gotData != `{"x":1}` {
		t.Fatalf("data = %q", mock.gotData)
	}
}

func TestCallFunction_InvokeErrorInBand(t *testing.T) {
	mock := &mockProvider{callExecutionID: "exec-2", callErr: "boom"}
	w := do(t, mock, http.MethodPost, "/functions/us-central1/f1/call", `{"data":"{}"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp CallResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "boom" || resp.ExecutionID != "exec-2" {
		t.Fatalf("invoke error not in-band: %+v", resp)
	}
}

func TestIam_GetAndSet(t *testing.T) {
	mock := &mockProvider{pol: policy.Policy{Bindings: []any{map[string]any{"role": "roles/cloudfunctions.invoker", "members": []any{"user:a@b.com"}}}}}
	w := do(t, mock, http.MethodGet, "/functions/us-central1/f1/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "f1" {
		t.Fatalf("iam target = %q", mock.gotID)
	}

	w = do(t, mock, http.MethodPut, "/functions/us-central1/f1/iam", `{"bindings":[{"role":"roles/cloudfunctions.invoker","members":["user:a@b.com"]}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("set status = %d: %s", w.Code, w.Body.String())
	}
	if mock.gotIamBody == nil {
		t.Fatal("iam body not passed")
	}
	if _, ok := mock.gotIamBody["bindings"]; !ok {
		t.Fatalf("bindings not passed: %+v", mock.gotIamBody)
	}
}

func TestListDeliveries_RendersRecords(t *testing.T) {
	mock := &mockProvider{records: []functionsstore.Delivery{{
		ID:         "d1",
		FunctionID: "f1",
		EventType:  "google.cloud.pubsub.topic.v1.messagePublished",
		Status:     functionsstore.DeliveryDeadLetter,
		Attempts:   3,
		CreateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}}}
	w := do(t, mock, http.MethodGet, "/functions/us-central1/f1/deliveries", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotFunction != "f1" || mock.gotLocation != "us-central1" {
		t.Fatalf("deliveries target = %q/%q", mock.gotLocation, mock.gotFunction)
	}
	var resp ListDeliveriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.Deliveries[0].Status != functionsstore.DeliveryDeadLetter || resp.Deliveries[0].Attempts != 3 {
		t.Fatalf("unexpected deliveries: %+v", resp)
	}
}
