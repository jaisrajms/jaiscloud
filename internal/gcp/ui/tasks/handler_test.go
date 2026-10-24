package tasksui

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
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	queues []tasksstore.Queue
	queue  tasksstore.Queue
	tasks  []tasksstore.Task
	task   tasksstore.Task
	pol    policy.Policy
	err    error

	gotProject  string
	gotLocation string
	gotQueue    string
	gotTask     string
	gotCreate   tasksstore.Queue
	gotTaskIn   tasksstore.Task
	gotIamBody  map[string]any
	gotMask     []string
	deleted     bool
	action      string
}

func (m *mockProvider) ListQueuesByProject(_ context.Context, project string) ([]tasksstore.Queue, error) {
	m.gotProject = project
	return m.queues, m.err
}

func (m *mockProvider) GetQueue(_ context.Context, _, location, name string) (tasksstore.Queue, error) {
	m.gotLocation, m.gotQueue = location, name
	return m.queue, m.err
}

func (m *mockProvider) CreateQueue(_ context.Context, _, location string, q tasksstore.Queue) (tasksstore.Queue, error) {
	m.gotLocation, m.gotCreate = location, q
	if m.err != nil {
		return tasksstore.Queue{}, m.err
	}
	q.Location = location
	return q, nil
}

func (m *mockProvider) UpdateQueue(_ context.Context, _, location, name string, upd tasksstore.Queue, mask []string) (tasksstore.Queue, error) {
	m.gotLocation, m.gotQueue, m.gotCreate, m.gotMask = location, name, upd, mask
	return upd, m.err
}

func (m *mockProvider) DeleteQueue(_ context.Context, _, location, name string) error {
	m.gotLocation, m.gotQueue, m.deleted = location, name, true
	return m.err
}

func (m *mockProvider) PauseQueue(_ context.Context, _, location, name string) (tasksstore.Queue, error) {
	return m.mutateQueue("pause", location, name)
}

func (m *mockProvider) ResumeQueue(_ context.Context, _, location, name string) (tasksstore.Queue, error) {
	return m.mutateQueue("resume", location, name)
}

func (m *mockProvider) PurgeQueue(_ context.Context, _, location, name string) (tasksstore.Queue, error) {
	return m.mutateQueue("purge", location, name)
}

func (m *mockProvider) mutateQueue(action, location, name string) (tasksstore.Queue, error) {
	m.action, m.gotLocation, m.gotQueue = action, location, name
	return m.queue, m.err
}

func (m *mockProvider) QueueGetIamPolicy(_ context.Context, _, location, queue string) (policy.Policy, error) {
	m.gotLocation, m.gotQueue = location, queue
	return m.pol, m.err
}

func (m *mockProvider) QueueSetIamPolicy(_ context.Context, _, location, queue string, body map[string]any) (policy.Policy, error) {
	m.gotLocation, m.gotQueue, m.gotIamBody = location, queue, body
	return m.pol, m.err
}

func (m *mockProvider) ListTasks(_ context.Context, _, location, queue string) ([]tasksstore.Task, error) {
	m.gotLocation, m.gotQueue = location, queue
	return m.tasks, m.err
}

func (m *mockProvider) GetTask(_ context.Context, _, location, queue, name string) (tasksstore.Task, error) {
	m.gotLocation, m.gotQueue, m.gotTask = location, queue, name
	return m.task, m.err
}

func (m *mockProvider) CreateTask(_ context.Context, _, location, queue string, t tasksstore.Task) (tasksstore.Task, error) {
	m.gotLocation, m.gotQueue, m.gotTaskIn = location, queue, t
	if m.err != nil {
		return tasksstore.Task{}, m.err
	}
	t.Location, t.Queue = location, queue
	return t, nil
}

func (m *mockProvider) DeleteTask(_ context.Context, _, location, queue, name string) error {
	m.gotLocation, m.gotQueue, m.gotTask, m.deleted = location, queue, name, true
	return m.err
}

func (m *mockProvider) RunTask(_ context.Context, _, location, queue, name string) (tasksstore.Task, error) {
	m.action, m.gotLocation, m.gotQueue, m.gotTask = "run", location, queue, name
	return m.task, m.err
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

func queue(name, location string) tasksstore.Queue {
	return tasksstore.Queue{
		ProjectID: "test-project",
		Location:  location,
		Name:      name,
		State:     tasksstore.StateRunning,
		RateLimits: &tasksstore.RateLimits{
			MaxDispatchesPerSecond:  10,
			MaxBurstSize:            20,
			MaxConcurrentDispatches: 30,
		},
		RetryConfig: &tasksstore.RetryConfig{
			MaxAttempts:  5,
			MinBackoff:   100 * time.Millisecond,
			MaxBackoff:   time.Hour,
			MaxDoublings: 4,
		},
	}
}

func task(name string) tasksstore.Task {
	return tasksstore.Task{
		ProjectID:        "test-project",
		Location:         "us-central1",
		Queue:            "q1",
		Name:             name,
		Target:           tasksstore.TargetHTTP,
		HTTP:             &tasksstore.HttpRequest{URL: "https://example.com/hook", HTTPMethod: "POST"},
		ScheduleTime:     time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		DispatchDeadline: time.Minute,
	}
}

func TestListQueues_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{queues: []tasksstore.Queue{
		queue("alpha", "europe-west1"),
		queue("beta", "us-central1"),
	}}

	w := do(t, mock, http.MethodGet, "/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListQueuesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Queues) != 2 {
		t.Fatalf("got %+v, want 2 queues", resp)
	}
	if resp.Queues[0].Name != "alpha" || resp.Queues[0].Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", resp.Queues[0])
	}
	if resp.Queues[0].State != "RUNNING" || resp.Queues[0].MaxDispatchesPerSecond != 10 {
		t.Fatalf("rate limits not rendered: %+v", resp.Queues[0])
	}
}

func TestGetQueue_PassesLocationAndQueue(t *testing.T) {
	mock := &mockProvider{queue: queue("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/queues/us-central1/alpha", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotQueue != "alpha" {
		t.Fatalf("resolved location/queue = %q/%q", mock.gotLocation, mock.gotQueue)
	}
	var body Queue
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "alpha" || body.State != "RUNNING" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestCreateQueue_RequiresLocation(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/queues", `{"name":"q1","maxDispatchesPerSecond":10}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateQueue_CreatedAndMapsLimits(t *testing.T) {
	mock := &mockProvider{}
	body := `{"name":"q1","location":"us-central1","maxDispatchesPerSecond":50,"maxBurstSize":10,"maxConcurrentDispatches":20,"maxAttempts":5,"minBackoff":"100ms","maxBackoff":"1h","maxDoublings":4}`
	w := do(t, mock, http.MethodPost, "/queues", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotCreate.Name != "q1" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	if mock.gotCreate.RateLimits == nil || mock.gotCreate.RateLimits.MaxDispatchesPerSecond != 50 {
		t.Fatalf("rate limits not mapped: %+v", mock.gotCreate)
	}
	if mock.gotCreate.RetryConfig == nil || mock.gotCreate.RetryConfig.MinBackoff != 100*time.Millisecond {
		t.Fatalf("retry config not mapped: %+v", mock.gotCreate)
	}
}

func TestCreateQueue_RejectsBadDuration(t *testing.T) {
	body := `{"name":"q1","location":"us-central1","minBackoff":"soon"}`
	w := do(t, &mockProvider{}, http.MethodPost, "/queues", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestUpdateQueue_UsesPathIdentity(t *testing.T) {
	mock := &mockProvider{queue: queue("alpha", "us-central1")}
	body := `{"name":"ignored","location":"ignored","maxDispatchesPerSecond":25}`
	w := do(t, mock, http.MethodPut, "/queues/us-central1/alpha", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotQueue != "alpha" {
		t.Fatalf("resolved location/queue = %q/%q", mock.gotLocation, mock.gotQueue)
	}
	if mock.gotCreate.Name != "alpha" {
		t.Fatalf("path identity not applied: %+v", mock.gotCreate)
	}
	// A selective mask must preserve fields the form does not manage.
	if len(mock.gotMask) != 2 || mock.gotMask[0] != "rateLimits" || mock.gotMask[1] != "retryConfig" {
		t.Fatalf("update mask = %v, want [rateLimits retryConfig]", mock.gotMask)
	}
}

func TestQueueActions_Dispatch(t *testing.T) {
	for _, action := range []string{"pause", "resume", "purge"} {
		mock := &mockProvider{queue: queue("alpha", "us-central1")}
		w := do(t, mock, http.MethodPost, "/queues/us-central1/alpha/"+action, "")
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200: %s", action, w.Code, w.Body.String())
		}
		if mock.action != action || mock.gotLocation != "us-central1" || mock.gotQueue != "alpha" {
			t.Fatalf("%s not dispatched: %+v", action, mock)
		}
	}
}

func TestDeleteQueue_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/queues/us-central1/alpha", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotLocation != "us-central1" || mock.gotQueue != "alpha" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestGetQueue_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/queues/us-central1/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestQueueIam_RoundTrip(t *testing.T) {
	mock := &mockProvider{pol: policy.Policy{
		Version:  1,
		Etag:     "abc",
		Bindings: []any{map[string]any{"role": "roles/cloudtasks.enqueuer", "members": []any{"user:a@b.com"}}},
	}}
	w := do(t, mock, http.MethodGet, "/queues/us-central1/alpha/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "roles/cloudtasks.enqueuer") {
		t.Fatalf("policy not rendered: %s", w.Body.String())
	}

	put := do(t, mock, http.MethodPut, "/queues/us-central1/alpha/iam",
		`{"bindings":[{"role":"roles/editor","members":["user:x@y.com"]}],"etag":"abc","version":1}`)
	if put.Code != http.StatusOK {
		t.Fatalf("set status = %d: %s", put.Code, put.Body.String())
	}
	if mock.gotIamBody == nil {
		t.Fatal("iam body not forwarded")
	}
	bindings, ok := mock.gotIamBody["bindings"].([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("bindings not mapped: %+v", mock.gotIamBody)
	}
}

func TestListTasks_FlattensTarget(t *testing.T) {
	mock := &mockProvider{tasks: []tasksstore.Task{task("t1")}}
	w := do(t, mock, http.MethodGet, "/queues/us-central1/q1/tasks", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListTasksResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.Tasks[0].HTTPURL != "https://example.com/hook" {
		t.Fatalf("target not rendered: %+v", resp)
	}
}

func TestCreateTask_MapsHTTPTarget(t *testing.T) {
	mock := &mockProvider{}
	body := `{"name":"t1","target":"http","httpUrl":"https://x/hook","httpMethod":"POST","dispatchDeadline":"1m"}`
	w := do(t, mock, http.MethodPost, "/queues/us-central1/q1/tasks", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotTaskIn.HTTP == nil || mock.gotTaskIn.HTTP.URL != "https://x/hook" {
		t.Fatalf("http target not mapped: %+v", mock.gotTaskIn)
	}
	if mock.gotTaskIn.DispatchDeadline != time.Minute {
		t.Fatalf("dispatchDeadline = %v, want 1m", mock.gotTaskIn.DispatchDeadline)
	}
}

func TestCreateTask_RejectsUnknownTarget(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/queues/us-central1/q1/tasks", `{"name":"t1","target":"grpc"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestRunTask_Dispatch(t *testing.T) {
	mock := &mockProvider{task: task("t1")}
	w := do(t, mock, http.MethodPost, "/queues/us-central1/q1/tasks/t1/run", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.action != "run" || mock.gotTask != "t1" || mock.gotQueue != "q1" {
		t.Fatalf("run not dispatched: %+v", mock)
	}
}

func TestDeleteTask_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/queues/us-central1/q1/tasks/t1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotTask != "t1" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestCreateTask_MapsAppEngineTarget(t *testing.T) {
	mock := &mockProvider{}
	body := `{"target":"appengine","appEngineUri":"/hook","appEngineMethod":"POST"}`
	w := do(t, mock, http.MethodPost, "/queues/us-central1/q1/tasks", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotTaskIn.AppEngine == nil || mock.gotTaskIn.AppEngine.RelativeURI != "/hook" {
		t.Fatalf("app engine target not mapped: %+v", mock.gotTaskIn)
	}
	if mock.gotTaskIn.Target != tasksstore.TargetAppEngine {
		t.Fatalf("target = %q, want %q", mock.gotTaskIn.Target, tasksstore.TargetAppEngine)
	}
}

func TestCreateTask_RejectsBadScheduleTime(t *testing.T) {
	body := `{"target":"http","httpUrl":"https://x","scheduleTime":"tomorrow"}`
	w := do(t, &mockProvider{}, http.MethodPost, "/queues/us-central1/q1/tasks", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestGetTask_PassesIDs(t *testing.T) {
	mock := &mockProvider{task: task("t1")}
	w := do(t, mock, http.MethodGet, "/queues/us-central1/q1/tasks/t1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotQueue != "q1" || mock.gotTask != "t1" {
		t.Fatalf("resolved = %q/%q/%q", mock.gotLocation, mock.gotQueue, mock.gotTask)
	}
	var body Task
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "t1" || body.HTTPURL != "https://example.com/hook" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestQueueIam_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "no queue", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/queues/us-central1/alpha/iam", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
	}
}

func TestListQueues_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "nope", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/queues", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "nope") {
		t.Fatalf("body = %s", w.Body.String())
	}
}
