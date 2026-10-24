package schedulerui

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
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/model"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	jobs []schedstore.Job
	job  schedstore.Job
	err  error

	gotLocation string
	gotName     string
	gotCreate   schedstore.Job
	deleted     bool
	action      string
}

func (m *mockProvider) ListJobsByProject(_ context.Context, _ string) ([]schedstore.Job, error) {
	return m.jobs, m.err
}

func (m *mockProvider) GetJob(_ context.Context, _, location, name string) (schedstore.Job, error) {
	m.gotLocation, m.gotName = location, name
	return m.job, m.err
}

func (m *mockProvider) CreateJob(_ context.Context, _, location string, j schedstore.Job) (schedstore.Job, error) {
	m.gotLocation, m.gotCreate = location, j
	if m.err != nil {
		return schedstore.Job{}, m.err
	}
	j.Location = location
	return j, nil
}

func (m *mockProvider) UpdateJob(_ context.Context, _, location, name string, upd schedstore.Job, _ []string) (schedstore.Job, error) {
	m.gotLocation, m.gotName, m.gotCreate = location, name, upd
	return upd, m.err
}

func (m *mockProvider) DeleteJob(_ context.Context, _, location, name string) error {
	m.gotLocation, m.gotName, m.deleted = location, name, true
	return m.err
}

func (m *mockProvider) PauseJob(_ context.Context, _, location, name string) (schedstore.Job, error) {
	return m.mutate("pause", location, name)
}

func (m *mockProvider) ResumeJob(_ context.Context, _, location, name string) (schedstore.Job, error) {
	return m.mutate("resume", location, name)
}

func (m *mockProvider) RunJob(_ context.Context, _, location, name string) (schedstore.Job, error) {
	return m.mutate("run", location, name)
}

func (m *mockProvider) mutate(action, location, name string) (schedstore.Job, error) {
	m.action, m.gotLocation, m.gotName = action, location, name
	return m.job, m.err
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

func job(name, location string) schedstore.Job {
	return schedstore.Job{
		ProjectID:      "test-project",
		Location:       location,
		Name:           name,
		Schedule:       "0 * * * *",
		TimeZone:       "UTC",
		State:          schedstore.StateEnabled,
		HTTP:           &schedstore.HttpTarget{URI: "https://example.com/hook", HTTPMethod: "POST"},
		ScheduleTime:   time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		UserUpdateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestListJobs_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{jobs: []schedstore.Job{
		job("alpha", "europe-west1"),
		job("beta", "us-central1"),
	}}

	w := do(t, mock, http.MethodGet, "/jobs", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListJobsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Jobs) != 2 {
		t.Fatalf("got %+v, want 2 jobs", resp)
	}
	if resp.Jobs[0].Name != "alpha" || resp.Jobs[0].Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", resp.Jobs[0])
	}
	if resp.Jobs[0].Target != "http" || resp.Jobs[0].HTTPURI != "https://example.com/hook" {
		t.Fatalf("target not rendered: %+v", resp.Jobs[0])
	}
}

func TestGetJob_PassesLocationAndJob(t *testing.T) {
	mock := &mockProvider{job: job("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/jobs/us-central1/alpha", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotName != "alpha" {
		t.Fatalf("resolved location/job = %q/%q", mock.gotLocation, mock.gotName)
	}
	var body Job
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "alpha" || body.State != "ENABLED" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestCreateJob_RequiresLocation(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/jobs", `{"name":"a","schedule":"0 * * * *","target":"http","httpUri":"https://x"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateJob_CreatedAndMapsTarget(t *testing.T) {
	mock := &mockProvider{}
	body := `{"name":"a","location":"us-central1","schedule":"0 * * * *","timeZone":"UTC","target":"http","httpUri":"https://x/hook","httpMethod":"POST","retryCount":3,"attemptDeadline":"1m"}`
	w := do(t, mock, http.MethodPost, "/jobs", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotCreate.Name != "a" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	if mock.gotCreate.HTTP == nil || mock.gotCreate.HTTP.URI != "https://x/hook" {
		t.Fatalf("http target not mapped: %+v", mock.gotCreate)
	}
	if mock.gotCreate.Target != schedstore.TargetHTTP {
		t.Fatalf("target = %q, want %q", mock.gotCreate.Target, schedstore.TargetHTTP)
	}
	if mock.gotCreate.RetryConfig == nil || mock.gotCreate.RetryConfig.RetryCount != 3 {
		t.Fatalf("retry config not mapped: %+v", mock.gotCreate)
	}
	if mock.gotCreate.AttemptDeadline != time.Minute {
		t.Fatalf("attemptDeadline = %v, want 1m", mock.gotCreate.AttemptDeadline)
	}
}

func TestCreateJob_RejectsUnknownTarget(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/jobs", `{"name":"a","location":"us-central1","schedule":"* * * * *","target":"grpc"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestUpdateJob_UsesPathIdentity(t *testing.T) {
	mock := &mockProvider{job: job("alpha", "us-central1")}
	body := `{"name":"ignored","location":"ignored","schedule":"*/5 * * * *","target":"pubsub","pubsubTopic":"projects/p/topics/t"}`
	w := do(t, mock, http.MethodPut, "/jobs/us-central1/alpha", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotName != "alpha" {
		t.Fatalf("resolved location/job = %q/%q", mock.gotLocation, mock.gotName)
	}
	if mock.gotCreate.PubSub == nil || mock.gotCreate.PubSub.TopicName != "projects/p/topics/t" {
		t.Fatalf("pubsub target not mapped: %+v", mock.gotCreate)
	}
}

func TestJobActions_Dispatch(t *testing.T) {
	for _, action := range []string{"pause", "resume", "run"} {
		mock := &mockProvider{job: job("alpha", "us-central1")}
		w := do(t, mock, http.MethodPost, "/jobs/us-central1/alpha/"+action, "")
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200: %s", action, w.Code, w.Body.String())
		}
		if mock.action != action || mock.gotLocation != "us-central1" || mock.gotName != "alpha" {
			t.Fatalf("%s not dispatched: %+v", action, mock)
		}
	}
}

func TestDeleteJob_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/jobs/us-central1/alpha", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotLocation != "us-central1" || mock.gotName != "alpha" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestGetJob_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/jobs/us-central1/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestListJobs_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "nope", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/jobs", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "nope") {
		t.Fatalf("body = %s", w.Body.String())
	}
}
