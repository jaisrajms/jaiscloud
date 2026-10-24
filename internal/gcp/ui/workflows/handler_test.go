package workflowsui

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
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	workflows []workflowsstore.Workflow
	workflow  workflowsstore.Workflow
	execs     []workflowsstore.Execution
	next      string
	exec      workflowsstore.Execution
	err       error

	gotProject      string
	gotLocation     string
	gotWorkflow     string
	gotExecution    string
	gotCreate       WorkflowInput
	gotUpdate       WorkflowUpdateInput
	gotArgument     string
	gotCallLogLevel string
	gotLabels       map[string]string
	gotPageSize     int
	gotPageToken    string
	deleted         bool
}

func (m *mockProvider) ListWorkflowsByProject(_ context.Context, project string) ([]workflowsstore.Workflow, error) {
	m.gotProject = project
	return m.workflows, m.err
}

func (m *mockProvider) GetWorkflow(_ context.Context, _, location, id string) (workflowsstore.Workflow, error) {
	m.gotLocation, m.gotWorkflow = location, id
	return m.workflow, m.err
}

func (m *mockProvider) CreateWorkflow(_ context.Context, _, location string, in WorkflowInput) (workflowsstore.Workflow, error) {
	m.gotLocation, m.gotCreate = location, in
	if m.err != nil {
		return workflowsstore.Workflow{}, m.err
	}
	return workflowsstore.Workflow{ID: in.ID, Location: location, State: "ACTIVE"}, nil
}

func (m *mockProvider) UpdateWorkflow(_ context.Context, _, location, id string, in WorkflowUpdateInput) (workflowsstore.Workflow, error) {
	m.gotLocation, m.gotWorkflow, m.gotUpdate = location, id, in
	return workflowsstore.Workflow{ID: id, Location: location, Description: in.Description}, m.err
}

func (m *mockProvider) DeleteWorkflow(_ context.Context, _, location, id string) error {
	m.gotLocation, m.gotWorkflow, m.deleted = location, id, true
	return m.err
}

func (m *mockProvider) ListExecutions(_ context.Context, _, location, workflowID string, pageSize int, pageToken string) ([]workflowsstore.Execution, string, error) {
	m.gotLocation, m.gotWorkflow = location, workflowID
	m.gotPageSize, m.gotPageToken = pageSize, pageToken
	return m.execs, m.next, m.err
}

func (m *mockProvider) GetExecution(_ context.Context, _, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	m.gotLocation, m.gotWorkflow, m.gotExecution = location, workflowID, executionID
	return m.exec, m.err
}

func (m *mockProvider) RunWorkflow(_ context.Context, _, location, workflowID, argument, callLogLevel string, labels map[string]string) (workflowsstore.Execution, error) {
	m.gotLocation, m.gotWorkflow = location, workflowID
	m.gotArgument, m.gotCallLogLevel, m.gotLabels = argument, callLogLevel, labels
	if m.err != nil {
		return workflowsstore.Execution{}, m.err
	}
	return workflowsstore.Execution{ID: "exec-1", WorkflowID: workflowID, Location: location, State: "SUCCEEDED"}, nil
}

func (m *mockProvider) CancelExecution(_ context.Context, _, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	m.gotLocation, m.gotWorkflow, m.gotExecution = location, workflowID, executionID
	return m.exec, m.err
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

func wf(id, location string) workflowsstore.Workflow {
	return workflowsstore.Workflow{
		ID:             id,
		Location:       location,
		Description:    "d",
		State:          "ACTIVE",
		RevisionID:     "000001-a4d",
		ServiceAccount: "sa@p.iam.gserviceaccount.com",
		SourceContents: "main:\n  steps:\n    - done:\n        return: 1",
		CallLogLevel:   "LOG_ERRORS_ONLY",
		CreateTime:     time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
	}
}

func TestListWorkflows_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{workflows: []workflowsstore.Workflow{
		wf("alpha", "europe-west1"),
		wf("beta", "us-central1"),
	}}
	w := do(t, mock, http.MethodGet, "/workflows", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListWorkflowsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Workflows) != 2 {
		t.Fatalf("got %+v, want 2 workflows", resp)
	}
	if resp.Workflows[0].ID != "alpha" || resp.Workflows[0].Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", resp.Workflows[0])
	}
	if resp.Workflows[0].Name != "projects/test-project/locations/europe-west1/workflows/alpha" {
		t.Fatalf("name not rendered: %q", resp.Workflows[0].Name)
	}
	if resp.Workflows[0].State != "ACTIVE" || resp.Workflows[0].RevisionID != "000001-a4d" {
		t.Fatalf("fields not rendered: %+v", resp.Workflows[0])
	}
}

func TestGetWorkflow_PassesLocationAndID(t *testing.T) {
	mock := &mockProvider{workflow: wf("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/workflows/us-central1/alpha", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotWorkflow != "alpha" {
		t.Fatalf("resolved location/id = %q/%q", mock.gotLocation, mock.gotWorkflow)
	}
	var body Workflow
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ID != "alpha" || body.SourceContents == "" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestCreateWorkflow_RequiresLocation(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/workflows", `{"id":"alpha"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkflow_RequiresID(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/workflows", `{"location":"us-central1"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkflow_CreatedAndMapsFields(t *testing.T) {
	mock := &mockProvider{}
	body := `{"id":"alpha","location":"us-central1","description":"d","sourceContents":"main:\n  steps: []","serviceAccount":"sa@p.iam.gserviceaccount.com","callLogLevel":"LOG_ERRORS_ONLY","labels":{"env":"prod"},"userEnvVars":{"K":"V"}}`
	w := do(t, mock, http.MethodPost, "/workflows", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotCreate.ID != "alpha" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	if mock.gotCreate.SourceContents == "" || mock.gotCreate.Labels["env"] != "prod" || mock.gotCreate.UserEnvVars["K"] != "V" {
		t.Fatalf("fields not mapped: %+v", mock.gotCreate)
	}
}

func TestUpdateWorkflow_UsesPathIdentity(t *testing.T) {
	mock := &mockProvider{workflow: wf("alpha", "us-central1")}
	body := `{"description":"new","sourceContents":"main:\n  steps: []","updateMask":"description,sourceContents"}`
	w := do(t, mock, http.MethodPut, "/workflows/us-central1/alpha", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotWorkflow != "alpha" {
		t.Fatalf("resolved location/id = %q/%q", mock.gotLocation, mock.gotWorkflow)
	}
	if mock.gotUpdate.Description != "new" || mock.gotUpdate.UpdateMask != "description,sourceContents" {
		t.Fatalf("update not mapped: %+v", mock.gotUpdate)
	}
}

func TestDeleteWorkflow_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/workflows/us-central1/alpha", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotLocation != "us-central1" || mock.gotWorkflow != "alpha" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestListExecutions_PassesPaging(t *testing.T) {
	mock := &mockProvider{
		execs: []workflowsstore.Execution{{ID: "e1", WorkflowID: "alpha", Location: "us-central1", State: "SUCCEEDED"}},
		next:  "tok",
	}
	w := do(t, mock, http.MethodGet, "/workflows/us-central1/alpha/executions?pageSize=25&pageToken=abc", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotPageSize != 25 || mock.gotPageToken != "abc" {
		t.Fatalf("paging not forwarded: %d/%q", mock.gotPageSize, mock.gotPageToken)
	}
	var resp ListExecutionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.NextPageToken != "tok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Executions[0].Name != "projects/test-project/locations/us-central1/workflows/alpha/executions/e1" {
		t.Fatalf("execution name not rendered: %q", resp.Executions[0].Name)
	}
}

func TestListExecutions_DefaultsPageSize(t *testing.T) {
	mock := &mockProvider{}
	do(t, mock, http.MethodGet, "/workflows/us-central1/alpha/executions", "")
	if mock.gotPageSize != 500 {
		t.Fatalf("pageSize = %d, want 500", mock.gotPageSize)
	}
}

func TestRunWorkflow_CreatedAndMapsInput(t *testing.T) {
	mock := &mockProvider{}
	body := `{"argument":"{\"x\":1}","callLogLevel":"LOG_ALL_CALLS","labels":{"run":"1"}}`
	w := do(t, mock, http.MethodPost, "/workflows/us-central1/alpha/executions", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotWorkflow != "alpha" {
		t.Fatalf("run not dispatched: %+v", mock)
	}
	if mock.gotArgument != `{"x":1}` || mock.gotCallLogLevel != "LOG_ALL_CALLS" || mock.gotLabels["run"] != "1" {
		t.Fatalf("run input not mapped: %+v", mock)
	}
}

func TestGetExecution_PassesIDs(t *testing.T) {
	mock := &mockProvider{exec: workflowsstore.Execution{ID: "e1", WorkflowID: "alpha", Location: "us-central1", State: "FAILED"}}
	w := do(t, mock, http.MethodGet, "/workflows/us-central1/alpha/executions/e1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotWorkflow != "alpha" || mock.gotExecution != "e1" {
		t.Fatalf("resolved workflow/execution = %q/%q", mock.gotWorkflow, mock.gotExecution)
	}
}

func TestCancelExecution_Dispatches(t *testing.T) {
	mock := &mockProvider{exec: workflowsstore.Execution{ID: "e1", WorkflowID: "alpha", Location: "us-central1", State: "SUCCEEDED"}}
	w := do(t, mock, http.MethodPost, "/workflows/us-central1/alpha/executions/e1/cancel", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotWorkflow != "alpha" || mock.gotExecution != "e1" {
		t.Fatalf("cancel not dispatched: %+v", mock)
	}
}

func TestWorkflow_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/workflows/us-central1/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
