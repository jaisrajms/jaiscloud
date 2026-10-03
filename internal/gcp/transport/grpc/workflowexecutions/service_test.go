package workflowexecutions

import (
	"context"
	"strings"
	"testing"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/workflowexecutions"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
)

const source = "main:\n  params: [a, b]\n  steps:\n    - init:\n        assign:\n          - sum: ${a + b}\n    - done:\n        return: ${sum}\n"

func newService(t *testing.T) *Service {
	t.Helper()
	s := workflowsstore.NewMemoryStore()
	w := workflowsstore.Workflow{ID: "wf1", Location: "us-central1", SourceContents: source,
		State: "ACTIVE", RevisionID: "000001-a4d"}
	if err := s.CreateWorkflow(context.Background(), "proj", "us-central1", "wf1", w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return NewService(core.NewService(s, workflowengine.New()), "default-proj")
}

const parent = "projects/proj/locations/us-central1/workflows/wf1"

func create(t *testing.T, s *Service) *executionspb.Execution {
	t.Helper()
	e, err := s.CreateExecution(context.Background(), &executionspb.CreateExecutionRequest{
		Parent:    parent,
		Execution: &executionspb.Execution{Argument: `{"a": 20, "b": 22}`},
	})
	if err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	return e
}

func TestCreateExecutionProto(t *testing.T) {
	s := newService(t)
	e := create(t, s)
	if e.GetState() != executionspb.Execution_SUCCEEDED {
		t.Fatalf("state = %v, want SUCCEEDED", e.GetState())
	}
	if e.GetResult() != "42" {
		t.Fatalf("result = %q, want 42", e.GetResult())
	}
	if e.GetWorkflowRevisionId() != "000001-a4d" {
		t.Fatalf("revision = %q", e.GetWorkflowRevisionId())
	}
	if e.GetDuration() == nil {
		t.Fatalf("duration is nil")
	}
	if !strings.HasPrefix(e.GetName(), parent+"/executions/") {
		t.Fatalf("name = %q", e.GetName())
	}
}

func TestGetExecutionViews(t *testing.T) {
	s := newService(t)
	e := create(t, s)

	// Default view is FULL for Get.
	full, err := s.GetExecution(context.Background(), &executionspb.GetExecutionRequest{Name: e.GetName()})
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if full.GetResult() != "42" {
		t.Fatalf("default Get result = %q, want 42", full.GetResult())
	}

	basic, err := s.GetExecution(context.Background(), &executionspb.GetExecutionRequest{
		Name: e.GetName(), View: executionspb.ExecutionView_BASIC,
	})
	if err != nil {
		t.Fatalf("GetExecution BASIC: %v", err)
	}
	if basic.GetResult() != "" || basic.GetArgument() != "" {
		t.Fatalf("BASIC view leaked payload: %+v", basic)
	}
	if basic.GetState() != executionspb.Execution_SUCCEEDED {
		t.Fatalf("BASIC state = %v", basic.GetState())
	}
}

func TestListExecutionsViews(t *testing.T) {
	s := newService(t)
	e := create(t, s)

	list, err := s.ListExecutions(context.Background(), &executionspb.ListExecutionsRequest{Parent: parent})
	if err != nil {
		t.Fatalf("ListExecutions: %v", err)
	}
	found := false
	for _, got := range list.GetExecutions() {
		if got.GetName() == e.GetName() {
			found = true
			// Default list view is BASIC.
			if got.GetResult() != "" {
				t.Fatalf("default list leaked result: %q", got.GetResult())
			}
		}
	}
	if !found {
		t.Fatalf("created execution not in list")
	}

	listFull, err := s.ListExecutions(context.Background(), &executionspb.ListExecutionsRequest{
		Parent: parent, View: executionspb.ExecutionView_FULL,
	})
	if err != nil {
		t.Fatalf("ListExecutions FULL: %v", err)
	}
	foundFull := false
	for _, got := range listFull.GetExecutions() {
		if got.GetName() == e.GetName() {
			foundFull = true
			if got.GetResult() != "42" {
				t.Fatalf("FULL list result = %q, want 42", got.GetResult())
			}
		}
	}
	if !foundFull {
		t.Fatalf("created execution not in FULL list")
	}
}

func TestCancelTerminalExecutionProto(t *testing.T) {
	s := newService(t)
	e := create(t, s)
	got, err := s.CancelExecution(context.Background(), &executionspb.CancelExecutionRequest{Name: e.GetName()})
	if err != nil {
		t.Fatalf("CancelExecution: %v", err)
	}
	if got.GetState() != executionspb.Execution_SUCCEEDED {
		t.Fatalf("cancel changed terminal state to %v", got.GetState())
	}
}

func TestMissingWorkflowStatus(t *testing.T) {
	s := NewService(core.NewService(workflowsstore.NewMemoryStore(), workflowengine.New()), "default-proj")
	_, err := s.CreateExecution(context.Background(), &executionspb.CreateExecutionRequest{
		Parent:    "projects/proj/locations/us-central1/workflows/missing",
		Execution: &executionspb.Execution{},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}

func TestBadArgumentStatus(t *testing.T) {
	s := newService(t)
	_, err := s.CreateExecution(context.Background(), &executionspb.CreateExecutionRequest{
		Parent:    parent,
		Execution: &executionspb.Execution{Argument: "not json"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestCallLogLevelRoundTrip(t *testing.T) {
	s := newService(t)
	e, err := s.CreateExecution(context.Background(), &executionspb.CreateExecutionRequest{
		Parent:    parent,
		Execution: &executionspb.Execution{CallLogLevel: executionspb.Execution_LOG_ALL_CALLS},
	})
	if err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if e.GetCallLogLevel() != executionspb.Execution_LOG_ALL_CALLS {
		t.Fatalf("callLogLevel = %v, want LOG_ALL_CALLS", e.GetCallLogLevel())
	}
}
