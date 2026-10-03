package workflowexecutions

import (
	"context"
	"errors"
	"testing"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"
)

const assignReturnSource = "main:\n  params: [a, b]\n  steps:\n    - init:\n        assign:\n          - sum: ${a + b}\n    - done:\n        return: ${sum}\n"

func newService(t *testing.T, source string) *Service {
	t.Helper()
	s := workflowsstore.NewMemoryStore()
	w := workflowsstore.Workflow{ID: "wf1", Location: "us-central1", SourceContents: source,
		State: "ACTIVE", RevisionID: "000001-a4d"}
	if err := s.CreateWorkflow(context.Background(), "proj", "us-central1", "wf1", w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return NewService(s, workflowengine.New())
}

func providerCode(t *testing.T, err error) string {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	return perr.Code
}

func TestCreateAndGetExecution(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)

	e, err := s.CreateExecution(ctx, "proj", "us-central1", "wf1", CreateExecutionInput{Argument: `{"a": 20, "b": 22}`})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if e.State != "SUCCEEDED" {
		t.Fatalf("state = %q, want SUCCEEDED", e.State)
	}
	if e.Result != "42" {
		t.Fatalf("result = %q, want 42", e.Result)
	}
	if e.WorkflowRevisionID != "000001-a4d" {
		t.Fatalf("revision = %q", e.WorkflowRevisionID)
	}
	wantName := ExecutionName("proj", "us-central1", "wf1", e.ID)
	if got := ExecutionName("proj", e.Location, e.WorkflowID, e.ID); got != wantName {
		t.Fatalf("name = %q, want %q", got, wantName)
	}

	got, err := s.GetExecution(ctx, "proj", "us-central1", "wf1", e.ID, ViewFull)
	if err != nil {
		t.Fatalf("get execution: %v", err)
	}
	if got.Result != "42" || got.State != "SUCCEEDED" {
		t.Fatalf("unexpected get: %+v", got)
	}

	// BASIC view drops the payloads.
	basic, err := s.GetExecution(ctx, "proj", "us-central1", "wf1", e.ID, ViewBasic)
	if err != nil {
		t.Fatalf("get basic: %v", err)
	}
	if basic.Result != "" || basic.Argument != "" || basic.Error != nil {
		t.Fatalf("BASIC view leaked payload fields: %+v", basic)
	}
	if basic.State != "SUCCEEDED" || basic.WorkflowRevisionID != "000001-a4d" {
		t.Fatalf("BASIC view dropped summary fields: %+v", basic)
	}
}

func TestListExecutionsPagingAndView(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)
	for i := 0; i < 3; i++ {
		if _, err := s.CreateExecution(ctx, "proj", "us-central1", "wf1", CreateExecutionInput{}); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	page, next, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, 2, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 2 || next == "" {
		t.Fatalf("page len = %d next = %q, want 2 and non-empty", len(page), next)
	}
	rest, next2, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, 2, next)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(rest) != 1 || next2 != "" {
		t.Fatalf("page 2 len = %d next = %q, want 1 and empty", len(rest), next2)
	}

	// BASIC list omits results.
	basic, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewBasic, 10, "")
	if err != nil {
		t.Fatalf("list basic: %v", err)
	}
	for _, e := range basic {
		if e.Result != "" {
			t.Fatalf("BASIC list leaked result: %+v", e)
		}
	}
}

func TestCancelTerminalExecution(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)
	e, err := s.CreateExecution(ctx, "proj", "us-central1", "wf1", CreateExecutionInput{Argument: `{"a": 1, "b": 2}`})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.CancelExecution(ctx, "proj", "us-central1", "wf1", e.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got.State != "SUCCEEDED" {
		t.Fatalf("cancel changed a terminal execution to %q", got.State)
	}
}

func TestCreateMissingWorkflow(t *testing.T) {
	s := NewService(workflowsstore.NewMemoryStore(), workflowengine.New())
	_, err := s.CreateExecution(context.Background(), "proj", "us-central1", "missing", CreateExecutionInput{})
	if code := providerCode(t, err); code != "NotFound" {
		t.Fatalf("code = %q, want NotFound", code)
	}
}

func TestCreateBadArgument(t *testing.T) {
	s := newService(t, assignReturnSource)
	_, err := s.CreateExecution(context.Background(), "proj", "us-central1", "wf1", CreateExecutionInput{Argument: "not json"})
	if code := providerCode(t, err); code != "InvalidArgument" {
		t.Fatalf("code = %q, want InvalidArgument", code)
	}
}

func TestCreateExecutionBuiltinEnvVars(t *testing.T) {
	source := `main:
  steps:
    - done:
        return: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID") + "/" + sys.get_env("GOOGLE_CLOUD_LOCATION") + "/" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_ID") + "/" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_REVISION_ID")}
`
	s := newService(t, source)
	e, err := s.CreateExecution(context.Background(), "proj", "us-central1", "wf1", CreateExecutionInput{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if e.Result != `"proj/us-central1/wf1/000001-a4d"` {
		t.Fatalf("built-in resolution = %v", e.Result)
	}
}

func TestCreateExecutionFailsLoud(t *testing.T) {
	s := newService(t, "main:\n  steps:\n    - x:\n        call: http.delete\n        args:\n          url: \"http://x\"\n")
	e, err := s.CreateExecution(context.Background(), "proj", "us-central1", "wf1", CreateExecutionInput{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if e.State != "FAILED" {
		t.Fatalf("state = %q, want FAILED", e.State)
	}
	if e.Error == nil || e.Error.Payload == "" {
		t.Fatalf("missing error payload: %+v", e.Error)
	}
}

func TestCreateExecutionInvalidYamlReturnsInvalidArgument(t *testing.T) {
	s := newService(t, "main: [unclosed")
	_, err := s.CreateExecution(context.Background(), "proj", "us-central1", "wf1", CreateExecutionInput{})
	if code := providerCode(t, err); code != "InvalidArgument" {
		t.Fatalf("code = %q, want InvalidArgument", code)
	}
}

func TestParseName(t *testing.T) {
	loc, wf, ex := ParseName("projects/p/locations/us-central1/workflows/w1/executions/e1")
	if loc != "us-central1" || wf != "w1" || ex != "e1" {
		t.Fatalf("ParseName = %q/%q/%q", loc, wf, ex)
	}
	loc, wf, ex = ParseName("locations/us-central1/workflows/w1/executions")
	if loc != "us-central1" || wf != "w1" || ex != "" {
		t.Fatalf("ParseName collection = %q/%q/%q", loc, wf, ex)
	}
	if p := ProjectFromName("projects/p/locations/x"); p != "p" {
		t.Fatalf("ProjectFromName = %q, want p", p)
	}
	if p := ProjectFromName("locations/x"); p != "" {
		t.Fatalf("ProjectFromName relative = %q, want empty", p)
	}
}
