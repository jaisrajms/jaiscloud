package workflowexecutions

import (
	"context"
	"testing"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// failedSource drives an execution to FAILED: the engine rejects the unknown
// stdlib call at run time.
const failedSource = "main:\n  steps:\n    - x:\n        call: http.delete\n        args:\n          url: \"http://x\"\n"

// seedWorkflow registers an extra workflow in the service's store so a test can
// produce executions with different outcomes.
func seedWorkflow(t *testing.T, s *Service, id, source string) {
	t.Helper()
	w := workflowsstore.Workflow{ID: id, Location: "us-central1", SourceContents: source, State: "ACTIVE", RevisionID: "000001-a4d"}
	if err := s.workflows.CreateWorkflow(context.Background(), "proj", "us-central1", id, w); err != nil {
		t.Fatalf("seed workflow %s: %v", id, err)
	}
}

func run(t *testing.T, s *Service, workflowID string, in CreateExecutionInput) workflowsstore.Execution {
	t.Helper()
	e, err := s.CreateExecution(context.Background(), "proj", "us-central1", workflowID, in)
	if err != nil {
		t.Fatalf("create execution on %s: %v", workflowID, err)
	}
	return e
}

func TestListExecutionsFilter(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)
	prod := run(t, s, "wf1", CreateExecutionInput{Argument: `{"a": 1, "b": 1}`, Labels: map[string]string{"env": "prod"}})
	run(t, s, "wf1", CreateExecutionInput{Argument: `{"a": 2, "b": 2}`, Labels: map[string]string{"env": "dev"}})
	seedWorkflow(t, s, "wf2", failedSource)
	failed := run(t, s, "wf2", CreateExecutionInput{})
	if failed.State != "FAILED" {
		t.Fatalf("failed execution state = %q", failed.State)
	}

	cases := []struct {
		filter string
		want   int
	}{
		{"", 2},
		{`state="SUCCEEDED"`, 2},
		{`state="FAILED"`, 0},
		{`state:"SUCC"`, 2},
		{`labels.env="prod"`, 1},
		{`labels.env!="prod"`, 1},
		{`labels.env:"pro"`, 1},
		{`labels."env":"prod"`, 1},
		{`labels:*`, 2},
		{`labels.missing:*`, 0},
		{`executionId="` + prod.ID + `"`, 1},
		{`workflowRevisionId="000001-a4d"`, 2},
		{`startTime>"2020-01-01"`, 2},
	}
	for _, tc := range cases {
		page, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, tc.filter, "", 0, "")
		if err != nil {
			t.Fatalf("filter %q: %v", tc.filter, err)
		}
		if len(page) != tc.want {
			t.Errorf("filter %q matched %d, want %d", tc.filter, len(page), tc.want)
		}
	}

	for _, bad := range []string{
		`stepName="x"`,           // documented but unimplemented field
		`duration="1s"`,          // documented but unimplemented field
		`state="A" OR state="B"`, // OR is outside the grammar
		`labels.env="x" AND`,     // dangling AND
		`startTime="not-a-date"`, // unparseable timestamp
	} {
		if _, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, bad, "", 0, ""); err == nil {
			t.Errorf("filter %q: expected error", bad)
		}
	}
}

func TestListExecutionsOrderAndPaging(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)
	first := run(t, s, "wf1", CreateExecutionInput{Argument: `{"a": 1, "b": 1}`})
	second := run(t, s, "wf1", CreateExecutionInput{Argument: `{"a": 2, "b": 2}`})

	// Default order is the documented descending createTime.
	a, b := first, second
	if a.StartTime.Before(b.StartTime) || (a.StartTime.Equal(b.StartTime) && a.ID > b.ID) {
		a, b = b, a
	}
	wantDefault := []string{a.ID, b.ID}
	page, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, "", "", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := executionIDs(page); !sameStrings(got, wantDefault) {
		t.Fatalf("default order = %v, want %v", got, wantDefault)
	}

	// orderBy executionId desc.
	wantAsc := []string{first.ID, second.ID}
	if first.ID > second.ID {
		wantAsc = []string{second.ID, first.ID}
	}
	wantDesc := []string{wantAsc[1], wantAsc[0]}
	page, _, err = s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, "", "executionId desc", 0, "")
	if err != nil {
		t.Fatalf("order: %v", err)
	}
	if got := executionIDs(page); !sameStrings(got, wantDesc) {
		t.Fatalf("executionId desc = %v, want %v", got, wantDesc)
	}

	// Page through one at a time: every execution exactly once, in order.
	var walked []string
	token := ""
	for {
		page, next, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, "", "executionId desc", 1, token)
		if err != nil {
			t.Fatalf("paged list: %v", err)
		}
		walked = append(walked, executionIDs(page)...)
		if next == "" {
			break
		}
		token = next
	}
	if !sameStrings(walked, wantDesc) {
		t.Fatalf("paged walk = %v, want %v", walked, wantDesc)
	}

	// Unsupported orderBy and negative pageSize fail loud.
	if _, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, "", "duration", 0, ""); err == nil {
		t.Error("expected error for unsupported orderBy")
	}
	if _, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, "", "", -1, ""); err == nil {
		t.Error("expected error for negative pageSize")
	}
}

func TestExecutionFilterWithViewProjection(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)
	run(t, s, "wf1", CreateExecutionInput{Argument: `{"a": 1, "b": 2}`})

	// A full-view filtered list still carries the argument; a BASIC list is
	// projected. The filter sees the underlying record either way.
	full, _, err := s.ListExecutions(ctx, "proj", "us-central1", "wf1", ViewFull, `state="SUCCEEDED"`, "", 0, "")
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	if len(full) != 1 || full[0].Argument == "" {
		t.Fatalf("full filter result = %+v", full)
	}
}

func TestListExecutionsUnknownWorkflowNotFound(t *testing.T) {
	ctx := context.Background()
	s := newService(t, assignReturnSource)
	if _, _, err := s.ListExecutions(ctx, "proj", "us-central1", "nope", ViewBasic, "", "", 0, ""); err == nil {
		t.Fatal("expected NotFound for an unknown workflow")
	} else if code := providerCode(t, err); code != "NotFound" {
		t.Fatalf("code = %s, want NotFound", code)
	}
	// A bad filter must not mask the missing parent.
	if _, _, err := s.ListExecutions(ctx, "proj", "us-central1", "nope", ViewBasic, `uid="x"`, "", 0, ""); err == nil {
		t.Fatal("expected NotFound for an unknown workflow even with a bad filter")
	} else if code := providerCode(t, err); code != "NotFound" {
		t.Fatalf("code = %s, want NotFound", code)
	}
}

func TestExecutionPageSize(t *testing.T) {
	cases := []struct {
		in   int
		view View
		want int
	}{
		{0, ViewBasic, 100},
		{-5, ViewBasic, -5}, // negative passes through for the shared pager to reject
		{50, ViewBasic, 50},
		{5000, ViewBasic, 1000},
		{5000, ViewFull, 100},
		{250, ViewFull, 100},
		{10, ViewFull, 10},
	}
	for _, tc := range cases {
		if got := executionPageSize(tc.in, tc.view); got != tc.want {
			t.Errorf("executionPageSize(%d, %v) = %d, want %d", tc.in, tc.view, got, tc.want)
		}
	}
}

func executionIDs(execs []workflowsstore.Execution) []string {
	out := make([]string, 0, len(execs))
	for _, e := range execs {
		out = append(out, e.ID)
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
