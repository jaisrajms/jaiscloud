package workflows

import (
	"context"
	"testing"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

func newListService() *Service {
	return NewService(workflowsstore.NewMemoryStore())
}

func TestListWorkflowsFilter(t *testing.T) {
	ctx := context.Background()
	s := newListService()
	seed := []CreateInput{
		{ID: "wf-a", SourceContents: simpleSource, Labels: map[string]string{"env": "prod"}},
		{ID: "wf-b", SourceContents: simpleSource, Labels: map[string]string{"env": "dev"}},
		{ID: "wf-c", SourceContents: simpleSource},
	}
	for _, in := range seed {
		if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", in); err != nil {
			t.Fatalf("seed %s: %v", in.ID, err)
		}
	}

	cases := []struct {
		filter string
		want   []string
	}{
		{"", []string{"wf-a", "wf-b", "wf-c"}},
		{`state="ACTIVE"`, []string{"wf-a", "wf-b", "wf-c"}},
		{`state="ERROR"`, nil},
		{`labels.env="prod"`, []string{"wf-a"}},
		{`labels.env:*`, []string{"wf-a", "wf-b"}},
		{`labels."env":"prod"`, []string{"wf-a"}},
		{`labels:*`, []string{"wf-a", "wf-b"}},
		{`labels.env!="prod"`, []string{"wf-b", "wf-c"}},
		{`name="projects/proj/locations/us-central1/workflows/wf-a"`, []string{"wf-a"}},
		{`name:"wf-a"`, []string{"wf-a"}},
		{`labels.env="prod" AND state="ACTIVE"`, []string{"wf-a"}},
		{`createTime>"2020-01-01"`, []string{"wf-a", "wf-b", "wf-c"}},
	}
	for _, tc := range cases {
		page, _, err := s.ListWorkflows(ctx, "proj", "us-central1", tc.filter, "", 0, "")
		if err != nil {
			t.Fatalf("filter %q: %v", tc.filter, err)
		}
		if got := ids(page); !equal(got, tc.want) {
			t.Errorf("filter %q = %v, want %v", tc.filter, got, tc.want)
		}
	}
}

func TestListWorkflowsFilterFailsLoud(t *testing.T) {
	ctx := context.Background()
	s := newListService()
	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf-a", SourceContents: simpleSource}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, bad := range []string{
		`uid="x"`,                          // unsupported field
		`description="x"`,                  // unsupported field
		`labels.env="x" OR state="ACTIVE"`, // OR is outside the grammar
		`name="x" AND`,                     // dangling AND
		`state`,                            // missing operator
	} {
		if _, _, err := s.ListWorkflows(ctx, "proj", "us-central1", bad, "", 0, ""); err == nil {
			t.Errorf("filter %q: expected error", bad)
		}
	}
}

func TestListWorkflowsOrderAndPaging(t *testing.T) {
	ctx := context.Background()
	s := newListService()
	for _, id := range []string{"wf-b", "wf-c", "wf-a"} {
		if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: id, SourceContents: simpleSource}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	// Default order is name (id) ascending.
	page, _, err := s.ListWorkflows(ctx, "proj", "us-central1", "", "", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := ids(page); !equal(got, []string{"wf-a", "wf-b", "wf-c"}) {
		t.Fatalf("default order = %v", got)
	}

	// orderBy name desc.
	page, _, err = s.ListWorkflows(ctx, "proj", "us-central1", "", "name desc", 0, "")
	if err != nil {
		t.Fatalf("order: %v", err)
	}
	if got := ids(page); !equal(got, []string{"wf-c", "wf-b", "wf-a"}) {
		t.Fatalf("name desc = %v", got)
	}

	// Page through name desc one at a time; every item exactly once.
	var walked []string
	token := ""
	for {
		page, next, err := s.ListWorkflows(ctx, "proj", "us-central1", "", "name desc", 1, token)
		if err != nil {
			t.Fatalf("paged list: %v", err)
		}
		for _, w := range page {
			walked = append(walked, w.ID)
		}
		if next == "" {
			break
		}
		token = next
	}
	if !equal(walked, []string{"wf-c", "wf-b", "wf-a"}) {
		t.Fatalf("paged walk = %v", walked)
	}

	// Unsupported orderBy and negative pageSize fail loud.
	if _, _, err := s.ListWorkflows(ctx, "proj", "us-central1", "", "uid", 0, ""); err == nil {
		t.Error("expected error for unsupported orderBy")
	}
	if _, _, err := s.ListWorkflows(ctx, "proj", "us-central1", "", "", -1, ""); err == nil {
		t.Error("expected error for negative pageSize")
	}
}

func TestValidateWorkflowID(t *testing.T) {
	valid := []string{"a", "wf1", "my-workflow_1", "A123"}
	for _, id := range valid {
		if err := validateWorkflowID(id); err != nil {
			t.Errorf("valid id %q rejected: %v", id, err)
		}
	}
	invalid := []string{"", "1a", "-a", "_a", "a.b", "a/b", "a b", "trail-", "trail_", "wf-"}
	for _, id := range invalid {
		if err := validateWorkflowID(id); err == nil {
			t.Errorf("invalid id %q accepted", id)
		}
	}
	long := "a"
	for len(long) < 65 {
		long += "b"
	}
	if err := validateWorkflowID(long); err == nil {
		t.Error("over-long id accepted")
	}
}

func TestValidateSource(t *testing.T) {
	if err := validateSource(simpleSource); err != nil {
		t.Errorf("valid source rejected: %v", err)
	}
	if err := validateSource(""); err == nil {
		t.Error("empty source must be rejected (a definition is required)")
	}
	for _, bad := range []string{
		"main: [not, a, map]",
		"steps:\n  - r:\n      return: 1\n", // no main routine
		"main:\n  params: [a]\n",            // no steps
		"\tnot: valid: yaml",                // YAML syntax error
	} {
		if err := validateSource(bad); err == nil {
			t.Errorf("invalid source accepted: %q", bad)
		}
	}
}

func TestCreateAndUpdateRejectInvalidInput(t *testing.T) {
	ctx := context.Background()
	s := newListService()

	// Invalid workflowId is rejected before anything is stored.
	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "1bad", SourceContents: simpleSource}); err == nil {
		t.Error("expected invalid workflowId to be rejected")
	}
	// Invalid definition is rejected at create.
	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf", SourceContents: "not-a-workflow: true"}); err == nil {
		t.Error("expected invalid definition to be rejected at create")
	}
	// None of the rejected creates were stored.
	page, _, err := s.ListWorkflows(ctx, "proj", "us-central1", "", "", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("rejected creates were stored: %v", ids(page))
	}

	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf", SourceContents: simpleSource}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// An update that claims sourceContents with a malformed value is rejected.
	if _, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf", UpdateMask: "sourceContents", SourceContents: "not-a-workflow: true",
	}); err == nil {
		t.Error("expected invalid definition to be rejected at update")
	}
}

func ids(wfs []workflowsstore.Workflow) []string {
	out := make([]string, 0, len(wfs))
	for _, w := range wfs {
		out = append(out, w.ID)
	}
	return out
}

func equal(a, b []string) bool {
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
