package workflowsui

import (
	"context"
	"testing"

	workflowscore "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// validSource is a minimal structurally-valid workflow definition. create and
// update validate the definition (as real Cloud Workflows does), so these
// management-path tests must seed a source the engine accepts.
const validSource = "main:\n  steps:\n    - done:\n        return: ok\n"

// newTestProvider wires the real Workflows core over a memory store. The
// executions core is nil: these tests exercise the management path only.
func newTestProvider() (*Provider, *workflowscore.Service) {
	core := workflowscore.NewService(workflowsstore.NewMemoryStore())
	return NewProvider(core, nil), core
}

func TestProvider_ListWorkflowsByProject_AggregatesAndSorts(t *testing.T) {
	p, core := newTestProvider()
	ctx := context.Background()
	for _, in := range []workflowscore.CreateInput{
		{ID: "alpha", SourceContents: validSource},
		{ID: "beta", SourceContents: validSource},
	} {
		loc := "us-central1"
		if in.ID == "beta" {
			loc = "europe-west1"
		}
		if _, _, err := core.CreateWorkflow(ctx, "test-project", loc, in); err != nil {
			t.Fatalf("seed %s: %v", in.ID, err)
		}
	}
	// A different project must not leak in.
	if _, _, err := core.CreateWorkflow(ctx, "other-project", "us-central1", workflowscore.CreateInput{ID: "gamma", SourceContents: validSource}); err != nil {
		t.Fatalf("seed other project: %v", err)
	}

	got, err := p.ListWorkflowsByProject(ctx, "test-project")
	if err != nil {
		t.Fatalf("ListWorkflowsByProject: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d workflows, want 2", len(got))
	}
	// Sorted by location: europe-west1/beta before us-central1/alpha.
	if got[0].ID != "beta" || got[0].Location != "europe-west1" {
		t.Fatalf("first row = %+v, want beta/europe-west1", got[0])
	}
	if got[1].ID != "alpha" || got[1].Location != "us-central1" {
		t.Fatalf("second row = %+v, want alpha/us-central1", got[1])
	}
}

func TestProvider_UpdateWorkflow_DefaultMaskRoundTripsSource(t *testing.T) {
	p, core := newTestProvider()
	ctx := context.Background()
	created, _, err := core.CreateWorkflow(ctx, "test-project", "us-central1", workflowscore.CreateInput{
		ID: "alpha", SourceContents: validSource, Description: "d",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The edit form round-trips the loaded source, so a description-only change
	// must not bump the revision.
	updated, err := p.UpdateWorkflow(ctx, "test-project", "us-central1", "alpha", WorkflowUpdateInput{
		Description:    "new",
		SourceContents: validSource,
	})
	if err != nil {
		t.Fatalf("UpdateWorkflow: %v", err)
	}
	if updated.Description != "new" || updated.SourceContents != validSource {
		t.Fatalf("unexpected update: %+v", updated)
	}
	if updated.RevisionID != created.RevisionID {
		t.Fatalf("revision bumped on unchanged source: %q -> %q", created.RevisionID, updated.RevisionID)
	}
}

func TestProvider_UpdateWorkflow_ExplicitMaskPreservesOtherFields(t *testing.T) {
	p, core := newTestProvider()
	ctx := context.Background()
	if _, _, err := core.CreateWorkflow(ctx, "test-project", "us-central1", workflowscore.CreateInput{
		ID: "alpha", SourceContents: validSource, Description: "d",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// An explicit description-only mask must leave sourceContents untouched even
	// though the input carries an empty value for it.
	updated, err := p.UpdateWorkflow(ctx, "test-project", "us-central1", "alpha", WorkflowUpdateInput{
		Description: "only",
		UpdateMask:  "description",
	})
	if err != nil {
		t.Fatalf("UpdateWorkflow: %v", err)
	}
	if updated.SourceContents != validSource {
		t.Fatalf("explicit mask did not preserve source: %+v", updated)
	}
}
