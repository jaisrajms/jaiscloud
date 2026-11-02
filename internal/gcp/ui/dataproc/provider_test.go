package dataprocui

import (
	"context"
	"encoding/json"
	"testing"

	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

func newTestProvider() *Provider {
	core := dataproccore.NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return NewProvider(core)
}

// TestProviderForwardsToTheCore covers the thin adapter: every method must reach
// the transport-neutral core (success or its error) and drop the operation a
// cluster mutation returns.
func TestProviderForwardsToTheCore(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if list, err := p.ListAllClusters(ctx, "proj"); err != nil || len(list) != 0 {
		t.Fatalf("ListAllClusters = %+v, %v", list, err)
	}
	if list, err := p.ListAllJobs(ctx, "proj"); err != nil || len(list) != 0 {
		t.Fatalf("ListAllJobs = %+v, %v", list, err)
	}
	if list, err := p.ListAllWorkflowTemplates(ctx, "proj"); err != nil || len(list) != 0 {
		t.Fatalf("ListAllWorkflowTemplates = %+v, %v", list, err)
	}

	c, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", dataproccore.ClusterInput{
		Config: json.RawMessage(`{"gceClusterConfig":{"zoneUri":"us-central1-a"}}`),
	})
	if err != nil || c.Name != "c1" || c.Region != "us-central1" {
		t.Fatalf("CreateCluster = %+v, %v", c, err)
	}
	if got, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil || got.Name != "c1" {
		t.Fatalf("GetCluster = %+v, %v", got, err)
	}
	if updated, err := p.UpdateCluster(ctx, "proj", "us-central1", "c1",
		dataproccore.ClusterInput{Labels: map[string]string{"env": "prod"}}, []string{"labels"}); err != nil || updated.Labels["env"] != "prod" {
		t.Fatalf("UpdateCluster = %+v, %v", updated, err)
	}
	// The lifecycle methods return the record; the operation is dropped.
	if _, err := p.StopCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if _, err := p.StartCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}

	// An unknown cluster surfaces the core's error for each job/cluster read.
	if _, err := p.GetJob(ctx, "proj", "us-central1", "nope"); err == nil {
		t.Fatal("GetJob on an unknown job returned nil error")
	}
	if _, err := p.CancelJob(ctx, "proj", "us-central1", "nope"); err == nil {
		t.Fatal("CancelJob on an unknown job returned nil error")
	}
	if _, err := p.SubmitJob(ctx, "proj", "us-central1", dataproccore.JobInput{
		PlacementClusterName: "nope", Type: "sparkJob", TypeJob: json.RawMessage(`{"mainClass":"Main"}`),
	}); err == nil {
		t.Fatal("SubmitJob on an unknown cluster returned nil error")
	}

	// Workflow templates have no lazy state: CRUD round-trips through the core.
	def := map[string]any{
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c1"}},
		"jobs":      []any{map[string]any{"stepId": "a", "sparkJob": map[string]any{"mainClass": "Main"}}},
	}
	tpl, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", dataproccore.WorkflowTemplateInput{ID: "wt1", Definition: def})
	if err != nil || tpl.TemplateID != "wt1" || tpl.Version != 1 {
		t.Fatalf("CreateWorkflowTemplate = %+v, %v", tpl, err)
	}
	if got, err := p.GetWorkflowTemplate(ctx, "proj", "us-central1", "wt1"); err != nil || got.TemplateID != "wt1" {
		t.Fatalf("GetWorkflowTemplate = %+v, %v", got, err)
	}
	if updated, err := p.UpdateWorkflowTemplate(ctx, "proj", "us-central1",
		dataproccore.WorkflowTemplateInput{ID: "wt1", Version: 1, Definition: def}); err != nil || updated.Version != 2 {
		t.Fatalf("UpdateWorkflowTemplate = %+v, %v", updated, err)
	}
	// The cluster exists but is still settling, so instantiate reaches the core
	// and reports its own outcome; either result covers the adapter.
	_, _ = p.InstantiateWorkflowTemplate(ctx, "proj", "us-central1", "wt1", 2, nil)
	if err := p.DeleteWorkflowTemplate(ctx, "proj", "us-central1", "wt1", 2); err != nil {
		t.Fatalf("DeleteWorkflowTemplate: %v", err)
	}
	if err := p.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
}
