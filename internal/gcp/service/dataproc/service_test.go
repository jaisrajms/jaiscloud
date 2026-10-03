package dataproc

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// TestJobSubstateRunning verifies the dataproc.v1 JobStatus.substate field: a
// job is born PENDING, and the RUNNING state it reaches carries the real QUEUED
// substate, both in the stored JobStatus and in the rendered job response.
func TestJobSubstateRunning(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	j := jobToStore("proj", "us-central1", JobInputFromMap(map[string]any{
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	if j.Status.State != "PENDING" {
		t.Fatalf("submitted job status = %+v, want PENDING", j.Status)
	}

	if err := p.store.CreateJob(ctx, "proj", "us-central1", j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// Mock mode settles one hop per read: PENDING -> SETUP_DONE -> RUNNING.
	got, err := p.GetJob(ctx, "proj", "us-central1", j.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status.State != "SETUP_DONE" {
		t.Fatalf("first read state = %q, want SETUP_DONE", got.Status.State)
	}
	got, err = p.GetJob(ctx, "proj", "us-central1", j.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status.State != "RUNNING" || got.Status.Substate != "QUEUED" {
		t.Fatalf("running job status = %+v, want RUNNING/QUEUED", got.Status)
	}
	status, _ := JobJSON(got)["status"].(map[string]any)
	if status["substate"] != "QUEUED" {
		t.Fatalf("GetJob status.substate = %v, want QUEUED", status["substate"])
	}
}

// TestUpdateJob_Labels verifies the gRPC-only UpdateJob path merges labels.
func TestUpdateJob_Labels(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j1"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	updated, err := p.UpdateJob(ctx, "proj", "us-central1", "j1",
		JobInput{Labels: map[string]string{"env": "prod"}}, []string{"labels"})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if updated.Labels["env"] != "prod" {
		t.Fatalf("labels = %v, want env=prod", updated.Labels)
	}
}

// TestUpdateJob_NotFound reports 404 for a missing job.
func TestUpdateJob_NotFound(t *testing.T) {
	p := newProvider(t)
	_, err := p.UpdateJob(context.Background(), "proj", "us-central1", "nope", JobInput{}, nil)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404 NotFound, got %v", err)
	}
}

// TestParseMask covers the REST updateMask query parsing.
func TestParseMask(t *testing.T) {
	got := ParseMask(" labels , config.workerConfig.numInstances ")
	if len(got) != 2 || got[0] != "labels" || got[1] != "config.workerConfig.numInstances" {
		t.Fatalf("ParseMask = %v", got)
	}
	if got := ParseMask(""); len(got) != 0 {
		t.Fatalf("ParseMask(empty) = %v", got)
	}
}

// TestUpdateCluster_NestedConfigMask verifies nested config.* mask paths apply
// (snake_case from gRPC FieldMask and camelCase from the REST query), rather
// than silently no-opping.
func TestUpdateCluster_NestedConfigMask(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
		Config: []byte(`{"workerConfig":{"numInstances":3}}`),
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	for _, mask := range [][]string{
		{"config.worker_config.num_instances"},
		{"config.workerConfig.numInstances"},
	} {
		if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
			Config: []byte(`{"workerConfig":{"numInstances":5}}`),
		}, mask); err != nil {
			t.Fatalf("UpdateCluster(%v): %v", mask, err)
		}
	}
	c, err := p.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	wc, _ := mustJSONMap(c.Config)["workerConfig"].(map[string]any)
	if got := wc["numInstances"]; got != float64(5) {
		t.Fatalf("numInstances = %v, want 5 (mask did not apply)", got)
	}
}

// TestSubmitJob_PlacementClusterUUID verifies a submitted job captures the
// cluster's output-only UUID as placement.clusterUuid (dataproc.v1.JobPlacement)
// and renders it on the wire, so REST and gRPC agree.
func TestSubmitJob_PlacementClusterUUID(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j1"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if c.ClusterUUID == "" || j.PlacementClusterUUID != c.ClusterUUID {
		t.Fatalf("stored placement clusterUuid = %q, want cluster UUID %q", j.PlacementClusterUUID, c.ClusterUUID)
	}
	placement, _ := JobJSON(j)["placement"].(map[string]any)
	if placement["clusterUuid"] != c.ClusterUUID {
		t.Fatalf("rendered placement.clusterUuid = %v, want %q", placement["clusterUuid"], c.ClusterUUID)
	}
}

// TestResourceNames verifies the core uses the centralized formatters.
func TestResourceNames(t *testing.T) {
	if got, want := ClusterName("p", "us-central1", "c"), "projects/p/regions/us-central1/clusters/c"; got != want {
		t.Errorf("ClusterName = %q, want %q", got, want)
	}
	if got, want := JobName("p", "us-central1", "j"), "projects/p/regions/us-central1/jobs/j"; got != want {
		t.Errorf("JobName = %q, want %q", got, want)
	}
	if got, want := OperationName("p", "us-central1", "o"), "projects/p/regions/us-central1/operations/o"; got != want {
		t.Errorf("OperationName = %q, want %q", got, want)
	}
}

// TestDiagnoseCluster satisfies coverage of the fail-loud stub.
func TestDiagnoseCluster(t *testing.T) {
	p := newProvider(t)
	if err := p.DiagnoseCluster(); err == nil {
		t.Fatal("expected DiagnoseCluster to return Unimplemented")
	}
}
