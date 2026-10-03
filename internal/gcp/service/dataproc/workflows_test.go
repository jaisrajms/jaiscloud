package dataproc

import (
	"context"
	"encoding/json"
	"testing"

	"jaiscloud/internal/model"
)

// pollWorkflowDone polls an operation until it is done (or fails the test).
func pollWorkflowDone(t *testing.T, p *Service, project, region, opID string) map[string]any {
	t.Helper()
	for i := 0; i < 32; i++ {
		op, err := p.GetOperation(context.Background(), project, region, opID)
		if err != nil {
			t.Fatalf("GetOperation: %v", err)
		}
		if op.Done {
			return OperationJSON(op)
		}
	}
	t.Fatalf("workflow operation %s did not complete", opID)
	return nil
}

func inlineTemplateDef(jobType string) map[string]any {
	return map[string]any{
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "wf-managed"}},
		"jobs": []any{
			map[string]any{"stepId": "a", jobType: map[string]any{"mainPythonFileUri": "gs://bucket/main.py"}},
		},
	}
}

func TestInstantiateInlineWorkflow_SingleStep(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	op, err := p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1", inlineTemplateDef("pysparkJob"))
	if err != nil {
		t.Fatalf("InstantiateInlineWorkflowTemplate: %v", err)
	}
	if op.Done {
		t.Fatal("instantiate returned a done operation; want in-flight")
	}
	rendered := pollWorkflowDone(t, p, "proj", "us-central1", op.ID)

	meta, _ := rendered["metadata"].(map[string]any)
	if meta["@type"] != "type.googleapis.com/google.cloud.dataproc.v1.WorkflowMetadata" {
		t.Fatalf("metadata @type = %v", meta["@type"])
	}
	if meta["state"] != "DONE" {
		t.Fatalf("workflow state = %v, want DONE", meta["state"])
	}
	graph, _ := meta["graph"].(map[string]any)
	nodes, _ := graph["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes = %v", graph["nodes"])
	}
	node, _ := nodes[0].(map[string]any)
	if node["state"] != "COMPLETED" || node["jobId"] == "" {
		t.Fatalf("node = %v, want COMPLETED with a jobId", node)
	}
}

func TestInstantiateWorkflow_ByID_WithParameters(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	def := map[string]any{
		"id":        "wf",
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "wf-params"}},
		"parameters": []any{
			map[string]any{"name": "bucket", "fields": []any{"jobs[*].pysparkJob.mainPythonFileUri"}},
		},
		"jobs": []any{
			map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://{bucket}/main.py"}},
		},
	}
	if _, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "wf", Definition: def}); err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}
	op, err := p.InstantiateWorkflowTemplate(ctx, "proj", "us-central1", "wf", 0, map[string]string{"bucket": "my-bucket"})
	if err != nil {
		t.Fatalf("InstantiateWorkflowTemplate: %v", err)
	}
	rendered := pollWorkflowDone(t, p, "proj", "us-central1", op.ID)
	meta, _ := rendered["metadata"].(map[string]any)
	if meta["template"] != "projects/proj/regions/us-central1/workflowTemplates/wf" {
		t.Fatalf("template = %v", meta["template"])
	}
	node := meta["graph"].(map[string]any)["nodes"].([]any)[0].(map[string]any)
	job, err := p.GetJob(ctx, "proj", "us-central1", node["jobId"].(string))
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	var typeJob map[string]any
	if err := json.Unmarshal(job.TypeJob, &typeJob); err != nil {
		t.Fatalf("job type body: %v", err)
	}
	if typeJob["mainPythonFileUri"] != "gs://my-bucket/main.py" {
		t.Fatalf("substituted uri = %v, want gs://my-bucket/main.py", typeJob["mainPythonFileUri"])
	}
}

func TestInstantiateWorkflow_DAGOrder(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	def := map[string]any{
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "wf-dag"}},
		"jobs": []any{
			map[string]any{"stepId": "b", "prerequisiteStepIds": []any{"a"}, "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/b.py"}},
			map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
		},
	}
	op, err := p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1", def)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	rendered := pollWorkflowDone(t, p, "proj", "us-central1", op.ID)
	nodes := rendered["metadata"].(map[string]any)["graph"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(nodes))
	}
	for _, n := range nodes {
		if n.(map[string]any)["state"] != "COMPLETED" {
			t.Fatalf("node not completed: %v", n)
		}
	}
}

func TestInstantiateWorkflow_UnsupportedJobFails(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	def := map[string]any{
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "wf-fail"}},
		"jobs": []any{
			map[string]any{"stepId": "a", "hadoopJob": map[string]any{"mainClass": "com.example.Main"}},
		},
	}
	op, err := p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1", def)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	rendered := pollWorkflowDone(t, p, "proj", "us-central1", op.ID)
	meta := rendered["metadata"].(map[string]any)
	if meta["state"] != "DONE" {
		t.Fatalf("workflow state = %v, want DONE", meta["state"])
	}
	node := meta["graph"].(map[string]any)["nodes"].([]any)[0].(map[string]any)
	if node["state"] != "FAILED" {
		t.Fatalf("node state = %v, want FAILED", node["state"])
	}
}

func TestInstantiateWorkflow_ClusterSelectorNoMatch(t *testing.T) {
	p := newProvider(t)
	_, err := p.InstantiateInlineWorkflowTemplate(context.Background(), "proj", "us-central1", map[string]any{
		"placement": map[string]any{"clusterSelector": map[string]any{"clusterLabels": map[string]any{"env": "missing"}}},
		"jobs":      []any{map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}}},
	})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 {
		t.Fatalf("expected 400 FailedPrecondition, got %v", err)
	}
}
