package dataproc

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

func validTemplateDef(id string) map[string]any {
	return map[string]any{
		"id":        id,
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
		"jobs": []any{
			map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
		},
	}
}

func wantProviderStatus(t *testing.T, err error, status int) {
	t.Helper()
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != status {
		t.Fatalf("expected HTTP %d ProviderError, got %v", status, err)
	}
}

func TestWorkflowTemplateCRUDRoundTrip(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	created, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Definition: validTemplateDef("t1")})
	if err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}
	if created.Version != 1 {
		t.Fatalf("version = %d, want 1", created.Version)
	}

	got, err := p.GetWorkflowTemplate(ctx, "proj", "us-central1", "t1", 0)
	if err != nil {
		t.Fatalf("GetWorkflowTemplate: %v", err)
	}
	rendered := WorkflowTemplateJSON(got)
	if rendered["name"] != "projects/proj/regions/us-central1/workflowTemplates/t1" {
		t.Fatalf("name = %v", rendered["name"])
	}
	if rendered["id"] != "t1" || rendered["version"] != int32(1) {
		t.Fatalf("id/version = %v/%v", rendered["id"], rendered["version"])
	}
	if _, ok := rendered["createTime"]; !ok {
		t.Fatalf("missing createTime in %v", rendered)
	}

	// Get with the matching explicit version works; a wrong one is NotFound.
	if _, err := p.GetWorkflowTemplate(ctx, "proj", "us-central1", "t1", 1); err != nil {
		t.Fatalf("GetWorkflowTemplate(version=1): %v", err)
	}
	_, err = p.GetWorkflowTemplate(ctx, "proj", "us-central1", "t1", 2)
	wantProviderStatus(t, err, 404)

	// Delete then Get → 404.
	if err := p.DeleteWorkflowTemplate(ctx, "proj", "us-central1", "t1", 0); err != nil {
		t.Fatalf("DeleteWorkflowTemplate: %v", err)
	}
	_, err = p.GetWorkflowTemplate(ctx, "proj", "us-central1", "t1", 0)
	wantProviderStatus(t, err, 404)
}

func TestWorkflowTemplateCreateDuplicate(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Definition: validTemplateDef("t1")}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Definition: validTemplateDef("t1")})
	wantProviderStatus(t, err, 409)
}

func TestWorkflowTemplateUpdateVersionOCC(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Definition: validTemplateDef("t1")}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Mismatched (and zero) version → ABORTED / 409.
	_, err := p.UpdateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Version: 0, Definition: validTemplateDef("t1")})
	wantProviderStatus(t, err, 409)
	_, err = p.UpdateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Version: 7, Definition: validTemplateDef("t1")})
	wantProviderStatus(t, err, 409)

	def := validTemplateDef("t1")
	def["labels"] = map[string]any{"env": "prod"}
	updated, err := p.UpdateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Version: 1, Definition: def})
	if err != nil {
		t.Fatalf("UpdateWorkflowTemplate: %v", err)
	}
	if updated.Version != 2 {
		t.Fatalf("version = %d, want 2", updated.Version)
	}
	rendered := WorkflowTemplateJSON(updated)
	labels, _ := rendered["labels"].(map[string]any)
	if labels["env"] != "prod" {
		t.Fatalf("labels = %v, want env=prod", rendered["labels"])
	}
}

func TestWorkflowTemplateValidation(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	cases := map[string]map[string]any{
		"missing placement": {
			"id":   "t",
			"jobs": []any{map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}}},
		},
		"no jobs": {
			"id":        "t",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
		},
		"duplicate stepId": {
			"id":        "t",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
			"jobs": []any{
				map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
				map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/b.py"}},
			},
		},
		"unknown prerequisite": {
			"id":        "t",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
			"jobs": []any{
				map[string]any{"stepId": "a", "prerequisiteStepIds": []any{"nope"}, "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
			},
		},
		"cycle": {
			"id":        "t",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
			"jobs": []any{
				map[string]any{"stepId": "a", "prerequisiteStepIds": []any{"b"}, "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
				map[string]any{"stepId": "b", "prerequisiteStepIds": []any{"a"}, "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/b.py"}},
			},
		},
		"no job type": {
			"id":        "t",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
			"jobs":      []any{map[string]any{"stepId": "a"}},
		},
		"multiple job types": {
			"id":        "t",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
			"jobs": []any{map[string]any{
				"stepId":     "a",
				"sparkJob":   map[string]any{"mainClass": "com.example.Main"},
				"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"},
			}},
		},
	}
	for name, def := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t", Definition: def})
			wantProviderStatus(t, err, 400)
		})
	}
}

func TestWorkflowTemplateListPagination(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: id, Definition: validTemplateDef(id)}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	page, next, err := p.ListWorkflowTemplates(ctx, "proj", "us-central1", 2, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 2 || next == "" {
		t.Fatalf("page1 = %d templates, next=%q", len(page), next)
	}
	page2, next2, err := p.ListWorkflowTemplates(ctx, "proj", "us-central1", 2, next)
	if err != nil {
		t.Fatalf("list page2: %v", err)
	}
	if len(page2) != 1 || next2 != "" {
		t.Fatalf("page2 = %d templates, next=%q", len(page2), next2)
	}
}
