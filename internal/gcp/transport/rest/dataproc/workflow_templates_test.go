package dataproc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

func wfTemplateDef(id string) map[string]any {
	return map[string]any{
		"id":        id,
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
		"jobs": []any{
			map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
		},
	}
}

// TestCodecDeriveWorkflowTemplate covers the REST path → action routing for the
// workflowTemplates surface, including the custom-method collection form.
func TestCodecDeriveWorkflowTemplate(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/workflowTemplates", "CreateWorkflowTemplate"},
		{http.MethodGet, "/v1/projects/proj/regions/us-central1/workflowTemplates", "ListWorkflowTemplates"},
		{http.MethodGet, "/v1/projects/proj/regions/us-central1/workflowTemplates/t1", "GetWorkflowTemplate"},
		{http.MethodPut, "/v1/projects/proj/regions/us-central1/workflowTemplates/t1", "UpdateWorkflowTemplate"},
		{http.MethodDelete, "/v1/projects/proj/regions/us-central1/workflowTemplates/t1", "DeleteWorkflowTemplate"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/workflowTemplates:instantiateInline", "InstantiateInlineWorkflowTemplate"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/workflowTemplates/t1:instantiate", "InstantiateWorkflowTemplate"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
		nr, err := c.Decode(req, []byte("{}"))
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		if nr.Action != tc.want {
			t.Fatalf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.want)
		}
		if !strings.HasPrefix(nr.Params["name"].(string), "regions/us-central1/workflowTemplates") {
			t.Fatalf("%s %s: name = %v", tc.method, tc.path, nr.Params["name"])
		}
	}
	// The id is extracted on the item path.
	nr, err := c.Decode(httptest.NewRequest(http.MethodGet, "/v1/projects/proj/regions/us-central1/workflowTemplates/t1", nil), nil)
	if err != nil {
		t.Fatalf("decode item: %v", err)
	}
	if nr.Params["workflowTemplateId"] != "t1" {
		t.Fatalf("workflowTemplateId = %v, want t1", nr.Params["workflowTemplateId"])
	}
}

// TestWorkflowTemplateRESTSurface round-trips the CRUD handlers.
func TestWorkflowTemplateRESTSurface(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	nrBase := func(body map[string]any) *model.NormalizedRequest {
		nr := testNR(map[string]any{"region": "us-central1"})
		if body != nil {
			nr.Params["body"] = body
		}
		return nr
	}

	if _, err := p.CreateWorkflowTemplate(ctx, nrBase(wfTemplateDef("t1"))); err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}

	nr := nrBase(nil)
	nr.Params["workflowTemplateId"] = "t1"
	resp, err := p.GetWorkflowTemplate(ctx, nr)
	if err != nil {
		t.Fatalf("GetWorkflowTemplate: %v", err)
	}
	if resp.Data["name"] != "projects/proj/regions/us-central1/workflowTemplates/t1" {
		t.Fatalf("name = %v", resp.Data["name"])
	}

	listResp, err := p.ListWorkflowTemplates(ctx, nrBase(nil))
	if err != nil {
		t.Fatalf("ListWorkflowTemplates: %v", err)
	}
	if items, _ := listResp.Data["templates"].([]any); len(items) != 1 {
		t.Fatalf("templates = %v, want 1", listResp.Data["templates"])
	}

	def := wfTemplateDef("t1")
	def["version"] = float64(1)
	if _, err := p.UpdateWorkflowTemplate(ctx, nrBase(def)); err != nil {
		t.Fatalf("UpdateWorkflowTemplate: %v", err)
	}

	if _, err := p.DeleteWorkflowTemplate(ctx, nr); err != nil {
		t.Fatalf("DeleteWorkflowTemplate: %v", err)
	}
	if _, err := p.GetWorkflowTemplate(ctx, nr); err == nil {
		t.Fatal("expected NotFound after delete")
	}
}

func TestInstantiateInlineWorkflowREST(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	nr := testNR(map[string]any{"region": "us-central1", "body": map[string]any{
		"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "wf-rest"}},
		"jobs":      []any{map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}}},
	}})
	resp, err := p.InstantiateInlineWorkflowTemplate(ctx, nr)
	if err != nil {
		t.Fatalf("InstantiateInlineWorkflowTemplate: %v", err)
	}
	if resp.Data["done"] != false {
		t.Fatalf("done = %v, want false", resp.Data["done"])
	}
	name, _ := resp.Data["name"].(string)
	idx := strings.LastIndex(name, "/operations/")
	if idx < 0 {
		t.Fatalf("operation name malformed: %q", name)
	}
	opID := name[idx+len("/operations/"):]

	var done bool
	for i := 0; i < 32; i++ {
		opNR := testNR(map[string]any{"region": "us-central1", "operationId": opID})
		opResp, err := p.GetOperation(ctx, opNR)
		if err != nil {
			t.Fatalf("GetOperation: %v", err)
		}
		if opResp.Data["done"] == true {
			done = true
			meta, _ := opResp.Data["metadata"].(map[string]any)
			if meta["state"] != "DONE" {
				t.Fatalf("workflow state = %v, want DONE", meta["state"])
			}
			break
		}
	}
	if !done {
		t.Fatal("workflow operation did not complete")
	}
}
