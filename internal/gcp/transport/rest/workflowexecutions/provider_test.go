package workflowexecutions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"

	core "jaiscloud/internal/gcp/service/workflowexecutions"
)

const source = "main:\n  params: [a, b]\n  steps:\n    - init:\n        assign:\n          - sum: ${a + b}\n    - done:\n        return: ${sum}\n"

func newProvider(t *testing.T) *Provider {
	t.Helper()
	s := workflowsstore.NewMemoryStore()
	w := workflowsstore.Workflow{ID: "wf1", Location: "us-central1", SourceContents: source,
		State: "ACTIVE", RevisionID: "000001-a4d"}
	if err := s.CreateWorkflow(context.Background(), "proj", "us-central1", "wf1", w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	coreSvc := core.NewService(s, workflowengine.New())
	return NewProvider(coreSvc, "default-proj")
}

func TestCodecDecode(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method, path string
		wantAction   string
	}{
		{http.MethodPost, "/v1/projects/proj/locations/us-central1/workflows/wf1/executions", "CreateExecution"},
		{http.MethodGet, "/v1/projects/proj/locations/us-central1/workflows/wf1/executions", "ListExecutions"},
		{http.MethodGet, "/v1/projects/proj/locations/us-central1/workflows/wf1/executions/e1", "GetExecution"},
		{http.MethodPost, "/v1/projects/proj/locations/us-central1/workflows/wf1/executions/e1:cancel", "CancelExecution"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := c.Decode(r, nil)
		if err != nil {
			t.Fatalf("%s %s: decode: %v", tc.method, tc.path, err)
		}
		if nr.Service != "workflowexecutions" {
			t.Fatalf("%s: service = %q", tc.path, nr.Service)
		}
		if nr.Action != tc.wantAction {
			t.Fatalf("%s: action = %q, want %q", tc.path, nr.Action, tc.wantAction)
		}
		if nr.Params["project"] != "proj" || nr.Params["location"] != "us-central1" || nr.Params["workflowId"] != "wf1" {
			t.Fatalf("%s: params = %v", tc.path, nr.Params)
		}
	}
}

func TestCodecDecodeUnsupported(t *testing.T) {
	c := NewCodec()
	for _, path := range []string{
		"/v1/projects/proj/locations/us-central1/workflows/wf1/executions/e1/stepEntries",
		"/v1/projects/proj/locations/us-central1/workflows/wf1/executions/e1:exportData",
		"/v1/projects/proj/locations/us-central1/workflows/wf1",
		// Trailing slash / empty execution id must not select an item action.
		"/v1/projects/proj/locations/us-central1/workflows/wf1/executions/",
		"/v1/projects/proj/locations/us-central1/workflows/wf1/executions/:cancel",
	} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if _, err := c.Decode(r, nil); err == nil {
			t.Fatalf("%s: expected 404", path)
		}
	}
}

func TestCodecDecodeQueryDoesNotOverridePath(t *testing.T) {
	c := NewCodec()
	r := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/locations/us-central1/workflows/wf1/executions/e1?project=evil&location=other&workflowId=evil&executionId=evil", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Params["project"] != "proj" || nr.Params["location"] != "us-central1" ||
		nr.Params["workflowId"] != "wf1" || nr.Params["executionId"] != "e1" {
		t.Fatalf("query overrode path params: %v", nr.Params)
	}
}

func TestProviderRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	create := &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{
		"project":    "proj",
		"location":   "us-central1",
		"workflowId": "wf1",
		"body":       map[string]any{"argument": `{"a": 20, "b": 22}`},
	}}
	resp, err := p.CreateExecution(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.Data["state"] != "SUCCEEDED" || resp.Data["result"] != "42" {
		t.Fatalf("unexpected create data: %v", resp.Data)
	}
	name, _ := resp.Data["name"].(string)
	if !strings.HasPrefix(name, "projects/proj/locations/us-central1/workflows/wf1/executions/") {
		t.Fatalf("name = %q", name)
	}
	execID := name[strings.LastIndex(name, "/")+1:]

	get := &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1", "executionId": execID,
	}}
	gresp, err := p.GetExecution(ctx, get)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gresp.Data["result"] != "42" {
		t.Fatalf("get result = %v", gresp.Data["result"])
	}

	// BASIC view via the view param drops the result.
	basic := &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1", "executionId": execID, "view": "BASIC",
	}}
	bresp, err := p.GetExecution(ctx, basic)
	if err != nil {
		t.Fatalf("get basic: %v", err)
	}
	if _, ok := bresp.Data["result"]; ok {
		t.Fatalf("BASIC get leaked result: %v", bresp.Data)
	}

	list := &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1",
	}}
	lresp, err := p.ListExecutions(ctx, list)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := lresp.Data["executions"].([]any)
	if len(items) != 1 {
		t.Fatalf("list len = %d, want 1", len(items))
	}

	cancel := &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1", "executionId": execID,
	}}
	cresp, err := p.CancelExecution(ctx, cancel)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cresp.Data["state"] != "SUCCEEDED" {
		t.Fatalf("cancel changed terminal execution: %v", cresp.Data["state"])
	}
}
