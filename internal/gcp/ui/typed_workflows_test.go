package ui

import (
	"context"
	"reflect"
	"testing"

	workflowexecutionscore "jaiscloud/internal/gcp/service/workflowexecutions"
	workflowscore "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	restworkflowexecutions "jaiscloud/internal/gcp/transport/rest/workflowexecutions"
	restworkflows "jaiscloud/internal/gcp/transport/rest/workflows"
	workflowsui "jaiscloud/internal/gcp/ui/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

func newWorkflows(t *testing.T) (workflowsui.ProviderInterface, map[string]provider.HandlerFunc) {
	t.Helper()
	st := workflowsstore.NewMemoryStore()
	wf := workflowscore.NewService(st)
	ex := workflowexecutionscore.NewService(st, workflowengine.New())
	builtins := map[string]provider.HandlerFunc{}
	for k, v := range restworkflows.NewProvider(wf, "proj").Routes() {
		builtins[k] = v
	}
	for k, v := range restworkflowexecutions.NewProvider(ex, "proj").Routes() {
		builtins[k] = v
	}
	return workflowsui.NewProvider(wf, ex), builtins
}

const wfSource = "main:\n  steps:\n    - done:\n        return: 1\n"

func TestWorkflowsConsolePassthroughMatchesDirect(t *testing.T) {
	direct, builtins := newWorkflows(t)
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeWorkflows(typedFixture(builtins, wrap), direct)
	ctx := context.Background()

	w, err := p.CreateWorkflow(ctx, "proj", "us-central1", workflowsui.WorkflowInput{
		ID: "w1", ServiceAccount: "sa@proj.iam.gserviceaccount.com", SourceContents: wfSource, Labels: map[string]string{"a": "b"}})
	if err != nil || w.ID != "w1" || w.Location != "us-central1" {
		t.Fatalf("CreateWorkflow = %+v, %v", w, err)
	}
	want, err := direct.GetWorkflow(ctx, "proj", "us-central1", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.GetWorkflow(ctx, "proj", "us-central1", "w1"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("workflow through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	if list, err := p.ListWorkflowsByProject(ctx, "proj"); err != nil || len(list) != 1 || !reflect.DeepEqual(list[0], want) {
		t.Fatalf("ListWorkflowsByProject = %+v, %v", list, err)
	}
	if u, err := p.UpdateWorkflow(ctx, "proj", "us-central1", "w1", workflowsui.WorkflowUpdateInput{
		Description: "edited", ServiceAccount: "sa@proj.iam.gserviceaccount.com", SourceContents: wfSource, Labels: map[string]string{"a": "b"}}); err != nil || u.Description != "edited" {
		t.Fatalf("UpdateWorkflow = %+v, %v", u, err)
	}

	e, err := p.RunWorkflow(ctx, "proj", "us-central1", "w1", `{"x":1}`, "", nil)
	if err != nil || e.ID == "" || e.WorkflowID != "w1" {
		t.Fatalf("RunWorkflow = %+v, %v", e, err)
	}
	wantE, err := direct.GetExecution(ctx, "proj", "us-central1", "w1", e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.GetExecution(ctx, "proj", "us-central1", "w1", e.ID); err != nil || !reflect.DeepEqual(got, wantE) {
		t.Fatalf("execution through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, wantE, err)
	}
	if list, _, err := p.ListExecutions(ctx, "proj", "us-central1", "w1", 10, ""); err != nil || len(list) != 1 {
		t.Fatalf("ListExecutions = %+v, %v", list, err)
	}
	if _, err := p.CancelExecution(ctx, "proj", "us-central1", "w1", e.ID); err != nil {
		t.Logf("CancelExecution on a finished run: %v", err) // a finished execution cannot be cancelled
	}
	if err := p.DeleteWorkflow(ctx, "proj", "us-central1", "w1"); err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}
	for _, s := range seen {
		if s.origin != model.OriginUI {
			t.Errorf("%s: plugin saw origin %q, want ui", s.key, s.origin)
		}
	}
}

func TestWorkflowsConsoleShowsWhatThePluginServes(t *testing.T) {
	direct, builtins := newWorkflows(t)
	p := routeWorkflows(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Workflow.ListWorkflows": replaceWith(map[string]any{"workflows": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/workflows/from-plugin", "state": "ACTIVE", "sourceContents": "x"}}}),
	}), direct)
	ws, err := p.ListWorkflowsByProject(context.Background(), "proj")
	if err != nil || len(ws) != 1 {
		t.Fatalf("ListWorkflowsByProject = %+v, %v", ws, err)
	}
	if w := ws[0]; w.ID != "from-plugin" || w.Location != "europe-west1" || w.State != "ACTIVE" {
		t.Fatalf("the console must show the plugin's workflow, got %+v", w)
	}
}
