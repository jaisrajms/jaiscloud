package dataproc

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const wtIAMName = "projects/proj/regions/us-central1/workflowTemplates/t1"

// seedTemplate creates the template every IAM test targets.
func seedTemplate(t *testing.T, p *Service) {
	t.Helper()
	if _, err := p.CreateWorkflowTemplate(context.Background(), "proj", "us-central1",
		WorkflowTemplateInput{ID: "t1", Definition: validTemplateDef("t1")}); err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}
}

func TestParseIAMResource(t *testing.T) {
	got, ok := ParseIAMResource(wtIAMName)
	if !ok {
		t.Fatalf("ParseIAMResource(%q) not recognized", wtIAMName)
	}
	want := IAMResource{Project: "proj", Region: "us-central1", Template: "t1"}
	if got != want {
		t.Fatalf("parsed = %+v, want %+v", got, want)
	}
	if want.Key() != "us-central1/t1" {
		t.Fatalf("key = %q, want us-central1/t1", want.Key())
	}

	bad := []string{
		"",
		"projects/proj/regions/us-central1/clusters/c1",
		"projects/proj/regions/us-central1/jobs/j1",
		"projects/proj/locations/us-central1/workflowTemplates/t1",
		"projects/proj/regions/us-central1/workflowTemplates",
		"projects//regions/us-central1/workflowTemplates/t1",
		"projects/proj/regions/us-central1/workflowTemplates/t1/extra",
		"projects/proj/regions/us-central1/workflowTemplates/t1:getIamPolicy",
	}
	for _, name := range bad {
		if _, ok := ParseIAMResource(name); ok {
			t.Errorf("ParseIAMResource(%q) = ok, want rejected", name)
		}
	}
}

// TestWorkflowTemplateIAMDefaultEmpty: get on an existing template returns the
// empty default policy (version 1, default etag, empty bindings).
func TestWorkflowTemplateIAMDefaultEmpty(t *testing.T) {
	p := newProvider(t)
	seedTemplate(t, p)
	pol, err := p.GetIamPolicy(context.Background(), wtIAMName)
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if pol.Version != 1 || pol.Etag == "" || len(pol.Bindings) != 0 {
		t.Fatalf("default policy = %+v, want version 1 / default etag / no bindings", pol)
	}
}

// TestWorkflowTemplateIAMConditionRoundTrip: a conditional binding is stored at
// version 3 and reads back with its condition intact.
func TestWorkflowTemplateIAMConditionRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedTemplate(t, p)

	condition := map[string]any{
		"title":       "only-today",
		"description": "time bounded",
		"expression":  "request.time < timestamp(\"2027-01-01T00:00:00Z\")",
	}
	set, err := p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":      "roles/dataproc.worker",
				"members":   []any{"user:a@example.com"},
				"condition": condition,
			}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if set.Version != 3 {
		t.Fatalf("stored version = %d, want 3", set.Version)
	}

	got, err := p.GetIamPolicy(ctx, wtIAMName)
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if got.Version != 3 {
		t.Fatalf("read-back version = %d, want 3", got.Version)
	}
	bm, _ := got.Bindings[0].(map[string]any)
	if bm["role"] != "roles/dataproc.worker" {
		t.Fatalf("role = %v", bm["role"])
	}
	cm, _ := bm["condition"].(map[string]any)
	if cm["title"] != condition["title"] || cm["expression"] != condition["expression"] || cm["description"] != condition["description"] {
		t.Fatalf("condition did not round-trip: %v", cm)
	}
}

// TestWorkflowTemplateIAMStaleEtag: etag OCC is enforced on the template policy
// plane (stale etag -> ABORTED/409).
func TestWorkflowTemplateIAMStaleEtag(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedTemplate(t, p)

	first, err := p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/viewer", "members": []any{"user:a@example.com"},
		}}},
	})
	if err != nil {
		t.Fatalf("initial SetIamPolicy: %v", err)
	}
	_, err = p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{
			"etag":     "stale",
			"bindings": []any{map[string]any{"role": "roles/editor", "members": []any{"user:b@example.com"}}},
		},
	})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Status != "ABORTED" || pe.HTTPStatus != 409 {
		t.Fatalf("stale etag = %v, want ABORTED/409", err)
	}
	if got := p.mustGetPolicy(t, wtIAMName); got.Etag != first.Etag {
		t.Fatal("rejected write changed the stored policy")
	}
}

// mustGetPolicy fetches a stored policy or fails the test.
func (s *Service) mustGetPolicy(t *testing.T, resource string) policy.Policy {
	t.Helper()
	got, err := s.GetIamPolicy(context.Background(), resource)
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	return got
}

// TestWorkflowTemplateIAMMissingTemplate: every verb is NotFound before the
// policy plane is touched, and no policy row is created.
func TestWorkflowTemplateIAMMissingTemplate(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	missing := "projects/proj/regions/us-central1/workflowTemplates/absent"

	if _, err := p.GetIamPolicy(ctx, missing); providerStatus(err) != 404 {
		t.Fatalf("GetIamPolicy(missing) = %v, want 404", err)
	}
	_, err := p.SetIamPolicy(ctx, missing, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/viewer", "members": []any{"user:a@example.com"},
		}}},
	})
	if providerStatus(err) != 404 {
		t.Fatalf("SetIamPolicy(missing) = %v, want 404", err)
	}
	if _, err := p.TestIamPermissions(ctx, missing, []string{"dataproc.workflowTemplates.get"}); providerStatus(err) != 404 {
		t.Fatalf("TestIamPermissions(missing) = %v, want 404", err)
	}

	// The rejected calls must not have written a policy row for the missing id.
	if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, WorkflowTemplatePolicyType, "us-central1/absent"); err == nil {
		t.Fatal("a NotFound IAM call created a policy row")
	}
}

// TestWorkflowTemplateIAMWrongShape: a non-template name is InvalidArgument.
func TestWorkflowTemplateIAMWrongShape(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	for _, name := range []string{
		"projects/proj/regions/us-central1/clusters/c1",
		"projects/proj/locations/global/workflowTemplates/t1",
		"projects/proj/regions/us-central1/workflowTemplates/t1:getIamPolicy",
	} {
		if _, err := p.GetIamPolicy(ctx, name); providerStatus(err) != 400 {
			t.Fatalf("GetIamPolicy(%q) = %v, want 400", name, err)
		}
		if _, err := p.SetIamPolicy(ctx, name, map[string]any{}); providerStatus(err) != 400 {
			t.Fatalf("SetIamPolicy(%q) = %v, want 400", name, err)
		}
		if _, err := p.TestIamPermissions(ctx, name, nil); providerStatus(err) != 400 {
			t.Fatalf("TestIamPermissions(%q) = %v, want 400", name, err)
		}
	}
}

// TestWorkflowTemplateIAMTestPermissions echoes the requested permissions.
func TestWorkflowTemplateIAMTestPermissions(t *testing.T) {
	p := newProvider(t)
	seedTemplate(t, p)
	perms := []string{"dataproc.workflowTemplates.get", "dataproc.workflowTemplates.update"}
	got, err := p.TestIamPermissions(context.Background(), wtIAMName, perms)
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(got) != len(perms) || got[0] != perms[0] || got[1] != perms[1] {
		t.Fatalf("permissions = %v, want %v", got, perms)
	}
}

// providerStatus returns the HTTP status of a ProviderError, or 0.
func providerStatus(err error) int {
	pe, ok := err.(*model.ProviderError)
	if !ok {
		return 0
	}
	return pe.HTTPStatus
}
