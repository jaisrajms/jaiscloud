package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     params,
		ResourceID: resource.ResourceID("proj"),
	}
}

func newProvider() *Provider {
	return NewProvider(core.NewService(store.NewMemoryResourceStore()), "proj")
}

func TestGetProjectV1Shape(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.GetProject(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("get project: %v", err)
	}
	if resp.Data["projectId"] != "proj" {
		t.Errorf("projectId = %v", resp.Data["projectId"])
	}
	if resp.Data["projectNumber"] != resource.ProjectNumber("proj") {
		t.Errorf("projectNumber = %v, want %v", resp.Data["projectNumber"], resource.ProjectNumber("proj"))
	}
	if resp.Data["name"] != "proj" {
		t.Errorf("name = %v, want proj (v1 name = displayName)", resp.Data["name"])
	}
	if resp.Data["lifecycleState"] != "ACTIVE" {
		t.Errorf("lifecycleState = %v, want ACTIVE", resp.Data["lifecycleState"])
	}
	if _, ok := resp.Data["etag"]; ok {
		t.Error("v1 Project has no etag field; adapter must not emit one")
	}
}

func TestSetIamPolicyUnwrapsV1Envelope(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	// The v1 SetIamPolicyRequest wraps the policy in a "policy" field.
	resp, err := p.SetIamPolicy(ctx, newNR(map[string]any{
		"project": "proj",
		"body": map[string]any{"policy": map[string]any{
			"bindings": []any{map[string]any{"role": "roles/owner", "members": []any{"user:a@example.com"}}},
		}},
	}))
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	bindings, _ := resp.Data["bindings"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("bindings = %v, want 1", resp.Data["bindings"])
	}

	got, err := p.GetIamPolicy(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if got.Data["etag"] == "" {
		t.Error("stored policy must carry an etag")
	}
}

func TestMissingProjectIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	// No path project, no account, no default -> InvalidArgument.
	p := NewProvider(core.NewService(store.NewMemoryResourceStore()), "")
	bare := func() *model.NormalizedRequest {
		return &model.NormalizedRequest{Params: map[string]any{}}
	}
	for name, fn := range map[string]func() error{
		"getProject":         func() error { _, err := p.GetProject(ctx, bare()); return err },
		"getIamPolicy":       func() error { _, err := p.GetIamPolicy(ctx, bare()); return err },
		"setIamPolicy":       func() error { _, err := p.SetIamPolicy(ctx, bare()); return err },
		"testIamPermissions": func() error { _, err := p.TestIamPermissions(ctx, bare()); return err },
	} {
		var pe *model.ProviderError
		if err := fn(); !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Errorf("%s: err = %v, want 400 InvalidArgument", name, err)
		}
	}
}

func TestTestIamPermissions(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.TestIamPermissions(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    map[string]any{"permissions": []any{"resourcemanager.projects.get", "resourcemanager.projects.setIamPolicy"}},
	}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	perms, _ := resp.Data["permissions"].([]string)
	if len(perms) != 2 {
		t.Fatalf("permissions = %v, want 2", resp.Data["permissions"])
	}
}
