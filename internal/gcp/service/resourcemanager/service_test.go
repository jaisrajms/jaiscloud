package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newService() *Service { return NewService(store.NewMemoryResourceStore()) }

func binding(role, member string) map[string]any {
	return map[string]any{"role": role, "members": []any{member}}
}

func TestGetProject(t *testing.T) {
	ctx := context.Background()
	s := newService()
	p, err := s.GetProject(ctx, "proj")
	if err != nil {
		t.Fatalf("get project: %v", err)
	}
	if p.ProjectID != "proj" {
		t.Errorf("projectID = %q, want proj", p.ProjectID)
	}
	if p.ProjectNumber != resource.ProjectNumber("proj") {
		t.Errorf("projectNumber = %q, want %q", p.ProjectNumber, resource.ProjectNumber("proj"))
	}
	if p.DisplayName != "proj" {
		t.Errorf("displayName = %q, want proj", p.DisplayName)
	}
	if p.State != StateActive {
		t.Errorf("state = %q, want ACTIVE", p.State)
	}
	if p.Etag == "" {
		t.Error("etag must be non-empty")
	}
	if got := ProjectName("proj"); got != "projects/proj" {
		t.Errorf("ProjectName = %q, want projects/proj", got)
	}
}

func TestParseProjectName(t *testing.T) {
	for _, tc := range []struct {
		in string
		id string
		ok bool
	}{
		{"projects/proj", "proj", true},
		{"proj", "proj", true},
		{"projects/a/b", "", false},
		{"projects/", "", false},
		{"", "", false},
	} {
		id, ok := ParseProjectName(tc.in)
		if id != tc.id || ok != tc.ok {
			t.Errorf("ParseProjectName(%q) = (%q,%v), want (%q,%v)", tc.in, id, ok, tc.id, tc.ok)
		}
	}
}

func TestIamPolicyMergeAndOcc(t *testing.T) {
	ctx := context.Background()
	s := newService()

	// Read the empty default policy and use its etag for the first write.
	pol, err := s.GetIamPolicy(ctx, "proj")
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if pol.Etag == "" {
		t.Fatal("default policy has no etag")
	}

	// First member.
	pol, err = s.SetIamPolicy(ctx, "proj", PolicyInput{
		Etag:     pol.Etag,
		Bindings: []any{binding("roles/pubsub.publisher", "serviceAccount:sa@proj.iam.gserviceaccount.com")},
	})
	if err != nil {
		t.Fatalf("setIamPolicy #1: %v", err)
	}
	if pol.Etag == "" {
		t.Fatal("etag must rotate on write")
	}

	// Second member, merged on top of the first using the fresh etag — both
	// grants must survive (this is exactly the provider's read-modify-write).
	merged := append(append([]any{}, pol.Bindings...), binding("roles/pubsub.viewer", "user:compat-viewer@example.com"))
	if _, err := s.SetIamPolicy(ctx, "proj", PolicyInput{Etag: pol.Etag, Bindings: merged}); err != nil {
		t.Fatalf("setIamPolicy #2: %v", err)
	}

	pol, err = s.GetIamPolicy(ctx, "proj")
	if err != nil {
		t.Fatalf("getIamPolicy after merges: %v", err)
	}
	if len(pol.Bindings) != 2 {
		t.Fatalf("bindings = %d, want 2 (both grants survive): %v", len(pol.Bindings), pol.Bindings)
	}

	// A stale etag is rejected with ABORTED/409.
	_, err = s.SetIamPolicy(ctx, "proj", PolicyInput{Etag: "stale-etag", Bindings: []any{}})
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("stale etag error = %T (%v), want *model.ProviderError", err, err)
	}
	if pe.HTTPStatus != 409 || pe.Status != "ABORTED" {
		t.Fatalf("stale etag = %+v, want 409 ABORTED", pe)
	}
}

func TestMissingProjectIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	s := newService()
	calls := map[string]func() error{
		"getProject":         func() error { _, err := s.GetProject(ctx, ""); return err },
		"getIamPolicy":       func() error { _, err := s.GetIamPolicy(ctx, ""); return err },
		"setIamPolicy":       func() error { _, err := s.SetIamPolicy(ctx, "", PolicyInput{}); return err },
		"testIamPermissions": func() error { _, err := s.TestIamPermissions(ctx, "", nil); return err },
	}
	for name, fn := range calls {
		var pe *model.ProviderError
		if err := fn(); !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Errorf("%s: err = %v, want 400 InvalidArgument", name, err)
		}
	}
}

func TestTestIamPermissions(t *testing.T) {
	ctx := context.Background()
	s := newService()
	perms, err := s.TestIamPermissions(ctx, "proj", []string{"resourcemanager.projects.get", "resourcemanager.projects.setIamPolicy"})
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	if len(perms) != 2 {
		t.Fatalf("permissions = %v, want 2", perms)
	}
}
