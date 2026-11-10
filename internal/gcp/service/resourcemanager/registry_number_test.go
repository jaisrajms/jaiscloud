package resourcemanager

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/store"
)

// TestProjectByNumber covers the resolution the IAM wildcard needs: the project
// number is a hash of the id, so the resolver enumerates the configured, the
// registered, and the observed project ids and compares synthesized numbers.
func TestProjectByNumber(t *testing.T) {
	ctx := context.Background()
	s, res := newRegistryService()

	// A configured project resolves without any registry entry.
	number := resource.ProjectNumber("default-proj")
	if got, ok, err := s.ProjectByNumber(ctx, number); err != nil || !ok || got != "default-proj" {
		t.Fatalf("configured project = (%q, %v, %v), want default-proj", got, ok, err)
	}

	// A registered project resolves too.
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "created-proj"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	created := resource.ProjectNumber("created-proj")
	if got, ok, err := s.ProjectByNumber(ctx, created); err != nil || !ok || got != "created-proj" {
		t.Fatalf("registered project = (%q, %v, %v), want created-proj", got, ok, err)
	}

	// A project the wire only observed resolves as well: a wildcard IAM read
	// can follow a request that named it.
	s.ObserveAccount(ctx, "observed-proj", "compute")
	observed := resource.ProjectNumber("observed-proj")
	if got, ok, err := s.ProjectByNumber(ctx, observed); err != nil || !ok || got != "observed-proj" {
		t.Fatalf("observed project = (%q, %v, %v), want observed-proj", got, ok, err)
	}

	// An unknown number matches nothing, and an empty number is not a miss that
	// touches the store.
	if got, ok, err := s.ProjectByNumber(ctx, "999999999999999"); err != nil || ok || got != "" {
		t.Errorf("unknown number = (%q, %v, %v), want no match", got, ok, err)
	}
	if got, ok, err := s.ProjectByNumber(ctx, ""); err != nil || ok || got != "" {
		t.Errorf("empty number = (%q, %v, %v), want no match", got, ok, err)
	}

	// A numeric account is a project-number reference the wire addressed, not a
	// project identity: it must not be observed, and it must never resolve its
	// own number. The Terraform provider's projects/00000000000 probe relies on
	// staying unknown (TF4-1-1).
	s.ObserveAccount(ctx, "00000000000", "serviceusage")
	for _, id := range s.observedProjectIDs(ctx) {
		if id == "00000000000" {
			t.Fatalf("numeric account %q was recorded as an observed project", id)
		}
	}
	if got, ok, err := s.ProjectByNumber(ctx, "00000000000"); err != nil || ok || got != "" {
		t.Errorf("zero number = (%q, %v, %v), want no match", got, ok, err)
	}

	// An older state may already hold a numeric registry key; the resolver
	// skips it rather than resolving the key to itself.
	data, err := json.Marshal(synthesizeProject("00000000000"))
	if err != nil {
		t.Fatalf("marshal legacy row: %v", err)
	}
	if err := res.Upsert(ctx, "00000000000", store.GlobalRegion,
		store.ResourceEntry{Type: rtProject, ID: "00000000000", Data: data}); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	if got, ok, err := s.ProjectByNumber(ctx, "00000000000"); err != nil || ok || got != "" {
		t.Errorf("legacy numeric row = (%q, %v, %v), want no match", got, ok, err)
	}

	// A registry read failure is surfaced, never reported as a miss.
	broken := NewService(&errStore{ResourceStore: store.NewMemoryResourceStore(), failList: true})
	if _, ok, err := broken.ProjectByNumber(ctx, created); !errors.Is(err, errInjected) || ok {
		t.Errorf("failing registry = (ok=%v, err=%v), want the store error", ok, err)
	}
}
