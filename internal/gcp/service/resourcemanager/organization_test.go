package resourcemanager

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/store"
)

// failingGetStore fails every Get, so OrganizationNumberForProject's error path
// is covered as well as its answers.
type failingGetStore struct {
	store.ResourceStore
}

var errGetInjected = errors.New("injected get failure")

func (failingGetStore) Get(context.Context, string, string, string, string) (store.ResourceEntry, error) {
	return store.ResourceEntry{}, errGetInjected
}

// TestOrganizationNumberForProject covers the ID-token claim's source: the
// numeric organization of the project's parent reference, and the documented
// null for a project that has no organization.
func TestOrganizationNumberForProject(t *testing.T) {
	ctx := context.Background()
	s, res := newRegistryService()

	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "org-project-1", Parent: "organizations/123456"}); err != nil {
		t.Fatalf("create organization project: %v", err)
	}
	got, err := OrganizationNumberForProject(ctx, res, "org-project-1")
	if err != nil {
		t.Fatalf("OrganizationNumberForProject: %v", err)
	}
	if got == nil || *got != 123456 {
		t.Errorf("organization number = %v, want 123456", got)
	}

	// A folder parent is not an organization: the hierarchy above a folder is
	// not modelled.
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "folder-project-1", Parent: "folders/987"}); err != nil {
		t.Fatalf("create folder project: %v", err)
	}
	if got, err := OrganizationNumberForProject(ctx, res, "folder-project-1"); err != nil || got != nil {
		t.Errorf("folder project organization number = %v (err %v), want nil", got, err)
	}

	// A project with no registry entry (the emulator synthesizes it) and an
	// empty project id both report no organization.
	if got, err := OrganizationNumberForProject(ctx, res, "never-created"); err != nil || got != nil {
		t.Errorf("unregistered project organization number = %v (err %v), want nil", got, err)
	}
	if got, err := OrganizationNumberForProject(ctx, res, ""); err != nil || got != nil {
		t.Errorf("empty project organization number = %v (err %v), want nil", got, err)
	}

	// A malformed or non-numeric parent reference is not an organization number.
	if err := res.Upsert(ctx, "odd-project", store.GlobalRegion, store.ResourceEntry{
		Type: rtProject, ID: "odd-project",
		Data: mustJSONBytes(Project{ProjectID: "odd-project", Parent: "organizations/not-a-number"}),
	}); err != nil {
		t.Fatalf("seed odd project: %v", err)
	}
	if got, err := OrganizationNumberForProject(ctx, res, "odd-project"); err != nil || got != nil {
		t.Errorf("non-numeric organization = %v (err %v), want nil", got, err)
	}

	// A storage failure is reported, never silently answered as "no
	// organization" (that would put a wrong claim in a signed token).
	if _, err := OrganizationNumberForProject(ctx, failingGetStore{ResourceStore: res}, "org-project-1"); !errors.Is(err, errGetInjected) {
		t.Errorf("store failure = %v, want the injected error", err)
	}
}

func mustJSONBytes(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
