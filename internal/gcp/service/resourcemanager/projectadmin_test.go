package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/model"
)

func TestUpdateProjectMasked(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{
		ProjectID: "update-me-123", DisplayName: "Before", Labels: map[string]string{"keep": "yes", "drop": "no"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A mask of display_name only leaves labels untouched.
	p, op, err := s.UpdateProject(ctx, "update-me-123", UpdateProjectInput{
		DisplayName: "After", Labels: nil, UpdateMask: []string{"display_name"},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if p.DisplayName != "After" {
		t.Errorf("displayName = %q, want After", p.DisplayName)
	}
	if p.Labels["keep"] != "yes" || p.Labels["drop"] != "no" {
		t.Errorf("labels mutated without a labels mask: %v", p.Labels)
	}
	if !op.Done {
		t.Errorf("sync update operation = %+v, want done", op)
	}
	if op.Verb != "update" {
		t.Errorf("operation verb = %q, want update", op.Verb)
	}

	// A labels mask replaces the whole map (a nil map clears it).
	p, _, err = s.UpdateProject(ctx, "update-me-123", UpdateProjectInput{
		Labels: map[string]string{"fresh": "value"}, UpdateMask: []string{"labels"},
	})
	if err != nil {
		t.Fatalf("update labels: %v", err)
	}
	if len(p.Labels) != 1 || p.Labels["fresh"] != "value" {
		t.Errorf("labels = %v, want the replacement set", p.Labels)
	}
	p, _, err = s.UpdateProject(ctx, "update-me-123", UpdateProjectInput{UpdateMask: []string{"labels"}})
	if err != nil {
		t.Fatalf("clear labels: %v", err)
	}
	if len(p.Labels) != 0 {
		t.Errorf("labels = %v, want cleared", p.Labels)
	}

	// A `*` mask applies every mutable field (AIP-134 full replacement).
	p, _, err = s.UpdateProject(ctx, "update-me-123", UpdateProjectInput{
		DisplayName: "Star Name", Labels: map[string]string{"star": "yes"}, UpdateMask: []string{"*"},
	})
	if err != nil {
		t.Fatalf("star update: %v", err)
	}
	if p.DisplayName != "Star Name" || p.Labels["star"] != "yes" {
		t.Errorf("star update = %+v, want Star Name with star=yes", p)
	}
}

func TestUpdateProjectDefaultsToPopulatedFields(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{
		ProjectID: "default-mask-1", DisplayName: "Old Name", Labels: map[string]string{"env": "old"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// No mask: the populated display name is applied, the absent labels are not.
	p, _, err := s.UpdateProject(ctx, "default-mask-1", UpdateProjectInput{DisplayName: "New"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if p.DisplayName != "New" || p.Labels["env"] != "old" {
		t.Errorf("update = %+v, want New with env=old", p)
	}
}

func TestUpdateProjectEtagOccAndRotation(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "occ-proj-123"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	before, err := s.GetProject(ctx, "occ-proj-123")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	after, _, err := s.UpdateProject(ctx, "occ-proj-123", UpdateProjectInput{
		DisplayName: "Renamed", UpdateMask: []string{"display_name"}, Etag: before.Etag,
	})
	if err != nil {
		t.Fatalf("update with matching etag: %v", err)
	}
	if after.Etag == before.Etag || after.Etag == "" {
		t.Errorf("etag did not rotate: before=%q after=%q", before.Etag, after.Etag)
	}

	// A stale etag (the pre-update value) is rejected with ABORTED/409.
	_, _, err = s.UpdateProject(ctx, "occ-proj-123", UpdateProjectInput{
		DisplayName: "Stale", UpdateMask: []string{"display_name"}, Etag: before.Etag,
	})
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("stale etag err = %T (%v), want *model.ProviderError", err, err)
	}
	if pe.HTTPStatus != 409 || pe.Status != "ABORTED" {
		t.Fatalf("stale etag = %+v, want 409 ABORTED", pe)
	}
	// The rejected write did not change the project.
	got, err := s.GetProject(ctx, "occ-proj-123")
	if err != nil {
		t.Fatalf("get after reject: %v", err)
	}
	if got.DisplayName != "Renamed" {
		t.Errorf("displayName = %q, want Renamed (rejected write must not apply)", got.DisplayName)
	}
}

func TestUpdateProjectErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.UpdateProject(ctx, "never-created", UpdateProjectInput{DisplayName: "X", UpdateMask: []string{"display_name"}}); codeOf(t, err) != "NotFound" {
		t.Errorf("update unknown code = %q, want NotFound", codeOf(t, err))
	}
	if _, _, err := s.UpdateProject(ctx, "default-proj", UpdateProjectInput{UpdateMask: []string{"bogus"}}); codeOf(t, err) != "InvalidArgument" {
		t.Errorf("bad mask code = %q, want InvalidArgument", codeOf(t, err))
	}
	if _, _, err := s.UpdateProject(ctx, "", UpdateProjectInput{}); codeOf(t, err) != "InvalidArgument" {
		t.Errorf("empty project code = %q, want InvalidArgument", codeOf(t, err))
	}
	// A configured id with no registry entry is materialized by the write.
	p, _, err := s.UpdateProject(ctx, "extra-proj1", UpdateProjectInput{DisplayName: "Materialized", UpdateMask: []string{"display_name"}})
	if err != nil {
		t.Fatalf("update configured project: %v", err)
	}
	if p.DisplayName != "Materialized" || p.CreateTime.IsZero() {
		t.Errorf("materialized project = %+v, want Materialized with a createTime", p)
	}
}

func TestMoveProject(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "move-me-1234", Parent: "organizations/111"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	p, op, err := s.MoveProject(ctx, "move-me-1234", "folders/222")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if p.Parent != "folders/222" {
		t.Errorf("parent = %q, want folders/222", p.Parent)
	}
	if op.Verb != "move" || !op.Done {
		t.Errorf("operation = %+v, want done move", op)
	}

	// Moving to the current parent is an idempotent no-op.
	same, _, err := s.MoveProject(ctx, "move-me-1234", "folders/222")
	if err != nil {
		t.Fatalf("idempotent move: %v", err)
	}
	if same.Parent != "folders/222" {
		t.Errorf("idempotent move parent = %q", same.Parent)
	}

	// Bad destinations, unknown ids and a non-ACTIVE project.
	for _, dest := range []string{"", "projects/1", "folders/", "123", "folders/1/2", "organizations/abc", "folders/12a"} {
		if _, _, err := s.MoveProject(ctx, "move-me-1234", dest); codeOf(t, err) != "InvalidArgument" {
			t.Errorf("move dest %q code = %q, want InvalidArgument", dest, codeOf(t, err))
		}
	}
	if _, _, err := s.MoveProject(ctx, "never-created", "folders/1"); codeOf(t, err) != "NotFound" {
		t.Errorf("move unknown code = %q, want NotFound", codeOf(t, err))
	}
	if _, _, err := s.DeleteProject(ctx, "move-me-1234"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := s.MoveProject(ctx, "move-me-1234", "folders/333"); codeOf(t, err) != "FailedPrecondition" {
		t.Errorf("move deleted code = %q, want FailedPrecondition", codeOf(t, err))
	}
}

func TestSearchProjects(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService(WithKnownProjects("", nil))
	created := []struct {
		id, display, parent string
		labels              map[string]string
	}{
		{"search-apple-1", "Apple One", "folders/1", map[string]string{"team": "fruit"}},
		{"search-apple-2", "Apple Two", "folders/1", map[string]string{"team": "fruit"}},
		{"search-banana-1", "Banana One", "organizations/9", map[string]string{"team": "fruit"}},
		{"search-cherry-1", "Cherry One", "folders/1", map[string]string{"team": "stone"}},
	}
	for _, c := range created {
		if _, _, err := s.CreateProject(ctx, CreateProjectInput{
			ProjectID: c.id, DisplayName: c.display, Parent: c.parent, Labels: c.labels,
		}); err != nil {
			t.Fatalf("create(%s): %v", c.id, err)
		}
	}
	if _, _, err := s.DeleteProject(ctx, "search-apple-2"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// A query narrows the set (DELETE_REQUESTED projects stay visible to search).
	page, next, err := s.SearchProjects(ctx, 0, "", "labels.team:fruit")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if ids := projectIDs(page); !equalStrings(ids, []string{"search-apple-1", "search-apple-2", "search-banana-1"}) {
		t.Errorf("search = %v, want the three fruit projects", ids)
	}
	if next != "" {
		t.Errorf("nextPageToken = %q, want empty", next)
	}

	// The parent.type+parent.id by-parent query ANDs the two.
	page, _, err = s.SearchProjects(ctx, 0, "", "parent.type:folder parent.id:1")
	if err != nil {
		t.Fatalf("by-parent search: %v", err)
	}
	if ids := projectIDs(page); !equalStrings(ids, []string{"search-apple-1", "search-apple-2", "search-cherry-1"}) {
		t.Errorf("by-parent search = %v, want the folder/1 projects", ids)
	}

	// Pagination composes with the query.
	first, token, err := s.SearchProjects(ctx, 2, "", "labels.team:fruit")
	if err != nil {
		t.Fatalf("search page 1: %v", err)
	}
	if len(first) != 2 || token == "" {
		t.Fatalf("page 1 = %v token=%q, want 2 items and a token", projectIDs(first), token)
	}
	second, token2, err := s.SearchProjects(ctx, 2, token, "labels.team:fruit")
	if err != nil {
		t.Fatalf("search page 2: %v", err)
	}
	if len(second) != 1 || token2 != "" {
		t.Fatalf("page 2 = %v token=%q, want 1 item and no token", projectIDs(second), token2)
	}

	// An invalid query is InvalidArgument, never a silently unfiltered page.
	if _, _, err := s.SearchProjects(ctx, 0, "", "bogus"); codeOf(t, err) != "InvalidArgument" {
		t.Errorf("invalid query code = %q, want InvalidArgument", codeOf(t, err))
	}

	// An empty query returns every project.
	all, _, err := s.SearchProjects(ctx, 0, "", "")
	if err != nil {
		t.Fatalf("empty search: %v", err)
	}
	if len(all) != len(created) {
		t.Errorf("empty search = %v, want all %d", projectIDs(all), len(created))
	}
}
