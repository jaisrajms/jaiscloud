package resourcemanager

import (
	"context"
	"testing"
)

// sampleProjects is the fixture the filter tests match against.
func sampleProjects() []Project {
	return []Project{
		{ProjectID: "howl-story", DisplayName: "Howl", State: StateActive,
			Parent: "folders/123", Labels: map[string]string{"color": "red", "size": "big"}},
		{ProjectID: "howitzer-1", DisplayName: "Howitzer", State: StateActive,
			Parent: "organizations/456", Labels: map[string]string{"color": "blue"}},
		{ProjectID: "quiet-forest", DisplayName: "Quiet Forest", State: StateDeleteRequested,
			Parent: "folders/123"},
	}
}

// matchesFilter compiles filter and returns the ids of the sample projects it
// admits.
func matchesFilter(t *testing.T, filter string) []string {
	t.Helper()
	pred, err := compileProjectFilter(filter)
	if err != nil {
		t.Fatalf("compile(%q): %v", filter, err)
	}
	var out []string
	for _, p := range sampleProjects() {
		if pred.match(p) {
			out = append(out, p.ProjectID)
		}
	}
	return out
}

func TestProjectFilterMatches(t *testing.T) {
	cases := []struct {
		filter string
		want   []string
	}{
		{"", []string{"howl-story", "howitzer-1", "quiet-forest"}},
		{"id:howl-story", []string{"howl-story"}},
		{"id:HOWL-STORY", []string{"howl-story"}},           // case-insensitive
		{"NAME:how*", []string{"howl-story", "howitzer-1"}}, // case-insensitive field
		{"name:Howl", []string{"howl-story"}},
		{"name:\"Quiet Forest\"", []string{"quiet-forest"}}, // quoted value
		{"name:quiet*", []string{"quiet-forest"}},
		{"labels.color:*", []string{"howl-story", "howitzer-1"}},
		{"Labels.color:red", []string{"howl-story"}}, // case-insensitive prefix
		{"labels.color:red", []string{"howl-story"}},
		{"labels.color:RED", []string{"howl-story"}}, // case-insensitive
		{"labels.size:big", []string{"howl-story"}},
		{"lifecycleState:ACTIVE", []string{"howl-story", "howitzer-1"}},
		{"lifecycleState:delete_requested", []string{"quiet-forest"}},
		// v3 aliases for the same fields.
		{"displayName:Howl", []string{"howl-story"}},
		{"projectId:howitzer-1", []string{"howitzer-1"}},
		{"state:ACTIVE", []string{"howl-story", "howitzer-1"}},
		// The stored parent reference and its derived type/id.
		{"parent:folders/123", []string{"howl-story", "quiet-forest"}},
		{"parent:organizations/*", []string{"howitzer-1"}},
		{"parent.type:folder", []string{"howl-story", "quiet-forest"}},
		{"parent.type:organization", []string{"howitzer-1"}},
		{"parent.id:123", []string{"howl-story", "quiet-forest"}},
		// A by-parent query (both parent.type and parent.id) ANDs the two.
		{"parent.type:folder parent.id:123", []string{"howl-story", "quiet-forest"}},
		{"parent.type:folder parent.id:999", nil},
		// A mixed by-parent query ANDs the parent pair with the OR of the
		// remaining clauses.
		{"parent.type:folder parent.id:123 name:Howl", []string{"howl-story"}},
		{"parent.type:folder parent.id:123 name:zzz", nil},
		// A bare `labels` clause matches a label name or value.
		{"labels:red", []string{"howl-story"}},
		{"labels:big", []string{"howl-story"}},
		{"labels:color", []string{"howl-story", "howitzer-1"}},
		{"labels:*", []string{"howl-story", "howitzer-1"}},
		// Multiple clauses are OR-ed (Discovery: match any of the fields).
		{"labels.color:red labels.size:big", []string{"howl-story"}},
		{"name:howl* lifecycleState:DELETE_REQUESTED", []string{"howl-story", "quiet-forest"}},
		// A bare wildcard matches any project.
		{"name:*", []string{"howl-story", "howitzer-1", "quiet-forest"}},
	}
	for _, tc := range cases {
		if got := matchesFilter(t, tc.filter); !equalStrings(got, tc.want) {
			t.Errorf("filter %q = %v, want %v", tc.filter, got, tc.want)
		}
	}
}

func TestProjectFilterRejects(t *testing.T) {
	for _, filter := range []string{
		"bogus",             // missing ":value"
		"id",                // missing ":value"
		":value",            // missing field
		"id:",               // empty value
		"labels.:red",       // empty label key
		"parent.type:",      // empty value
		"parentx:folder",    // unsupported field
		"unknown:value",     // unsupported field
		"id:value extra",    // dangling field
		"id:\"unterminated", // unterminated quote
		"id:one three:",     // missing value after the final colon
	} {
		if _, err := compileProjectFilter(filter); err == nil {
			t.Errorf("compile(%q) succeeded, want InvalidArgument", filter)
		} else if codeOf(t, err) != "InvalidArgument" {
			t.Errorf("compile(%q) code = %q, want InvalidArgument", filter, codeOf(t, err))
		}
	}
}

// TestListProjectsFilterComposesWithPagination asserts the filter narrows the
// set before the page cursor is applied.
func TestListProjectsFilterComposesWithPagination(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService(WithKnownProjects("", nil))
	created := []struct {
		id      string
		display string
		labels  map[string]string
	}{
		{"env-apple-1", "Apple One", map[string]string{"team": "fruit"}},
		{"env-apple-2", "Apple Two", map[string]string{"team": "fruit"}},
		{"env-banana-1", "Banana One", map[string]string{"team": "fruit"}},
		{"env-cherry-1", "Cherry One", map[string]string{"team": "stone"}},
	}
	for _, c := range created {
		if _, _, err := s.CreateProject(ctx, CreateProjectInput{
			ProjectID: c.id, DisplayName: c.display, Labels: c.labels,
		}); err != nil {
			t.Fatalf("create(%s): %v", c.id, err)
		}
	}

	// labels.team:fruit matches apple-1, apple-2, banana-1.
	first, next, err := s.ListProjects(ctx, 2, "", false, "labels.team:fruit")
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if ids := projectIDs(first); !equalStrings(ids, []string{"env-apple-1", "env-apple-2"}) {
		t.Fatalf("page 1 = %v, want the first two fruit projects", ids)
	}
	if next == "" {
		t.Fatal("page 1 token empty, want a continuation token")
	}
	second, next2, err := s.ListProjects(ctx, 2, next, false, "labels.team:fruit")
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if ids := projectIDs(second); !equalStrings(ids, []string{"env-banana-1"}) {
		t.Fatalf("page 2 = %v, want env-banana-1 only", ids)
	}
	if next2 != "" {
		t.Fatalf("page 2 token = %q, want empty", next2)
	}

	// A filter that matches nothing yields an empty page and no token.
	empty, next3, err := s.ListProjects(ctx, 0, "", false, "labels.team:vegetable")
	if err != nil {
		t.Fatalf("list empty: %v", err)
	}
	if len(empty) != 0 || next3 != "" {
		t.Fatalf("empty filter page = %v token=%q, want none", projectIDs(empty), next3)
	}

	// An invalid filter is InvalidArgument (never a silently unfiltered page).
	if _, _, err := s.ListProjects(ctx, 0, "", false, "bogus"); codeOf(t, err) != "InvalidArgument" {
		t.Errorf("invalid filter code = %q, want InvalidArgument", codeOf(t, err))
	}
}
