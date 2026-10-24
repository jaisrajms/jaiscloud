package resourcemanager

import (
	"context"
	"encoding/json"
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
	coreSvc := core.NewService(store.NewMemoryResourceStore(), core.WithKnownProjects("proj", nil))
	return NewProvider(coreSvc, "proj")
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

// createNR builds a projects.create NormalizedRequest for projectID.
func createNR(projectID, displayName string) *model.NormalizedRequest {
	body := map[string]any{"projectId": projectID}
	if displayName != "" {
		body["name"] = displayName
	}
	return newNR(map[string]any{"body": body})
}

func TestCreateProjectReturnsOperation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.CreateProject(ctx, createNR("create-me-123", "Create Me"))
	if err != nil {
		t.Fatalf("createProject: %v", err)
	}
	if done, _ := resp.Data["done"].(bool); !done {
		t.Errorf("done = %v, want true (sync mode)", resp.Data["done"])
	}
	if name, _ := resp.Data["name"].(string); name == "" {
		t.Errorf("operation name = %v, want operations/{id}", resp.Data["name"])
	}
	meta, _ := resp.Data["metadata"].(map[string]any)
	if got := meta["@type"]; got != v1CreationStatusType {
		t.Errorf("metadata @type = %v, want %v", got, v1CreationStatusType)
	}
	project, _ := resp.Data["response"].(map[string]any)
	if got := project["@type"]; got != v1ProjectType {
		t.Errorf("response @type = %v, want %v", got, v1ProjectType)
	}
	if project["projectId"] != "create-me-123" || project["name"] != "Create Me" {
		t.Errorf("response project = %v, want projectId/name round-trip", project)
	}
}

// TestCreateProjectParentAndLabels covers the v1 create body conversion of the
// ResourceId parent and the labels object.
func TestCreateProjectParentAndLabels(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	nr := createNR("parent-proj-1", "Parent Project")
	body := nr.Params["body"].(map[string]any)
	body["parent"] = map[string]any{"type": "organization", "id": "123"}
	body["labels"] = map[string]any{"env": "test"}
	resp, err := p.CreateProject(ctx, nr)
	if err != nil {
		t.Fatalf("createProject: %v", err)
	}
	project, _ := resp.Data["response"].(map[string]any)
	labels, _ := project["labels"].(map[string]string)
	if labels["env"] != "test" {
		t.Errorf("labels = %v, want env=test", project["labels"])
	}

	// The parent is stored on the canonical project (v1 does not render it).
	got, err := p.core.GetProject(ctx, "parent-proj-1")
	if err != nil {
		t.Fatalf("getProject: %v", err)
	}
	if got.Parent != "organizations/123" {
		t.Errorf("parent = %q, want organizations/123", got.Parent)
	}
}

// TestListProjectsPageSizeString covers a pageSize arriving as a string from
// the codec's query parsing (the form the HTTP surface actually produces).
func TestListProjectsPageSizeString(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, id := range []string{"page-one-1234", "page-two-1234", "page-three-123"} {
		if _, err := p.CreateProject(ctx, createNR(id, "")); err != nil {
			t.Fatalf("create(%s): %v", id, err)
		}
	}
	resp, err := p.ListProjects(ctx, newNR(map[string]any{"pageSize": "2"}))
	if err != nil {
		t.Fatalf("listProjects: %v", err)
	}
	projects, _ := resp.Data["projects"].([]any)
	if len(projects) != 2 {
		t.Fatalf("page 1 size = %d, want 2", len(projects))
	}
	if resp.Data["nextPageToken"] == "" || resp.Data["nextPageToken"] == nil {
		t.Error("page 1 must carry a nextPageToken")
	}
}

func TestListProjectsIncludesCreated(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	if _, err := p.CreateProject(ctx, createNR("list-me-123", "")); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp, err := p.ListProjects(ctx, newNR(map[string]any{"pageSize": float64(10)}))
	if err != nil {
		t.Fatalf("listProjects: %v", err)
	}
	projects, _ := resp.Data["projects"].([]any)
	ids := map[string]bool{}
	for _, raw := range projects {
		m, _ := raw.(map[string]any)
		id, _ := m["projectId"].(string)
		ids[id] = true
	}
	if !ids["list-me-123"] || !ids["proj"] {
		t.Errorf("list = %v, want created + configured default", ids)
	}
}

func TestListProjectsFilter(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, id := range []string{"filter-alpha-1", "filter-beta-1"} {
		if _, err := p.CreateProject(ctx, createNR(id, "")); err != nil {
			t.Fatalf("create(%s): %v", id, err)
		}
	}

	// An id filter narrows the page to the matching project.
	resp, err := p.ListProjects(ctx, newNR(map[string]any{"filter": "id:filter-alpha-1"}))
	if err != nil {
		t.Fatalf("listProjects(filter): %v", err)
	}
	projects, _ := resp.Data["projects"].([]any)
	if len(projects) != 1 {
		t.Fatalf("filtered page = %v, want exactly filter-alpha-1", projects)
	}
	if m, _ := projects[0].(map[string]any); m["projectId"] != "filter-alpha-1" {
		t.Fatalf("filtered page project = %v, want filter-alpha-1", projects[0])
	}

	// lifecycleState:ACTIVE drops the DELETE_REQUESTED project (which the v1
	// list otherwise keeps visible).
	if _, err := p.DeleteProject(ctx, newNR(map[string]any{"project": "filter-beta-1"})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp, err = p.ListProjects(ctx, newNR(map[string]any{"filter": "lifecycleState:ACTIVE"}))
	if err != nil {
		t.Fatalf("listProjects(ACTIVE): %v", err)
	}
	if containsProject(resp, "filter-beta-1") {
		t.Error("lifecycleState:ACTIVE must exclude the DELETE_REQUESTED project")
	}
	resp, err = p.ListProjects(ctx, newNR(map[string]any{"filter": "lifecycleState:DELETE_REQUESTED"}))
	if err != nil {
		t.Fatalf("listProjects(DELETE_REQUESTED): %v", err)
	}
	if !containsProject(resp, "filter-beta-1") || containsProject(resp, "filter-alpha-1") {
		t.Errorf("lifecycleState:DELETE_REQUESTED page = %v, want only filter-beta-1", resp.Data["projects"])
	}

	// An invalid filter is 400 InvalidArgument, never a silently unfiltered page.
	_, err = p.ListProjects(ctx, newNR(map[string]any{"filter": "bogus"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Errorf("invalid filter err = %v, want 400 InvalidArgument", err)
	}
}

func TestCreateProjectDuplicateConflict(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	if _, err := p.CreateProject(ctx, createNR("dup-proj-1234", "")); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := p.CreateProject(ctx, createNR("dup-proj-1234", ""))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 409 {
		t.Errorf("duplicate err = %v, want 409 AlreadyExists", err)
	}
}

func TestUpdateProjectV1(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	if _, err := p.CreateProject(ctx, createNR("update-proj-123", "Before Name")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The v1 body is the Project resource: name = display name, labels.
	resp, err := p.UpdateProject(ctx, newNR(map[string]any{
		"project": "update-proj-123",
		"body":    map[string]any{"name": "After Name", "labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("updateProject: %v", err)
	}
	if resp.Data["name"] != "After Name" {
		t.Errorf("name = %v, want After Name", resp.Data["name"])
	}
	if labels, _ := resp.Data["labels"].(map[string]string); labels["env"] != "prod" {
		t.Errorf("labels = %v, want env=prod", resp.Data["labels"])
	}
	if _, ok := resp.Data["etag"]; ok {
		t.Error("v1 Project has no etag field; adapter must not emit one")
	}

	// Omitting labels leaves them unchanged (merge, not full replace).
	resp, err = p.UpdateProject(ctx, newNR(map[string]any{
		"project": "update-proj-123",
		"body":    map[string]any{"name": "Third Name"},
	}))
	if err != nil {
		t.Fatalf("updateProject (name only): %v", err)
	}
	if labels, _ := resp.Data["labels"].(map[string]string); labels["env"] != "prod" {
		t.Errorf("labels = %v, want env=prod preserved", resp.Data["labels"])
	}

	// An explicit empty labels object clears them.
	resp, err = p.UpdateProject(ctx, newNR(map[string]any{
		"project": "update-proj-123",
		"body":    map[string]any{"labels": map[string]any{}},
	}))
	if err != nil {
		t.Fatalf("updateProject (clear labels): %v", err)
	}
	if _, ok := resp.Data["labels"]; ok {
		t.Errorf("labels = %v, want cleared", resp.Data["labels"])
	}

	// Updating an unknown project is 404.
	_, err = p.UpdateProject(ctx, newNR(map[string]any{
		"project": "never-created",
		"body":    map[string]any{"name": "Ghost Project"},
	}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 404 {
		t.Errorf("update unknown err = %v, want 404 NotFound", err)
	}
}

// TestUpdateProjectParentV1 covers the v1 ResourceId parent round-trip: create
// with a parent, render it, and reparent it through update.
func TestUpdateProjectParentV1(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	nr := createNR("parent-proj-9", "Parent Proj")
	nr.Params["body"].(map[string]any)["parent"] = map[string]any{"type": "folder", "id": "111"}
	if _, err := p.CreateProject(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := p.GetProject(ctx, newNR(map[string]any{"project": "parent-proj-9"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	parent, _ := resp.Data["parent"].(map[string]any)
	if parent["type"] != "folder" || parent["id"] != "111" {
		t.Errorf("parent = %v, want {folder 111}", resp.Data["parent"])
	}

	// A parent in the update body moves the project.
	resp, err = p.UpdateProject(ctx, newNR(map[string]any{
		"project": "parent-proj-9",
		"body":    map[string]any{"parent": map[string]any{"type": "organization", "id": "222"}},
	}))
	if err != nil {
		t.Fatalf("update parent: %v", err)
	}
	parent, _ = resp.Data["parent"].(map[string]any)
	if parent["type"] != "organization" || parent["id"] != "222" {
		t.Errorf("updated parent = %v, want {organization 222}", resp.Data["parent"])
	}

	// An incomplete parent is InvalidArgument.
	_, err = p.UpdateProject(ctx, newNR(map[string]any{
		"project": "parent-proj-9",
		"body":    map[string]any{"parent": map[string]any{"type": "folder"}},
	}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Errorf("incomplete parent err = %v, want 400 InvalidArgument", err)
	}
}

func TestDeleteAndUndeleteProjects(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	if _, err := p.CreateProject(ctx, createNR("del-me-123", "")); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp, err := p.DeleteProject(ctx, newNR(map[string]any{"project": "del-me-123"}))
	if err != nil {
		t.Fatalf("deleteProject: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("delete response = %v, want Empty", resp.Data)
	}

	resp, err = p.ListProjects(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	// v1 keeps DELETE_REQUESTED projects visible to list until deletion
	// completes, which the emulator never does.
	if got := projectState(resp, "del-me-123"); got != "DELETE_REQUESTED" {
		t.Errorf("deleted project state in list = %q, want DELETE_REQUESTED", got)
	}

	if _, err := p.UndeleteProject(ctx, newNR(map[string]any{"project": "del-me-123"})); err != nil {
		t.Fatalf("undeleteProject: %v", err)
	}
	resp, err = p.ListProjects(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list after undelete: %v", err)
	}
	if got := projectState(resp, "del-me-123"); got != "ACTIVE" {
		t.Errorf("undeleted project state in list = %q, want ACTIVE", got)
	}
}

func TestDeleteUnknownProjectNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	_, err := p.DeleteProject(ctx, newNR(map[string]any{"project": "never-created"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 404 {
		t.Errorf("delete unknown err = %v, want 404 NotFound", err)
	}
}

func TestResolveOperation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.CreateProject(ctx, createNR("op-proj-12345", ""))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	name, _ := resp.Data["name"].(string)

	got, handled, err := p.ResolveOperation(ctx, "proj", name)
	if err != nil || !handled {
		t.Fatalf("resolve(%s) handled=%v err=%v", name, handled, err)
	}
	if done, _ := got["done"].(bool); !done {
		t.Errorf("resolved operation = %v, want done", got)
	}
	if _, ok := got["response"].(map[string]any); !ok {
		t.Errorf("resolved operation = %v, want a response", got)
	}

	// A name outside the top-level shape, or an unknown id, is not handled.
	if _, handled, _ := p.ResolveOperation(ctx, "proj", "projects/p/locations/l/operations/x"); handled {
		t.Error("location-scoped name must not be handled")
	}
	if _, handled, err := p.ResolveOperation(ctx, "proj", "operations/missing"); handled || err != nil {
		t.Errorf("unknown id handled=%v err=%v, want false/nil", handled, err)
	}
}

func containsProject(resp *model.ProviderResponse, id string) bool {
	return projectState(resp, id) != ""
}

// projectState returns the lifecycleState of id in a list response, or "".
func projectState(resp *model.ProviderResponse, id string) string {
	projects, _ := resp.Data["projects"].([]any)
	for _, raw := range projects {
		m, ok := raw.(map[string]any)
		if !ok || m["projectId"] != id {
			continue
		}
		s, _ := m["lifecycleState"].(string)
		return s
	}
	return ""
}

// TestListProjectsNonIntegerPageSize pins the int32 query binding: a
// non-integer pageSize is INVALID_ARGUMENT rather than a silent zero.
func TestListProjectsNonIntegerPageSize(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, v := range []any{"abc", "-0.5", "1.5"} {
		_, err := p.ListProjects(ctx, newNR(map[string]any{"pageSize": v}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("ListProjects pageSize=%#v err = %v, want InvalidArgument/400", v, err)
		}
	}
	if _, err := p.ListProjects(ctx, newNR(map[string]any{"pageSize": "5"})); err != nil {
		t.Errorf("ListProjects pageSize=5: %v", err)
	}
}

// TestIntOf covers every shape the IAM policy `version` body field can arrive
// in (a decoded JSON number, an explicit int from a normalized request, a
// string from a query-style caller, and json.Number), plus the unparseable and
// absent fallbacks. intOf no longer parses pageSize (paging.ParseSize does), so
// this keeps the surviving helper's branches pinned.
func TestIntOf(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want int
	}{
		{float64(3), 3},
		{int(4), 4},
		{"5", 5},
		{json.Number("6"), 6},
		{"not-a-number", 0},
		{json.Number("x"), 0},
		{nil, 0},
	} {
		if got := intOf(tc.in); got != tc.want {
			t.Errorf("intOf(%#v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
