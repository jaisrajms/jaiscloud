package ui

import (
	"context"
	"testing"

	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	restresourcemanager "jaiscloud/internal/gcp/transport/rest/resourcemanager"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// v1Visible compares the fields the v1 REST shape carries, which is what the
// console shows once a plugin is in the path.
func v1Visible(a, b resourcemanagercore.Project) bool {
	return a.ProjectID == b.ProjectID && a.ProjectNumber == b.ProjectNumber && a.DisplayName == b.DisplayName &&
		a.State == b.State && a.Parent == b.Parent && a.CreateTime.Equal(b.CreateTime) && len(a.Labels) == len(b.Labels)
}

func TestResourceManagerConsolePassthroughMatchesDirect(t *testing.T) {
	core := resourcemanagercore.NewService(store.NewMemoryResourceStore())
	builtins := restresourcemanager.NewProvider(core, "proj").Routes()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeResourceManager(typedFixture(builtins, wrap), core)
	ctx := context.Background()

	created, op, err := p.CreateProject(ctx, resourcemanagercore.CreateProjectInput{ProjectID: "demo-one", DisplayName: "Demo One", Labels: map[string]string{"env": "dev"}})
	if err != nil || created.ProjectID != "demo-one" || !op.Done {
		t.Fatalf("CreateProject = %+v, %+v, %v", created, op, err)
	}
	want, err := core.GetProject(ctx, "demo-one")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.GetProject(ctx, "demo-one"); err != nil || !v1Visible(got, want) {
		t.Fatalf("project through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	list, _, err := p.ListProjects(ctx, 0, "", false, "")
	found := false
	for _, pr := range list {
		found = found || pr.ProjectID == "demo-one"
	}
	if err != nil || !found {
		t.Fatalf("ListProjects = %+v, %v", list, err)
	}
	if pr, o, err := p.DeleteProject(ctx, "demo-one"); err != nil || pr.State != "DELETE_REQUESTED" || o.Verb != "delete" {
		t.Fatalf("DeleteProject = %+v, %+v, %v", pr, o, err)
	}
	if pr, _, err := p.UndeleteProject(ctx, "demo-one"); err != nil || pr.State != "ACTIVE" {
		t.Fatalf("UndeleteProject = %+v, %v", pr, err)
	}
	if len(seen) < 6 {
		t.Fatalf("the plugin saw only %d calls", len(seen))
	}
}

func TestResourceManagerConsoleShowsWhatThePluginServes(t *testing.T) {
	core := resourcemanagercore.NewService(store.NewMemoryResourceStore())
	p := routeResourceManager(typedFixture(restresourcemanager.NewProvider(core, "proj").Routes(), map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"ResourceManager.ProjectList": replaceWith(map[string]any{"projects": []any{map[string]any{
			"projectId": "from-plugin", "name": "From Plugin", "lifecycleState": "ACTIVE", "parent": map[string]any{"type": "organization", "id": "42"}}}}),
	}), core)
	list, _, err := p.ListProjects(context.Background(), 0, "", false, "")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListProjects = %+v, %v", list, err)
	}
	if pr := list[0]; pr.ProjectID != "from-plugin" || pr.DisplayName != "From Plugin" || pr.Parent != "organizations/42" {
		t.Fatalf("the console must show the plugin's project, got %+v", pr)
	}
}
