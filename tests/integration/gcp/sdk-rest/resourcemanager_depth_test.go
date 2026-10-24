package sdkrest_test

import (
	"context"
	"encoding/json"
	"testing"

	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"

	"github.com/stretchr/testify/require"
)

// TestSDKResourceManagerProjectMetadataDepth exercises the v1 project metadata
// surface the shallow suite leaves out: create with a parent + labels, read the
// v1 ResourceId parent shape, filter the list by label and parent, and update
// display name + labels through projects.update.
func TestSDKResourceManagerProjectMetadataDepth(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	id := unique("proj")
	op, err := svc.Projects.Create(&cloudresourcemanager.Project{
		ProjectId: id,
		Name:      "Depth Project",
		Parent:    &cloudresourcemanager.ResourceId{Type: "organization", Id: "123456"},
		Labels:    map[string]string{"env": "dev", "team": "platform"},
	}).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "create must complete synchronously")

	var created cloudresourcemanager.Project
	require.NoError(t, json.Unmarshal(op.Response, &created))
	require.Equal(t, id, created.ProjectId)
	require.Equal(t, "Depth Project", created.Name)
	require.Equal(t, "ACTIVE", created.LifecycleState)
	require.Equal(t, "dev", created.Labels["env"])
	require.Equal(t, "platform", created.Labels["team"])
	// The v1 Project carries its parent as a ResourceId ({type,id}); the core
	// stores "organizations/{id}" and the v1 adapter emits the singular type.
	require.NotNil(t, created.Parent)
	require.Equal(t, "organization", created.Parent.Type)
	require.Equal(t, "123456", created.Parent.Id)

	got, err := svc.Projects.Get(id).Do()
	require.NoError(t, err)
	require.Equal(t, "Depth Project", got.Name)
	require.Equal(t, "dev", got.Labels["env"])
	require.Equal(t, "123456", got.Parent.Id)

	// The v1 list filter selects by label value, and a by-parent query ANDs
	// parent.type with parent.id.
	byLabel, err := svc.Projects.List().Filter("labels.env:dev").Do()
	require.NoError(t, err)
	require.True(t, listHasProject(byLabel, id), "labels.env:dev must keep the created project")

	byParent, err := svc.Projects.List().Filter("parent.type:organization parent.id:123456").Do()
	require.NoError(t, err)
	require.True(t, listHasProject(byParent, id), "by-parent filter must keep the created project")
	for _, p := range byParent.Projects {
		require.Equal(t, "123456", p.Parent.Id, "by-parent filter must exclude projects under another parent")
	}

	// projects.update replaces the masked fields; an omitted label key is
	// cleared because the labels map is applied as a whole.
	updated, err := svc.Projects.Update(id, &cloudresourcemanager.Project{
		Name:   "Renamed Project",
		Labels: map[string]string{"env": "prod"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "Renamed Project", updated.Name)
	require.Equal(t, "prod", updated.Labels["env"])
	require.NotContains(t, updated.Labels, "team", "labels are replaced, not merged")

	// The update persisted.
	got, err = svc.Projects.Get(id).Do()
	require.NoError(t, err)
	require.Equal(t, "Renamed Project", got.Name)
	require.Equal(t, "prod", got.Labels["env"])
}

// TestSDKResourceManagerProjectMove exercises the v1 projects.update parent
// handling, which the core models as MoveProject: the request parent is
// "organizations/{id}" (canonical), and the v1 ResourceId round-trips.
func TestSDKResourceManagerProjectMove(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	id := unique("proj")
	_, err = svc.Projects.Create(&cloudresourcemanager.Project{
		ProjectId: id,
		Name:      "Move Project",
		Parent:    &cloudresourcemanager.ResourceId{Type: "organization", Id: "111111"},
	}).Do()
	require.NoError(t, err)

	// A parent-only update moves the project without touching its metadata.
	moved, err := svc.Projects.Update(id, &cloudresourcemanager.Project{
		Parent: &cloudresourcemanager.ResourceId{Type: "folder", Id: "789"},
	}).Do()
	require.NoError(t, err)
	require.NotNil(t, moved.Parent)
	require.Equal(t, "folder", moved.Parent.Type)
	require.Equal(t, "789", moved.Parent.Id)
	require.Equal(t, "Move Project", moved.Name, "a parent-only update must not clear the display name")

	got, err := svc.Projects.Get(id).Do()
	require.NoError(t, err)
	require.Equal(t, "folder", got.Parent.Type)
	require.Equal(t, "789", got.Parent.Id)

	// A malformed destination parent is an InvalidArgument (HTTP 400). The
	// core requires a numeric id, so a non-numeric folder id is rejected.
	_, err = svc.Projects.Update(id, &cloudresourcemanager.Project{
		Parent: &cloudresourcemanager.ResourceId{Type: "folder", Id: "not-a-number"},
	}).Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")
}

// TestSDKResourceManagerProjectIAM exercises project-level
// getIamPolicy/setIamPolicy with etag optimistic concurrency control and
// testIamPermissions through the official v1 client.
func TestSDKResourceManagerProjectIAM(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	id := unique("proj")
	_, err = svc.Projects.Create(&cloudresourcemanager.Project{
		ProjectId: id,
		Name:      "IAM Project",
	}).Do()
	require.NoError(t, err)

	// The default policy carries an etag.
	pol, err := svc.Projects.GetIamPolicy(id, &cloudresourcemanager.GetIamPolicyRequest{}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pol.Etag)

	// setIamPolicy with the matching etag succeeds and returns a fresh etag.
	set, err := svc.Projects.SetIamPolicy(id, &cloudresourcemanager.SetIamPolicyRequest{
		Policy: &cloudresourcemanager.Policy{
			Etag:     pol.Etag,
			Bindings: []*cloudresourcemanager.Binding{{Role: "roles/viewer", Members: []string{"user:alice@example.com"}}},
		},
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, set.Etag)
	require.Len(t, set.Bindings, 1)
	require.Equal(t, "roles/viewer", set.Bindings[0].Role)

	// getIamPolicy reflects the stored bindings.
	pol, err = svc.Projects.GetIamPolicy(id, &cloudresourcemanager.GetIamPolicyRequest{}).Do()
	require.NoError(t, err)
	require.Len(t, pol.Bindings, 1)
	require.Equal(t, "roles/viewer", pol.Bindings[0].Role)

	// A stale etag is rejected with ABORTED (HTTP 409) — not a generic 409
	// ALREADY_EXISTS — matching real Cloud IAM's etag OCC.
	_, err = svc.Projects.SetIamPolicy(id, &cloudresourcemanager.SetIamPolicyRequest{
		Policy: &cloudresourcemanager.Policy{
			Etag:     "BOGUS=",
			Bindings: []*cloudresourcemanager.Binding{{Role: "roles/editor", Members: []string{"user:bob@example.com"}}},
		},
	}).Do()
	requireGCPStatus(t, err, 409, "ABORTED")

	// testIamPermissions echoes the requested set (the emulator does not enforce
	// IAM, so every permission is granted).
	perms, err := svc.Projects.TestIamPermissions(id, &cloudresourcemanager.TestIamPermissionsRequest{
		Permissions: []string{"resourcemanager.projects.get", "resourcemanager.projects.update"},
	}).Do()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"resourcemanager.projects.get", "resourcemanager.projects.update"}, perms.Permissions)
}

// TestSDKResourceManagerInvalidCreate verifies the project-id grammar is
// enforced: an id that does not match the documented 6–30 char lowercase
// grammar is an InvalidArgument (HTTP 400).
func TestSDKResourceManagerInvalidCreate(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	// Uppercase is illegal, and the id is too short.
	_, err = svc.Projects.Create(&cloudresourcemanager.Project{ProjectId: "Bad"}).Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")
}
