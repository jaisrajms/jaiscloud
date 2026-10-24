package sdkrest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/googleapi"
)

// TestSDKResourceManagerProjects exercises the Cloud Resource Manager v1 project
// surface through the official apiary client: create (which returns a
// google.longrunning.Operation), get, list, delete, undelete, and the
// duplicate-create Conflict. It is the v1 counterpart of the gRPC v3
// conformance probes.
func TestSDKResourceManagerProjects(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	id := unique("proj")
	op, err := svc.Projects.Create(&cloudresourcemanager.Project{ProjectId: id, Name: "SDK Project"}).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "create operation must be done in sync mode")

	var created cloudresourcemanager.Project
	require.NoError(t, json.Unmarshal(op.Response, &created))
	require.Equal(t, id, created.ProjectId)

	got, err := svc.Projects.Get(id).Do()
	require.NoError(t, err)
	require.Equal(t, "ACTIVE", got.LifecycleState)

	list, err := svc.Projects.List().Do()
	require.NoError(t, err)
	require.True(t, listHasProject(list, id), "created project must be listed")

	_, err = svc.Projects.Delete(id).Do()
	require.NoError(t, err)
	// v1 keeps DELETE_REQUESTED projects visible to list until deletion
	// completes, which the emulator never does.
	list, err = svc.Projects.List().Do()
	require.NoError(t, err)
	require.True(t, listHasProject(list, id), "DELETE_REQUESTED project stays visible to v1 list")

	_, err = svc.Projects.Undelete(id, &cloudresourcemanager.UndeleteProjectRequest{}).Do()
	require.NoError(t, err)
	list, err = svc.Projects.List().Do()
	require.NoError(t, err)
	require.True(t, listHasProject(list, id), "undeleted project must return to the list")

	// The v1 filter narrows the list: id:<id> returns only this project.
	filtered, err := svc.Projects.List().Filter("id:" + id).Do()
	require.NoError(t, err)
	require.True(t, listHasProject(filtered, id), "id filter must keep the created project")
	for _, p := range filtered.Projects {
		require.Equal(t, id, p.ProjectId, "id filter must exclude every other project")
	}

	// A malformed filter is a 400 InvalidArgument.
	_, err = svc.Projects.List().Filter("bogus").Do()
	var ferr *googleapi.Error
	require.ErrorAs(t, err, &ferr)
	require.Equal(t, 400, ferr.Code, "malformed filter must be InvalidArgument")

	// A duplicate create is a 409 Conflict.
	_, err = svc.Projects.Create(&cloudresourcemanager.Project{ProjectId: id}).Do()
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	require.Equal(t, 409, gerr.Code)
}

func listHasProject(list *cloudresourcemanager.ListProjectsResponse, id string) bool {
	for _, p := range list.Projects {
		if p.ProjectId == id {
			return true
		}
	}
	return false
}
