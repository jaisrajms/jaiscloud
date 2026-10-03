package sdkrest_test

import (
	"context"
	"encoding/json"
	"testing"

	"google.golang.org/api/cloudfunctions/v1"
	cloudfunctionsv2 "google.golang.org/api/cloudfunctions/v2"
	"google.golang.org/api/option"

	"github.com/stretchr/testify/require"
)

// TestSDKCloudFunctions exercises the Cloud Functions v1 REST apiary client
// against the emulator: create (by resource name in the body), get, list,
// generateDownloadUrl, locations.list, call, and delete. Create/Delete return a
// done google.longrunning.Operation (the emulator completes synchronously).
func TestSDKCloudFunctions(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctions.NewService(ctx, option.WithEndpoint(endpoint()), option.WithoutAuthentication())
	require.NoError(t, err)

	const parent = "projects/proj/locations/us-central1"
	name := parent + "/functions/" + unique("fn")

	op, err := svc.Projects.Locations.Functions.Create(parent, &cloudfunctions.CloudFunction{
		Name:       name,
		Runtime:    "nodejs20",
		EntryPoint: "helloWorld",
	}).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "create should return a done Operation")
	var created cloudfunctions.CloudFunction
	require.NoError(t, json.Unmarshal(op.Response, &created))
	require.Equal(t, name, created.Name)
	require.Equal(t, "nodejs20", created.Runtime)

	fn, err := svc.Projects.Locations.Functions.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, name, fn.Name)
	require.Equal(t, "nodejs20", fn.Runtime)
	require.Equal(t, "helloWorld", fn.EntryPoint)
	require.Equal(t, "ACTIVE", fn.Status)

	list, err := svc.Projects.Locations.Functions.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Functions)

	dl, err := svc.Projects.Locations.Functions.GenerateDownloadUrl(name, &cloudfunctions.GenerateDownloadUrlRequest{}).Do()
	require.NoError(t, err)
	require.Contains(t, dl.DownloadUrl, "storage.googleapis.com")

	// locations.list resolves on the shared project-locations path (served by
	// the Memorystore handler, which returns the same
	// google.cloud.location.Location records).
	locations, err := svc.Projects.Locations.List("projects/proj").Do()
	require.NoError(t, err)
	require.NotEmpty(t, locations.Locations)

	call, err := svc.Projects.Locations.Functions.Call(name, &cloudfunctions.CallFunctionRequest{
		Data: "ping",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "ping", call.Result)
	require.NotEmpty(t, call.ExecutionId)

	op, err = svc.Projects.Locations.Functions.Delete(name).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "delete should return a done Operation")

	_, err = svc.Projects.Locations.Functions.Get(name).Do()
	require.Error(t, err)
}

// TestSDKCloudFunctionsRuntimes exercises the v2 runtime catalog through the
// official apiary client: the list is non-empty, nodejs20 is GEN_2/GA, and an
// AIP-160 name filter narrows it.
func TestSDKCloudFunctionsRuntimes(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctionsv2.NewService(ctx, option.WithEndpoint(endpoint()), option.WithoutAuthentication())
	require.NoError(t, err)

	const parent = "projects/proj/locations/us-central1"
	resp, err := svc.Projects.Locations.Runtimes.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, resp.Runtimes)

	var node *cloudfunctionsv2.Runtime
	for _, rt := range resp.Runtimes {
		if rt.Name == "nodejs20" {
			node = rt
		}
	}
	require.NotNil(t, node, "nodejs20 missing from runtime catalog")
	require.Equal(t, "GEN_2", node.Environment)
	require.Equal(t, "GA", node.Stage)
	require.Equal(t, "Node.js 20", node.DisplayName)

	filtered, err := svc.Projects.Locations.Runtimes.List(parent).Filter(`name="python312"`).Do()
	require.NoError(t, err)
	require.Len(t, filtered.Runtimes, 1)
	require.Equal(t, "python312", filtered.Runtimes[0].Name)
}
