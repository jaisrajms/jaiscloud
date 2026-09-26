package gcp_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFunctionsAcceptanceFlow covers the Cloud Functions v1 wire surface:
// create → get → list → generateDownloadUrl → locations.list → call (mock
// echo) → delete, over the JSON REST API. Create/Update/Delete return a done
// google.longrunning.Operation.
func TestFunctionsAcceptanceFlow(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	base := "/v1/projects/" + project + "/locations/" + location + "/functions"

	// Create (functionId query param) returns a done google.longrunning.Operation
	// whose response is the created Function.
	resp, body := do(t, "POST", base+"?functionId=hello",
		[]byte(`{"runtime":"nodejs20","entryPoint":"helloWorld"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	op := jsonMap(t, body)
	require.Equal(t, true, op["done"])
	fn, _ := op["response"].(map[string]any)
	require.Equal(t, "type.googleapis.com/google.cloud.functions.v1.CloudFunction", fn["@type"])
	require.Equal(t, "projects/proj/locations/us-central1/functions/hello", fn["name"])
	require.Equal(t, "ACTIVE", fn["status"])
	require.Equal(t, "nodejs20", fn["runtime"])

	// Get.
	resp, body = do(t, "GET", base+"/hello", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "helloWorld", jsonMap(t, body)["entryPoint"])

	// List.
	resp, body = do(t, "GET", base, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	items, _ := jsonMap(t, body)["functions"].([]any)
	require.Len(t, items, 1)

	// generateDownloadUrl (custom method) returns a synthesized URL.
	resp, body = do(t, "POST", base+"/hello:generateDownloadUrl",
		[]byte(`{}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, jsonMap(t, body)["downloadUrl"], "storage.googleapis.com")

	// locations.list resolves on the shared project-locations path.
	resp, body = do(t, "GET", "/v1/projects/"+project+"/locations", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	locations, _ := jsonMap(t, body)["locations"].([]any)
	require.NotEmpty(t, locations)

	// Call (mock echo returns the request data).
	resp, body = do(t, "POST", base+"/hello:call",
		[]byte(`{"data":"ping"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	call := jsonMap(t, body)
	require.Equal(t, "ping", call["result"])
	require.NotEmpty(t, call["executionId"])

	// Delete returns a done Operation whose response is a typed
	// google.protobuf.Empty Any (gax unpacks it as Empty).
	resp, body = do(t, "DELETE", base+"/hello", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	del := jsonMap(t, body)
	require.Equal(t, true, del["done"])
	delResp, ok := del["response"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "type.googleapis.com/google.protobuf.Empty", delResp["@type"])
	require.Len(t, delResp, 1)

	// Gone after delete.
	resp, _ = do(t, "GET", base+"/hello", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
