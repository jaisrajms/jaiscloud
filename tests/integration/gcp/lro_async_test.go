package gcp_test

import (
	"context"
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// This file is the live end-to-end gate for the opt-in async long-running
// operation (LRO) mode. It is skipped unless JAISCLOUD_LRO_ASYNC=1, so the
// default (synchronous) `make test-integration-gcp` run stays green.
// `make test-lro-async-gcp` starts jaiscloud-gcp with
// JAISCLOUD_LRO_MODE=async and sets the variable, then runs TestLROAsync.

const (
	lroProject  = "proj"
	lroLocation = "us-central1"
	// lroGRPCEndpoint is the gRPC listener `make test-lro-async-gcp` starts the
	// emulator on. The generic google.longrunning.Operations service is
	// gRPC-only, so the registry e2e dials it here.
	lroGRPCEndpoint = "localhost:8081"
)

// lroJSONHeaders is the create-content type every LRO create expects.
func lroJSONHeaders() map[string]string {
	return map[string]string{"Content-Type": "application/json"}
}

// TestLROAsync proves that in the opt-in async mode a create returns an
// in-flight operation (done:false) whose location-scoped name can be polled
// through the REST operations.get surface until it settles (done:true with the
// resource in response). Each service shares the
// projects/{project}/locations/{location}/operations/{id} namespace on the
// single emulator host.
func TestLROAsync(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	resetState(t)

	cases := []struct {
		name       string
		createPath string
		createBody string
		wantName   string
	}{
		{
			name:       "workflows",
			createPath: "/v1/projects/" + lroProject + "/locations/" + lroLocation + "/workflows?workflowId=async-wf",
			createBody: `{"sourceContents":"main:\n  steps:\n    - return: ok"}`,
			wantName:   "projects/" + lroProject + "/locations/" + lroLocation + "/workflows/async-wf",
		},
		{
			name:       "metastore",
			createPath: "/v1/projects/" + lroProject + "/locations/" + lroLocation + "/services?serviceId=async-svc",
			createBody: `{}`,
			wantName:   "projects/" + lroProject + "/locations/" + lroLocation + "/services/async-svc",
		},
		{
			name:       "managedkafka",
			createPath: "/v1/projects/" + lroProject + "/locations/" + lroLocation + "/clusters?clusterId=async-c1",
			createBody: `{}`,
			wantName:   "projects/" + lroProject + "/locations/" + lroLocation + "/clusters/async-c1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := do(t, "POST", tc.createPath, []byte(tc.createBody), lroJSONHeaders())
			require.Equal(t, http.StatusOK, resp.StatusCode, "create: %s", body)

			op := jsonMap(t, body)
			done, ok := op["done"].(bool)
			require.True(t, ok, "create operation has no boolean done: %s", body)
			require.False(t, done, "async create must return done:false: %s", body)

			name, _ := op["name"].(string)
			require.Regexp(t,
				regexp.MustCompile(`^projects/`+regexp.QuoteMeta(lroProject)+`/locations/`+regexp.QuoteMeta(lroLocation)+`/operations/[^/]+$`),
				name, "operation name must be location-scoped: %s", body)
			_, hasResponse := op["response"]
			require.False(t, hasResponse, "in-flight operation must not carry response: %s", body)

			// Poll operations.get until the operation settles. The window is
			// JAISCLOUD_LRO_DELAY (2s in the make target); allow far more than
			// that for scheduling jitter.
			pollPath := "/v1/" + name
			deadline := time.Now().Add(20 * time.Second)
			for {
				presp, pbody := do(t, "GET", pollPath, nil, nil)
				require.Equal(t, http.StatusOK, presp.StatusCode, "poll: %s", pbody)
				settled := jsonMap(t, pbody)
				if isDone, _ := settled["done"].(bool); isDone {
					response, ok := settled["response"].(map[string]any)
					require.True(t, ok, "settled operation must carry response: %s", pbody)
					require.Equal(t, tc.wantName, response["name"], "settled response name: %s", pbody)
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("operation %s did not settle within 20s; last: %s", name, pbody)
				}
				time.Sleep(200 * time.Millisecond)
			}
		})
	}
}

// TestLROAsyncServiceUsage proves the top-level operations/{id} namespace is
// pollable in async mode: a Service Usage enable returns an in-flight operation
// named operations/{id}, and GET /v1/operations/{id} resolves it through the
// Service Usage fallback wired into the Functions v1 REST route (which owns the
// shared path) until it settles with the typed EnableServiceResponse.
func TestLROAsyncServiceUsage(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	resetState(t)

	const service = "run.googleapis.com"
	resp, body := do(t, "POST", "/v1/projects/"+lroProject+"/services/"+service+":enable", nil, lroJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "enable: %s", body)

	op := jsonMap(t, body)
	done, ok := op["done"].(bool)
	require.True(t, ok, "enable operation has no boolean done: %s", body)
	require.False(t, done, "async enable must return done:false: %s", body)

	name, _ := op["name"].(string)
	require.Regexp(t, regexp.MustCompile(`^operations/[^/]+$`), name,
		"serviceusage operation name must be top-level: %s", body)
	_, hasResponse := op["response"]
	require.False(t, hasResponse, "in-flight operation must not carry response: %s", body)

	pollPath := "/v1/" + name
	deadline := time.Now().Add(20 * time.Second)
	for {
		presp, pbody := do(t, "GET", pollPath, nil, nil)
		require.Equal(t, http.StatusOK, presp.StatusCode, "poll: %s", pbody)
		settled := jsonMap(t, pbody)
		if isDone, _ := settled["done"].(bool); isDone {
			response, ok := settled["response"].(map[string]any)
			require.True(t, ok, "settled operation must carry response: %s", pbody)
			svc, ok := response["service"].(map[string]any)
			require.True(t, ok, "settled response must carry service: %s", pbody)
			require.Equal(t, "projects/"+lroProject+"/services/"+service, svc["name"],
				"settled service name: %s", pbody)
			require.Equal(t, "ENABLED", svc["state"], "settled service state: %s", pbody)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s did not settle within 20s; last: %s", name, pbody)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestLROAsyncOperationsRegistry proves the generic google.longrunning.Operations
// gRPC service is registry-backed in the opt-in async mode: a Service Usage
// enable's top-level operations/{id} is Get- and List-able through the official
// longrunning client, an operation name no service owns is NotFound (real GCP
// semantics, not the lenient terminal stub), and the operation is deletable
// (after which a get is NotFound).
func TestLROAsyncOperationsRegistry(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	resetState(t)

	// Create an in-flight Service Usage operation through the REST surface.
	const service = "registry-test.googleapis.com"
	resp, body := do(t, "POST", "/v1/projects/"+lroProject+"/services/"+service+":enable", nil, lroJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "enable: %s", body)
	op := jsonMap(t, body)
	name, _ := op["name"].(string)
	require.Regexp(t, regexp.MustCompile(`^operations/[^/]+$`), name,
		"serviceusage operation name must be top-level: %s", body)

	conn, err := grpc.NewClient(lroGRPCEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "dial gRPC")
	t.Cleanup(func() { _ = conn.Close() })
	client := longrunningpb.NewOperationsClient(conn)
	ctx := context.Background()

	// Get resolves the Service Usage operation.
	got, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
	require.NoError(t, err, "GetOperation(owned)")
	require.Equal(t, name, got.GetName())

	// An unowned name is NotFound: the strict contract replaces the lenient
	// terminal stub in async mode.
	_, err = client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/does-not-exist"})
	require.Equal(t, codes.NotFound, status.Code(err), "GetOperation(unknown)")

	// The top-level parent lists the persisted operation. The make target sets
	// JAISCLOUD_GCP_PROJECT_ID=proj so the registry resolves the same project
	// the REST create used.
	list, err := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: "operations"})
	require.NoError(t, err, "ListOperations")
	found := false
	for _, listed := range list.GetOperations() {
		if listed.GetName() == name {
			found = true
		}
	}
	require.True(t, found, "ListOperations must include %s: %+v", name, list.GetOperations())

	// Cancel validates the operation without removing it (the emulator does not
	// model cancellation); it must stay resolvable afterwards.
	_, err = client.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: name})
	require.NoError(t, err, "CancelOperation(owned)")
	_, err = client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
	require.NoError(t, err, "GetOperation(after cancel)")

	// Delete removes it; a subsequent get is NotFound.
	_, err = client.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: name})
	require.NoError(t, err, "DeleteOperation(owned)")
	_, err = client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
	require.Equal(t, codes.NotFound, status.Code(err), "GetOperation(deleted)")
}
