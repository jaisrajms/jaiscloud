// Package sdk_workflows_test exercises the jaiscloud-gcp emulator's Cloud
// Workflows surface (workflows.googleapis.com + workflowexecutions.googleapis.com)
// through the official Google REST apiary clients. This validates wire-level
// parity with the real SDKs.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_workflows_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/api/workflowexecutions/v1"
	"google.golang.org/api/workflows/v1"

	"github.com/stretchr/testify/require"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

const source = `main:
  params: [a, b]
  steps:
    - init:
        assign:
          - sum: ${a + b}
    - done:
        return: ${sum}
`

func TestSDKWorkflows(t *testing.T) {
	ctx := context.Background()
	wfs, err := workflows.NewService(ctx, opts()...)
	require.NoError(t, err)
	execs, err := workflowexecutions.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj/locations/us-central1"
	workflowID := unique("wf")
	name := parent + "/workflows/" + workflowID

	// Create returns a done LRO in the emulator.
	_, err = wfs.Projects.Locations.Workflows.Create(parent, &workflows.Workflow{
		Name:           name,
		SourceContents: source,
		Description:    "sdk test",
	}).WorkflowId(workflowID).Do()
	require.NoError(t, err)

	wf, err := wfs.Projects.Locations.Workflows.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, name, wf.Name)
	require.Equal(t, "ACTIVE", wf.State)
	require.Contains(t, wf.SourceContents, "return")
	require.NotEmpty(t, wf.RevisionId)

	list, err := wfs.Projects.Locations.Workflows.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Workflows)

	// Execute: assign a+b and return the sum.
	exec, err := execs.Projects.Locations.Workflows.Executions.Create(name, &workflowexecutions.Execution{
		Argument: `{"a": 20, "b": 22}`,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "SUCCEEDED", exec.State)
	require.Equal(t, "42", exec.Result)
	require.Equal(t, wf.RevisionId, exec.WorkflowRevisionId)

	got, err := execs.Projects.Locations.Workflows.Executions.Get(exec.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "SUCCEEDED", got.State)
	require.Equal(t, "42", got.Result)

	elist, err := execs.Projects.Locations.Workflows.Executions.List(name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, elist.Executions)

	// Cancel on a terminal execution is a no-op that returns the execution.
	cancelled, err := execs.Projects.Locations.Workflows.Executions.Cancel(exec.Name, &workflowexecutions.CancelExecutionRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, "SUCCEEDED", cancelled.State)

	// Get a missing execution → error.
	_, err = execs.Projects.Locations.Workflows.Executions.Get(name + "/executions/does-not-exist").Do()
	require.Error(t, err)

	// Delete and verify gone.
	_, err = wfs.Projects.Locations.Workflows.Delete(name).Do()
	require.NoError(t, err)
	_, err = wfs.Projects.Locations.Workflows.Get(name).Do()
	require.Error(t, err)
}

// TestSDKWorkflowsOperationsListDelete exercises the location-scoped operations
// surface through the official client: a create LRO is listed by
// projects.locations.operations.list, then removed by operations.delete (which
// answers Empty) so a subsequent poll is NotFound.
func TestSDKWorkflowsOperationsListDelete(t *testing.T) {
	ctx := context.Background()
	wfs, err := workflows.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj/locations/us-central1"
	id := unique("wfops")
	name := parent + "/workflows/" + id
	op, err := wfs.Projects.Locations.Workflows.Create(parent, &workflows.Workflow{
		SourceContents: source,
	}).WorkflowId(id).Do()
	require.NoError(t, err)
	require.NotEmpty(t, op.Name)

	// The create operation appears in the location's operations list. (Its done
	// state is mode-dependent — the opt-in async mode returns it in flight — so
	// only its presence is asserted here.)
	list, err := wfs.Projects.Locations.Operations.List(parent).Do()
	require.NoError(t, err)
	var found bool
	for _, o := range list.Operations {
		if o.Name == op.Name {
			found = true
			require.Equal(t, op.Name, o.Name)
			break
		}
	}
	require.Truef(t, found, "create operation %s not listed", op.Name)

	// Delete answers Empty and the operation is gone.
	_, err = wfs.Projects.Locations.Operations.Delete(op.Name).Do()
	require.NoError(t, err)
	_, err = wfs.Projects.Locations.Operations.Get(op.Name).Do()
	require.Error(t, err, "operation must be gone after delete")

	_, err = wfs.Projects.Locations.Workflows.Delete(name).Do()
	require.NoError(t, err)
}

func TestSDKWorkflowsFailureFailsLoud(t *testing.T) {
	ctx := context.Background()
	wfs, err := workflows.NewService(ctx, opts()...)
	require.NoError(t, err)
	execs, err := workflowexecutions.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj/locations/us-central1"
	workflowID := unique("wf")
	name := parent + "/workflows/" + workflowID

	// Unknown stdlib function must fail the execution (not silently no-op).
	badSource := "main:\n  steps:\n    - x:\n        call: http.delete\n        args:\n          url: \"http://x\"\n"
	_, err = wfs.Projects.Locations.Workflows.Create(parent, &workflows.Workflow{
		Name:           name,
		SourceContents: badSource,
	}).WorkflowId(workflowID).Do()
	require.NoError(t, err)

	exec, err := execs.Projects.Locations.Workflows.Executions.Create(name, &workflowexecutions.Execution{}).Do()
	require.NoError(t, err)
	require.Equal(t, "FAILED", exec.State)
	require.NotNil(t, exec.Error)
	require.NotEmpty(t, exec.Error.Payload)
}
