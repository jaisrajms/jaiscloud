//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	workflows "cloud.google.com/go/workflows/apiv1"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executions "cloud.google.com/go/workflows/executions/apiv1"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/api/iterator"
)

// parityExecutionLocation is the region the prerequisite workflow is deployed
// to.
const parityExecutionLocation = "us-central1"

// parityExecutionSource is a deterministic a+b workflow whose execution result
// is the JSON number 42.
const parityExecutionSource = "main:\n  params: [a, b]\n  steps:\n    - init:\n        assign:\n          - sum: ${a + b}\n    - done:\n        return: ${sum}\n"

// parityExecutionArgument is the execution argument (a+b=42).
const parityExecutionArgument = `{"a": 20, "b": 22}`

// workflowExecutionsScenario compares the Cloud Workflow Executions surface over
// REST and gRPC: deploy a workflow → create → get → list → cancel → delete the
// workflow. An execution cannot exist without an owning workflow, so the first
// step deploys the run-unique workflow the flow runs under; the execution is
// then read back over both transports and diffed. The create is also cross-diffed
// head-to-head as a mutation, both transports creating an execution under that
// same workflow (so the two runs share the workflow revision they record).
func workflowExecutionsScenario() Scenario {
	const id = "parity-wfexec"

	parent := func(e *Env) string {
		return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, parityExecutionLocation)
	}
	// workflowName is the prerequisite workflow; executionsParent is the same
	// workflow seen as the executions' parent. It is deliberately built from the
	// run suffix alone (not Env.Resource), so the mutation-parity twins run
	// against the one shared workflow rather than a per-side deploy.
	workflowName := func(e *Env) string { return parent(e) + "/workflows/" + e.Cfg.ResourceName(id) }
	executionsParent := workflowName

	newWorkflowsClient := func(ctx context.Context, e *Env) (*workflows.Client, error) {
		return workflows.NewClient(ctx, e.GRPCClientOptions()...)
	}
	newExecutionsClient := func(ctx context.Context, e *Env) (*executions.Client, error) {
		return executions.NewClient(ctx, e.GRPCClientOptions()...)
	}

	// deployWorkflow deploys the run-unique workflow over the management API (a
	// real deployment deploys a workflow, then runs it over the executions API).
	deployWorkflow := func(ctx context.Context, e *Env) error {
		c, err := newWorkflowsClient(ctx, e)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
			Parent: parent(e),
			Workflow: &workflowspb.Workflow{
				Name:       workflowName(e),
				SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: parityExecutionSource},
			},
			WorkflowId: e.Cfg.ResourceName(id),
		})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	// deleteWorkflow removes the prerequisite workflow through the REST
	// management API.
	deleteWorkflow := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, "/v1/"+workflowName(e))
	}

	// createExecutionGRPC creates an execution of the shared workflow and is
	// side-effect free, so the read flow and the mutation-parity step can both
	// call it without sharing state.
	createExecutionGRPC := func(ctx context.Context, e *Env) (*executionspb.Execution, error) {
		c, err := newExecutionsClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateExecution(ctx, &executionspb.CreateExecutionRequest{
			Parent: executionsParent(e),
			Execution: &executionspb.Execution{
				Argument: parityExecutionArgument,
			},
		})
	}
	createExecutionREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		body := fmt.Sprintf(`{"argument":%q}`, parityExecutionArgument)
		return e.Rest(ctx, "POST", "/v1/"+executionsParent(e)+"/executions", body)
	}

	// execName is the read flow's execution — the one the gRPC create produced,
	// addressed by the read steps over both transports.
	var execName string

	return Scenario{Service: "workflowexecutions", Steps: []Step{
		{
			Op:     "DeployWorkflow",
			Mutate: deployWorkflow,
		},
		{
			Op: "CreateExecution",
			Mutate: func(ctx context.Context, e *Env) error {
				ex, err := createExecutionGRPC(ctx, e)
				if err != nil {
					return err
				}
				execName = ex.GetName()
				return nil
			},
		},
		{
			Op: "GetExecution",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newExecutionsClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetExecution(ctx, &executionspb.GetExecutionRequest{
					Name: execName,
					View: executionspb.ExecutionView_FULL,
				})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+execName+"?view=FULL", "")
			},
		},
		{
			Op:    "ListExecutions",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newExecutionsClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListExecutions(ctx, &executionspb.ListExecutionsRequest{
					Parent: executionsParent(e),
					View:   executionspb.ExecutionView_FULL,
				})
				out := &executionspb.ListExecutionsResponse{}
				for {
					ex, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Executions = append(out.Executions, ex)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+executionsParent(e)+"/executions?view=FULL", "")
			},
		},
		{
			Op: "CancelExecution",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newExecutionsClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.CancelExecution(ctx, &executionspb.CancelExecutionRequest{Name: execName})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "POST", "/v1/"+execName+":cancel", "")
			},
		},
		{
			// Both transports create an execution under the shared workflow; the
			// twins run before the workflow is deleted below. An execution has no
			// delete RPC (real GCP exposes only cancel/get/list/create), so the
			// mutation has no Cleanup of its own.
			Op: "CreateExecution (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					return createExecutionGRPC(ctx, e)
				},
				REST:    createExecutionREST,
				Cleanup: nil,
			},
		},
		{
			Op:     "DeleteWorkflow",
			Mutate: deleteWorkflow,
		},
	}}
}
