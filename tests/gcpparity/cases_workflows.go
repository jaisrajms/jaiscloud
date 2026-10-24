//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	workflows "cloud.google.com/go/workflows/apiv1"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const parityWorkflowSource = "main:\n  steps:\n    - r:\n        return: 1\n"

// workflowsScenario compares the Cloud Workflows definition surface over REST
// and gRPC: create → get → list → update(description, over REST) → get → delete.
func workflowsScenario() Scenario {
	id := "parity-workflow"
	const location = "us-central1"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, location) }
	name := func(e *Env) string { return parent(e) + "/workflows/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*workflows.Client, error) {
		return workflows.NewClient(ctx, e.GRPCClientOptions()...)
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
			Parent: parent(e),
			Workflow: &workflowspb.Workflow{
				Name:       name(e),
				SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: parityWorkflowSource},
			},
			WorkflowId: e.Resource(id),
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		body := fmt.Sprintf(`{"sourceContents":%q}`, parityWorkflowSource)
		return e.RestOperationResource(ctx, "POST", "/v1/"+parent(e)+"/workflows?workflowId="+e.Resource(id), body)
	}
	del := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+name(e)) }

	return Scenario{Service: "workflows", Steps: []Step{
		{
			Op: "CreateWorkflow",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
					Parent: parent(e),
					Workflow: &workflowspb.Workflow{
						Name: name(e),
						SourceCode: &workflowspb.Workflow_SourceContents{
							SourceContents: parityWorkflowSource,
						},
					},
					WorkflowId: e.Resource(id),
				})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetWorkflow",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e)+"?view=FULL", "")
			},
		},
		{
			Op:    "ListWorkflows",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListWorkflows(ctx, &workflowspb.ListWorkflowsRequest{Parent: parent(e)})
				out := &workflowspb.ListWorkflowsResponse{}
				for {
					w, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Workflows = append(out.Workflows, w)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/workflows", "")
			},
		},
		{
			Op: "UpdateWorkflow (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+name(e)+"?updateMask=description", `{"description":"parity updated"}`)
				return err
			},
		},
		{
			Op: "GetWorkflowAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e)+"?view=FULL", "")
			},
		},
		{
			Op: "DeleteWorkflow",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "DELETE", "/v1/"+name(e), "")
				return err
			},
		},
		{
			Op: "CreateWorkflow (parity)",
			Mutation: &MutationParity{
				GRPC:    createGRPC,
				REST:    createREST,
				Cleanup: del,
			},
		},
		{
			Op: "UpdateWorkflow (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createGRPC(ctx, e); err != nil {
						return nil, err
					}
					c, err := newClient(ctx, e)
					if err != nil {
						return nil, err
					}
					defer c.Close()
					op, err := c.UpdateWorkflow(ctx, &workflowspb.UpdateWorkflowRequest{
						Workflow:   &workflowspb.Workflow{Name: name(e), Description: "parity updated"},
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					})
					if err != nil {
						return nil, err
					}
					return op.Wait(ctx)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createREST(ctx, e); err != nil {
						return nil, err
					}
					return e.RestOperationResource(ctx, "PATCH", "/v1/"+name(e)+"?updateMask=description", `{"description":"parity updated"}`)
				},
				Cleanup: del,
			},
		},
	}}
}
