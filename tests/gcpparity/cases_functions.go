//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	functionsv2 "cloud.google.com/go/functions/apiv2"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const functionsLocation = "us-central1"

// functionsScenario compares the Cloud Functions v2 surface over REST and gRPC:
// create → get → list → update(labels, over REST) → get → delete.
func functionsScenario() Scenario {
	id := "parity-fn"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, functionsLocation) }
	name := func(e *Env) string { return parent(e) + "/functions/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*functionsv2.FunctionClient, error) {
		return functionsv2.NewFunctionClient(ctx, e.GRPCClientOptions()...)
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.CreateFunction(ctx, &functionspb.CreateFunctionRequest{
			Parent:     parent(e),
			FunctionId: e.Resource(id),
			Function: &functionspb.Function{
				Labels:      map[string]string{"parity": "true"},
				BuildConfig: &functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
			},
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.RestOperationResource(ctx, "POST", "/v2/"+parent(e)+"/functions?functionId="+e.Resource(id),
			`{"labels":{"parity":"true"},"buildConfig":{"runtime":"nodejs20","entryPoint":"handler"}}`)
	}
	del := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v2/"+name(e)) }

	return Scenario{Service: "functions", Steps: []Step{
		{
			Op: "CreateFunction",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.CreateFunction(ctx, &functionspb.CreateFunctionRequest{
					Parent:     parent(e),
					FunctionId: e.Resource(id),
					Function: &functionspb.Function{
						Labels:      map[string]string{"parity": "true"},
						BuildConfig: &functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
					},
				})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetFunction",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op:    "ListFunctions",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListFunctions(ctx, &functionspb.ListFunctionsRequest{Parent: parent(e)})
				out := &functionspb.ListFunctionsResponse{}
				for {
					f, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Functions = append(out.Functions, f)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+parent(e)+"/functions", "")
			},
		},
		{
			Op: "UpdateFunction (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v2/"+name(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
				return err
			},
		},
		{
			Op: "GetFunctionAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op: "DeleteFunction",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: name(e)})
				if err != nil {
					return err
				}
				return op.Wait(ctx)
			},
		},
		{
			Op: "CreateFunction (parity)",
			Mutation: &MutationParity{
				GRPC:    createGRPC,
				REST:    createREST,
				Cleanup: del,
			},
		},
		{
			Op: "UpdateFunction (parity)",
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
					op, err := c.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{
						Function:   &functionspb.Function{Name: name(e), Labels: map[string]string{"parity": "updated"}},
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
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
					return e.RestOperationResource(ctx, "PATCH", "/v2/"+name(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
				},
				Cleanup: del,
			},
		},
	}}
}
