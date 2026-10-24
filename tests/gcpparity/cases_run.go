//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	run "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const runLocation = "us-central1"

// runScenario compares the Cloud Run service surface over REST and gRPC:
// create → get → list → update(labels, over REST) → get → delete.
func cloudRunScenario() Scenario {
	id := "parity-run"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, runLocation) }
	name := func(e *Env) string { return parent(e) + "/services/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*run.ServicesClient, error) {
		return run.NewServicesClient(ctx, e.GRPCClientOptions()...)
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.CreateService(ctx, &runpb.CreateServiceRequest{
			Parent:    parent(e),
			ServiceId: e.Resource(id),
			Service: &runpb.Service{
				Labels: map[string]string{"parity": "true"},
				Template: &runpb.RevisionTemplate{
					Containers: []*runpb.Container{{Image: "nginx:latest", Ports: []*runpb.ContainerPort{{ContainerPort: 80}}}},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.RestOperationResource(ctx, "POST", "/v2/"+parent(e)+"/services?serviceId="+e.Resource(id),
			`{"labels":{"parity":"true"},"template":{"containers":[{"image":"nginx:latest","ports":[{"containerPort":80}]}]}}`)
	}
	del := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v2/"+name(e)) }

	return Scenario{Service: "run", Steps: []Step{
		{
			Op: "CreateService",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.CreateService(ctx, &runpb.CreateServiceRequest{
					Parent:    parent(e),
					ServiceId: e.Resource(id),
					Service: &runpb.Service{
						Labels: map[string]string{"parity": "true"},
						Template: &runpb.RevisionTemplate{
							Containers: []*runpb.Container{{Image: "nginx:latest", Ports: []*runpb.ContainerPort{{ContainerPort: 80}}}},
						},
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
			Op: "GetService",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetService(ctx, &runpb.GetServiceRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op:    "ListServices",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListServices(ctx, &runpb.ListServicesRequest{Parent: parent(e)})
				out := &runpb.ListServicesResponse{}
				for {
					s, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Services = append(out.Services, s)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+parent(e)+"/services", "")
			},
		},
		{
			Op: "UpdateService (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v2/"+name(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
				return err
			},
		},
		{
			Op: "GetServiceAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetService(ctx, &runpb.GetServiceRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op: "DeleteService",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: name(e)})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "CreateService (parity)",
			Mutation: &MutationParity{
				GRPC:    createGRPC,
				REST:    createREST,
				Cleanup: del,
			},
		},
		{
			Op: "UpdateService (parity)",
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
					op, err := c.UpdateService(ctx, &runpb.UpdateServiceRequest{
						Service:    &runpb.Service{Name: name(e), Labels: map[string]string{"parity": "updated"}},
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
