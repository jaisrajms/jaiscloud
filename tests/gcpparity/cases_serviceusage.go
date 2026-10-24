//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	serviceusage "cloud.google.com/go/serviceusage/apiv1"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/api/iterator"
)

// serviceUsageScenario compares the Service Usage surface over REST and gRPC:
// get a disabled service → enable it → get → list. A service with no explicit
// state is reported DISABLED by both transports, so the initial get is a real
// read even before enabling.
func serviceUsageScenario() Scenario {
	const svc = "storage.googleapis.com"
	name := func(e *Env) string { return fmt.Sprintf("projects/%s/services/%s", e.Cfg.Project, svc) }

	newClient := func(ctx context.Context, e *Env) (*serviceusage.Client, error) {
		return serviceusage.NewClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "serviceusage", Steps: []Step{
		{
			Op: "GetService",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetService(ctx, &serviceusagepb.GetServiceRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op: "EnableService",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: name(e)})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetServiceAfterEnable",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetService(ctx, &serviceusagepb.GetServiceRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op: "ListServices",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListServices(ctx, &serviceusagepb.ListServicesRequest{Parent: e.Cfg.ProjectPath()})
				out := &serviceusagepb.ListServicesResponse{}
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
				return e.Rest(ctx, "GET", "/v1/"+e.Cfg.ProjectPath()+"/services", "")
			},
		},
		{
			// EnableService is the only Service Usage mutation and its resource
			// name is not run-unique (a real DNS service id), so the two sides
			// intentionally target the same service: enable is an idempotent
			// upsert, and the diff compares the resulting Service shape.
			Op: "EnableService (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					c, err := newClient(ctx, e)
					if err != nil {
						return nil, err
					}
					defer c.Close()
					op, err := c.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: name(e)})
					if err != nil {
						return nil, err
					}
					return op.Wait(ctx)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					return e.RestOperationResource(ctx, "POST", "/v1/"+name(e)+":enable", "")
				},
			},
		},
	}}
}
