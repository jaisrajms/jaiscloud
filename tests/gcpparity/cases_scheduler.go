//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	scheduler "cloud.google.com/go/scheduler/apiv1"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// schedulerScenario compares the Cloud Scheduler job surface over REST and gRPC:
// create → get → list → update(description, over REST) → get → delete.
func schedulerScenario() Scenario {
	id := "parity-job"
	const location = "us-central1"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, location) }
	jobName := func(e *Env) string { return parent(e) + "/jobs/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*scheduler.CloudSchedulerClient, error) {
		return scheduler.NewCloudSchedulerClient(ctx, e.GRPCClientOptions()...)
	}
	job := func(e *Env) *schedulerpb.Job {
		return &schedulerpb.Job{
			Name:     jobName(e),
			Schedule: "* * * * *",
			TimeZone: "UTC",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
				Uri:        "http://example.com/parity",
				HttpMethod: schedulerpb.HttpMethod_GET,
			}},
		}
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: parent(e), Job: job(e)})
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		body := fmt.Sprintf(`{"name":%q,"schedule":"* * * * *","timeZone":"UTC","httpTarget":{"uri":"http://example.com/parity","httpMethod":"GET"}}`, jobName(e))
		return e.Rest(ctx, "POST", "/v1/"+parent(e)+"/jobs", body)
	}
	del := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+jobName(e)) }

	return Scenario{Service: "scheduler", Steps: []Step{
		{
			Op: "CreateJob",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateJob(ctx, &schedulerpb.CreateJobRequest{
					Parent: parent(e),
					Job: &schedulerpb.Job{
						Name:     jobName(e),
						Schedule: "* * * * *",
						TimeZone: "UTC",
						Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
							Uri:        "http://example.com/parity",
							HttpMethod: schedulerpb.HttpMethod_GET,
						}},
					},
				})
				return err
			},
		},
		{
			Op: "GetJob",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+jobName(e), "")
			},
		},
		{
			Op:    "ListJobs",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: parent(e)})
				out := &schedulerpb.ListJobsResponse{}
				for {
					j, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Jobs = append(out.Jobs, j)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/jobs", "")
			},
		},
		{
			Op: "UpdateJob (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+jobName(e)+"?updateMask=description", `{"description":"parity updated"}`)
				return err
			},
		},
		{
			Op: "GetJobAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+jobName(e), "")
			},
		},
		{
			Op: "DeleteJob",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v1/"+jobName(e))
			},
		},
		{
			Op: "CreateJob (parity)",
			Mutation: &MutationParity{
				GRPC:    createGRPC,
				REST:    createREST,
				Cleanup: del,
			},
		},
		{
			Op: "UpdateJob (parity)",
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
					return c.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{
						Job:        &schedulerpb.Job{Name: jobName(e), Description: "parity updated"},
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					})
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createREST(ctx, e); err != nil {
						return nil, err
					}
					return e.Rest(ctx, "PATCH", "/v1/"+jobName(e)+"?updateMask=description", `{"description":"parity updated"}`)
				},
				Cleanup: del,
			},
		},
	}}
}
