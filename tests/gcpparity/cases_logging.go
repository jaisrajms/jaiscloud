//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"

	logging "cloud.google.com/go/logging/apiv2"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// loggingScenario compares the Cloud Logging Admin v2 log-bucket surface over
// REST and gRPC: create → get → list → update(description, over REST) → get →
// delete.
func loggingScenario() Scenario {
	id := "parity-bucket"
	locationParent := func(e *Env) string { return e.Cfg.ProjectPath() + "/locations/global" }
	name := func(e *Env) string { return locationParent(e) + "/buckets/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*logging.ConfigClient, error) {
		return logging.NewConfigClient(ctx, e.GRPCClientOptions()...)
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateBucket(ctx, &loggingpb.CreateBucketRequest{
			Parent:   locationParent(e),
			BucketId: e.Resource(id),
			Bucket:   &loggingpb.LogBucket{Description: "parity", RetentionDays: 30},
		})
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", "/v2/"+locationParent(e)+"/buckets?bucketId="+e.Resource(id),
			`{"description":"parity","retentionDays":30}`)
	}
	del := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v2/"+name(e)) }

	return Scenario{Service: "logging", Steps: []Step{
		{
			Op: "CreateBucket",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateBucket(ctx, &loggingpb.CreateBucketRequest{
					Parent:   locationParent(e),
					BucketId: e.Resource(id),
					Bucket:   &loggingpb.LogBucket{Description: "parity", RetentionDays: 30},
				})
				return err
			},
		},
		{
			Op: "GetBucket",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op:    "ListBuckets",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListBuckets(ctx, &loggingpb.ListBucketsRequest{Parent: locationParent(e)})
				out := &loggingpb.ListBucketsResponse{}
				for {
					b, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Buckets = append(out.Buckets, b)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+locationParent(e)+"/buckets", "")
			},
		},
		{
			Op: "UpdateBucket (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v2/"+name(e)+"?updateMask=description", `{"description":"parity updated"}`)
				return err
			},
		},
		{
			Op: "UpdateBucketRestrictedFields (gRPC)",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.UpdateBucket(ctx, &loggingpb.UpdateBucketRequest{
					Name: name(e),
					Bucket: &loggingpb.LogBucket{
						RetentionDays:    45,
						RestrictedFields: []string{"jsonPayload.secret", "labels"},
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention_days", "restricted_fields"}},
				})
				return err
			},
		},
		{
			Op: "GetBucketAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op: "DeleteBucket",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v2/"+name(e))
			},
		},
		{
			Op: "CreateBucket (parity)",
			Mutation: &MutationParity{
				GRPC:    createGRPC,
				REST:    createREST,
				Cleanup: del,
			},
		},
		{
			Op: "UpdateBucket (parity)",
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
					return c.UpdateBucket(ctx, &loggingpb.UpdateBucketRequest{
						Name:       name(e),
						Bucket:     &loggingpb.LogBucket{Description: "parity updated"},
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					})
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createREST(ctx, e); err != nil {
						return nil, err
					}
					return e.Rest(ctx, "PATCH", "/v2/"+name(e)+"?updateMask=description", `{"description":"parity updated"}`)
				},
				Cleanup: del,
			},
		},
	}}
}
