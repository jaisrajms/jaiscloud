//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// secretManagerScenario compares the Secret Manager secret-admin surface over
// REST and gRPC: create → get → list → update(labels, over REST) → get → delete.
func secretManagerScenario() Scenario {
	id := "parity-secret"
	name := func(e *Env) string { return e.Cfg.ProjectPath() + "/secrets/" + e.Resource(id) }
	parent := func(e *Env) string { return e.Cfg.ProjectPath() }

	newClient := func(ctx context.Context, e *Env) (*secretmanager.Client, error) {
		return secretmanager.NewClient(ctx, e.GRPCClientOptions()...)
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
			Parent:   parent(e),
			SecretId: e.Resource(id),
			Secret: &secretmanagerpb.Secret{
				Replication: &secretmanagerpb.Replication{
					Replication: &secretmanagerpb.Replication_Automatic_{
						Automatic: &secretmanagerpb.Replication_Automatic{},
					},
				},
			},
		})
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", "/v1/"+parent(e)+"/secrets?secretId="+e.Resource(id), `{"replication":{"automatic":{}}}`)
	}
	del := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+name(e)) }

	return Scenario{Service: "secretmanager", Steps: []Step{
		{
			Op: "CreateSecret",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
					Parent:   parent(e),
					SecretId: e.Resource(id),
					Secret: &secretmanagerpb.Secret{
						Replication: &secretmanagerpb.Replication{
							Replication: &secretmanagerpb.Replication_Automatic_{
								Automatic: &secretmanagerpb.Replication_Automatic{},
							},
						},
					},
				})
				return err
			},
		},
		{
			Op: "GetSecret",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op:    "ListSecrets",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: parent(e)})
				out := &secretmanagerpb.ListSecretsResponse{}
				for {
					s, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Secrets = append(out.Secrets, s)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/secrets", "")
			},
		},
		{
			Op: "UpdateSecret (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				body := `{"labels":{"parity":"true"}}`
				_, err := e.Rest(ctx, "PATCH", "/v1/"+name(e)+"?updateMask=labels", body)
				return err
			},
		},
		{
			Op: "GetSecretAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op: "DeleteSecret",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v1/"+name(e))
			},
		},
		{
			Op: "CreateSecret (parity)",
			Mutation: &MutationParity{
				GRPC:    createGRPC,
				REST:    createREST,
				Cleanup: del,
			},
		},
		{
			Op: "UpdateSecret (parity)",
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
					return c.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
						Secret:     &secretmanagerpb.Secret{Name: name(e), Labels: map[string]string{"parity": "true"}},
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
					})
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createREST(ctx, e); err != nil {
						return nil, err
					}
					return e.Rest(ctx, "PATCH", "/v1/"+name(e)+"?updateMask=labels", `{"labels":{"parity":"true"}}`)
				},
				Cleanup: del,
			},
		},
	}}
}
