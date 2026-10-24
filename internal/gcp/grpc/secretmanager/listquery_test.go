package secretmanager

import (
	"context"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGRPCListFilter verifies the gRPC Secret Manager list methods honor the
// documented `filter` field, in parity with the REST provider.
func TestGRPCListFilter(t *testing.T) {
	client, _, cleanup := secretTestService(t)
	defer cleanup()
	ctx := context.Background()

	create := func(id, team string) {
		t.Helper()
		sec := &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}},
		}}
		if team != "" {
			sec.Labels = map[string]string{"team": team}
		}
		if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
			Parent: "projects/test", SecretId: id, Secret: sec,
		}); err != nil {
			t.Fatalf("CreateSecret %s: %v", id, err)
		}
	}
	create("db-password", "alpha")
	create("api-key", "beta")

	resp, err := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{
		Parent: "projects/test", Filter: `labels.team=alpha`,
	})
	if err != nil {
		t.Fatalf("filtered ListSecrets: %v", err)
	}
	if len(resp.GetSecrets()) != 1 || resp.GetSecrets()[0].GetName() != "projects/test/secrets/db-password" {
		t.Fatalf("labels.team=alpha = %v, want only db-password", resp.GetSecrets())
	}
	if resp.GetTotalSize() != 0 {
		t.Errorf("TotalSize = %d, want 0 once filtered", resp.GetTotalSize())
	}

	_, err = client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{
		Parent: "projects/test", Filter: `name:"a" OR name:"b"`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid filter code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}
