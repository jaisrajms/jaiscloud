package grpcconformance

import (
	"context"
	"fmt"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// secretManagerRotationChecks covers Cloud SQL managed rotation
// (SecretManagerService.EnableManagedRotation / RotateSecret) via the official
// client. The emulator has no Cloud SQL data plane, so it stores the generated
// password as a version and records the managed-rotation status rather than
// applying the password to a database user (GA7-D1). Both RPCs return a
// SecretVersion, which is what the client observes.
//
// The fixture secret is per-run unique (cfg.ResourceName), so the one-shot
// EnableManagedRotation is always fresh; RotateSecret deletes it afterwards.
func secretManagerRotationChecks() []Check {
	return []Check{
		{Service: "secretmanager", RPC: "EnableManagedRotation", Method: "EnableManagedRotation", KeyField: "new ENABLED version + managed_rotation_status ACTIVE", Run: checkSMREnableManagedRotation},
		{Service: "secretmanager", RPC: "RotateSecret", Method: "RotateSecret", KeyField: "second ENABLED version created", Run: checkSMRRotateSecret},
	}
}

const smrSecretID = "gcpc-grpc-smr-secret"

func smrSecretName(cfg Config) string {
	return secretProject(cfg) + "/secrets/" + cfg.ResourceName(smrSecretID)
}

// smrEnsureSecret creates the managed-rotation fixture secret if it is absent.
func smrEnsureSecret(ctx context.Context, client *secretmanager.Client, cfg Config) error {
	_, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: cfg.ResourceName(smrSecretID),
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateSecret(fixture): %w", err)
	}
	return nil
}

func smrCloudSQLRequest(cfg Config) *secretmanagerpb.EnableManagedRotationRequest {
	return &secretmanagerpb.EnableManagedRotationRequest{
		Parent: smrSecretName(cfg),
		Credentials: &secretmanagerpb.EnableManagedRotationRequest_CloudSqlSingleUserCredentials{
			CloudSqlSingleUserCredentials: &secretmanagerpb.EnableManagedRotationRequest_CloudSQLSingleUserCredentials{
				InstanceId: "gcpc-cloudsql-instance",
				Username:   "gcpc-cloudsql-user",
			},
		},
	}
}

func checkSMREnableManagedRotation(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smrEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	version, err := client.EnableManagedRotation(ctx, smrCloudSQLRequest(cfg))
	if err != nil {
		return fmt.Errorf("EnableManagedRotation: %w", err)
	}
	if !strings.HasSuffix(version.GetName(), "/versions/1") {
		return fmt.Errorf("EnableManagedRotation created %q, want .../versions/1", version.GetName())
	}
	if version.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("EnableManagedRotation state = %v, want ENABLED", version.GetState())
	}
	sec, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: smrSecretName(cfg)})
	if err != nil {
		return fmt.Errorf("GetSecret after enable: %w", err)
	}
	if st := sec.GetRotation().GetManagedRotationStatus().GetState(); st != secretmanagerpb.Rotation_ManagedRotationStatus_ACTIVE {
		return fmt.Errorf("managed_rotation_status.state = %v, want ACTIVE", st)
	}
	return nil
}

func checkSMRRotateSecret(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	version, err := client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: smrSecretName(cfg)})
	if err != nil {
		return fmt.Errorf("RotateSecret: %w", err)
	}
	if !strings.HasSuffix(version.GetName(), "/versions/2") {
		return fmt.Errorf("RotateSecret created %q, want .../versions/2", version.GetName())
	}
	if version.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("RotateSecret state = %v, want ENABLED", version.GetState())
	}
	// Clean up the per-run fixture.
	return client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: smrSecretName(cfg)})
}
