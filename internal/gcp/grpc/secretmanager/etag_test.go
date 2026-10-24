package secretmanager

import (
	"context"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestGRPCEtagPreconditions covers the gRPC surface of the etag contract
// (DC13): Secret.etag on delete/update and SecretVersion.etag on
// disable/enable/destroy must be rendered and enforced, matching REST.
func TestGRPCEtagPreconditions(t *testing.T) {
	client, _, cleanup := secretTestService(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "etag-secret",
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetEtag() == "" {
		t.Fatal("CreateSecret returned no etag")
	}
	createEtag := created.GetEtag()

	// A metadata update bumps the etag; the pre-update etag is then stale.
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:   created.GetName(),
			Etag:   createEtag,
			Labels: map[string]string{"env": "prod"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("update with the current etag: %v", err)
	}
	if updated.GetEtag() == createEtag {
		t.Fatal("UpdateSecret did not change the etag")
	}
	freshEtag := updated.GetEtag()

	// Update with the pre-update etag is rejected and changes nothing.
	if _, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:   created.GetName(),
			Etag:   createEtag,
			Labels: map[string]string{"env": "dev"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale update code = %v, want Aborted (%v)", status.Code(err), err)
	}
	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetLabels()["env"] != "prod" {
		t.Fatalf("stale update was applied: labels = %v", got.GetLabels())
	}

	// Delete with a stale etag is rejected; the secret survives.
	if _, err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{
		Name: created.GetName(),
		Etag: createEtag,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale delete code = %v, want Aborted (%v)", status.Code(err), err)
	}
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("stale-etag delete removed the secret: %v", err)
	}

	// A version response carries an etag (it used to be absent on gRPC).
	added, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  created.GetName(),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte("hello")},
	})
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	if added.GetEtag() == "" {
		t.Fatal("AddSecretVersion returned no etag")
	}
	versionName := added.GetName()
	enabledEtag := added.GetEtag()

	if _, err := client.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{
		Name: versionName,
		Etag: "BOGUS=",
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("bogus disable code = %v, want Aborted (%v)", status.Code(err), err)
	}
	still, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: versionName})
	if err != nil {
		t.Fatalf("getVersion: %v", err)
	}
	if still.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		t.Fatalf("rejected disable changed state to %v", still.GetState())
	}

	disabled, err := client.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{
		Name: versionName,
		Etag: enabledEtag,
	})
	if err != nil {
		t.Fatalf("disable with the current etag: %v", err)
	}
	if disabled.GetEtag() == enabledEtag {
		t.Fatal("the version etag must change on a state transition")
	}
	if disabled.GetState() != secretmanagerpb.SecretVersion_DISABLED {
		t.Fatalf("state = %v, want DISABLED", disabled.GetState())
	}

	// The pre-disable etag is stale for the enable.
	if _, err := client.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{
		Name: versionName,
		Etag: enabledEtag,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale enable code = %v, want Aborted (%v)", status.Code(err), err)
	}
	reenabled, err := client.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{
		Name: versionName,
		Etag: disabled.GetEtag(),
	})
	if err != nil {
		t.Fatalf("enable with the current etag: %v", err)
	}
	if _, err := client.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{
		Name: versionName,
		Etag: "BOGUS=",
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("bogus destroy code = %v, want Aborted (%v)", status.Code(err), err)
	}
	destroyed, err := client.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{
		Name: versionName,
		Etag: reenabled.GetEtag(),
	})
	if err != nil {
		t.Fatalf("destroy with the current etag: %v", err)
	}
	if destroyed.GetState() != secretmanagerpb.SecretVersion_DESTROYED {
		t.Fatalf("state = %v, want DESTROYED", destroyed.GetState())
	}

	// Delete with the current secret etag passes.
	if _, err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{
		Name: created.GetName(),
		Etag: freshEtag,
	}); err != nil {
		t.Fatalf("delete with the current etag: %v", err)
	}
}
