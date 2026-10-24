// Tests for DC70 over the official gRPC client: `versionDestroyTtl` schedules a
// version's destruction (SECRET_VERSION_DESTROY_SCHEDULED, then
// SECRET_VERSION_DESTROY) and `ttl`/`expireTime` expires the secret with
// SECRET_DELETE deleteType EXPIRATION.
package secretmanager

import (
	"context"
	"testing"
	"time"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/clock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGRPCDelayedVersionDestroy(t *testing.T) {
	ctx := context.Background()
	client, pub, _, cleanup := newEventTestServer(t)
	defer cleanup()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	scheduledAt := t0.Add(24 * time.Hour)

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "s",
		Secret: &secretmanagerpb.Secret{
			Topics:            []*secretmanagerpb.Topic{{Name: "projects/test/topics/a"}},
			VersionDestroyTtl: durationpb.New(24 * time.Hour),
		},
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if got := created.GetVersionDestroyTtl().AsDuration(); got != 24*time.Hour {
		t.Fatalf("versionDestroyTtl = %v, want 24h", got)
	}
	version, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  created.GetName(),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("AddSecretVersion: %v", err)
	}
	pub.drain()

	destroyed, err := client.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: version.GetName()})
	if err != nil {
		t.Fatalf("DestroySecretVersion: %v", err)
	}
	if destroyed.GetState() != secretmanagerpb.SecretVersion_DISABLED {
		t.Fatalf("state = %v, want DISABLED", destroyed.GetState())
	}
	if got := destroyed.GetScheduledDestroyTime().AsTime(); !got.Equal(scheduledAt) {
		t.Fatalf("scheduledDestroyTime = %v, want %v", got, scheduledAt)
	}
	if destroyed.GetDestroyTime() != nil {
		t.Fatalf("scheduled destroy carries destroyTime %v", destroyed.GetDestroyTime())
	}
	wantGRPCEvents(t, pub.drain(), "SECRET_VERSION_DESTROY_SCHEDULED")

	if _, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: version.GetName()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("access of a scheduled version = %v, want FailedPrecondition", err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: scheduledAt.Add(time.Second)})
	got, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: version.GetName()})
	if err != nil {
		t.Fatalf("GetSecretVersion after the delay: %v", err)
	}
	if got.GetState() != secretmanagerpb.SecretVersion_DESTROYED {
		t.Fatalf("state = %v, want DESTROYED", got.GetState())
	}
	if !got.GetDestroyTime().AsTime().Equal(scheduledAt) {
		t.Fatalf("destroyTime = %v, want %v", got.GetDestroyTime().AsTime(), scheduledAt)
	}
	wantGRPCEvents(t, pub.drain(), "SECRET_VERSION_DESTROY")
}

func TestGRPCSecretExpiration(t *testing.T) {
	ctx := context.Background()
	client, pub, _, cleanup := newEventTestServer(t)
	defer cleanup()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	expireAt := t0.Add(24 * time.Hour)

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "s",
		Secret: &secretmanagerpb.Secret{
			Topics: []*secretmanagerpb.Topic{{Name: "projects/test/topics/a"}},
			Expiration: &secretmanagerpb.Secret_Ttl{
				Ttl: durationpb.New(24 * time.Hour),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if !created.GetExpireTime().AsTime().Equal(expireAt) {
		t.Fatalf("expireTime = %v, want %v", created.GetExpireTime().AsTime(), expireAt)
	}
	if created.GetTtl() != nil {
		t.Fatalf("the input-only ttl is echoed: %v", created.GetTtl())
	}
	if _, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  created.GetName(),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte("hi")},
	}); err != nil {
		t.Fatalf("AddSecretVersion: %v", err)
	}
	pub.drain()

	clock.SetGlobalClock(clock.FixedClock{T: expireAt})
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSecret at expiry = %v, want NotFound", err)
	}
	msgs := pub.drain()
	wantGRPCEvents(t, msgs, "SECRET_DELETE")
	if msgs[0].attrs["deleteType"] != "EXPIRATION" {
		t.Fatalf("deleteType = %q, want EXPIRATION", msgs[0].attrs["deleteType"])
	}
	if _, err := client.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("ListSecretVersions after expiry = %v, want NotFound", err)
	}

	// A masked, fieldless expire_time update removes an expiration.
	live, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "live",
		Secret: &secretmanagerpb.Secret{
			Expiration: &secretmanagerpb.Secret_ExpireTime{
				ExpireTime: timestamppb.New(t0.Add(100 * time.Hour)),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateSecret live: %v", err)
	}
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: live.GetName()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"expire_time"}},
	})
	if err != nil {
		t.Fatalf("UpdateSecret removing the expiration: %v", err)
	}
	if updated.GetExpireTime() != nil {
		t.Fatalf("expireTime survived removal: %v", updated.GetExpireTime())
	}
}

// TestGRPCLifecycleFieldsRespectTheMask: an update mask that does not name the
// lifecycle fields ignores them even when the request carries values.
func TestGRPCLifecycleFieldsRespectTheMask(t *testing.T) {
	ctx := context.Background()
	client, _, _, cleanup := newEventTestServer(t)
	defer cleanup()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	expireAt := t0.Add(24 * time.Hour)

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "s",
		Secret: &secretmanagerpb.Secret{
			VersionDestroyTtl: durationpb.New(24 * time.Hour),
			Expiration:        &secretmanagerpb.Secret_Ttl{Ttl: durationpb.New(24 * time.Hour)},
		},
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:              created.GetName(),
			Labels:            map[string]string{"env": "prod"},
			VersionDestroyTtl: durationpb.New(48 * time.Hour),
			Expiration:        &secretmanagerpb.Secret_Ttl{Ttl: durationpb.New(48 * time.Hour)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateSecret labels: %v", err)
	}
	if updated.GetVersionDestroyTtl().AsDuration() != 24*time.Hour {
		t.Fatalf("an unmasked delay was applied: %v", updated.GetVersionDestroyTtl())
	}
	if !updated.GetExpireTime().AsTime().Equal(expireAt) {
		t.Fatalf("an unmasked expiration was applied: %v", updated.GetExpireTime())
	}
}
