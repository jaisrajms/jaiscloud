package secretmanager

import (
	"context"
	"testing"
	"time"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func waitGRPCEvent(t *testing.T, pub *grpcFakePublisher, timeout time.Duration, want string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, m := range pub.drain() {
			if m.attrs["eventType"] == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestGRPCBackgroundRotationNotification covers DC71 on the gRPC service: a due
// rotation publishes SECRET_ROTATE and reschedules with no client read.
func TestGRPCBackgroundRotationNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := NewService(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore()), "test")
	pub := &grpcFakePublisher{}
	svc.SetEventPublisher(pub)
	svc.SetSweepInterval(10 * time.Millisecond)
	t0 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	svc.Start(ctx)
	defer svc.Stop()

	if _, err := svc.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/proj",
		SecretId: "rot",
		Secret: &secretmanagerpb.Secret{
			Topics: rotateTopic("projects/proj/topics/rot"),
			Rotation: &secretmanagerpb.Rotation{
				NextRotationTime: timestamppb.New(t0.Add(time.Hour)),
				RotationPeriod:   durationpb.New(time.Hour),
			},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	pub.drain()

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Hour)})
	waitGRPCEvent(t, pub, 2*time.Second, "SECRET_ROTATE")

	sec, err := svc.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: "projects/proj/secrets/rot"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := sec.GetRotation().GetNextRotationTime().AsTime(); !got.Equal(t0.Add(2 * time.Hour)) {
		t.Fatalf("next rotation = %v, want %v", got, t0.Add(2*time.Hour))
	}
}
