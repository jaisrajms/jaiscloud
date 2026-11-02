package secretmanager

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/timestamp"
)

func waitEventTypes(t *testing.T, pub *fakePublisher, timeout time.Duration, want ...string) []publishedMessage {
	t.Helper()
	pending := make(map[string]bool, len(want))
	for _, w := range want {
		pending[w] = true
	}
	var seen []publishedMessage
	deadline := time.Now().Add(timeout)
	for {
		msgs := pub.drain()
		seen = append(seen, msgs...)
		for _, m := range msgs {
			delete(pending, m.attrs["eventType"])
		}
		if len(pending) == 0 {
			return seen
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %v (saw %v)", want, eventTypes(seen))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestBackgroundRotationNotification covers DC71: a due rotation publishes
// SECRET_ROTATE and advances the schedule with no client reads.
func TestBackgroundRotationNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, pub := newTestProvider(t)
	p.SetSweepInterval(10 * time.Millisecond)
	t0 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)
	p.Start(ctx)
	defer p.Stop()

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "rot",
		"body": map[string]any{
			"topics": []any{map[string]any{"name": "projects/proj/topics/rot"}},
			"rotation": map[string]any{
				"nextRotationTime": timestamp.Canonical(t0.Add(time.Hour)),
				"rotationPeriod":   "3600s",
			},
		},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	pub.drain()

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Hour)})
	waitEventTypes(t, pub, 2*time.Second, "SECRET_ROTATE")

	// The schedule advanced to the next period without any client read.
	stored, err := p.secrets.GetSecret(ctx, "proj", "rot")
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if stored.Rotation == nil || stored.Rotation.NextRotationTime != timestamp.Canonical(t0.Add(2*time.Hour)) {
		t.Fatalf("next rotation = %+v, want %v", stored.Rotation, timestamp.Canonical(t0.Add(2*time.Hour)))
	}
}

// TestBackgroundExpirationAndScheduledDestroy covers DC72's proactive half:
// expiration and delayed destruction happen even without a read, and the
// events are published exactly once.
func TestBackgroundExpirationAndScheduledDestroy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, pub := newTestProvider(t)
	p.SetSweepInterval(10 * time.Millisecond)
	t0 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)
	p.Start(ctx)
	defer p.Stop()

	// Expiration.
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "exp",
		"body": map[string]any{
			"topics": []any{map[string]any{"name": "projects/proj/topics/exp"}},
			"ttl":    "86400s",
		},
	})); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	// Scheduled destruction.
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "dest",
		"body": map[string]any{
			"topics":            []any{map[string]any{"name": "projects/proj/topics/dest"}},
			"versionDestroyTtl": "86400s",
		},
	})); err != nil {
		t.Fatalf("create dest: %v", err)
	}
	added, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/dest",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	versionName, _ := added.Data["name"].(string)
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": versionName})); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	pub.drain()

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(24 * time.Hour)})
	waitEventTypes(t, pub, 2*time.Second, "SECRET_DELETE", "SECRET_VERSION_DESTROY")

	// The secret is gone without a read, and the version is terminal.
	if _, err := p.secrets.GetSecret(ctx, "proj", "exp"); err == nil {
		t.Fatal("expired secret still present without a read")
	}
	v, err := p.secrets.GetVersion(ctx, "proj", "dest", "1")
	if err != nil || v.State != "DESTROYED" || v.DestroyTime.IsZero() {
		t.Fatalf("scheduled version = %+v err=%v, want DESTROYED", v, err)
	}
}

// TestAccessGateAtExactDueInstant covers DC72's strict read gate: the boundary
// itself is already expired/destroyed even before the background sweep runs.
func TestAccessGateAtExactDueInstant(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "dest",
		"body":     map[string]any{"versionDestroyTtl": "86400s"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	added, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/dest",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	name, _ := added.Data["name"].(string)
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": name})); err != nil {
		t.Fatalf("destroy: %v", err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(24 * time.Hour)})
	if _, err := p.Access(ctx, newNR(map[string]any{"name": name})); err == nil || errStatus(err) != 400 {
		t.Fatalf("access at scheduledDestroyTime = %v, want FAILED_PRECONDITION/400", err)
	}
	v, err := p.secrets.GetVersion(ctx, "proj", "dest", "1")
	if err != nil || v.State != "DESTROYED" {
		t.Fatalf("version after gate = %+v err=%v, want DESTROYED", v, err)
	}
}
