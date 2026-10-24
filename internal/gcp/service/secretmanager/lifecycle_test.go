package secretmanager

import (
	"context"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/gcp/timestamp"
)

func lifecycleClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

func TestExpireSecretIfDue(t *testing.T) {
	ctx := context.Background()
	st := secretmanagerstore.NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lifecycleClock(t, now)

	// Not due: untouched and reported as not deleted.
	if err := st.CreateSecret(ctx, "proj", "live", secretmanagerstore.Secret{
		ID: "live", NextVer: 1, ExpireTime: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("create live: %v", err)
	}
	if _, deleted, err := ExpireSecretIfDue(ctx, st, "proj", "live"); err != nil || deleted {
		t.Fatalf("live secret = deleted %v, err %v; want untouched", deleted, err)
	}
	if _, err := st.GetSecret(ctx, "proj", "live"); err != nil {
		t.Fatalf("a live secret was deleted: %v", err)
	}

	// A secret without an expiration never expires.
	if err := st.CreateSecret(ctx, "proj", "eternal", secretmanagerstore.Secret{ID: "eternal", NextVer: 1}); err != nil {
		t.Fatalf("create eternal: %v", err)
	}
	if _, deleted, err := ExpireSecretIfDue(ctx, st, "proj", "eternal"); err != nil || deleted {
		t.Fatalf("secret without expiration = deleted %v, err %v", deleted, err)
	}

	// Due: the pre-delete record is returned and the versions are cascaded.
	if err := st.CreateSecret(ctx, "proj", "old", secretmanagerstore.Secret{
		ID: "old", NextVer: 2, ExpireTime: now,
	}); err != nil {
		t.Fatalf("create old: %v", err)
	}
	if err := st.CreateVersion(ctx, "proj", secretmanagerstore.Version{
		SecretID: "old", VersionID: "1", State: "ENABLED", CreateTime: now,
	}); err != nil {
		t.Fatalf("create version: %v", err)
	}
	expired, deleted, err := ExpireSecretIfDue(ctx, st, "proj", "old")
	if err != nil || !deleted {
		t.Fatalf("due secret = deleted %v, err %v; want deleted", deleted, err)
	}
	if expired.ID != "old" || !expired.ExpireTime.Equal(now) {
		t.Fatalf("pre-delete record = %+v", expired)
	}
	if _, err := st.GetSecret(ctx, "proj", "old"); !errors.Is(err, secretmanagerstore.ErrNoSuchSecret) {
		t.Fatalf("expired secret still present: %v", err)
	}
	if _, err := st.GetVersion(ctx, "proj", "old", "1"); !errors.Is(err, secretmanagerstore.ErrNoSuchVersion) {
		t.Fatalf("expired secret's version still present: %v", err)
	}

	// Missing: neither deleted nor an error (a concurrent request won the sweep).
	if _, deleted, err := ExpireSecretIfDue(ctx, st, "proj", "absent"); err != nil || deleted {
		t.Fatalf("missing secret = deleted %v, err %v", deleted, err)
	}
}

func TestFinalizeScheduledDestroy(t *testing.T) {
	ctx := context.Background()
	st := secretmanagerstore.NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(-time.Minute)
	lifecycleClock(t, now)

	if err := st.CreateSecret(ctx, "proj", "s", secretmanagerstore.Secret{ID: "s", NextVer: 2}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Not due: unchanged.
	if err := st.CreateVersion(ctx, "proj", secretmanagerstore.Version{
		SecretID: "s", VersionID: "1", State: "DISABLED", CreateTime: now,
		ScheduledDestroyTime: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("create pending version: %v", err)
	}
	pending, err := st.GetVersion(ctx, "proj", "s", "1")
	if err != nil {
		t.Fatalf("get pending: %v", err)
	}
	got, destroyed := FinalizeScheduledDestroy(ctx, st, "proj", "s", pending)
	if destroyed || got.State != "DISABLED" || !got.ScheduledDestroyTime.Equal(pending.ScheduledDestroyTime) {
		t.Fatalf("pending version = %+v, destroyed %v", got, destroyed)
	}

	// Due: DESTROYED with destroyTime equal to the scheduled instant.
	if err := st.CreateVersion(ctx, "proj", secretmanagerstore.Version{
		SecretID: "s", VersionID: "2", State: "DISABLED", CreateTime: now,
		ScheduledDestroyTime: scheduledAt,
	}); err != nil {
		t.Fatalf("create due version: %v", err)
	}
	due, err := st.GetVersion(ctx, "proj", "s", "2")
	if err != nil {
		t.Fatalf("get due: %v", err)
	}
	got, destroyed = FinalizeScheduledDestroy(ctx, st, "proj", "s", due)
	if !destroyed || got.State != "DESTROYED" || !got.DestroyTime.Equal(scheduledAt) {
		t.Fatalf("due version = %+v, destroyed %v", got, destroyed)
	}
	stored, err := st.GetVersion(ctx, "proj", "s", "2")
	if err != nil || stored.State != "DESTROYED" || !stored.DestroyTime.Equal(scheduledAt) {
		t.Fatalf("stored version = %+v / %v", stored, err)
	}

	// A DESTROYED version is never due again.
	if _, destroyed := FinalizeScheduledDestroy(ctx, st, "proj", "s", stored); destroyed {
		t.Fatal("a destroyed version was finalized twice")
	}

	// A version that disappeared under us is reported unchanged, not destroyed.
	gone := secretmanagerstore.Version{SecretID: "s", VersionID: "404", State: "DISABLED", ScheduledDestroyTime: scheduledAt}
	if got, destroyed := FinalizeScheduledDestroy(ctx, st, "proj", "s", gone); destroyed || got.State != "DISABLED" {
		t.Fatalf("missing version = %+v, destroyed %v", got, destroyed)
	}
}

// TestExpirationEdgeParsing covers the malformed-duration shapes and the
// negative-duration renderer branch that the provider tests cannot reach.
func TestExpirationEdgeParsing(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, in := range []string{"+3600s", "3600.s", "3600.1234567890s", "9223372036855s", "18446744073709551616s"} {
		if _, err := ParseRotationDuration(in); err == nil {
			t.Errorf("ParseRotationDuration(%q) = nil error, want 400", in)
		}
	}
	if got := DurationString(-time.Hour); got != "-3600s" {
		t.Errorf("DurationString(-1h) = %q", got)
	}
	at, err := ParseExpireTime(now.Add(2*time.Hour).Format(time.RFC3339), now)
	if err != nil || !at.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("ParseExpireTime = %v, %v", at, err)
	}
	if got := expireTimeString(time.Time{}); got != "" {
		t.Errorf("expireTimeString(zero) = %q", got)
	}
	if got := expireTimeString(now); got != timestamp.Canonical(now) {
		t.Errorf("expireTimeString(now) = %q, want %q", got, timestamp.Canonical(now))
	}
}

func TestVersionEtagCoversScheduledDestroy(t *testing.T) {
	v := secretmanagerstore.Version{
		SecretID: "s", VersionID: "1", State: "DISABLED",
		CreateTime:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ScheduledDestroyTime: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	name := "projects/p/secrets/s/versions/1"
	withSchedule := VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, timestamp.Canonical(v.ScheduledDestroyTime))
	without := VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, "")
	if withSchedule == without {
		t.Fatal("the version etag does not cover scheduledDestroyTime")
	}
	if err := CheckVersionEtag(name, v, withSchedule); err != nil {
		t.Fatalf("the current scheduled etag was rejected: %v", err)
	}
}
