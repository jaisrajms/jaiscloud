package secretmanager

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	expire := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	s.CreateSecret(ctx, "proj", "a", Secret{ID: "a", Labels: map[string]string{"k": "v"}, KmsKeyName: "ck",
		VersionDestroyTTL: "86400s", ExpireTime: expire})
	s.CreateVersion(ctx, "proj", Version{SecretID: "a", VersionID: "1", State: "DISABLED", Data: "aGk=",
		KmsKeyName: "ck", WrappedDEK: []byte("1:ciphertext"),
		ScheduledDestroyTime: expire.Add(-time.Hour),
		ReplicaDEKs:          []ReplicaDEK{{Location: "us-east1", KmsKeyName: "ck-east", WrappedDEK: []byte("2:east")}}})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sec, err := dst.GetSecret(ctx, "proj", "a")
	if err != nil || sec.Labels["k"] != "v" || sec.KmsKeyName != "ck" {
		t.Fatalf("restored secret wrong: %+v / %v", sec, err)
	}
	if sec.VersionDestroyTTL != "86400s" || !sec.ExpireTime.Equal(expire) {
		t.Fatalf("restored secret lifecycle fields wrong: %+v", sec)
	}
	v, err := dst.GetVersion(ctx, "proj", "a", "1")
	if err != nil || v.Data != "aGk=" || v.KmsKeyName != "ck" || string(v.WrappedDEK) != "1:ciphertext" {
		t.Fatalf("restored version wrong: %+v / %v", v, err)
	}
	if !v.ScheduledDestroyTime.Equal(expire.Add(-time.Hour)) {
		t.Fatalf("restored scheduledDestroyTime = %v", v.ScheduledDestroyTime)
	}
	if len(v.ReplicaDEKs) != 1 || v.ReplicaDEKs[0].Location != "us-east1" || string(v.ReplicaDEKs[0].WrappedDEK) != "2:east" {
		t.Fatalf("restored replica DEKs wrong: %+v / %v", v.ReplicaDEKs, err)
	}
}
