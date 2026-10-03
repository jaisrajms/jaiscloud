package secretmanager

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateSecret(ctx, "proj", "a", Secret{ID: "a", Labels: map[string]string{"k": "v"}})
	s.CreateVersion(ctx, "proj", Version{SecretID: "a", VersionID: "1", State: "ENABLED", Data: "aGk="})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sec, err := dst.GetSecret(ctx, "proj", "a")
	if err != nil || sec.Labels["k"] != "v" {
		t.Fatalf("restored secret wrong: %+v / %v", sec, err)
	}
	v, err := dst.GetVersion(ctx, "proj", "a", "1")
	if err != nil || v.Data != "aGk=" {
		t.Fatalf("restored version wrong: %+v / %v", v, err)
	}
}
