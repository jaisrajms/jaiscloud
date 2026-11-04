package gcs

import (
	"bytes"
	"context"
	"testing"
)

// iamConfigurationMeta is the bucket-meta shape the storage provider persists
// for iamConfiguration (normalized: uniformBucketLevelAccess.enabled plus
// publicAccessPrevention; bucketPolicyOnly is derived on read).
func iamConfigurationMeta(uniform bool, pap string) map[string]any {
	return map[string]any{
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": uniform},
			"publicAccessPrevention":   pap,
		},
	}
}

// assertIamConfiguration checks the stored nested object survived whatever
// round-trip the caller exercised.
func assertIamConfiguration(t *testing.T, meta map[string]any, wantUniform bool, wantPAP string) {
	t.Helper()
	ic, ok := meta["iamConfiguration"].(map[string]any)
	if !ok {
		t.Fatalf("iamConfiguration missing from stored bucket meta: %v", meta)
	}
	uba, _ := ic["uniformBucketLevelAccess"].(map[string]any)
	if got, _ := uba["enabled"].(bool); got != wantUniform {
		t.Errorf("uniformBucketLevelAccess.enabled = %v, want %v", got, wantUniform)
	}
	if got, _ := ic["publicAccessPrevention"].(string); got != wantPAP {
		t.Errorf("publicAccessPrevention = %q, want %q", got, wantPAP)
	}
}

// TestMemoryObjectStoreIamConfigurationRoundTrip pins that the bucket IAM
// configuration is part of the stored bucket metadata, not a value re-derived on
// read: a create, a metageneration-only update and a snapshot/restore all keep
// it.
func TestMemoryObjectStoreIamConfigurationRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()

	meta := iamConfigurationMeta(true, "enforced")
	meta["name"] = "bkt"
	if err := s.CreateBucket(ctx, "proj", "bkt", meta); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	got, err := s.GetBucket(ctx, "bkt")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	assertIamConfiguration(t, got, true, "enforced")

	// The provider's UpdateBucketMetaAtomic path rebuilds the meta around the
	// fields a PATCH touches; iamConfiguration must ride along untouched.
	if _, err := s.UpdateBucketMetaAtomic(ctx, "bkt", func(m map[string]any) (map[string]any, error) {
		m["metageneration"] = "2"
		m["labels"] = map[string]string{"env": "test"}
		return m, nil
	}); err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	got, _ = s.GetBucket(ctx, "bkt")
	assertIamConfiguration(t, got, true, "enforced")

	// Snapshot/restore is the same JSON round-trip the Postgres meta column
	// performs, so this also proves the field survives --dsn persistence.
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryObjectStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := dst.GetBucket(ctx, "bkt")
	if err != nil {
		t.Fatalf("get restored bucket: %v", err)
	}
	assertIamConfiguration(t, restored, true, "enforced")
}
