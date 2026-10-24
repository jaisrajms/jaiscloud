package kms

import (
	"testing"
	"time"

	kmsstore "jaiscloud/internal/gcp/store/kms"
)

// TestVersionMapTimestampSpelling pins the protojson spelling of the
// CryptoKeyVersion createTime/generateTime google-datetime fields.
func TestVersionMapTimestampSpelling(t *testing.T) {
	out := versionMap(newNR(nil), "global", "kr", "key", kmsstore.Version{
		Version:    "1",
		State:      "ENABLED",
		CreateTime: time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
	})
	if got := out["createTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("createTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["generateTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("generateTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
}
