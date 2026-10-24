package firestore

import (
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestDocumentMapTimestampSpelling pins the protojson spelling of the document
// createTime/updateTime google-datetime metadata fields.
func TestDocumentMapTimestampSpelling(t *testing.T) {
	out := documentMap(firestorestore.Document{
		Name:       "projects/p/databases/(default)/documents/c/d",
		CreateTime: time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		UpdateTime: time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
	})
	if got := out["createTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("createTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["updateTime"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("updateTime = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
}
