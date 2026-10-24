package firestore

import (
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestDocumentTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// document's google-datetime instants to the microsecond the REST provider
// (provider/firestore) emits. A Value.timestampValue is a data value, not a
// metadata field, and keeps its nanosecond precision (encodeValue).
func TestDocumentTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	doc := encodeDocument(firestorestore.Document{
		Name:       "projects/p/databases/(default)/documents/c/d1",
		CreateTime: instant, UpdateTime: instant,
	})
	if got := doc.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("createTime = %v, want %v", got, want)
	}
	if got := doc.GetUpdateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("updateTime = %v, want %v", got, want)
	}
}
