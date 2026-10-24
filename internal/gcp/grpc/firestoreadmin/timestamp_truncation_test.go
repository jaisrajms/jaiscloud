package firestoreadmin

import (
	"testing"
	"time"

	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
)

// TestDatabaseTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// database's google-datetime instants to the microsecond the REST renderer
// (provider/firestore admin_rest.go) emits.
func TestDatabaseTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	out := databaseToProto(firestoreprovider.DatabaseDef{
		Name:       "projects/p/databases/(default)",
		CreateTime: instant, UpdateTime: instant, DeleteTime: instant,
	})
	if got := out.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("createTime = %v, want %v", got, want)
	}
	if got := out.GetUpdateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("updateTime = %v, want %v", got, want)
	}
	if got := out.GetDeleteTime().AsTime(); !got.Equal(want) {
		t.Fatalf("deleteTime = %v, want %v", got, want)
	}
}
