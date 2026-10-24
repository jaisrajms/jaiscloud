package serviceaccount

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/store"
)

// TestCreateKeyValidAfterIsCanonical pins that a generated key's validAfterTime
// (an IAM google-datetime field) is spelled the way real GCP's protojson
// transcoder spells a proto Timestamp: the server-generated clock instant is
// truncated to microseconds, so no sub-microsecond digit reaches the wire.
func TestCreateKeyValidAfterIsCanonical(t *testing.T) {
	rs := store.NewMemoryResourceStore()
	k, err := CreateKey(context.Background(), rs, "acct", "sa@proj.iam.gserviceaccount.com")
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	ts, err := time.Parse(time.RFC3339Nano, k.ValidAfter)
	if err != nil {
		t.Fatalf("validAfterTime %q does not parse: %v", k.ValidAfter, err)
	}
	if ts.Nanosecond()%1000 != 0 {
		t.Fatalf("validAfterTime %q carries a sub-microsecond digit real GCP never sends", k.ValidAfter)
	}
}
