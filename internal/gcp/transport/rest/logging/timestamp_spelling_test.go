package logging

import (
	"testing"
	"time"
)

// TestUnixNanosRFC3339Spelling pins the protojson spelling of the Cloud Logging
// Admin google-datetime fields the helper renders.
func TestUnixNanosRFC3339Spelling(t *testing.T) {
	if got := unixNanosRFC3339(time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC).UnixNano()); got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("unixNanosRFC3339(.500) = %q, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := unixNanosRFC3339(time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC).UnixNano()); got != "2024-03-01T12:00:00.123456Z" {
		t.Fatalf("unixNanosRFC3339(sub-µs) = %q, want 2024-03-01T12:00:00.123456Z", got)
	}
}
