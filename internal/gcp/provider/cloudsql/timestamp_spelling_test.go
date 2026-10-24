package cloudsql

import (
	"testing"
	"time"
)

// TestFormatTimestampSpelling pins the protojson spelling of a Cloud SQL
// google-datetime field (Instance.createTime, operation times).
func TestFormatTimestampSpelling(t *testing.T) {
	if got := formatTimestamp(time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC)); got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("formatTimestamp(.500) = %q, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := formatTimestamp(time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)); got != "2024-03-01T12:00:00.123456Z" {
		t.Fatalf("formatTimestamp(sub-µs) = %q, want 2024-03-01T12:00:00.123456Z", got)
	}
}
