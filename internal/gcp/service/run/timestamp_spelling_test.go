package run

import (
	"testing"
	"time"
)

// TestFormatTimeSpelling pins that a Cloud Run google-datetime field is spelled
// the way real GCP's protojson transcoder spells a proto Timestamp: a
// trailing-zero fraction keeps its group boundary (".500", not RFC3339Nano's
// ".5") and a sub-microsecond instant is truncated. The gRPC transport renders
// through the same core maps, so this pins both transports.
func TestFormatTimeSpelling(t *testing.T) {
	got := formatTime(time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC))
	if got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("formatTime(.500) = %q, want 2024-03-01T12:00:00.500Z", got)
	}
	got = formatTime(time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC))
	if got != "2024-03-01T12:00:00.123456Z" {
		t.Fatalf("formatTime(sub-µs) = %q, want 2024-03-01T12:00:00.123456Z", got)
	}
	if got := formatTime(time.Time{}); got != "" {
		t.Fatalf("formatTime(zero) = %q, want empty", got)
	}
}
