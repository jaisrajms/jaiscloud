package monitoring

import (
	"testing"
	"time"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// TestPointToJSONTimestampSpelling pins the protojson spelling of the
// TimeInterval startTime/endTime google-datetime fields.
func TestPointToJSONTimestampSpelling(t *testing.T) {
	out := pointToJSON(monitoringstore.Point{
		StartTime: time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		EndTime:   time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
	})
	iv, ok := out["interval"].(map[string]any)
	if !ok {
		t.Fatalf("interval = %T, want map", out["interval"])
	}
	if got := iv["startTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("startTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := iv["endTime"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("endTime = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
}
