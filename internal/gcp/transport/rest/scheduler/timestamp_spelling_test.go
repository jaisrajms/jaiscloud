package scheduler

import (
	"testing"
	"time"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// TestJobToJSONTimestampSpelling pins the protojson spelling of the Job
// scheduleTime/lastAttemptTime/userUpdateTime google-datetime fields.
func TestJobToJSONTimestampSpelling(t *testing.T) {
	out := jobToJSON(schedstore.Job{
		ScheduleTime:    time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		LastAttemptTime: time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
		UserUpdateTime:  time.Date(2024, 3, 1, 12, 0, 2, 250_000_000, time.UTC),
	})
	if got := out["scheduleTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("scheduleTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["lastAttemptTime"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("lastAttemptTime = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
	if got := out["userUpdateTime"]; got != "2024-03-01T12:00:02.250Z" {
		t.Fatalf("userUpdateTime = %v, want 2024-03-01T12:00:02.250Z", got)
	}
}
