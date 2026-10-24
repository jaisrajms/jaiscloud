package scheduler

import (
	"testing"
	"time"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// TestJobTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST renderer
// (transport/rest/scheduler) emits, so the two transports agree.
func TestJobTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	out := jobToProto(schedstore.Job{
		ScheduleTime:    instant,
		LastAttemptTime: instant,
		UserUpdateTime:  instant,
	})
	for name, got := range map[string]time.Time{
		"scheduleTime":    out.GetScheduleTime().AsTime(),
		"lastAttemptTime": out.GetLastAttemptTime().AsTime(),
		"userUpdateTime":  out.GetUserUpdateTime().AsTime(),
	} {
		if !got.Equal(want) {
			t.Fatalf("%s = %v, want %v (truncated to microseconds)", name, got, want)
		}
	}
}
