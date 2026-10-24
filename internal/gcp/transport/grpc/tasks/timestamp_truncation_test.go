package tasks

import (
	"testing"
	"time"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// TestQueuePurgeTimeMicrosecondTruncation pins TD48: the gRPC adapter truncates
// a server google-datetime instant to the microsecond the REST renderer
// (transport/rest/tasks) emits.
func TestQueuePurgeTimeMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	q := queueToProto(tasksstore.Queue{
		ProjectID: "proj", Location: "us-central1", Name: "q1",
		PurgeTime: instant,
	})
	if got := q.GetPurgeTime().AsTime(); !got.Equal(want) {
		t.Fatalf("purgeTime = %v, want %v", got, want)
	}
}

// TestTaskTimesMicrosecondTruncation pins the same for the task schedule/create
// times.
func TestTaskTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	task := taskToProto(tasksstore.Task{
		ProjectID: "proj", Location: "us-central1", Queue: "q1", Name: "t1",
		ScheduleTime: instant, CreateTime: instant,
	})
	if got := task.GetScheduleTime().AsTime(); !got.Equal(want) {
		t.Fatalf("scheduleTime = %v, want %v", got, want)
	}
	if got := task.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("createTime = %v, want %v", got, want)
	}
}

// TestAttemptTimesMicrosecondTruncation pins the same for an attempt's
// schedule/dispatch/response times.
func TestAttemptTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	a := attemptToProto(&tasksstore.Attempt{
		ScheduleTime: instant, DispatchTime: instant, ResponseTime: instant,
	})
	for name, got := range map[string]time.Time{
		"scheduleTime": a.GetScheduleTime().AsTime(),
		"dispatchTime": a.GetDispatchTime().AsTime(),
		"responseTime": a.GetResponseTime().AsTime(),
	} {
		if !got.Equal(want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}
