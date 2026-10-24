package tasks

import (
	"testing"
	"time"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// TestTaskToJSONTimestampSpelling pins the protojson spelling of the Task
// scheduleTime/createTime google-datetime fields.
func TestTaskToJSONTimestampSpelling(t *testing.T) {
	out := taskToJSON(tasksstore.Task{
		ProjectID:    "proj",
		Location:     "us-central1",
		Queue:        "q",
		Name:         "t1",
		ScheduleTime: time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		CreateTime:   time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
	})
	if got := out["scheduleTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("scheduleTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["createTime"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("createTime = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
}
