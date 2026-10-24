package workflowexecutions

import (
	"testing"
	"time"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// TestExecutionToJSONTimestampSpelling pins the protojson spelling of the
// Execution startTime/endTime google-datetime fields.
func TestExecutionToJSONTimestampSpelling(t *testing.T) {
	out := executionToJSON(workflowsstore.Execution{
		ID:         "exec",
		WorkflowID: "wf",
		Location:   "us-central1",
		StartTime:  time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		EndTime:    time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
	}, "proj")
	if got := out["startTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("startTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["endTime"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("endTime = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
}
