package workflowexecutions

import (
	"testing"
	"time"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// TestExecutionTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates
// a server google-datetime instant to the microsecond the REST renderer
// (transport/rest/workflowexecutions) emits.
func TestExecutionTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	e := executionToProto(workflowsstore.Execution{
		ID: "e1", Location: "us-central1", WorkflowID: "wf1",
		StartTime: instant, EndTime: instant,
	}, "proj")
	if got := e.GetStartTime().AsTime(); !got.Equal(want) {
		t.Fatalf("startTime = %v, want %v", got, want)
	}
	if got := e.GetEndTime().AsTime(); !got.Equal(want) {
		t.Fatalf("endTime = %v, want %v", got, want)
	}
}
