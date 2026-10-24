package dataproc

import (
	"testing"
	"time"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// TestJobStatusStateStartTimeMicrosecondTruncation pins TD48: the gRPC adapter
// truncates a server google-datetime instant to the microsecond the REST core
// renderer (service/dataproc) emits.
func TestJobStatusStateStartTimeMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	out := jobStatusToProto(dpstore.JobStatus{StateStartTime: instant})
	if got := out.GetStateStartTime().AsTime(); !got.Equal(want) {
		t.Fatalf("stateStartTime = %v, want %v", got, want)
	}
}
