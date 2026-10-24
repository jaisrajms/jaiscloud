package workflows

import (
	"testing"
	"time"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// TestWorkflowJSONTimestampSpelling pins the protojson spelling of the
// Workflow createTime/updateTime/revisionCreateTime fields, which the gRPC
// transport inherits by transcoding this same map.
func TestWorkflowJSONTimestampSpelling(t *testing.T) {
	w := workflowsstore.Workflow{
		ID:                 "wf",
		Location:           "us-central1",
		CreateTime:         time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		UpdateTime:         time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
		RevisionCreateTime: time.Date(2024, 3, 1, 12, 0, 2, 250_000_000, time.UTC),
	}
	out := WorkflowJSON(w, "proj")
	if got := out["createTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("createTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["updateTime"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("updateTime = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
	if got := out["revisionCreateTime"]; got != "2024-03-01T12:00:02.250Z" {
		t.Fatalf("revisionCreateTime = %v, want 2024-03-01T12:00:02.250Z", got)
	}
}
