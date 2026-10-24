package workflows

import (
	"testing"
	"time"

	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"

	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// TestWorkflowTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST core renderer
// (service/workflows) emits, so the two transports agree.
func TestWorkflowTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	w := workflowToProto(workflowsstore.Workflow{
		ID:                 "w1",
		Location:           "us-central1",
		CreateTime:         instant,
		UpdateTime:         instant,
		RevisionCreateTime: instant,
	}, "proj")
	for name, got := range map[string]time.Time{
		"createTime":         w.GetCreateTime().AsTime(),
		"updateTime":         w.GetUpdateTime().AsTime(),
		"revisionCreateTime": w.GetRevisionCreateTime().AsTime(),
	} {
		if !got.Equal(want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestOperationTimesMicrosecondTruncation pins the same for the operation
// metadata create/end times.
func TestOperationTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	op, err := operationToProto(workflowsstore.Operation{
		ID: "op1", Location: "us-central1",
		CreateTime: instant, EndTime: instant, Done: true,
	}, "proj", nil)
	if err != nil {
		t.Fatalf("operationToProto: %v", err)
	}
	var md workflowspb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&md); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if got := md.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("metadata.createTime = %v, want %v", got, want)
	}
	if got := md.GetEndTime().AsTime(); !got.Equal(want) {
		t.Fatalf("metadata.endTime = %v, want %v", got, want)
	}
}
