package eventarc

import (
	"testing"
	"time"

	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"

	core "jaiscloud/internal/gcp/service/eventarc"
)

// TestOperationTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates
// a server google-datetime instant to the microsecond the REST core renderer
// (service/eventarc) emits.
func TestOperationTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	s := &Service{}
	op, err := s.operationToProto("proj", core.Operation{
		ID: "op1", Location: "us-central1", CreateTime: instant,
	}, nil)
	if err != nil {
		t.Fatalf("operationToProto: %v", err)
	}
	var md eventarcpb.OperationMetadata
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
