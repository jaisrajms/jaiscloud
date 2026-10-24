package functions

import (
	"testing"
	"time"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"

	core "jaiscloud/internal/gcp/service/functions"
)

// TestOperationMetadataTimesMicrosecondTruncation pins TD48: the gRPC operation
// metadata reports the stored operation create/end instants (the same ones the
// REST core renderer reports), truncated to the microsecond.
func TestOperationMetadataTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	op := core.Operation{ID: "op1", Verb: "create", CreateTime: instant, EndTime: instant, Done: true}

	v1, err := operationToProtoV1("proj", op)
	if err != nil {
		t.Fatalf("operationToProtoV1: %v", err)
	}
	var md1 functionspb.OperationMetadataV1
	if err := v1.GetMetadata().UnmarshalTo(&md1); err != nil {
		t.Fatalf("unmarshal v1 metadata: %v", err)
	}
	if got := md1.GetUpdateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("v1 metadata.updateTime = %v, want %v", got, want)
	}

	v2, err := operationToProtoV2("proj", op)
	if err != nil {
		t.Fatalf("operationToProtoV2: %v", err)
	}
	var md2 apiv2functionspb.OperationMetadata
	if err := v2.GetMetadata().UnmarshalTo(&md2); err != nil {
		t.Fatalf("unmarshal v2 metadata: %v", err)
	}
	if got := md2.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("v2 metadata.createTime = %v, want %v", got, want)
	}
	if got := md2.GetEndTime().AsTime(); !got.Equal(want) {
		t.Fatalf("v2 metadata.endTime = %v, want %v", got, want)
	}
}
