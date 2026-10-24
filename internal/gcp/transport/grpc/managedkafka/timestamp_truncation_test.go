package managedkafka

import (
	"testing"
	"time"

	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// TestClusterTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST core renderer
// (service/managedkafka) emits.
func TestClusterTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	c := clusterToProto(mkstore.Cluster{
		Name: "c1", Location: "us-central1",
		CreateTime: instant, UpdateTime: instant,
	}, "proj")
	if got := c.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("createTime = %v, want %v", got, want)
	}
	if got := c.GetUpdateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("updateTime = %v, want %v", got, want)
	}
}

// TestOperationTimesMicrosecondTruncation pins the same for the operation
// metadata create/end times.
func TestOperationTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	op, err := operationToProto(mkstore.Operation{
		ID: "op1", Location: "us-central1",
		CreateTime: instant, EndTime: instant, Done: true,
	}, "proj", nil)
	if err != nil {
		t.Fatalf("operationToProto: %v", err)
	}
	var md managedkafkapb.OperationMetadata
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
