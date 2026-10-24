package metastore

import (
	"testing"
	"time"

	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"

	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// TestOperationTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates
// a server google-datetime instant to the microsecond the REST core renderer
// (service/metastore) emits.
func TestOperationTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	op, err := operationToProto(metastorestore.Operation{
		ID: "op1", Location: "us-central1",
		CreateTime: instant, EndTime: instant, Done: true,
	}, "proj", nil)
	if err != nil {
		t.Fatalf("operationToProto: %v", err)
	}
	var md metastorepb.OperationMetadata
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
