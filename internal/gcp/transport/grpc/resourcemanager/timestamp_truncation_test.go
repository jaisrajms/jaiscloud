package resourcemanager

import (
	"testing"
	"time"

	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"

	core "jaiscloud/internal/gcp/service/resourcemanager"
)

// TestProjectTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST renderer
// (transport/rest/resourcemanager) emits.
func TestProjectTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	p := projectToProto(core.Project{
		ProjectID:  "p1",
		CreateTime: instant,
		UpdateTime: instant,
		DeleteTime: instant,
	})
	for name, got := range map[string]time.Time{
		"createTime": p.GetCreateTime().AsTime(),
		"updateTime": p.GetUpdateTime().AsTime(),
		"deleteTime": p.GetDeleteTime().AsTime(),
	} {
		if !got.Equal(want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestCreateOperationMetadataTimeMicrosecondTruncation pins the same for the
// create-operation typed metadata createTime.
func TestCreateOperationMetadataTimeMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	op, err := operationToProto(core.Operation{
		Name: "operations/op1", Verb: "create", Done: true,
		CreateTime: instant, Project: core.Project{ProjectID: "p1"},
	})
	if err != nil {
		t.Fatalf("operationToProto: %v", err)
	}
	var md resourcemanagerpb.CreateProjectMetadata
	if err := op.GetMetadata().UnmarshalTo(&md); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if got := md.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("metadata.createTime = %v, want %v", got, want)
	}
}
