package datastore

import (
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	core "jaiscloud/internal/gcp/service/datastore"
)

func TestKeyProtoRoundTripFullPathAndPartition(t *testing.T) {
	k := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "proj", NamespaceId: "ns", DatabaseId: "db"},
		Path: []*datastorepb.Key_PathElement{
			{Kind: "Parent", IdType: &datastorepb.Key_PathElement_Id{Id: 1}},
			{Kind: "Child", IdType: &datastorepb.Key_PathElement_Name{Name: "c"}},
		},
	}
	neutral, err := keyFromProto(k)
	if err != nil {
		t.Fatalf("keyFromProto: %v", err)
	}
	if neutral.Kind != "Child" || !neutral.HasName || neutral.Name != "c" || neutral.Namespace != "ns" || neutral.Database != "db" {
		t.Fatalf("keyFromProto = %+v", neutral)
	}
	if len(neutral.Ancestors) != 1 || neutral.Ancestors[0].Kind != "Parent" || neutral.Ancestors[0].ID != 1 {
		t.Fatalf("ancestors = %+v", neutral.Ancestors)
	}

	back := keyToProto(neutral, "proj")
	pid := back.GetPartitionId()
	if pid.GetProjectId() != "proj" || pid.GetNamespaceId() != "ns" || pid.GetDatabaseId() != "db" {
		t.Fatalf("partition = %+v", pid)
	}
	path := back.GetPath()
	if len(path) != 2 || path[0].GetKind() != "Parent" || path[0].GetId() != 1 || path[1].GetKind() != "Child" || path[1].GetName() != "c" {
		t.Fatalf("path = %+v", path)
	}
}

func TestKeyFromProtoRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		k    *datastorepb.Key
	}{
		{"empty kind", &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: ""}}}},
		{"incomplete ancestor", &datastorepb.Key{Path: []*datastorepb.Key_PathElement{
			{Kind: "Parent"},
			{Kind: "Child", IdType: &datastorepb.Key_PathElement_Id{Id: 1}},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := keyFromProto(tc.k); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestPropertyOpHasAncestorProto(t *testing.T) {
	if got := propertyOpFromProto(datastorepb.PropertyFilter_HAS_ANCESTOR); got != core.PropertyHasAncestor {
		t.Fatalf("propertyOpFromProto(HAS_ANCESTOR) = %v", got)
	}
}
