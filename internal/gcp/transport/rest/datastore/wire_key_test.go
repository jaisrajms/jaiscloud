package datastore

import (
	"testing"

	core "jaiscloud/internal/gcp/service/datastore"
)

func TestKeyWireRoundTripFullPathAndPartition(t *testing.T) {
	wire := map[string]any{
		"partitionId": map[string]any{"projectId": "proj", "namespaceId": "ns", "databaseId": "db"},
		"path": []any{
			map[string]any{"kind": "Parent", "id": "1"},
			map[string]any{"kind": "Child", "name": "c"},
		},
	}
	k, err := keyFromWire(wire)
	if err != nil {
		t.Fatalf("keyFromWire: %v", err)
	}
	if k.Kind != "Child" || !k.HasName || k.Name != "c" || k.Namespace != "ns" || k.Database != "db" {
		t.Fatalf("keyFromWire = %+v", k)
	}
	if len(k.Ancestors) != 1 || k.Ancestors[0].Kind != "Parent" || !k.Ancestors[0].HasID || k.Ancestors[0].ID != 1 {
		t.Fatalf("ancestors = %+v", k.Ancestors)
	}

	back := keyToWire(k, "proj")
	pid, ok := back["partitionId"].(map[string]any)
	if !ok || pid["projectId"] != "proj" || pid["namespaceId"] != "ns" || pid["databaseId"] != "db" {
		t.Fatalf("partition = %+v", back["partitionId"])
	}
	path, ok := back["path"].([]any)
	if !ok || len(path) != 2 {
		t.Fatalf("path = %+v", back["path"])
	}
	p0 := path[0].(map[string]any)
	if p0["kind"] != "Parent" || p0["id"] != "1" {
		t.Fatalf("path[0] = %+v", p0)
	}
	p1 := path[1].(map[string]any)
	if p1["kind"] != "Child" || p1["name"] != "c" {
		t.Fatalf("path[1] = %+v", p1)
	}
}

func TestKeyFromWireRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire map[string]any
	}{
		{"empty kind", map[string]any{"path": []any{map[string]any{"kind": ""}}}},
		{"incomplete ancestor", map[string]any{"path": []any{
			map[string]any{"kind": "Parent"},
			map[string]any{"kind": "Child", "id": "1"},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := keyFromWire(tc.wire); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestPropertyOpHasAncestorWire(t *testing.T) {
	if got := propertyOpFromWire("HAS_ANCESTOR"); got != core.PropertyHasAncestor {
		t.Fatalf("propertyOpFromWire(HAS_ANCESTOR) = %v", got)
	}
}

func TestRequestPartition(t *testing.T) {
	ns, db := requestPartition(map[string]any{
		"databaseId":  "top-db",
		"partitionId": map[string]any{"namespaceId": "ns"},
	})
	if ns != "ns" || db != "top-db" {
		t.Fatalf("requestPartition = %q %q", ns, db)
	}
	ns, db = requestPartition(map[string]any{
		"partitionId": map[string]any{"namespaceId": "ns", "databaseId": "part-db"},
	})
	if ns != "ns" || db != "part-db" {
		t.Fatalf("requestPartition (partition db) = %q %q", ns, db)
	}
}
