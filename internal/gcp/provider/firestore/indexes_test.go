package firestore

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func TestIndexPath(t *testing.T) {
	cases := []struct {
		name string
		want struct {
			db, cg, id string
			ok         bool
		}
	}{
		{
			name: "databases/(default)/collectionGroups/cities/indexes",
			want: struct {
				db, cg, id string
				ok         bool
			}{"(default)", "cities", "", true},
		},
		{
			name: "databases/(default)/collectionGroups/cities/indexes/abc123",
			want: struct {
				db, cg, id string
				ok         bool
			}{"(default)", "cities", "abc123", true},
		},
		{name: "databases/(default)/collectionGroups/cities", want: struct {
			db, cg, id string
			ok         bool
		}{"", "", "", false}},
	}
	for _, tc := range cases {
		db, cg, id, ok := indexPath(tc.name)
		if ok != tc.want.ok || db != tc.want.db || cg != tc.want.cg || id != tc.want.id {
			t.Errorf("indexPath(%q) = (%q,%q,%q,%v), want (%q,%q,%q,%v)",
				tc.name, db, cg, id, ok, tc.want.db, tc.want.cg, tc.want.id, tc.want.ok)
		}
	}
}

func TestIndexMapState(t *testing.T) {
	m := indexMap(indexDef{Name: "n", QueryScope: "COLLECTION", Fields: []indexField{{FieldPath: "a", Order: "ASCENDING"}}})
	if m["state"] != "READY" {
		t.Errorf("indexMap state = %v, want READY", m["state"])
	}
}

func TestCreateIndexReturnsOperation(t *testing.T) {
	ctx := context.Background()
	p := New(nil, store.NewMemoryResourceStore())

	nr := &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{},
		ResourceID: func(rt, n string) string { return "projects/proj/" + n },
	}
	nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"
	nr.Params["body"] = map[string]any{
		"queryScope": "COLLECTION",
		"fields": []any{
			map[string]any{"fieldPath": "a", "order": "ASCENDING"},
			map[string]any{"fieldPath": "b", "order": "ASCENDING"},
		},
	}

	resp, err := p.CreateIndex(ctx, nr)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("CreateIndex should return done=true, got %v", resp.Data["done"])
	}
	name, _ := resp.Data["name"].(string)
	if want := "projects/proj/databases/(default)/operations/"; len(name) < len(want) || name[:len(want)] != want {
		t.Errorf("CreateIndex operation name = %q, want prefix %q", name, want)
	}
	respIdx, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("CreateIndex response should contain the created index, got %v", resp.Data["response"])
	}
	if respIdx["state"] != "READY" {
		t.Errorf("created index state = %v, want READY", respIdx["state"])
	}
}

func TestListIndexesPaginationAndFilter(t *testing.T) {
	ctx := context.Background()
	p := New(nil, store.NewMemoryResourceStore())

	// Seed three indexes.
	for i, fp := range []string{"a", "b", "c"} {
		nr := &model.NormalizedRequest{
			AccountID:  "proj",
			Params:     map[string]any{},
			ResourceID: func(rt, n string) string { return "projects/proj/" + n },
		}
		nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"
		nr.Params["body"] = map[string]any{
			"queryScope": "COLLECTION",
			"fields": []any{
				map[string]any{"fieldPath": fp, "order": "ASCENDING"},
				map[string]any{"fieldPath": "x", "order": "ASCENDING"},
			},
		}
		if _, err := p.CreateIndex(ctx, nr); err != nil {
			t.Fatalf("seed index %d: %v", i, err)
		}
	}

	nr := &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{"pageSize": "2"},
		ResourceID: func(rt, n string) string { return "projects/proj/" + n },
	}
	nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"

	resp, err := p.ListIndexes(ctx, nr)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	indexes, _ := resp.Data["indexes"].([]any)
	if len(indexes) != 2 {
		t.Errorf("expected 2 indexes (pageSize=2), got %d", len(indexes))
	}
	if resp.Data["nextPageToken"] == "" {
		t.Errorf("expected nextPageToken with 3 indexes and pageSize=2")
	}

	// Filter: substring match on fieldPath.
	nr.Params["filter"] = "c"
	resp, err = p.ListIndexes(ctx, nr)
	if err != nil {
		t.Fatalf("ListIndexes with filter: %v", err)
	}
	indexes, _ = resp.Data["indexes"].([]any)
	if len(indexes) != 1 {
		t.Errorf("expected 1 index matching filter 'c', got %d", len(indexes))
	}
}
