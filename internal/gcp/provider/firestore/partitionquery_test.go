package firestore

import (
	"context"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestPartitionQuery covers documents.partitionQuery over REST: cursor
// partitions over a staged collection's ordered result set.
func TestPartitionQuery(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	// Seed 10 documents in "cities" so the split points are deterministic.
	for i := 0; i < 10; i++ {
		name := "projects/proj/databases/(default)/documents/cities/" + string(rune('a'+i))
		if err := p.store.CreateDocument(ctx, firestorestore.Document{
			Name:   name,
			Fields: map[string]*firestorestore.Value{"n": intField(int64(i))},
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	partition := func(count any) (*map[string]any, error) {
		nr := testNR()
		nr.Params["name"] = "databases/(default)/documents"
		nr.Params["body"] = map[string]any{
			"partitionCount":  count,
			"structuredQuery": map[string]any{"from": []any{map[string]any{"collectionId": "cities"}}},
		}
		resp, err := p.PartitionQuery(ctx, nr)
		if err != nil {
			return nil, err
		}
		return &resp.Data, nil
	}

	// A single partition is the full range: no split points.
	data, err := partition("1")
	if err != nil {
		t.Fatalf("partitionQuery(1): %v", err)
	}
	if parts, ok := (*data)["partitions"].([]any); !ok || len(parts) != 0 {
		t.Fatalf("partitions = %v, want empty for a single partition", (*data)["partitions"])
	}

	// Three partitions over 10 docs → 2 split cursors.
	data, err = partition("3")
	if err != nil {
		t.Fatalf("partitionQuery(3): %v", err)
	}
	parts, _ := (*data)["partitions"].([]any)
	if len(parts) != 2 {
		t.Fatalf("partitions = %v, want 2 split cursors", (*data)["partitions"])
	}
	first, _ := parts[0].(map[string]any)
	vals, _ := first["values"].([]any)
	cur, _ := vals[0].(map[string]any)
	ref, _ := cur["referenceValue"].(string)
	if want := "projects/proj/databases/(default)/documents/cities/" + string(rune('a'+3)); ref != want {
		t.Fatalf("first cursor = %q, want %q", ref, want)
	}

	// The int64 JSON mapping (a quoted number) is accepted too.
	if _, err := partition("3"); err != nil {
		t.Fatalf("partitionQuery(string count): %v", err)
	}
}
