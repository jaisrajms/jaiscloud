package datastore

import (
	"encoding/base64"
	"strconv"
	"testing"
)

// TestRESTOutputOnlyFields checks the JSON forms of the output-only fields real
// Datastore returns (AUD6-5): CommitResponse.indexUpdates + MutationResult
// createTime/updateTime, LookupResponse found createTime, and the
// QueryResultBatch readTime/snapshotVersion/endCursor and per-entity cursor.
func TestRESTOutputOnlyFields(t *testing.T) {
	c, p := newTestProvider(t)

	commit, err := call(t, c, p, "test", "commit", map[string]any{
		"mode": "NON_TRANSACTIONAL",
		"mutations": []any{map[string]any{
			"upsert": map[string]any{
				"key":        nameKey("Task", "a"),
				"properties": map[string]any{"Desc": map[string]any{"stringValue": "hi"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	var idx int
	switch v := commit.Data["indexUpdates"].(type) {
	case int:
		idx = v
	case int32:
		idx = int(v)
	default:
		t.Fatalf("indexUpdates = %T (%v), want a positive number", v, v)
	}
	if idx <= 0 {
		t.Fatalf("indexUpdates = %d, want > 0", idx)
	}
	mrs, _ := commit.Data["mutationResults"].([]any)
	if len(mrs) != 1 {
		t.Fatalf("mutationResults = %v", commit.Data["mutationResults"])
	}
	mr, _ := mrs[0].(map[string]any)
	if _, ok := mr["createTime"]; !ok {
		t.Fatalf("mutationResult missing createTime: %v", mr)
	}

	lookup, err := call(t, c, p, "test", "lookup", map[string]any{
		"keys": []any{nameKey("Task", "a")},
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	found, _ := lookup.Data["found"].([]any)
	found0, _ := found[0].(map[string]any)
	if _, ok := found0["createTime"]; !ok {
		t.Fatalf("found[0] missing createTime: %v", found0)
	}

	query, err := call(t, c, p, "test", "runQuery", map[string]any{
		"query": map[string]any{"kind": []any{map[string]any{"name": "Task"}}},
	})
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	batch, _ := query.Data["batch"].(map[string]any)
	if _, ok := batch["readTime"]; !ok {
		t.Fatalf("batch missing readTime: %v", batch)
	}
	if _, ok := batch["snapshotVersion"].(string); !ok {
		t.Fatalf("batch snapshotVersion = %T, want string", batch["snapshotVersion"])
	}
	if _, err := base64.StdEncoding.DecodeString(batch["endCursor"].(string)); err != nil {
		t.Fatalf("batch endCursor is not base64: %v", err)
	}
	ers, _ := batch["entityResults"].([]any)
	er0, _ := ers[0].(map[string]any)
	if _, ok := er0["createTime"]; !ok {
		t.Fatalf("entityResults[0] missing createTime: %v", er0)
	}
	if _, err := base64.StdEncoding.DecodeString(er0["cursor"].(string)); err != nil {
		t.Fatalf("entityResults[0] cursor is not base64: %v", err)
	}
	if _, err := strconv.Atoi(batch["snapshotVersion"].(string)); err != nil {
		t.Fatalf("snapshotVersion not numeric: %v", err)
	}
}
