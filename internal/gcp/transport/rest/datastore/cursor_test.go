package datastore

import (
	"testing"
)

// TestRESTRunQueryCursorPaging verifies the REST transport parses a request
// query.startCursor/endCursor (AUD6-6): the batch endCursor fed back as the next
// request's startCursor advances the page, and an endCursor bounds the range.
func TestRESTRunQueryCursorPaging(t *testing.T) {
	c, p := newTestProvider(t)

	for _, name := range []string{"a", "b", "c"} {
		if _, err := call(t, c, p, "test", "commit", map[string]any{
			"mode": "NON_TRANSACTIONAL",
			"mutations": []any{map[string]any{
				"upsert": map[string]any{
					"key":        nameKey("Task", name),
					"properties": map[string]any{"Desc": map[string]any{"stringValue": name}},
				},
			}},
		}); err != nil {
			t.Fatalf("commit %s: %v", name, err)
		}
	}
	runQuery := func(q map[string]any) map[string]any {
		t.Helper()
		resp, err := call(t, c, p, "test", "runQuery", map[string]any{"query": q})
		if err != nil {
			t.Fatalf("runQuery %+v: %v", q, err)
		}
		batch, _ := resp.Data["batch"].(map[string]any)
		return batch
	}
	names := func(batch map[string]any) []string {
		ers, _ := batch["entityResults"].([]any)
		out := make([]string, 0, len(ers))
		for _, raw := range ers {
			er, _ := raw.(map[string]any)
			ent, _ := er["entity"].(map[string]any)
			key, _ := ent["key"].(map[string]any)
			path, _ := key["path"].([]any)
			elem, _ := path[0].(map[string]any)
			out = append(out, elem["name"].(string))
		}
		return out
	}

	kind := []any{map[string]any{"name": "Task"}}
	page1 := runQuery(map[string]any{"kind": kind, "limit": 1})
	if got := names(page1); len(got) != 1 || got[0] != "a" {
		t.Fatalf("page1 names = %v, want [a]", got)
	}
	if page1["moreResults"] != "MORE_RESULTS_AFTER_LIMIT" {
		t.Fatalf("page1 moreResults = %v, want MORE_RESULTS_AFTER_LIMIT", page1["moreResults"])
	}
	endCursor, ok := page1["endCursor"].(string)
	if !ok || endCursor == "" {
		t.Fatalf("page1 endCursor = %v, want a base64 string", page1["endCursor"])
	}

	page2 := runQuery(map[string]any{"kind": kind, "startCursor": endCursor})
	if got := names(page2); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("page2 names = %v, want [b c]", got)
	}
	if page2["moreResults"] != "NO_MORE_RESULTS" {
		t.Fatalf("page2 moreResults = %v, want NO_MORE_RESULTS", page2["moreResults"])
	}

	bounded := runQuery(map[string]any{"kind": kind, "endCursor": endCursor})
	if got := names(bounded); len(got) != 1 || got[0] != "a" {
		t.Fatalf("endCursor range names = %v, want [a]", got)
	}
	if bounded["moreResults"] != "MORE_RESULTS_AFTER_CURSOR" {
		t.Fatalf("endCursor moreResults = %v, want MORE_RESULTS_AFTER_CURSOR", bounded["moreResults"])
	}
}
