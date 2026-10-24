package datastore

import (
	"context"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// seedCursorTasks stores four Task entities named a..d in the default
// partition. Single-character names keep the canonical key order equal to the
// name order regardless of the key encoding's length prefixes.
func seedCursorTasks(t *testing.T, s *Service) {
	t.Helper()
	for i, name := range []string{"a", "b", "c", "d"} {
		upsert(t, s, "p", nameKey("Task", name), intProp(int64(i+1)))
	}
}

func queryNames(t *testing.T, s *Service, ctx context.Context, q *Query) *QueryResult {
	t.Helper()
	res, err := s.RunQuery(ctx, "p", q, nil)
	if err != nil {
		t.Fatalf("RunQuery(%+v): %v", q, err)
	}
	return res
}

func resultNames(res *QueryResult) []string {
	names := make([]string, 0, len(res.Entities))
	for _, e := range res.Entities {
		k, ok := KeyFromCanonical(e.Entity.Key)
		if !ok {
			panic("unparseable canonical key " + e.Entity.Key)
		}
		names = append(names, k.Name)
	}
	return names
}

func equalNames(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestRunQueryStartCursorPages verifies that a batch's endCursor fed back as a
// request startCursor resumes at the next entity, so a client paging on
// MORE_RESULTS_AFTER_LIMIT advances instead of re-reading page one.
func TestRunQueryStartCursorPages(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	seedCursorTasks(t, s)

	limit := 1
	var cursor []byte
	var pages [][]string
	for i := 0; i < 4; i++ {
		res := queryNames(t, s, ctx, &Query{Kind: "Task", Limit: &limit, StartCursor: cursor})
		if len(res.Entities) != 1 {
			t.Fatalf("page %d returned %d entities, want 1", i+1, len(res.Entities))
		}
		pages = append(pages, resultNames(res))
		if i < 3 {
			if res.MoreResults != MoreResultsAfterLimit {
				t.Fatalf("page %d MoreResults = %v, want MORE_RESULTS_AFTER_LIMIT", i+1, res.MoreResults)
			}
			if len(res.EndCursor) == 0 {
				t.Fatalf("page %d has no endCursor", i+1)
			}
			cursor = res.EndCursor
		} else if res.MoreResults != MoreResultsNoMoreResults {
			t.Fatalf("last page MoreResults = %v, want NO_MORE_RESULTS", res.MoreResults)
		}
	}

	want := [][]string{{"a"}, {"b"}, {"c"}, {"d"}}
	for i := range want {
		if !equalNames(pages[i], want[i]) {
			t.Fatalf("page %d = %v, want %v", i+1, pages[i], want[i])
		}
	}
}

// TestRunQueryCursorComposesWithOffset locks the documented stage order
// (order + cursor, then offset): the cursor bounds the stream and the offset
// skips within it.
func TestRunQueryCursorComposesWithOffset(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	seedCursorTasks(t, s)

	first := queryNames(t, s, ctx, &Query{Kind: "Task", Offset: 0})
	// cursor(a) resumes strictly after "a", so offset 1 then skips "b".
	res := queryNames(t, s, ctx, &Query{Kind: "Task", StartCursor: entityCursor(first.Entities[0].Entity), Offset: 1})
	if got := resultNames(res); !equalNames(got, []string{"c", "d"}) {
		t.Fatalf("cursor+offset names = %v, want [c d]", got)
	}
	if res.Skipped != 1 {
		t.Fatalf("Skipped = %d, want 1", res.Skipped)
	}
	if string(res.SkippedCursor) != string(first.Entities[1].Cursor) {
		t.Fatalf("SkippedCursor = %q, want cursor(b)", res.SkippedCursor)
	}
}

// TestRunQueryEndCursorBoundsRange verifies the request endCursor stops at the
// entity it names (inclusive), reproducing the page the cursor came from.
func TestRunQueryEndCursorBoundsRange(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	seedCursorTasks(t, s)

	// Page one with limit 2 ends at "b"; its endCursor must reproduce [a b].
	limit := 2
	page1 := queryNames(t, s, ctx, &Query{Kind: "Task", Limit: &limit})
	if got := resultNames(page1); !equalNames(got, []string{"a", "b"}) {
		t.Fatalf("page1 names = %v, want [a b]", got)
	}
	res := queryNames(t, s, ctx, &Query{Kind: "Task", EndCursor: page1.EndCursor})
	if got := resultNames(res); !equalNames(got, []string{"a", "b"}) {
		t.Fatalf("endCursor range names = %v, want [a b]", got)
	}
	if res.MoreResults != MoreResultsAfterCursor {
		t.Fatalf("endCursor MoreResults = %v, want MORE_RESULTS_AFTER_CURSOR", res.MoreResults)
	}

	// A start+end pair returns the bounded interior only.
	res = queryNames(t, s, ctx, &Query{Kind: "Task", StartCursor: entityCursor(page1.Entities[0].Entity), EndCursor: page1.EndCursor})
	if got := resultNames(res); !equalNames(got, []string{"b"}) {
		t.Fatalf("start+end range names = %v, want [b]", got)
	}

	// start == end is an empty interval, not a one-element page.
	res = queryNames(t, s, ctx, &Query{Kind: "Task", StartCursor: page1.Entities[0].Cursor, EndCursor: page1.Entities[0].Cursor})
	if len(res.Entities) != 0 {
		t.Fatalf("start==end returned %v, want empty", resultNames(res))
	}

	// An end cursor naming the last result still reports AFTER_CURSOR (the
	// cursor, not the data set, ended the scan) — real GCP's behavior in the
	// AUD6-6 differential golden.
	full := queryNames(t, s, ctx, &Query{Kind: "Task"})
	res = queryNames(t, s, ctx, &Query{Kind: "Task", EndCursor: full.EndCursor})
	if got := resultNames(res); !equalNames(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("last endCursor names = %v, want [a b c d]", got)
	}
	if res.MoreResults != MoreResultsAfterCursor {
		t.Fatalf("last endCursor MoreResults = %v, want MORE_RESULTS_AFTER_CURSOR", res.MoreResults)
	}
}

// TestRunQueryUnknownCursorRejected verifies an unusable cursor fails closed
// with InvalidArgument instead of silently returning the full result set.
func TestRunQueryUnknownCursorRejected(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	seedCursorTasks(t, s)

	if _, err := s.RunQuery(ctx, "p", &Query{Kind: "Task", StartCursor: []byte("not-a-cursor")}, nil); err == nil {
		t.Fatal("unknown start cursor should be rejected")
	}
	if _, err := s.RunQuery(ctx, "p", &Query{Kind: "Task", EndCursor: []byte("not-a-cursor")}, nil); err == nil {
		t.Fatal("unknown end cursor should be rejected")
	}
}
