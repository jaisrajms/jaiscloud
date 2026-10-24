package datastore

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestRunQueryCursorPaging verifies the gRPC transport consumes a request
// start_cursor/end_cursor (AUD6-6): a batch's end_cursor fed back as the next
// request's start_cursor advances the page, and an end_cursor bounds the range.
func TestRunQueryCursorPaging(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{"Desc": strVal(name)})},
			}},
		}); err != nil {
			t.Fatalf("upsert %s: %v", name, err)
		}
	}
	runQuery := func(q *datastorepb.Query) *datastorepb.QueryResultBatch {
		t.Helper()
		resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
			ProjectId: "test",
			QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
		})
		if err != nil {
			t.Fatalf("run query: %v", err)
		}
		return resp.GetBatch()
	}
	names := func(b *datastorepb.QueryResultBatch) []string {
		out := make([]string, 0, len(b.GetEntityResults()))
		for _, er := range b.GetEntityResults() {
			out = append(out, er.GetEntity().GetKey().GetPath()[0].GetName())
		}
		return out
	}

	limit := int32(1)
	page1 := runQuery(&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Task"}}, Limit: wrapperspb.Int32(limit)})
	if got := names(page1); len(got) != 1 || got[0] != "a" {
		t.Fatalf("page1 names = %v, want [a]", got)
	}
	if page1.GetMoreResults() != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
		t.Fatalf("page1 more_results = %v, want MORE_RESULTS_AFTER_LIMIT", page1.GetMoreResults())
	}

	page2 := runQuery(&datastorepb.Query{
		Kind:        []*datastorepb.KindExpression{{Name: "Task"}},
		StartCursor: page1.GetEndCursor(),
	})
	if got := names(page2); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("page2 names = %v, want [b c]", got)
	}
	if page2.GetMoreResults() != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
		t.Fatalf("page2 more_results = %v, want NO_MORE_RESULTS", page2.GetMoreResults())
	}

	// An end_cursor at page1's boundary reproduces page1's range and reports
	// MORE_RESULTS_AFTER_CURSOR (the cursor, not the data set, ended the batch).
	bounded := runQuery(&datastorepb.Query{
		Kind:      []*datastorepb.KindExpression{{Name: "Task"}},
		EndCursor: page1.GetEndCursor(),
	})
	if got := names(bounded); len(got) != 1 || got[0] != "a" {
		t.Fatalf("endCursor range names = %v, want [a]", got)
	}
	if bounded.GetMoreResults() != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_CURSOR {
		t.Fatalf("endCursor more_results = %v, want MORE_RESULTS_AFTER_CURSOR", bounded.GetMoreResults())
	}
}
