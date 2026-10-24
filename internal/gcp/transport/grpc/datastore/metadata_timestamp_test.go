package datastore

import (
	"context"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"jaiscloud/internal/clock"
)

// TestMetadataTimestampsMicrosecondPrecision pins TD45 on the gRPC side: every
// response-level Datastore timestamp (read_time, commit_time, and the entity
// create_time/update_time) is a proto Timestamp real GCP holds at microsecond
// resolution, so the transport must not carry the sub-microsecond digits
// clock.Now() produces. REST renders the same truncation through the one
// canonical speller (the REST metadata spelling test).
func TestMetadataTimestampsMicrosecondPrecision(t *testing.T) {
	clock.SetGlobalClock(clock.FixedClock{T: time.Date(2026, 1, 1, 12, 0, 0, 500000987, time.UTC)})
	defer clock.SetGlobalClock(clock.RealClock{})

	want := time.Date(2026, 1, 1, 12, 0, 0, 500000000, time.UTC)
	ctx := context.Background()
	client, cleanup := testServer(t)
	defer cleanup()

	key := nameKey("Task", "a")
	commit, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	mr := commit.GetMutationResults()[0]
	for _, tc := range []struct {
		name string
		got  time.Time
	}{
		{"mutation_result create_time", mr.GetCreateTime().AsTime()},
		{"mutation_result update_time", mr.GetUpdateTime().AsTime()},
	} {
		if !tc.got.Equal(want) {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, want)
		}
	}

	look, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{key}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got := look.GetReadTime().AsTime(); !got.Equal(want) {
		t.Errorf("lookup read_time = %v, want %v", got, want)
	}
	if got := look.GetFound()[0].GetCreateTime().AsTime(); !got.Equal(want) {
		t.Errorf("found create_time = %v, want %v", got, want)
	}
	if got := look.GetFound()[0].GetUpdateTime().AsTime(); !got.Equal(want) {
		t.Errorf("found update_time = %v, want %v", got, want)
	}

	query, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		}},
	})
	if err != nil {
		t.Fatalf("run query: %v", err)
	}
	if got := query.GetBatch().GetReadTime().AsTime(); !got.Equal(want) {
		t.Errorf("batch read_time = %v, want %v", got, want)
	}

	agg, err := client.RunAggregationQuery(ctx, aggRequest("Task", nil, countAgg("count")))
	if err != nil {
		t.Fatalf("run aggregation query: %v", err)
	}
	if got := agg.GetBatch().GetReadTime().AsTime(); !got.Equal(want) {
		t.Errorf("aggregation batch read_time = %v, want %v", got, want)
	}

	txn := beginTxn(t, client)
	txnResp, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"Desc": strVal("bye")})},
	})
	if err != nil {
		t.Fatalf("transactional commit: %v", err)
	}
	if got := txnResp.GetCommitTime().AsTime(); !got.Equal(want) {
		t.Errorf("commit_time = %v, want %v", got, want)
	}
}
