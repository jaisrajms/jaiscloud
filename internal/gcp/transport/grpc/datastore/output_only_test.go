package datastore

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// TestCommitOutputOnlyFields checks the output-only CommitResponse fields real
// Datastore returns (AUD6-5): the per-mutation create_time/update_time and the
// commit-wide index_updates count.
func TestCommitOutputOnlyFields(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	key := nameKey("Task", "a")
	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if resp.GetIndexUpdates() <= 0 {
		t.Fatalf("index_updates = %d, want > 0", resp.GetIndexUpdates())
	}
	mr := resp.GetMutationResults()[0]
	if mr.GetCreateTime() == nil {
		t.Fatal("mutation_result.create_time must be set")
	}
	if mr.GetUpdateTime() == nil {
		t.Fatal("mutation_result.update_time must be set")
	}
	created := mr.GetCreateTime().AsTime()

	// An update preserves create_time while advancing update_time.
	upd, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"Desc": strVal("bye")})},
		}},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	umr := upd.GetMutationResults()[0]
	if umr.GetCreateTime() == nil || !umr.GetCreateTime().AsTime().Equal(created) {
		t.Fatalf("update create_time = %v, want preserved %v", umr.GetCreateTime(), created)
	}
}

// TestCommitDeleteOmitsCreateTime checks that a delete's MutationResult carries
// no create_time/update_time, matching real Datastore.
func TestCommitDeleteOmitsCreateTime(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	key := nameKey("Task", "a")
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Delete{Delete: key},
		}},
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	mr := resp.GetMutationResults()[0]
	if mr.GetCreateTime() != nil {
		t.Fatalf("delete create_time = %v, want unset", mr.GetCreateTime())
	}
	if mr.GetUpdateTime() != nil {
		t.Fatalf("delete update_time = %v, want unset", mr.GetUpdateTime())
	}
}

// TestLookupCreateTime checks that a found EntityResult carries create_time and
// a missing one does not (real Datastore's EntityResult contract).
func TestLookupCreateTime(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	key := nameKey("Task", "a")
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	look, err := client.Lookup(ctx, &datastorepb.LookupRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{key, nameKey("Task", "missing")},
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	found := look.GetFound()[0]
	if found.GetCreateTime() == nil {
		t.Fatal("found create_time must be set")
	}
	if found.GetUpdateTime() == nil {
		t.Fatal("found update_time must be set")
	}
	if got := look.GetMissing()[0]; got.GetCreateTime() != nil {
		t.Fatalf("missing create_time = %v, want unset", got.GetCreateTime())
	}
}

// TestRunQueryOutputOnlyFields checks the QueryResultBatch output-only fields
// real Datastore returns (AUD6-5): read_time, snapshot_version, end_cursor and
// the per-entity cursor.
func TestRunQueryOutputOnlyFields(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		}},
	})
	if err != nil {
		t.Fatalf("run query: %v", err)
	}
	batch := resp.GetBatch()
	if batch.GetReadTime() == nil {
		t.Fatal("batch read_time must be set")
	}
	if batch.GetSnapshotVersion() == 0 {
		t.Fatal("batch snapshot_version must be set")
	}
	if len(batch.GetEndCursor()) == 0 {
		t.Fatal("batch end_cursor must be set")
	}
	ers := batch.GetEntityResults()
	if len(ers) != 1 {
		t.Fatalf("entity_results = %d, want 1", len(ers))
	}
	if len(ers[0].GetCursor()) == 0 {
		t.Fatal("entity_result cursor must be set")
	}
	if ers[0].GetCreateTime() == nil {
		t.Fatal("entity_result create_time must be set")
	}
}

// TestRunQuerySkippedCursor checks that an offset query sets skipped_results and
// the matching skipped_cursor (real Datastore's QueryResultBatch contract).
func TestRunQuerySkippedCursor(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	for _, name := range []string{"a", "b"} {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{"Desc": strVal(name)})},
			}},
		}); err != nil {
			t.Fatalf("upsert %s: %v", name, err)
		}
	}
	offset := int32(1)
	resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind:   []*datastorepb.KindExpression{{Name: "Task"}},
			Offset: offset,
		}},
	})
	if err != nil {
		t.Fatalf("run query: %v", err)
	}
	batch := resp.GetBatch()
	if batch.GetSkippedResults() != 1 {
		t.Fatalf("skipped_results = %d, want 1", batch.GetSkippedResults())
	}
	if len(batch.GetSkippedCursor()) == 0 {
		t.Fatal("skipped_cursor must be set when skipped_results != 0")
	}
}

// TestCommitConflictCarriesCreateTime checks that a conflict-detected mutation
// still reports the current entity's create_time (real Datastore reports the
// entity's current state for a rejected mutation).
func TestCommitConflictCarriesCreateTime(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	key := nameKey("Task", "a")
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	stale := int64(999)
	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation:                 &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("bye")})},
			ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: stale},
		}},
	})
	if err != nil {
		t.Fatalf("conflicting upsert: %v", err)
	}
	mr := resp.GetMutationResults()[0]
	if !mr.GetConflictDetected() {
		t.Fatal("conflict_detected = false, want true")
	}
	if mr.GetCreateTime() == nil {
		t.Fatal("conflicting mutation must report the current entity's create_time")
	}
}
