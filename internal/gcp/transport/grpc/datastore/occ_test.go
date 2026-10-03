package datastore

import (
	"context"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"jaiscloud/internal/clock"
)

// TestLookupReturnsRealVersion verifies EntityResult.Version reflects the
// entity's actual write history (1 after insert, 2 after update) instead of
// being hardcoded to 1 — the field a real client reads to determine the
// base_version for a subsequent conditional write.
func TestLookupReturnsRealVersion(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	key := nameKey("Task", "a")

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(key, map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	lookup := func() int64 {
		t.Helper()
		resp, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{key}})
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if len(resp.GetFound()) != 1 {
			t.Fatalf("expected 1 found entity, got %d", len(resp.GetFound()))
		}
		return resp.GetFound()[0].GetVersion()
	}

	if v := lookup(); v != 1 {
		t.Fatalf("version after insert = %d, want 1", v)
	}

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"n": intVal(2)})},
		}},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if v := lookup(); v != 2 {
		t.Fatalf("version after update = %d, want 2", v)
	}
}

// TestCommitBaseVersionConflictRejected is the core regression test: a
// Mutation carrying base_version (real Datastore's per-mutation
// optimistic-concurrency precondition) that no longer matches the entity's
// current version must be rejected (ConflictDetected=true, not applied) —
// not silently applied unconditionally, which was the bug (the proto field
// was decoded and never read).
func TestCommitBaseVersionConflictRejected(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	key := nameKey("Task", "a")

	insertResp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(key, map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	insertedVersion := insertResp.GetMutationResults()[0].GetVersion()
	if insertedVersion != 1 {
		t.Fatalf("inserted version = %d, want 1", insertedVersion)
	}

	// A concurrent writer updates the entity, advancing its version.
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"n": intVal(2)})},
		}},
	}); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	// A stale writer, still holding the version from the original insert,
	// attempts a conditional update. This must be rejected, not silently
	// applied on top of the newer state.
	staleResp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation:                 &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"n": intVal(999)})},
			ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: insertedVersion},
		}},
	})
	if err != nil {
		t.Fatalf("stale commit should not error at the RPC level, got: %v", err)
	}
	results := staleResp.GetMutationResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].GetConflictDetected() {
		t.Fatal("expected ConflictDetected=true for a stale base_version, got false")
	}

	// The stale write must NOT have applied: n must still be 2, not 999.
	lookupResp, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{key}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	got := lookupResp.GetFound()[0].GetEntity().GetProperties()["n"].GetIntegerValue()
	if got != 2 {
		t.Fatalf("n = %d after a rejected conditional write, want 2 (the stale write must not have applied)", got)
	}
}

// TestCommitBaseVersionMatchSucceeds is the positive case: a Mutation whose
// base_version DOES match the entity's current version must apply normally.
func TestCommitBaseVersionMatchSucceeds(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	key := nameKey("Task", "a")

	insertResp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(key, map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	v := insertResp.GetMutationResults()[0].GetVersion()

	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation:                 &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"n": intVal(2)})},
			ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: v},
		}},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.GetMutationResults()[0].GetConflictDetected() {
		t.Fatal("expected no conflict for a matching base_version")
	}
	if got := resp.GetMutationResults()[0].GetVersion(); got != v+1 {
		t.Fatalf("version after update = %d, want %d", got, v+1)
	}
}

// TestCommitPartialConflictDoesNotAbortOtherMutations verifies real
// Datastore's per-mutation (not per-Commit) conflict semantics: a
// conflicting mutation in a batch does not prevent OTHER, non-conflicting
// mutations in the same Commit call from applying.
func TestCommitPartialConflictDoesNotAbortOtherMutations(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	keyA := nameKey("Task", "a")
	keyB := nameKey("Task", "b")

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{
			{Operation: &datastorepb.Mutation_Insert{Insert: entity(keyA, map[string]*datastorepb.Value{"n": intVal(1)})}},
		},
	}); err != nil {
		t.Fatalf("seed a: %v", err)
	}

	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{
			// Conflicting: wrong base_version for an existing entity.
			{
				Operation:                 &datastorepb.Mutation_Update{Update: entity(keyA, map[string]*datastorepb.Value{"n": intVal(999)})},
				ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: 12345},
			},
			// Non-conflicting: a plain insert of a different entity, no precondition.
			{Operation: &datastorepb.Mutation_Insert{Insert: entity(keyB, map[string]*datastorepb.Value{"n": intVal(7)})}},
		},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	results := resp.GetMutationResults()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if !results[0].GetConflictDetected() {
		t.Fatal("expected mutation 0 (stale base_version) to conflict")
	}
	if results[1].GetConflictDetected() {
		t.Fatal("mutation 1 (plain insert) must not be marked as conflicting")
	}

	// b must have been inserted despite a's conflict in the same Commit.
	lookupResp, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{keyB}})
	if err != nil {
		t.Fatalf("lookup b: %v", err)
	}
	if len(lookupResp.GetFound()) != 1 {
		t.Fatal("entity b must exist — its non-conflicting insert must not have been aborted by a's conflict")
	}
}

// TestFrozenClockUpdateTimeToken locks the update_time optimistic-concurrency
// token end to end: it is emitted on Commit and Lookup, is strictly monotonic
// per entity even under a frozen clock, and a stale update_time precondition is
// reported as conflict_detected instead of silently applied (the G7 lost-update
// regression, mirroring Firestore's monotonic UpdateTime guard).
func TestFrozenClockUpdateTimeToken(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	frozen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	key := nameKey("Task", "frozen")
	ins, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(key, map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	stamp := ins.GetMutationResults()[0].GetUpdateTime()
	if stamp == nil {
		t.Fatal("Commit MutationResult.update_time must be set so a client can pass it forward")
	}
	observed := stamp.AsTime()

	lookup := func() *datastorepb.EntityResult {
		t.Helper()
		resp, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{key}})
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if len(resp.GetFound()) != 1 {
			t.Fatalf("expected 1 found entity, got %d", len(resp.GetFound()))
		}
		return resp.GetFound()[0]
	}

	if got := lookup().GetUpdateTime(); got == nil || !got.AsTime().Equal(observed) {
		t.Fatalf("Lookup update_time = %v, want %v (the Commit token)", got, observed)
	}

	// A mutation carrying the observed token must apply and advance it.
	upd := func(n int64) *datastorepb.Mutation {
		return &datastorepb.Mutation{
			Operation:                 &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"n": intVal(n)})},
			ConflictDetectionStrategy: &datastorepb.Mutation_UpdateTime{UpdateTime: timestamppb.New(observed)},
		}
	}
	applied, err := client.Commit(ctx, &datastorepb.CommitRequest{ProjectId: "test", Mutations: []*datastorepb.Mutation{upd(2)}})
	if err != nil {
		t.Fatalf("writer B commit: %v", err)
	}
	if applied.GetMutationResults()[0].GetConflictDetected() {
		t.Fatal("a matching update_time precondition must not conflict")
	}
	advanced := applied.GetMutationResults()[0].GetUpdateTime()
	if advanced == nil || !advanced.AsTime().After(observed) {
		t.Fatalf("writer B update_time = %v, want strictly after %v", advanced, observed)
	}

	// A second writer still holding the stale token must be flagged as a
	// conflict rather than clobbering writer B's write.
	stale, err := client.Commit(ctx, &datastorepb.CommitRequest{ProjectId: "test", Mutations: []*datastorepb.Mutation{upd(3)}})
	if err != nil {
		t.Fatalf("writer A commit: %v", err)
	}
	if !stale.GetMutationResults()[0].GetConflictDetected() {
		t.Fatal("a stale update_time precondition must be conflict_detected")
	}
	// A rejected mutation changed nothing, so its result carries the current
	// entity's update_time (real Datastore semantics).
	if got := stale.GetMutationResults()[0].GetUpdateTime(); got == nil || !got.AsTime().Equal(advanced.AsTime()) {
		t.Fatalf("conflict MutationResult.update_time = %v, want %v (the current entity's)", got, advanced)
	}
	if got := lookup().GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("n = %d after the rejected stale write, want 2 (writer B's update must not be lost)", got)
	}
}
