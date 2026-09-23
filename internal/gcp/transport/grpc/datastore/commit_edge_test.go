package datastore

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// commitNonTxn runs a single-mutation non-transactional Commit, returning the
// first MutationResult and the error.
func commitNonTxn(t *testing.T, client datastorepb.DatastoreClient, m *datastorepb.Mutation) (*datastorepb.MutationResult, error) {
	t.Helper()
	resp, err := client.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{m},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetMutationResults()[0], nil
}

// TestNonTransactionalConflictDetected covers the non-txn Commit's per-mutation
// conflict_detection_strategy outcome for every mutation kind: a stale
// base_version sets mutation_results[i].conflict_detected=true (the commit
// itself still SUCCEEDS in the non-transactional path) and the write is not
// applied.
func TestNonTransactionalConflictDetected(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1) // version 1
	stale := int64(999)

	mutate := func(m *datastorepb.Mutation) *datastorepb.Mutation {
		m.ConflictDetectionStrategy = &datastorepb.Mutation_BaseVersion{BaseVersion: stale}
		return m
	}
	cases := []*datastorepb.Mutation{
		mutate(&datastorepb.Mutation{Operation: &datastorepb.Mutation_Insert{Insert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(9)})}}),
		mutate(&datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(9)})}}),
		mutate(&datastorepb.Mutation{Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(9)})}}),
		mutate(&datastorepb.Mutation{Operation: &datastorepb.Mutation_Delete{Delete: nameKey("Task", "a")}}),
	}
	for _, m := range cases {
		mr, err := commitNonTxn(t, client, m)
		if err != nil {
			t.Fatalf("conflicting commit returned error for %T: %v", m.GetOperation(), err)
		}
		if !mr.GetConflictDetected() {
			t.Fatalf("%T: conflict_detected = false, want true", m.GetOperation())
		}
	}
	// None of the conflicting writes may have applied.
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 1 {
		t.Fatalf("a.n = %d after conflicting commits, want 1", got)
	}
}

// TestNonTransactionalUpsertAutoAllocatesKey covers the non-txn Upsert
// incomplete-key branch (Insert is covered elsewhere).
func TestNonTransactionalUpsertAutoAllocatesKey(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	mr, err := commitNonTxn(t, client, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(incompleteKey("Task"), map[string]*datastorepb.Value{"n": intVal(1)})},
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	k := mr.GetKey()
	if k == nil || len(k.GetPath()) == 0 || k.GetPath()[len(k.GetPath())-1].GetId() == 0 {
		t.Fatalf("upsert auto-allocated key missing id: %v", k)
	}
}

// TestNonTransactionalNoOperationMutation covers the non-txn Commit's default
// branch: a mutation with no operation is INVALID_ARGUMENT.
func TestNonTransactionalNoOperationMutation(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	if _, err := commitNonTxn(t, client, &datastorepb.Mutation{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no-operation mutation err = %v, want InvalidArgument", err)
	}
}

// TestNonTransactionalInvalidKeys covers the resolveEntity / entityFromProto /
// deleteKey error branches. A namespaced key is rejected by canonicalKey, which
// each mutation kind surfaces as INVALID_ARGUMENT; an incomplete Update key is
// rejected by the service's explicit empty-key check.
func TestNonTransactionalInvalidKeys(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	namespaced := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "test", NamespaceId: "ns"},
		Path:        []*datastorepb.Key_PathElement{{Kind: "Task", IdType: &datastorepb.Key_PathElement_Name{Name: "a"}}},
	}
	cases := []struct {
		name string
		m    *datastorepb.Mutation
	}{
		{"insert namespaced key", &datastorepb.Mutation{Operation: &datastorepb.Mutation_Insert{Insert: entity(namespaced, map[string]*datastorepb.Value{"n": intVal(1)})}}},
		{"upsert namespaced key", &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(namespaced, map[string]*datastorepb.Value{"n": intVal(1)})}}},
		{"update namespaced key", &datastorepb.Mutation{Operation: &datastorepb.Mutation_Update{Update: entity(namespaced, map[string]*datastorepb.Value{"n": intVal(1)})}}},
		{"delete namespaced key", &datastorepb.Mutation{Operation: &datastorepb.Mutation_Delete{Delete: namespaced}}},
		{"update incomplete key", &datastorepb.Mutation{Operation: &datastorepb.Mutation_Update{Update: entity(incompleteKey("Task"), map[string]*datastorepb.Value{"n": intVal(1)})}}},
	}
	for _, tc := range cases {
		if _, err := commitNonTxn(t, client, tc.m); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s err = %v, want InvalidArgument", tc.name, err)
		}
	}
}
