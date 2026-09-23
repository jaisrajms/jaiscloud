package datastore

import (
	"context"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"jaiscloud/internal/clock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// readTxn builds ReadOptions carrying an explicit transaction selector, as the
// real proto does (read_options.transaction).
func readTxn(txn []byte) *datastorepb.ReadOptions {
	return &datastorepb.ReadOptions{
		ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txn},
	}
}

func beginTxn(t *testing.T, client datastorepb.DatastoreClient) []byte {
	t.Helper()
	resp, err := client.BeginTransaction(context.Background(), &datastorepb.BeginTransactionRequest{ProjectId: "test"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if len(resp.GetTransaction()) == 0 {
		t.Fatal("begin returned an empty transaction")
	}
	return resp.GetTransaction()
}

func upsertTask(t *testing.T, client datastorepb.DatastoreClient, name string, n int64) int64 {
	t.Helper()
	resp, err := client.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{"n": intVal(n)})},
		}},
	})
	if err != nil {
		t.Fatalf("upsert %s: %v", name, err)
	}
	return resp.GetMutationResults()[0].GetVersion()
}

func lookupTask(t *testing.T, client datastorepb.DatastoreClient, name string, txn []byte) *datastorepb.EntityResult {
	t.Helper()
	req := &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{nameKey("Task", name)}}
	if txn != nil {
		req.ReadOptions = readTxn(txn)
	}
	resp, err := client.Lookup(context.Background(), req)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	if len(resp.GetFound()) == 1 {
		return resp.GetFound()[0]
	}
	return nil
}

func txnCommit(t *testing.T, client datastorepb.DatastoreClient, txn []byte, muts ...*datastorepb.Mutation) (*datastorepb.CommitResponse, error) {
	t.Helper()
	return client.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId:           "test",
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: txn},
		Mutations:           muts,
	})
}

func TestTransactionLookupThenCommitSucceeds(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)

	found := lookupTask(t, client, "a", txn)
	if found == nil {
		t.Fatal("transactional lookup did not find the entity")
	}
	if found.GetVersion() != 1 {
		t.Fatalf("read version = %d, want 1", found.GetVersion())
	}

	resp, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(2)})},
	})
	if err != nil {
		t.Fatalf("transactional commit: %v", err)
	}
	if resp.GetCommitTime() == nil {
		t.Fatal("transactional commit must set commit_time")
	}
	if got := resp.GetMutationResults()[0].GetVersion(); got != 2 {
		t.Fatalf("commit result version = %d, want 2", got)
	}
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("n after commit = %d, want 2", got)
	}
}

func TestTransactionReadConflictAbortsAndNothingApplied(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)
	if found := lookupTask(t, client, "a", txn); found == nil {
		t.Fatal("transactional lookup did not find the entity")
	}

	// A concurrent non-transactional writer modifies the entity after the read.
	upsertTask(t, client, "a", 2)

	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(99)})},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}

	// The aborted transaction's write must not have applied.
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("n = %d after aborted commit, want 2", got)
	}

	// Any commit attempt terminates the transaction: reusing the token is now
	// invalid (real Datastore requires a fresh BeginTransaction to retry).
	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(3)})},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reused transaction err = %v, want InvalidArgument", err)
	}
}

func TestTransactionRollbackDiscards(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	_ = ctx

	txn := beginTxn(t, client)
	if _, err := client.Rollback(ctx, &datastorepb.RollbackRequest{ProjectId: "test", Transaction: txn}); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("commit after rollback err = %v, want InvalidArgument", err)
	}

	// Rollback is idempotent even for an unknown token.
	if _, err := client.Rollback(ctx, &datastorepb.RollbackRequest{ProjectId: "test", Transaction: []byte("nonexistent")}); err != nil {
		t.Fatalf("rollback unknown: %v", err)
	}
}

func TestTransactionPreconditionMismatchFailsPrecondition(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)

	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation:                 &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(99)})},
		ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: 12345},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit err = %v, want FailedPrecondition", err)
	}
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 1 {
		t.Fatalf("n = %d after precondition failure, want 1", got)
	}
}

func TestTransactionalWithoutTransactionIsInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mode:      datastorepb.CommitRequest_TRANSACTIONAL,
		// no transaction
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestUnknownTransactionCommitIsInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	_, err := txnCommit(t, client, []byte("does-not-exist"), &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestNonTransactionalCommitUnchanged(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	// No mode, no transaction: the historical non-transactional path.
	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if err != nil {
		t.Fatalf("non-transactional commit: %v", err)
	}
	if resp.GetCommitTime() != nil {
		t.Fatal("non-transactional commit must not set commit_time")
	}
	if got := resp.GetMutationResults()[0].GetVersion(); got != 1 {
		t.Fatalf("version = %d, want 1", got)
	}
}

// TestTwoTransactionsSameEntityOneAborts is the no-lost-update guarantee at the
// service level: two transactions that both read the same version race to
// commit; one wins and the other aborts.
func TestTwoTransactionsSameEntityOneAborts(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txnA := beginTxn(t, client)
	txnB := beginTxn(t, client)
	if lookupTask(t, client, "a", txnA) == nil || lookupTask(t, client, "a", txnB) == nil {
		t.Fatal("both transactions must observe the entity")
	}

	if _, err := txnCommit(t, client, txnA, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(100)})},
	}); err != nil {
		t.Fatalf("txn A commit: %v", err)
	}
	if _, err := txnCommit(t, client, txnB, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(200)})},
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("txn B commit err = %v, want Aborted", err)
	}

	// A's write survived; B's did not (no lost update).
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 100 {
		t.Fatalf("n = %d, want 100 (B must not have overwritten A)", got)
	}
}

func TestTransactionLookupMissingThenCreatedAborts(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	txn := beginTxn(t, client)
	if got := lookupTask(t, client, "a", txn); got != nil {
		t.Fatal("entity should not exist yet")
	}

	// A concurrent create of the key read as missing.
	upsertTask(t, client, "a", 1)

	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(99)})},
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}
}

func TestTransactionRunQueryRecordsReads(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	upsertTask(t, client, "a", 1)
	upsertTask(t, client, "b", 1)

	txn := beginTxn(t, client)
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Task"}}}
	if _, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId:   "test",
		ReadOptions: readTxn(txn),
		QueryType:   &datastorepb.RunQueryRequest_Query{Query: q},
	}); err != nil {
		t.Fatalf("transactional runquery: %v", err)
	}

	// Modify one of the entities the query returned.
	upsertTask(t, client, "b", 2)

	// A commit with no mutations still validates the read-set and must abort.
	if _, err := txnCommit(t, client, txn); status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}
}

// ─── coverage additions ──────────────────────────────────────────────────────

// TestTransactionCommitMixedMutations exercises the Upsert and Delete branches
// of the transactional path (the other tests only commit Insert/Update).
func TestTransactionCommitMixedMutations(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	upsertTask(t, client, "b", 1)
	txn := beginTxn(t, client)
	if lookupTask(t, client, "a", txn) == nil {
		t.Fatal("transactional lookup did not find a")
	}

	resp, err := txnCommit(t, client, txn,
		&datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "c"), map[string]*datastorepb.Value{"n": intVal(7)})}},
		&datastorepb.Mutation{Operation: &datastorepb.Mutation_Delete{Delete: nameKey("Task", "b")}},
		&datastorepb.Mutation{Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(2)})}},
	)
	if err != nil {
		t.Fatalf("transactional commit: %v", err)
	}
	if len(resp.GetMutationResults()) != 3 {
		t.Fatalf("mutation_results = %d, want 3", len(resp.GetMutationResults()))
	}
	if got := lookupTask(t, client, "c", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 7 {
		t.Fatalf("c.n = %d, want 7 (upsert)", got)
	}
	if r := lookupTask(t, client, "b", nil); r != nil {
		t.Fatalf("b should have been deleted, got %+v", r)
	}
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("a.n = %d, want 2 (update)", got)
	}
}

// TestTransactionCommitAutoAllocatesID covers the allocated-key branch: an
// Insert with an incomplete key inside a transaction gets a server-allocated
// id, returned in mutation_results[i].key.
func TestTransactionCommitAutoAllocatesID(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	txn := beginTxn(t, client)
	resp, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Insert{Insert: entity(incompleteKey("Task"), map[string]*datastorepb.Value{"n": intVal(1)})},
	})
	if err != nil {
		t.Fatalf("transactional commit: %v", err)
	}
	k := resp.GetMutationResults()[0].GetKey()
	if k == nil {
		t.Fatal("an auto-allocated insert must return the allocated key")
	}
	path := k.GetPath()
	if len(path) == 0 || path[len(path)-1].GetId() == 0 {
		t.Fatalf("auto-allocated key has no id: %v", k)
	}

	// An Upsert with an incomplete key auto-allocates too.
	txn2 := beginTxn(t, client)
	resp2, err := txnCommit(t, client, txn2, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(incompleteKey("Task"), map[string]*datastorepb.Value{"n": intVal(2)})},
	})
	if err != nil {
		t.Fatalf("transactional upsert commit: %v", err)
	}
	k2 := resp2.GetMutationResults()[0].GetKey()
	if k2 == nil || len(k2.GetPath()) == 0 || k2.GetPath()[len(k2.GetPath())-1].GetId() == 0 {
		t.Fatalf("upsert auto-allocated key missing id: %v", k2)
	}
}

// TestTransactionIncompleteUpdateKeyInvalid covers the transactional path's
// incomplete-Update-key rejection.
func TestTransactionIncompleteUpdateKeyInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	txn := beginTxn(t, client)
	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(incompleteKey("Task"), map[string]*datastorepb.Value{"n": intVal(1)})},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

// TestRollbackEmptyTransactionIdempotent covers Rollback with no transaction
// (a no-op, matching real Datastore's tolerance of an unknown transaction).
func TestRollbackEmptyTransactionIdempotent(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Rollback(ctx, &datastorepb.RollbackRequest{ProjectId: "test"}); err != nil {
		t.Fatalf("rollback with no transaction = %v, want nil", err)
	}
	if _, err := client.Rollback(ctx, &datastorepb.RollbackRequest{ProjectId: "test", Transaction: []byte("unknown")}); err != nil {
		t.Fatalf("rollback with unknown transaction = %v, want nil", err)
	}
}

// TestTransactionUpdateTimePreconditionMismatch covers the Mutation_UpdateTime
// conflict-detection branch (the other tests only use base_version).
func TestTransactionUpdateTimePreconditionMismatch(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)
	if lookupTask(t, client, "a", txn) == nil {
		t.Fatal("transactional lookup did not find a")
	}

	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation:                 &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(2)})},
		ConflictDetectionStrategy: &datastorepb.Mutation_UpdateTime{UpdateTime: timestamppb.New(time.Now().Add(-time.Hour))},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit err = %v, want FailedPrecondition", err)
	}
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 1 {
		t.Fatalf("n = %d after precondition failure, want 1", got)
	}
}

// TestTransactionCommitNoOperationMutation covers the transactional path's
// default branch: a mutation carrying no operation is INVALID_ARGUMENT.
func TestTransactionCommitNoOperationMutation(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	txn := beginTxn(t, client)
	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("commit err = %v, want InvalidArgument", err)
	}
}

// TestCommitExplicitNonTransactional covers the explicit NON_TRANSACTIONAL mode
// (the other tests rely on the MODE_UNSPECIFIED default).
func TestCommitExplicitNonTransactional(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "x"), map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if resp.GetCommitTime() != nil {
		t.Fatal("non-transactional commit must not set commit_time")
	}
	if got := lookupTask(t, client, "x", nil); got == nil {
		t.Fatal("upsert did not apply")
	}
}

// TestTransactionExpiredTTL verifies that a transaction past its TTL is evicted
// and can no longer be committed (real Datastore expires transactions too).
func TestTransactionExpiredTTL(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	clock.SetGlobalClock(clock.FixedClock{T: time.Now()})
	defer clock.SetGlobalClock(clock.RealClock{})

	txn := beginTxn(t, client)
	// Advance well past the ~270s transaction TTL.
	clock.SetGlobalClock(clock.FixedClock{T: time.Now().Add(10 * time.Minute)})

	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expired transaction err = %v, want InvalidArgument", err)
	}
}

// TestLookupRunQueryUnknownTransaction verifies a non-empty but unknown
// transaction selector is rejected on reads (not silently ignored).
func TestLookupRunQueryUnknownTransaction(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	bogus := []byte("not-a-real-transaction")

	_, err := client.Lookup(ctx, &datastorepb.LookupRequest{
		ProjectId:   "test",
		Keys:        []*datastorepb.Key{nameKey("Task", "a")},
		ReadOptions: readTxn(bogus),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("lookup unknown transaction err = %v, want InvalidArgument", err)
	}

	_, err = client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId:   "test",
		QueryType:   &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Task"}}}},
		ReadOptions: readTxn(bogus),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("runquery unknown transaction err = %v, want InvalidArgument", err)
	}
}
