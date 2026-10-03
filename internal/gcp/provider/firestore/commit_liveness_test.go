package firestore

import (
	"context"
	"testing"
)

// TestCommitAfterRollbackRejected ensures a Commit (and a txn-carrying read)
// with a transaction id that is no longer active is rejected with
// INVALID_ARGUMENT rather than silently treated as a non-transactional
// operation.
func TestCommitAfterRollbackRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	resp, err := p.BeginTransaction(ctx, testNR())
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	txnID, _ := resp.Data["transaction"].(string)
	if txnID == "" {
		t.Fatal("expected a non-empty transaction id")
	}

	// Roll the transaction back.
	rollbackNR := testNR()
	rollbackNR.Params["body"] = map[string]any{"transaction": txnID}
	if _, err := p.Rollback(ctx, rollbackNR); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Commit with the rolled-back transaction → INVALID_ARGUMENT.
	commitNR := testNR()
	commitNR.Params["body"] = map[string]any{
		"transaction": txnID,
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"b": map[string]any{"integerValue": "2"}},
				},
			},
		},
	}
	if _, err := p.Commit(ctx, commitNR); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for commit after rollback")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// The write must not have been applied.
	if _, err := p.store.GetDocument(ctx, name); err == nil {
		t.Fatal("expected document to be absent after rejected commit")
	}
}

func TestTxnReadAfterRollbackRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	resp, err := p.BeginTransaction(ctx, testNR())
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	txnID, _ := resp.Data["transaction"].(string)

	rollbackNR := testNR()
	rollbackNR.Params["body"] = map[string]any{"transaction": txnID}
	if _, err := p.Rollback(ctx, rollbackNR); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	getNR := testNR()
	getNR.Params["name"] = "databases/(default)/documents/cities/SF"
	getNR.Params["transaction"] = txnID
	if _, err := p.DocumentsGet(ctx, getNR); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for read after rollback")
	} else {
		assertInvalidArgumentErr(t, err)
	}
}

func TestCommitNeverBegunTransactionRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	// A well-formed transaction id that was never begun.
	txnID := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	commitNR := testNR()
	commitNR.Params["body"] = map[string]any{
		"transaction": txnID,
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"b": map[string]any{"integerValue": "2"}},
				},
			},
		},
	}
	if _, err := p.Commit(ctx, commitNR); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for never-begun transaction")
	} else {
		assertInvalidArgumentErr(t, err)
	}
}
