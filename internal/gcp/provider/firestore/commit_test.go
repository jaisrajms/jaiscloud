package firestore

import (
	"context"
	"errors"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
)

func assertProviderError(t *testing.T, err error, httpStatus int, status string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error (HTTP %d, status %q), got nil", httpStatus, status)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != httpStatus {
		t.Fatalf("expected HTTP %d, got %d", httpStatus, pe.HTTPStatus)
	}
	if status != "" && pe.Status != status {
		t.Fatalf("expected status %q, got %q", status, pe.Status)
	}
}

func TestTransactionMissingReadAbortsOnConcurrentCreate(t *testing.T) {
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

	// Read a missing document inside the transaction → NOT_FOUND.
	getNR := testNR()
	getNR.Params["name"] = "databases/(default)/documents/cities/SF"
	getNR.Params["transaction"] = txnID
	if _, err := p.DocumentsGet(ctx, getNR); err == nil {
		t.Fatal("expected NOT_FOUND for missing doc")
	} else {
		assertProviderError(t, err, 404, "")
	}

	// Create the document outside the transaction.
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name:   name,
		Fields: map[string]*firestorestore.Value{"a": intField(1)},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Commit a write in the transaction → ABORTED @409.
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
		t.Fatal("expected ABORTED for concurrent create after missing read")
	} else {
		assertProviderError(t, err, 409, "ABORTED")
	}
}

// TestTransactionExistencePreconditionAbortsBeforeConflict pins the store
// ordering: a transaction's read-set is re-validated before per-write
// preconditions, so a stale read (missing → concurrently created) yields the
// retryable ABORTED rather than a terminal ALREADY_EXISTS even when the write
// carries a currentDocument.exists=false precondition.
func TestTransactionExistencePreconditionAbortsBeforeConflict(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	resp, err := p.BeginTransaction(ctx, testNR())
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	txnID, _ := resp.Data["transaction"].(string)

	// Read the (missing) document inside the transaction → records Exists=false.
	getNR := testNR()
	getNR.Params["name"] = "databases/(default)/documents/cities/SF"
	getNR.Params["transaction"] = txnID
	if _, err := p.DocumentsGet(ctx, getNR); err == nil {
		t.Fatal("expected NOT_FOUND for missing doc")
	}

	// Create it outside the transaction.
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name:   name,
		Fields: map[string]*firestorestore.Value{"a": intField(1)},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Commit a create (exists=false, an existence precondition): the read-set
	// conflict must win (ABORTED), not ALREADY_EXISTS.
	commitNR := testNR()
	commitNR.Params["body"] = map[string]any{
		"transaction": txnID,
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"b": map[string]any{"integerValue": "2"}},
				},
				"currentDocument": map[string]any{"exists": false},
			},
		},
	}
	if _, err := p.Commit(ctx, commitNR); err == nil {
		t.Fatal("expected ABORTED for read-set conflict")
	} else {
		assertProviderError(t, err, 409, "ABORTED")
	}
}

func TestCommitRejectsMalformedTransaction(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	// Malformed transaction token → 400 INVALID_ARGUMENT.
	badNR := testNR()
	badNR.Params["body"] = map[string]any{
		"transaction": "not-base64!!!",
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"b": map[string]any{"integerValue": "2"}},
				},
			},
		},
	}
	if _, err := p.Commit(ctx, badNR); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for malformed transaction")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// The write must not have been applied (no silent non-transactional commit).
	if _, err := p.store.GetDocument(ctx, name); err == nil {
		t.Fatal("expected document to be absent after rejected commit")
	}
}

func TestRollbackRejectsMalformedTransaction(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := testNR()
	nr.Params["body"] = map[string]any{"transaction": "not-base64!!!"}
	if _, err := p.Rollback(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for malformed transaction")
	} else {
		assertInvalidArgumentErr(t, err)
	}
}

func TestCommitWithoutTransactionSucceeds(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	// Absent transaction → normal non-transactional commit.
	nr := testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"b": map[string]any{"integerValue": "2"}},
				},
			},
		},
	}
	if _, err := p.Commit(ctx, nr); err != nil {
		t.Fatalf("commit without transaction: %v", err)
	}
	if _, err := p.store.GetDocument(ctx, name); err != nil {
		t.Fatalf("expected document to be present after commit, got %v", err)
	}
}

func TestDocumentsGetMissingWithoutTransaction(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/MISSING"
	if _, err := p.DocumentsGet(ctx, nr); err == nil {
		t.Fatal("expected NOT_FOUND for missing doc")
	} else {
		assertProviderError(t, err, 404, "")
	}

	if len(p.readSets) != 0 {
		t.Fatalf("expected no read-set entries, got %d", len(p.readSets))
	}
}
