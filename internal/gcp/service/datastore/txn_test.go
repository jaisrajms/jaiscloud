package datastore

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// TestTransactionTTLBoundary locks the transaction TTL edge: a read-set is
// still active at exactly txnTTL (the comparison is strictly greater) and
// expires one second later. The boundary is written as a literal (not the
// txnTTL constant) so a changed constant is caught as well as a flipped
// comparator.
func TestTransactionTTLBoundary(t *testing.T) {
	const wantTTL = 270 * time.Second
	if txnTTL != wantTTL {
		t.Fatalf("txnTTL = %v, want %v (real Datastore's ~270s read-write window)", txnTTL, wantTTL)
	}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	txn, err := s.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := s.requireActive(txn); err != nil {
		t.Fatalf("a fresh transaction must be active: %v", err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(wantTTL)})
	if err := s.requireActive(txn); err != nil {
		t.Fatalf("a transaction at exactly txnTTL must still be active: %v", err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(wantTTL + time.Second)})
	if err := s.requireActive(txn); err == nil {
		t.Fatal("a transaction older than txnTTL must be expired")
	}
}

// TestReadSetFor covers the read-set extraction: empty/unknown tokens and a
// transaction with no reads all yield nil; recorded reads come back sorted by
// key.
func TestReadSetFor(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	if got := s.readSetFor(nil); got != nil {
		t.Fatalf("readSetFor(nil) = %v, want nil", got)
	}
	if got := s.readSetFor([]byte("unknown")); got != nil {
		t.Fatalf("readSetFor(unknown) = %v, want nil", got)
	}
	txn, err := s.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if got := s.readSetFor(txn); got != nil {
		t.Fatalf("readSetFor(no reads) = %v, want nil", got)
	}

	upsert(t, s, "p", nameKey("Task", "b"), intProp(2))
	upsert(t, s, "p", nameKey("Task", "a"), intProp(1))
	// Look them up in reverse key order to prove the result is sorted.
	if _, err := s.Lookup(ctx, "p", []Key{nameKey("Task", "b"), nameKey("Task", "a")}, txn); err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	reads := s.readSetFor(txn)
	if len(reads) != 2 {
		t.Fatalf("readSetFor = %d reads, want 2", len(reads))
	}
	if reads[0].Key > reads[1].Key {
		t.Fatalf("reads are not sorted by key: %q > %q", reads[0].Key, reads[1].Key)
	}
	for _, r := range reads {
		if !r.Exists || r.Version != 1 {
			t.Fatalf("read %q = %+v, want Exists v1", r.Key, r)
		}
	}

	// Rollback discards the read-set and deactivates the transaction.
	s.Rollback(ctx, txn)
	if got := s.readSetFor(txn); got != nil {
		t.Fatalf("readSetFor after rollback = %v, want nil", got)
	}
	if err := s.requireActive(txn); err == nil {
		t.Fatal("a rolled-back transaction must not be active")
	}
}

// TestCommitDispatchModes covers the mode dispatch: a TRANSACTIONAL commit
// requires a handle; NON_TRANSACTIONAL ignores any handle; MODE_UNSPECIFIED is
// transactional iff a handle is present.
func TestCommitDispatchModes(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	if _, err := s.Commit(ctx, "p", &CommitRequest{Mode: CommitModeTransactional}); err == nil {
		t.Fatal("TRANSACTIONAL commit without a transaction must be rejected")
	}

	// NON_TRANSACTIONAL ignores a bogus handle: no CommitTime is set.
	resp, err := s.Commit(ctx, "p", &CommitRequest{
		Mode:        CommitModeNonTransactional,
		Transaction: []byte("bogus"),
		Mutations:   []Mutation{{Op: MutationInsert, Key: nameKey("Task", "a"), Entity: intProp(1)}},
	})
	if err != nil {
		t.Fatalf("NON_TRANSACTIONAL commit: %v", err)
	}
	if !resp.CommitTime.IsZero() {
		t.Fatalf("a non-transactional commit must not set CommitTime, got %v", resp.CommitTime)
	}

	// MODE_UNSPECIFIED with a live handle takes the transactional path.
	txn, err := s.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	resp, err = s.Commit(ctx, "p", &CommitRequest{
		Transaction: txn,
		Mutations:   []Mutation{{Op: MutationInsert, Key: nameKey("Task", "b"), Entity: intProp(2)}},
	})
	if err != nil {
		t.Fatalf("unspecified-mode transactional commit: %v", err)
	}
	if resp.CommitTime.IsZero() {
		t.Fatal("a transactional commit must set CommitTime")
	}
	if err := s.requireActive(txn); err == nil {
		t.Fatal("a committed transaction must no longer be active")
	}
}

// TestTransactionalCommitAbortsOnReadConflict exercises the OCC protocol: a
// read observed at v1 that changed before commit aborts the whole transaction
// with ABORTED and applies nothing.
func TestTransactionalCommitAbortsOnReadConflict(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	key := nameKey("Task", "a")
	upsert(t, s, "p", key, intProp(1))

	txn, err := s.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if _, err := s.Lookup(ctx, "p", []Key{key}, txn); err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	// A concurrent writer bumps the entity's version.
	upsert(t, s, "p", key, intProp(99))

	_, err = s.Commit(ctx, "p", &CommitRequest{
		Mode:        CommitModeTransactional,
		Transaction: txn,
		Mutations:   []Mutation{{Op: MutationUpsert, Key: key, Entity: intProp(7)}},
	})
	if err == nil {
		t.Fatal("a commit whose read-set changed must be aborted")
	}
	pe := asProviderError(t, err)
	if pe.Code != "Aborted" || pe.HTTPStatus != 409 {
		t.Fatalf("conflict error = {%q,%d}, want Aborted/409", pe.Code, pe.HTTPStatus)
	}

	// The aborted transaction's mutation was not applied.
	got, err := s.Lookup(ctx, "p", []Key{key}, nil)
	if err != nil || len(got.Found) != 1 {
		t.Fatalf("Lookup after abort: %v (%d found)", err, len(got.Found))
	}
	if v := got.Found[0].Entity.Properties["n"]; v.IntegerValue == nil || *v.IntegerValue != 99 {
		t.Fatalf("aborted commit must not apply; n = %+v", v)
	}
}

// TestDeleteKeyAndReserveIDsValidation covers the small key validators the
// delete/reserve paths use: an incomplete key is rejected, a complete one is
// accepted.
func TestDeleteKeyAndReserveIDsValidation(t *testing.T) {
	if _, err := deleteKey(Key{Kind: "Task"}); err == nil {
		t.Fatal("deleteKey with an incomplete key must be rejected")
	}
	canon, err := deleteKey(nameKey("Task", "a"))
	if err != nil || canon == "" {
		t.Fatalf("deleteKey(complete) = %q, %v", canon, err)
	}

	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	if err := s.ReserveIDs(ctx, "p", []Key{{Kind: "Task"}}); err == nil {
		t.Fatal("ReserveIDs with an incomplete key must be rejected")
	}
	if err := s.ReserveIDs(ctx, "p", []Key{idKey("Task", 10)}); err != nil {
		t.Fatalf("ReserveIDs with a complete key: %v", err)
	}
	if _, err := s.AllocateIDs(ctx, "p", []Key{idKey("Task", 1)}); err == nil {
		t.Fatal("AllocateIDs with a complete key must be rejected")
	}
}

// TestCommitUpdateMissingIsNotFound pins DC18: real Datastore documents
// NOT_FOUND for "the request attempted to update an entity that does not exist"
// (Firestore in Datastore mode, "Errors and Error Handling"), on both commit
// paths. A delete of an absent entity stays idempotent, so the mapping is not
// over-broad.
func TestCommitUpdateMissingIsNotFound(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	missing := nameKey("Task", "ghost")

	cases := []struct {
		name    string
		mode    CommitMode
		beginTx bool
	}{
		{name: "non-transactional", mode: CommitModeNonTransactional},
		{name: "transactional", mode: CommitModeTransactional, beginTx: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &CommitRequest{
				Mode:      tc.mode,
				Mutations: []Mutation{{Op: MutationUpdate, Key: missing, Entity: intProp(1)}},
			}
			if tc.beginTx {
				txn, err := s.BeginTransaction(ctx)
				if err != nil {
					t.Fatalf("BeginTransaction: %v", err)
				}
				req.Transaction = txn
			}
			_, err := s.Commit(ctx, "p", req)
			pe := asProviderError(t, err)
			if pe.Code != "NotFound" || pe.HTTPStatus != 404 {
				t.Fatalf("update missing = {%q,%d}, want NotFound/404", pe.Code, pe.HTTPStatus)
			}
			// The failed mutation wrote nothing.
			got, err := s.Lookup(ctx, "p", []Key{missing}, nil)
			if err != nil || len(got.Found) != 0 || len(got.Missing) != 1 {
				t.Fatalf("Lookup after failed update = %d found / %d missing, %v; want the entity still absent",
					len(got.Found), len(got.Missing), err)
			}
			// A transactional miss still consumes the transaction handle.
			if tc.beginTx {
				if err := s.requireActive(req.Transaction); err == nil {
					t.Fatal("a commit that failed on a missing update must terminate the transaction")
				}
			}
		})
	}

	// Delete of an absent entity is documented as "may or may not already
	// exist" and is idempotent — never NOT_FOUND.
	if _, err := s.Commit(ctx, "p", &CommitRequest{
		Mode:      CommitModeNonTransactional,
		Mutations: []Mutation{{Op: MutationDelete, DeleteKey: Key{Kind: "Task", Name: "ghost", HasName: true}}},
	}); err != nil {
		t.Fatalf("delete of an absent entity = %v, want nil (idempotent)", err)
	}
}
