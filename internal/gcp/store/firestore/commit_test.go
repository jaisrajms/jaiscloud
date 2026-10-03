package firestore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testDoc(name string, t time.Time) Document {
	return Document{Name: name, Fields: map[string]*Value{"n": IntVal(1)}, CreateTime: t, UpdateTime: t}
}

func TestMemoryCommitAtomicAndPrecondition(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	name := "projects/p/databases/(default)/documents/cities/SF"

	if err := s.CreateDocument(ctx, testDoc(name, now)); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Precondition exists=true on a present doc → ok.
	existsTrue := true
	err := s.Commit(ctx, nil, []Write{{
		Name:         name,
		Document:     &Document{Name: name, Fields: map[string]*Value{"n": IntVal(2)}, CreateTime: now, UpdateTime: now.Add(time.Minute)},
		Precondition: &Precondition{Exists: &existsTrue},
	}})
	if err != nil {
		t.Fatalf("commit with exists precondition: %v", err)
	}

	// Precondition exists=false on a present doc → ErrDocumentExists (a create
	// on an existing document, matching real Firestore ALREADY_EXISTS).
	existsFalse := false
	err = s.Commit(ctx, nil, []Write{{
		Name:         name,
		Document:     &Document{Name: name, Fields: map[string]*Value{}, CreateTime: now, UpdateTime: now.Add(2 * time.Minute)},
		Precondition: &Precondition{Exists: &existsFalse},
	}})
	if !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("expected ErrDocumentExists, got %v", err)
	}

	// Precondition updateTime mismatch → ErrPreconditionFailed.
	stale := now.Add(-time.Hour)
	err = s.Commit(ctx, nil, []Write{{
		Name:         name,
		Document:     &Document{Name: name, Fields: map[string]*Value{}, CreateTime: now, UpdateTime: now.Add(2 * time.Minute)},
		Precondition: &Precondition{UpdateTime: &stale},
	}})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed on stale updateTime, got %v", err)
	}

	// Batch atomicity: a failing write aborts the whole batch.
	before, _ := s.GetDocument(ctx, name)
	bad := "projects/p/databases/(default)/documents/cities/MISSING"
	err = s.Commit(ctx, nil, []Write{
		{Name: name, Document: &Document{Name: name, Fields: map[string]*Value{"n": IntVal(99)}, CreateTime: now, UpdateTime: now.Add(3 * time.Minute)}},
		{Name: bad, Precondition: &Precondition{Exists: &existsTrue}}, // fails: missing
	})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
	after, _ := s.GetDocument(ctx, name)
	if !after.UpdateTime.Equal(before.UpdateTime) {
		t.Errorf("atomicity violated: first write applied despite batch failure")
	}
}

func TestMemoryCommitReadSetAbort(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	name := "projects/p/databases/(default)/documents/cities/SF"
	s.CreateDocument(ctx, testDoc(name, now))

	// Read-set records the doc at time now, then it is mutated.
	reads := []ReadRef{{Name: name, Exists: true, UpdateTime: now}}
	s.UpdateDocument(ctx, Document{Name: name, Fields: map[string]*Value{"n": IntVal(5)}, CreateTime: now, UpdateTime: now.Add(time.Minute)})

	err := s.Commit(ctx, reads, []Write{{Name: name, Document: &Document{Name: name, Fields: map[string]*Value{}, CreateTime: now, UpdateTime: now.Add(2 * time.Minute)}}})
	if !errors.Is(err, ErrAborted) {
		t.Fatalf("expected ErrAborted, got %v", err)
	}

	// Read-set records missing doc, then it is created → abort.
	s2 := NewMemoryStore()
	reads2 := []ReadRef{{Name: name, Exists: false}}
	s2.CreateDocument(ctx, testDoc(name, now))
	if err := s2.Commit(ctx, reads2, nil); !errors.Is(err, ErrAborted) {
		t.Fatalf("expected ErrAborted on missing→created, got %v", err)
	}
}

// TestMemoryCommitCreatePrecondition asserts the exists:false (create) vs
// exists:true precondition distinction: a create on an existing document returns
// *DocumentExistsError (errors.Is ErrDocumentExists → ALREADY_EXISTS), while an
// exists:true precondition on a missing document returns *DocumentMissingError
// (errors.Is ErrPreconditionFailed → NOT_FOUND at the provider).
func TestMemoryCommitCreatePrecondition(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	name := "projects/p/databases/(default)/documents/cities/SF"

	existsFalse := false
	existsTrue := true

	// create (exists:false) on an existing doc → ErrDocumentExists.
	s := NewMemoryStore()
	s.CreateDocument(ctx, testDoc(name, now))
	err := s.Commit(ctx, nil, []Write{{
		Name:         name,
		Document:     &Document{Name: name, Fields: map[string]*Value{"n": IntVal(2)}, CreateTime: now, UpdateTime: now.Add(time.Minute)},
		Precondition: &Precondition{Exists: &existsFalse},
	}})
	if !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("expected ErrDocumentExists on create-over-existing, got %v", err)
	}
	var existsErr *DocumentExistsError
	if !errors.As(err, &existsErr) || existsErr.Name != name {
		t.Fatalf("expected *DocumentExistsError{Name:%q}, got %v", name, err)
	}

	// exists:true on a missing doc → DocumentMissingError (errors.Is
	// ErrPreconditionFailed).
	s2 := NewMemoryStore()
	err = s2.Commit(ctx, nil, []Write{{
		Name:         name,
		Document:     &Document{Name: name, Fields: map[string]*Value{"n": IntVal(2)}, CreateTime: now, UpdateTime: now.Add(time.Minute)},
		Precondition: &Precondition{Exists: &existsTrue},
	}})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed on exists:true-over-missing, got %v", err)
	}
	var missingErr *DocumentMissingError
	if !errors.As(err, &missingErr) || missingErr.Name != name {
		t.Fatalf("expected *DocumentMissingError{Name:%q}, got %v", name, err)
	}

	// create (exists:false) on a missing doc → succeeds.
	s3 := NewMemoryStore()
	err = s3.Commit(ctx, nil, []Write{{
		Name:         name,
		Document:     &Document{Name: name, Fields: map[string]*Value{"n": IntVal(2)}, CreateTime: now, UpdateTime: now.Add(time.Minute)},
		Precondition: &Precondition{Exists: &existsFalse},
	}})
	if err != nil {
		t.Fatalf("expected create-over-missing to succeed, got %v", err)
	}
}
