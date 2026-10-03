package firestore

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory FirestoreStore. Documents are keyed by their full
// resource name.
type MemoryStore struct {
	mu   sync.RWMutex
	docs map[string]Document // full name → document
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{docs: make(map[string]Document)}
}

func (s *MemoryStore) GetDocument(_ context.Context, name string) (Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.docs[name]
	if !ok {
		return Document{}, ErrDocumentNotFound
	}
	return d, nil
}

func (s *MemoryStore) CreateDocument(_ context.Context, doc Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.docs[doc.Name]; ok {
		return ErrDocumentExists
	}
	normalizeDocument(&doc)
	s.docs[doc.Name] = doc
	return nil
}

func (s *MemoryStore) UpdateDocument(_ context.Context, doc Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.docs[doc.Name]; !ok {
		return ErrDocumentNotFound
	}
	normalizeDocument(&doc)
	s.docs[doc.Name] = doc
	return nil
}

// DeleteDocument is idempotent: deleting a non-existent document succeeds (real
// Firestore DeleteDocument does not error without a precondition).
func (s *MemoryStore) DeleteDocument(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.docs, name)
	return nil
}

func (s *MemoryStore) ListDocuments(_ context.Context, project, database string) ([]Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := "projects/" + project + "/databases/" + database + "/documents/"
	result := make([]Document, 0)
	for name, d := range s.docs {
		if strings.HasPrefix(name, prefix) {
			result = append(result, d)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

// Commit applies a batch of writes atomically. Read-set entries and write
// preconditions are validated against the current state under the store lock;
// a mismatch aborts the whole batch without applying any write.
func (s *MemoryStore) Commit(_ context.Context, reads []ReadRef, writes []Write) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Re-validate the transaction read-set.
	for _, r := range reads {
		d, ok := s.docs[r.Name]
		if r.Exists != ok {
			return ErrAborted
		}
		if ok && !d.UpdateTime.Equal(r.UpdateTime) {
			return ErrAborted
		}
	}

	// 2. Validate per-write preconditions.
	for _, w := range writes {
		if err := checkPrecondition(s.docs, w); err != nil {
			return err
		}
	}

	// 3. Apply all writes.
	for _, w := range writes {
		if w.Document == nil {
			delete(s.docs, w.Name)
			continue
		}
		doc := *w.Document
		normalizeDocument(&doc)
		s.docs[w.Name] = doc
	}
	return nil
}

// checkPrecondition validates a single write's precondition against the current
// document map (the store lock is held by the caller).
func checkPrecondition(docs map[string]Document, w Write) error {
	cur, exists := docs[w.Name]
	if w.Precondition == nil {
		return nil
	}
	if w.Precondition.Exists != nil {
		if *w.Precondition.Exists {
			if !exists {
				return &DocumentMissingError{Name: w.Name}
			}
		} else if exists {
			return &DocumentExistsError{Name: w.Name}
		}
		return nil
	}
	if w.Precondition.UpdateTime != nil {
		if !exists {
			return ErrPreconditionFailed
		}
		if !cur.UpdateTime.Equal(*w.Precondition.UpdateTime) {
			return ErrPreconditionFailed
		}
	}
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs = make(map[string]Document)
}
