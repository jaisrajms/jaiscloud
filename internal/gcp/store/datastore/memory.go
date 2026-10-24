package datastore

import (
	"context"
	"errors"
	"sort"
	"sync"

	"jaiscloud/internal/gcp/storeutil"
)

// MemoryStore is an in-memory Store. Entities are keyed by (project, key).
type MemoryStore struct {
	mu        sync.RWMutex
	entities  map[string]map[string]Entity // project → key → entity
	allocator map[string]int64             // project → next id
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entities:  make(map[string]map[string]Entity),
		allocator: make(map[string]int64),
	}
}

func (s *MemoryStore) Get(_ context.Context, project, key string) (Entity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entities[project][key]
	if !ok {
		return Entity{}, ErrEntityNotFound
	}
	return e, nil
}

func (s *MemoryStore) Insert(_ context.Context, project string, e Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entities[project][e.Key]; ok {
		return ErrEntityExists
	}
	if s.entities[project] == nil {
		s.entities[project] = make(map[string]Entity)
	}
	s.entities[project][e.Key] = e
	return nil
}

func (s *MemoryStore) Upsert(_ context.Context, project string, e Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entities[project] == nil {
		s.entities[project] = make(map[string]Entity)
	}
	s.entities[project][e.Key] = e
	return nil
}

func (s *MemoryStore) Update(_ context.Context, project string, e Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entities[project][e.Key]; !ok {
		return ErrEntityNotFound
	}
	s.entities[project][e.Key] = e
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, project, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entities[project], key)
	return nil
}

func (s *MemoryStore) ApplyMutation(_ context.Context, project string, kind MutationKind, e Entity, precondition *Precondition) (Entity, error) {
	var observed Entity
	result, err := storeutil.AtomicUpdate(&s.mu,
		func() (Entity, bool) { ent, ok := s.entities[project][e.Key]; return ent, ok },
		func(current Entity, exists bool) (Entity, error) {
			observed = current
			if !preconditionMatches(current, exists, precondition) {
				return Entity{}, ErrConflict
			}
			switch kind {
			case MutationInsert:
				if exists {
					return Entity{}, ErrEntityExists
				}
				e.Version = 1
			case MutationUpdate:
				if !exists {
					return Entity{}, ErrEntityNotFound
				}
				e.Version = current.Version + 1
			case MutationUpsert:
				e.Version = current.Version + 1
			}
			e = stampWrite(current, exists, e)
			return e, nil
		},
		func(ent Entity) {
			if s.entities[project] == nil {
				s.entities[project] = make(map[string]Entity)
			}
			s.entities[project][e.Key] = ent
		},
	)
	if errors.Is(err, ErrConflict) {
		// Report the entity's actual current (unchanged) state, not the zero
		// value AtomicUpdate discards on error — real Datastore's
		// MutationResult.version is "the version on the server after
		// processing," which for a rejected mutation is the version that
		// caused the rejection, useful for a client's retry.
		return observed, ErrConflict
	}
	return result, err
}

func (s *MemoryStore) DeleteConflictChecked(_ context.Context, project, key string, precondition *Precondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.entities[project][key]
	if !preconditionMatches(current, exists, precondition) {
		return ErrConflict
	}
	delete(s.entities[project], key)
	return nil
}

// Commit applies reads+writes atomically under one lock: the read-set is
// re-validated and every write is validated before any write is applied, so a
// concurrent mutation can neither land between the checks and the apply nor be
// observed half-applied. See Store.Commit's doc comment for the error contract.
func (s *MemoryStore) Commit(_ context.Context, project string, reads []ReadRef, writes []Write) ([]Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Re-validate the transaction read-set.
	for _, r := range reads {
		current, exists := s.entities[project][r.Key]
		if r.Exists != exists {
			return nil, ErrAborted
		}
		if exists && current.Version != r.Version {
			return nil, ErrAborted
		}
	}

	// 2. Validate every write and compute the entity to persist, without
	//    mutating anything yet (all-or-nothing).
	applied := make([]Entity, len(writes))
	for i, w := range writes {
		current, exists := s.entities[project][w.Key]
		e, err := resolveWrite(current, exists, w)
		if err != nil {
			return nil, err
		}
		applied[i] = e
	}

	// 3. Apply all writes now that every validation has passed.
	for i, w := range writes {
		if w.Op == WriteDelete {
			delete(s.entities[project], w.Key)
			continue
		}
		if s.entities[project] == nil {
			s.entities[project] = make(map[string]Entity)
		}
		s.entities[project][w.Key] = applied[i]
	}
	return applied, nil
}

func (s *MemoryStore) ListKind(_ context.Context, project, kind string) ([]Entity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Entity, 0)
	for _, e := range s.entities[project] {
		if kind != "" && e.Kind != kind {
			continue
		}
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *MemoryStore) AllocateIDs(_ context.Context, project string, n int) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.allocator[project]
	if start == 0 {
		start = 1
	}
	s.allocator[project] = start + int64(n)
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = start + int64(i)
	}
	return ids, nil
}

func (s *MemoryStore) AdvanceIDs(_ context.Context, project string, max int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if max >= s.allocator[project] {
		s.allocator[project] = max + 1
	}
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entities = make(map[string]map[string]Entity)
	s.allocator = make(map[string]int64)
}
