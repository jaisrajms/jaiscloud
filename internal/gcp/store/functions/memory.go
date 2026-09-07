package functions

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu        sync.RWMutex
	functions map[string]map[string]Function // projectID+"/"+location → id → function
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{functions: make(map[string]map[string]Function)}
}

func lkey(projectID, location string) string { return projectID + "/" + location }

func (s *MemoryStore) CreateFunction(_ context.Context, projectID, location, id string, f Function) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := lkey(projectID, location)
	if s.functions[key] == nil {
		s.functions[key] = make(map[string]Function)
	}
	if _, ok := s.functions[key][id]; ok {
		return ErrAlreadyExists
	}
	f.ID = id
	f.Location = location
	s.functions[key][id] = f
	return nil
}

func (s *MemoryStore) GetFunction(_ context.Context, projectID, location, id string) (Function, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.functions[lkey(projectID, location)][id]
	if !ok {
		return Function{}, ErrNoSuchFunction
	}
	return f, nil
}

func (s *MemoryStore) UpdateFunction(_ context.Context, projectID, location, id string, f Function) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := lkey(projectID, location)
	if _, ok := s.functions[key][id]; !ok {
		return ErrNoSuchFunction
	}
	f.ID = id
	f.Location = location
	s.functions[key][id] = f
	return nil
}

func (s *MemoryStore) UpdateFunctionAtomic(_ context.Context, projectID, location, id string, mutate func(Function) (Function, error)) (Function, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := lkey(projectID, location)
	current, ok := s.functions[key][id]
	if !ok {
		return Function{}, ErrNoSuchFunction
	}
	next, err := mutate(current)
	if err != nil {
		return Function{}, err
	}
	next.ID = id
	next.Location = location
	s.functions[key][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteFunction(_ context.Context, projectID, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := lkey(projectID, location)
	if _, ok := s.functions[key][id]; !ok {
		return ErrNoSuchFunction
	}
	delete(s.functions[key], id)
	return nil
}

func (s *MemoryStore) ListFunctions(_ context.Context, projectID, location string) ([]Function, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.functions[lkey(projectID, location)]
	result := make([]Function, 0, len(m))
	for _, f := range m {
		result = append(result, f)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) ListFunctionsAllLocations(_ context.Context, projectID string) ([]Function, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []Function
	for key, m := range s.functions {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, f := range m {
			result = append(result, f)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Location != result[j].Location {
			return result[i].Location < result[j].Location
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.functions = make(map[string]map[string]Function)
}
