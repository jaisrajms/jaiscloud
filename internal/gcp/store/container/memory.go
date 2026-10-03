package container

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	clusters   map[string]map[string]Cluster   // projectID+"/"+location → cluster name → cluster
	operations map[string]map[string]Operation // projectID+"/"+location → operation id → operation
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		clusters:   make(map[string]map[string]Cluster),
		operations: make(map[string]map[string]Operation),
	}
}

func scope(projectID, location string) string { return projectID + "/" + location }

func (s *MemoryStore) CreateCluster(_ context.Context, projectID, location string, c Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if s.clusters[key] == nil {
		s.clusters[key] = make(map[string]Cluster)
	}
	if _, ok := s.clusters[key][c.Name]; ok {
		return ErrAlreadyExists
	}
	c.ProjectID = projectID
	c.Location = location
	s.clusters[key][c.Name] = c
	return nil
}

func (s *MemoryStore) GetCluster(_ context.Context, projectID, location, name string) (Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clusters[scope(projectID, location)][name]
	if !ok {
		return Cluster{}, ErrNoSuchCluster
	}
	return c, nil
}

func (s *MemoryStore) DeleteCluster(_ context.Context, projectID, location, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if _, ok := s.clusters[key][name]; !ok {
		return ErrNoSuchCluster
	}
	delete(s.clusters[key], name)
	return nil
}

func (s *MemoryStore) ListClusters(_ context.Context, projectID, location string) ([]Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.clusters[scope(projectID, location)]
	result := make([]Cluster, 0, len(m))
	for _, c := range m {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if s.operations[key] == nil {
		s.operations[key] = make(map[string]Operation)
	}
	if _, ok := s.operations[key][op.Name]; ok {
		return ErrAlreadyExists
	}
	op.ProjectID = projectID
	op.Location = location
	s.operations[key][op.Name] = op
	return nil
}

func (s *MemoryStore) GetOperation(_ context.Context, projectID, location, name string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[scope(projectID, location)][name]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) ListOperations(_ context.Context, projectID, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[scope(projectID, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = make(map[string]map[string]Cluster)
	s.operations = make(map[string]map[string]Operation)
}
