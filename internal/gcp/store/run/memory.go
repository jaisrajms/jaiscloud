package run

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	services   map[string]map[string]Service   // project+"/"+location → service id → service
	revisions  map[string]map[string]Revision  // project+"/"+location+"/"+service → revision id → revision
	operations map[string]map[string]Operation // project+"/"+location → operation id → operation
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		services:   make(map[string]map[string]Service),
		revisions:  make(map[string]map[string]Revision),
		operations: make(map[string]map[string]Operation),
	}
}

func scope(project, location string) string { return project + "/" + location }

func revScope(project, location, service string) string {
	return project + "/" + location + "/" + service
}

func (s *MemoryStore) CreateService(_ context.Context, project, location string, svc Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(project, location)
	if s.services[key] == nil {
		s.services[key] = make(map[string]Service)
	}
	if _, ok := s.services[key][svc.ID]; ok {
		return ErrAlreadyExists
	}
	svc.ProjectID = project
	svc.Location = location
	s.services[key][svc.ID] = svc
	return nil
}

func (s *MemoryStore) GetService(_ context.Context, project, location, id string) (Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	svc, ok := s.services[scope(project, location)][id]
	if !ok {
		return Service{}, ErrNoSuchService
	}
	return svc, nil
}

func (s *MemoryStore) UpdateService(_ context.Context, project, location string, svc Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(project, location)
	if _, ok := s.services[key][svc.ID]; !ok {
		return ErrNoSuchService
	}
	svc.ProjectID = project
	svc.Location = location
	s.services[key][svc.ID] = svc
	return nil
}

func (s *MemoryStore) DeleteService(_ context.Context, project, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(project, location)
	if _, ok := s.services[key][id]; !ok {
		return ErrNoSuchService
	}
	delete(s.services[key], id)
	delete(s.revisions, revScope(project, location, id))
	return nil
}

func (s *MemoryStore) ListServices(_ context.Context, project, location string) ([]Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.services[scope(project, location)]
	result := make([]Service, 0, len(m))
	for _, svc := range m {
		result = append(result, svc)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// ListServicesByProject returns every service in a project across all
// locations, sorted by location then id.
func (s *MemoryStore) ListServicesByProject(_ context.Context, project string) ([]Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Service
	for _, byID := range s.services {
		for _, svc := range byID {
			if svc.ProjectID == project {
				result = append(result, svc)
			}
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

func (s *MemoryStore) CreateRevision(_ context.Context, project, location, service string, r Revision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := revScope(project, location, service)
	if s.revisions[key] == nil {
		s.revisions[key] = make(map[string]Revision)
	}
	if _, ok := s.revisions[key][r.ID]; ok {
		return ErrAlreadyExists
	}
	r.ProjectID = project
	r.Location = location
	r.Service = service
	s.revisions[key][r.ID] = r
	return nil
}

func (s *MemoryStore) GetRevision(_ context.Context, project, location, service, id string) (Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.revisions[revScope(project, location, service)][id]
	if !ok {
		return Revision{}, ErrNoSuchRevision
	}
	return r, nil
}

func (s *MemoryStore) ListRevisions(_ context.Context, project, location, service string) ([]Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.revisions[revScope(project, location, service)]
	result := make([]Revision, 0, len(m))
	for _, r := range m {
		result = append(result, r)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) DeleteRevision(_ context.Context, project, location, service, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := revScope(project, location, service)
	if _, ok := s.revisions[key][id]; !ok {
		return ErrNoSuchRevision
	}
	delete(s.revisions[key], id)
	return nil
}

func (s *MemoryStore) DeleteRevisions(_ context.Context, project, location, service string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.revisions, revScope(project, location, service))
	return nil
}

func (s *MemoryStore) CreateOperation(_ context.Context, project, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(project, location)
	if s.operations[key] == nil {
		s.operations[key] = make(map[string]Operation)
	}
	if _, ok := s.operations[key][op.ID]; ok {
		return ErrAlreadyExists
	}
	op.ProjectID = project
	op.Location = location
	s.operations[key][op.ID] = op
	return nil
}

func (s *MemoryStore) GetOperation(_ context.Context, project, location, id string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[scope(project, location)][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) ListOperations(_ context.Context, project, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[scope(project, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) DeleteOperation(_ context.Context, project, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(project, location)
	if _, ok := s.operations[key][id]; !ok {
		return ErrNoSuchOperation
	}
	delete(s.operations[key], id)
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = make(map[string]map[string]Service)
	s.revisions = make(map[string]map[string]Revision)
	s.operations = make(map[string]map[string]Operation)
}
