package functions

import (
	"context"
	"sort"
	"strings"
	"sync"

	"jaiscloud/internal/clock"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	functions  map[string]map[string]Function  // projectID+"/"+location → id → function
	operations map[string]map[string]Operation // projectID+"/"+location → id → operation
	deliveries map[string]map[string]Delivery  // projectID+"/"+location → id → delivery
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		functions:  make(map[string]map[string]Function),
		operations: make(map[string]map[string]Operation),
		deliveries: make(map[string]map[string]Delivery),
	}
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
	s.operations = make(map[string]map[string]Operation)
	s.deliveries = make(map[string]map[string]Delivery)
}

// --- Deliveries ---

func (s *MemoryStore) CreateDelivery(_ context.Context, projectID, location string, d Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.CreateTime.IsZero() {
		d.CreateTime = clock.Now()
	}
	if d.UpdateTime.IsZero() {
		d.UpdateTime = d.CreateTime
	}
	key := lkey(projectID, location)
	if s.deliveries[key] == nil {
		s.deliveries[key] = make(map[string]Delivery)
	}
	d.Project = projectID
	d.Location = location
	s.deliveries[key][d.ID] = d
	return nil
}

func (s *MemoryStore) UpdateDelivery(_ context.Context, projectID, location string, d Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := lkey(projectID, location)
	if _, ok := s.deliveries[key][d.ID]; !ok {
		return ErrNoSuchDelivery
	}
	d.Project = projectID
	d.Location = location
	s.deliveries[key][d.ID] = d
	return nil
}

func (s *MemoryStore) GetDelivery(_ context.Context, projectID, location, id string) (Delivery, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.deliveries[lkey(projectID, location)][id]
	if !ok {
		return Delivery{}, ErrNoSuchDelivery
	}
	return d, nil
}

func (s *MemoryStore) ListDeliveries(_ context.Context, projectID, location string) ([]Delivery, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.deliveries[lkey(projectID, location)]
	result := make([]Delivery, 0, len(m))
	for _, d := range m {
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// --- Operations ---

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.CreateTime.IsZero() {
		op.CreateTime = clock.Now()
	}
	if op.EndTime.IsZero() {
		op.EndTime = op.CreateTime
	}
	key := lkey(projectID, location)
	if s.operations[key] == nil {
		s.operations[key] = make(map[string]Operation)
	}
	op.Location = location
	s.operations[key][op.ID] = op
	return nil
}

func (s *MemoryStore) GetOperation(_ context.Context, projectID, location, id string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[lkey(projectID, location)][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) DeleteOperation(_ context.Context, projectID, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := lkey(projectID, location)
	if _, ok := s.operations[key][id]; !ok {
		return ErrNoSuchOperation
	}
	delete(s.operations[key], id)
	return nil
}

// GetOperationByID finds an operation by its (globally unique) id. The v1 Cloud
// Functions operations surface is top-level (operations/{id}) with no project or
// location segment, so the lookup is not scoped to the caller's project: it
// returns the operation wherever it lives, and the location/project are
// recovered from the stored record. ErrNoSuchOperation when absent.
func (s *MemoryStore) GetOperationByID(_ context.Context, _, id string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.operations {
		if op, ok := m[id]; ok {
			return op, nil
		}
	}
	return Operation{}, ErrNoSuchOperation
}

// DeleteOperationByID removes an operation by its globally unique id.
func (s *MemoryStore) DeleteOperationByID(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.operations {
		if _, ok := m[id]; ok {
			delete(m, id)
			return nil
		}
	}
	return ErrNoSuchOperation
}

func (s *MemoryStore) ListOperations(_ context.Context, projectID, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[lkey(projectID, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
