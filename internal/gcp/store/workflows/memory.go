package workflows

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	workflows  map[string]map[string]Workflow  // projectID+"/"+location → id → workflow
	executions map[string]map[string]Execution // projectID+"/"+location+"/"+workflowID → id → execution
	operations map[string]map[string]Operation // projectID+"/"+location → id → operation
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows:  make(map[string]map[string]Workflow),
		executions: make(map[string]map[string]Execution),
		operations: make(map[string]map[string]Operation),
	}
}

func wlkey(projectID, location string) string { return projectID + "/" + location }

func exkey(projectID, location, workflowID string) string {
	return projectID + "/" + location + "/" + workflowID
}

func (s *MemoryStore) CreateWorkflow(_ context.Context, projectID, location, id string, w Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := wlkey(projectID, location)
	if s.workflows[key] == nil {
		s.workflows[key] = make(map[string]Workflow)
	}
	if _, ok := s.workflows[key][id]; ok {
		return ErrAlreadyExists
	}
	w.ID = id
	w.Location = location
	s.workflows[key][id] = w
	return nil
}

func (s *MemoryStore) GetWorkflow(_ context.Context, projectID, location, id string) (Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workflows[wlkey(projectID, location)][id]
	if !ok {
		return Workflow{}, ErrNoSuchWorkflow
	}
	return w, nil
}

func (s *MemoryStore) UpdateWorkflow(_ context.Context, projectID, location, id string, w Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := wlkey(projectID, location)
	if _, ok := s.workflows[key][id]; !ok {
		return ErrNoSuchWorkflow
	}
	w.ID = id
	w.Location = location
	s.workflows[key][id] = w
	return nil
}

func (s *MemoryStore) UpdateWorkflowAtomic(_ context.Context, projectID, location, id string, mutate func(Workflow) (Workflow, error)) (Workflow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := wlkey(projectID, location)
	current, ok := s.workflows[key][id]
	if !ok {
		return Workflow{}, ErrNoSuchWorkflow
	}
	next, err := mutate(current)
	if err != nil {
		return Workflow{}, err
	}
	next.ID = id
	next.Location = location
	s.workflows[key][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteWorkflow(_ context.Context, projectID, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := wlkey(projectID, location)
	if _, ok := s.workflows[key][id]; !ok {
		return ErrNoSuchWorkflow
	}
	delete(s.workflows[key], id)
	delete(s.executions, exkey(projectID, location, id))
	return nil
}

func (s *MemoryStore) ListWorkflows(_ context.Context, projectID, location string) ([]Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.workflows[wlkey(projectID, location)]
	result := make([]Workflow, 0, len(m))
	for _, w := range m {
		result = append(result, w)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) CreateExecution(_ context.Context, projectID, location, workflowID, id string, e Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := exkey(projectID, location, workflowID)
	if s.executions[key] == nil {
		s.executions[key] = make(map[string]Execution)
	}
	if _, ok := s.executions[key][id]; ok {
		return ErrAlreadyExists
	}
	e.ID = id
	e.WorkflowID = workflowID
	e.Location = location
	s.executions[key][id] = e
	return nil
}

func (s *MemoryStore) GetExecution(_ context.Context, projectID, location, workflowID, id string) (Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.executions[exkey(projectID, location, workflowID)][id]
	if !ok {
		return Execution{}, ErrNoSuchExecution
	}
	return e, nil
}

func (s *MemoryStore) UpdateExecution(_ context.Context, projectID, location, workflowID, id string, e Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := exkey(projectID, location, workflowID)
	if _, ok := s.executions[key][id]; !ok {
		return ErrNoSuchExecution
	}
	e.ID = id
	e.WorkflowID = workflowID
	e.Location = location
	s.executions[key][id] = e
	return nil
}

func (s *MemoryStore) ListExecutions(_ context.Context, projectID, location, workflowID string) ([]Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.executions[exkey(projectID, location, workflowID)]
	result := make([]Execution, 0, len(m))
	for _, e := range m {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StartTime.After(result[j].StartTime) })
	return result, nil
}

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := wlkey(projectID, location)
	if s.operations[key] == nil {
		s.operations[key] = make(map[string]Operation)
	}
	op.ProjectID = projectID
	op.Location = location
	s.operations[key][op.ID] = op
	return nil
}

func (s *MemoryStore) GetOperation(_ context.Context, projectID, location, id string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[wlkey(projectID, location)][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows = make(map[string]map[string]Workflow)
	s.executions = make(map[string]map[string]Execution)
	s.operations = make(map[string]map[string]Operation)
}
