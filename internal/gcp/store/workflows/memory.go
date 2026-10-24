package workflows

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	workflows  map[string]map[string]Workflow  // projectID+"/"+location → id → workflow
	executions map[string]map[string]Execution // projectID+"/"+location+"/"+workflowID → id → execution
	operations map[string]map[string]Operation // projectID+"/"+location → id → operation
	revisions  map[string][]Revision           // projectID+"/"+location+"/"+workflowID → revisions (oldest first)
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows:  make(map[string]map[string]Workflow),
		executions: make(map[string]map[string]Execution),
		operations: make(map[string]map[string]Operation),
		revisions:  make(map[string][]Revision),
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
	// Mirror Postgres: only a workflow that carries a revision id gets an
	// initial revision (direct store callers may omit it).
	if w.RevisionID != "" {
		s.revisions[exkey(projectID, location, id)] = []Revision{revisionOf(w)}
	}
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
	current, ok := s.workflows[key][id]
	if !ok {
		return ErrNoSuchWorkflow
	}
	w.ID = id
	w.Location = location
	s.workflows[key][id] = w
	s.recordRevision(projectID, location, id, current, w)
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
	s.recordRevision(projectID, location, id, current, next)
	return next, nil
}

// recordRevision appends an immutable snapshot when next carries a different
// revision than prev. Callers must hold the write lock.
func (s *MemoryStore) recordRevision(projectID, location, id string, prev, next Workflow) {
	if next.RevisionID == "" || next.RevisionID == prev.RevisionID {
		return
	}
	k := exkey(projectID, location, id)
	s.revisions[k] = append(s.revisions[k], revisionOf(next))
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
	delete(s.revisions, exkey(projectID, location, id))
	return nil
}

// ListRevisions returns a workflow's revision history, newest first.
func (s *MemoryStore) ListRevisions(_ context.Context, projectID, location, workflowID string) ([]Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	revs := s.revisions[exkey(projectID, location, workflowID)]
	out := make([]Revision, len(revs))
	for i, r := range revs {
		out[len(revs)-1-i] = r // append order is oldest-first
	}
	return out, nil
}

// GetRevision returns one revision of a workflow, or ErrNoSuchRevision.
func (s *MemoryStore) GetRevision(_ context.Context, projectID, location, workflowID, revisionID string) (Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.revisions[exkey(projectID, location, workflowID)] {
		if r.RevisionID == revisionID {
			return r, nil
		}
	}
	return Revision{}, ErrNoSuchRevision
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

func (s *MemoryStore) ListWorkflowsByProject(_ context.Context, projectID string) ([]Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []Workflow
	for key, m := range s.workflows {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, w := range m {
			result = append(result, w)
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

func (s *MemoryStore) ListOperations(_ context.Context, projectID, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[wlkey(projectID, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) DeleteOperation(_ context.Context, projectID, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := wlkey(projectID, location)
	if _, ok := s.operations[key][id]; !ok {
		return ErrNoSuchOperation
	}
	delete(s.operations[key], id)
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows = make(map[string]map[string]Workflow)
	s.executions = make(map[string]map[string]Execution)
	s.operations = make(map[string]map[string]Operation)
	s.revisions = make(map[string][]Revision)
}
