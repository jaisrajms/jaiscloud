package dataproc

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	clusters   map[string]map[string]Cluster          // projectID+"/"+region → name → cluster
	jobs       map[string]map[string]Job              // projectID+"/"+region → id → job
	operations map[string]map[string]Operation        // projectID+"/"+region → id → operation
	templates  map[string]map[string]WorkflowTemplate // projectID+"/"+region → templateID → template
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		clusters:   make(map[string]map[string]Cluster),
		jobs:       make(map[string]map[string]Job),
		operations: make(map[string]map[string]Operation),
		templates:  make(map[string]map[string]WorkflowTemplate),
	}
}

func scopeKey(projectID, region string) string { return projectID + "/" + region }

func (s *MemoryStore) CreateCluster(_ context.Context, projectID, region string, c Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if s.clusters[key] == nil {
		s.clusters[key] = make(map[string]Cluster)
	}
	if _, ok := s.clusters[key][c.Name]; ok {
		return ErrAlreadyExists
	}
	c.ProjectID = projectID
	c.Region = region
	s.clusters[key][c.Name] = c
	return nil
}

func (s *MemoryStore) GetCluster(_ context.Context, projectID, region, name string) (Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clusters[scopeKey(projectID, region)][name]
	if !ok {
		return Cluster{}, ErrNoSuchCluster
	}
	return c, nil
}

func (s *MemoryStore) UpdateCluster(_ context.Context, projectID, region string, c Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if _, ok := s.clusters[key][c.Name]; !ok {
		return ErrNoSuchCluster
	}
	c.ProjectID = projectID
	c.Region = region
	s.clusters[key][c.Name] = c
	return nil
}

func (s *MemoryStore) UpdateClusterAtomic(_ context.Context, projectID, region, name string, mutate func(Cluster) (Cluster, error)) (Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	current, ok := s.clusters[key][name]
	if !ok {
		return Cluster{}, ErrNoSuchCluster
	}
	next, err := mutate(current)
	if err != nil {
		return Cluster{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	s.clusters[key][name] = next
	return next, nil
}

func (s *MemoryStore) DeleteCluster(_ context.Context, projectID, region, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if _, ok := s.clusters[key][name]; !ok {
		return ErrNoSuchCluster
	}
	delete(s.clusters[key], name)
	return nil
}

func (s *MemoryStore) ListClusters(_ context.Context, projectID, region string) ([]Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.clusters[scopeKey(projectID, region)]
	result := make([]Cluster, 0, len(m))
	for _, c := range m {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

// ListClustersByProject returns every cluster in a project across all regions,
// sorted by region then name.
func (s *MemoryStore) ListClustersByProject(_ context.Context, projectID string) ([]Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []Cluster
	for key, m := range s.clusters {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, c := range m {
			result = append(result, c)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Region != result[j].Region {
			return result[i].Region < result[j].Region
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (s *MemoryStore) CreateJob(_ context.Context, projectID, region string, j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if s.jobs[key] == nil {
		s.jobs[key] = make(map[string]Job)
	}
	if _, ok := s.jobs[key][j.JobID]; ok {
		return ErrAlreadyExists
	}
	j.ProjectID = projectID
	j.Region = region
	s.jobs[key][j.JobID] = j
	return nil
}

func (s *MemoryStore) GetJob(_ context.Context, projectID, region, jobID string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[scopeKey(projectID, region)][jobID]
	if !ok {
		return Job{}, ErrNoSuchJob
	}
	return j, nil
}

func (s *MemoryStore) UpdateJob(_ context.Context, projectID, region string, j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if _, ok := s.jobs[key][j.JobID]; !ok {
		return ErrNoSuchJob
	}
	j.ProjectID = projectID
	j.Region = region
	s.jobs[key][j.JobID] = j
	return nil
}

func (s *MemoryStore) UpdateJobAtomic(_ context.Context, projectID, region, jobID string, mutate func(Job) (Job, error)) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	current, ok := s.jobs[key][jobID]
	if !ok {
		return Job{}, ErrNoSuchJob
	}
	next, err := mutate(current)
	if err != nil {
		return Job{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	s.jobs[key][jobID] = next
	return next, nil
}

func (s *MemoryStore) DeleteJob(_ context.Context, projectID, region, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if _, ok := s.jobs[key][jobID]; !ok {
		return ErrNoSuchJob
	}
	delete(s.jobs[key], jobID)
	return nil
}

func (s *MemoryStore) ListJobs(_ context.Context, projectID, region string) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.jobs[scopeKey(projectID, region)]
	result := make([]Job, 0, len(m))
	for _, j := range m {
		result = append(result, j)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].JobID < result[j].JobID })
	return result, nil
}

// ListJobsByProject returns every job in a project across all regions, sorted
// by region then job id.
func (s *MemoryStore) ListJobsByProject(_ context.Context, projectID string) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []Job
	for key, m := range s.jobs {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, j := range m {
			result = append(result, j)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Region != result[j].Region {
			return result[i].Region < result[j].Region
		}
		return result[i].JobID < result[j].JobID
	})
	return result, nil
}

// --- Workflow templates ---

func (s *MemoryStore) CreateWorkflowTemplate(_ context.Context, projectID, region string, t WorkflowTemplate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if s.templates[key] == nil {
		s.templates[key] = make(map[string]WorkflowTemplate)
	}
	if _, ok := s.templates[key][t.TemplateID]; ok {
		return ErrAlreadyExists
	}
	t.ProjectID = projectID
	t.Region = region
	s.templates[key][t.TemplateID] = t
	return nil
}

func (s *MemoryStore) GetWorkflowTemplate(_ context.Context, projectID, region, templateID string) (WorkflowTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.templates[scopeKey(projectID, region)][templateID]
	if !ok {
		return WorkflowTemplate{}, ErrNoSuchWorkflowTemplate
	}
	return t, nil
}

func (s *MemoryStore) UpdateWorkflowTemplateAtomic(_ context.Context, projectID, region, templateID string, mutate func(WorkflowTemplate) (WorkflowTemplate, error)) (WorkflowTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	current, ok := s.templates[key][templateID]
	if !ok {
		return WorkflowTemplate{}, ErrNoSuchWorkflowTemplate
	}
	next, err := mutate(current)
	if err != nil {
		return WorkflowTemplate{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	s.templates[key][templateID] = next
	return next, nil
}

func (s *MemoryStore) DeleteWorkflowTemplate(_ context.Context, projectID, region, templateID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if _, ok := s.templates[key][templateID]; !ok {
		return ErrNoSuchWorkflowTemplate
	}
	delete(s.templates[key], templateID)
	return nil
}

func (s *MemoryStore) ListWorkflowTemplates(_ context.Context, projectID, region string) ([]WorkflowTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.templates[scopeKey(projectID, region)]
	result := make([]WorkflowTemplate, 0, len(m))
	for _, t := range m {
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TemplateID < result[j].TemplateID })
	return result, nil
}

// ListWorkflowTemplatesByProject returns every workflow template in a project
// across all regions, sorted by region then template id.
func (s *MemoryStore) ListWorkflowTemplatesByProject(_ context.Context, projectID string) ([]WorkflowTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []WorkflowTemplate
	for key, m := range s.templates {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, t := range m {
			result = append(result, t)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Region != result[j].Region {
			return result[i].Region < result[j].Region
		}
		return result[i].TemplateID < result[j].TemplateID
	})
	return result, nil
}

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, region string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if s.operations[key] == nil {
		s.operations[key] = make(map[string]Operation)
	}
	op.ProjectID = projectID
	op.Region = region
	s.operations[key][op.ID] = op
	return nil
}

func (s *MemoryStore) GetOperation(_ context.Context, projectID, region, id string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[scopeKey(projectID, region)][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) UpdateOperation(_ context.Context, projectID, region string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	if _, ok := s.operations[key][op.ID]; !ok {
		return ErrNoSuchOperation
	}
	op.ProjectID = projectID
	op.Region = region
	s.operations[key][op.ID] = op
	return nil
}

func (s *MemoryStore) UpdateOperationAtomic(_ context.Context, projectID, region, id string, mutate func(Operation) (Operation, error)) (Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scopeKey(projectID, region)
	current, ok := s.operations[key][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	next, err := mutate(current)
	if err != nil {
		return Operation{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	s.operations[key][id] = next
	return next, nil
}

// DeleteStaleOperations removes completed operations older than cutoff across
// all scopes. In-flight operations (Done=false) are retained: a poll still has
// to be able to advance and observe them.
func (s *MemoryStore) DeleteStaleOperations(_ context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for key, ops := range s.operations {
		for id, op := range ops {
			if op.Done && op.CreateTime.Before(cutoff) {
				delete(ops, id)
				n++
			}
		}
		if len(ops) == 0 {
			delete(s.operations, key)
		}
	}
	return n, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = make(map[string]map[string]Cluster)
	s.jobs = make(map[string]map[string]Job)
	s.operations = make(map[string]map[string]Operation)
	s.templates = make(map[string]map[string]WorkflowTemplate)
}
