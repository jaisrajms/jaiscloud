package dataproc

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	clusters   map[string]map[string]Cluster   // projectID+"/"+region → name → cluster
	jobs       map[string]map[string]Job       // projectID+"/"+region → id → job
	operations map[string]map[string]Operation // projectID+"/"+region → id → operation
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		clusters:   make(map[string]map[string]Cluster),
		jobs:       make(map[string]map[string]Job),
		operations: make(map[string]map[string]Operation),
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

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = make(map[string]map[string]Cluster)
	s.jobs = make(map[string]map[string]Job)
	s.operations = make(map[string]map[string]Operation)
}
