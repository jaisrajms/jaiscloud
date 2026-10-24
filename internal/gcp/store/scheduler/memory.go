package scheduler

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]map[string]Job // projectID+"/"+location → job name → job
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]map[string]Job)}
}

func jobScope(projectID, location string) string { return projectID + "/" + location }

func (s *MemoryStore) CreateJob(_ context.Context, projectID, location string, j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobScope(projectID, location)
	if s.jobs[key] == nil {
		s.jobs[key] = make(map[string]Job)
	}
	if _, ok := s.jobs[key][j.Name]; ok {
		return ErrAlreadyExists
	}
	j.ProjectID = projectID
	j.Location = location
	s.jobs[key][j.Name] = j
	return nil
}

func (s *MemoryStore) GetJob(_ context.Context, projectID, location, name string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[jobScope(projectID, location)][name]
	if !ok {
		return Job{}, ErrNoSuchJob
	}
	return j, nil
}

func (s *MemoryStore) UpdateJob(_ context.Context, projectID, location string, j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobScope(projectID, location)
	if _, ok := s.jobs[key][j.Name]; !ok {
		return ErrNoSuchJob
	}
	j.ProjectID = projectID
	j.Location = location
	s.jobs[key][j.Name] = j
	return nil
}

func (s *MemoryStore) UpdateJobAtomic(_ context.Context, projectID, location, name string, mutate func(Job) (Job, error)) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobScope(projectID, location)
	current, ok := s.jobs[key][name]
	if !ok {
		return Job{}, ErrNoSuchJob
	}
	next, err := mutate(current)
	if err != nil {
		return Job{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	s.jobs[key][name] = next
	return next, nil
}

func (s *MemoryStore) DeleteJob(_ context.Context, projectID, location, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobScope(projectID, location)
	if _, ok := s.jobs[key][name]; !ok {
		return ErrNoSuchJob
	}
	delete(s.jobs[key], name)
	return nil
}

func (s *MemoryStore) ListJobs(_ context.Context, projectID, location string) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.jobs[jobScope(projectID, location)]
	result := make([]Job, 0, len(m))
	for _, j := range m {
		result = append(result, j)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

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
		if result[i].Location != result[j].Location {
			return result[i].Location < result[j].Location
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (s *MemoryStore) ListAllJobs(_ context.Context) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Job
	for _, m := range s.jobs {
		for _, j := range m {
			result = append(result, j)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ProjectID != result[j].ProjectID {
			return result[i].ProjectID < result[j].ProjectID
		}
		if result[i].Location != result[j].Location {
			return result[i].Location < result[j].Location
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = make(map[string]map[string]Job)
}
