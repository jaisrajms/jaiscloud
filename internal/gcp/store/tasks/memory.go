package tasks

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu     sync.RWMutex
	queues map[string]map[string]Queue // projectID+"/"+location → queue name → queue
	tasks  map[string]map[string]Task  // projectID+"/"+location+"/"+queue → task name → task
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		queues: make(map[string]map[string]Queue),
		tasks:  make(map[string]map[string]Task),
	}
}

func queueScope(projectID, location string) string { return projectID + "/" + location }
func taskScope(projectID, location, queue string) string {
	return projectID + "/" + location + "/" + queue
}

func (s *MemoryStore) CreateQueue(_ context.Context, projectID, location string, q Queue) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := queueScope(projectID, location)
	if s.queues[key] == nil {
		s.queues[key] = make(map[string]Queue)
	}
	if _, ok := s.queues[key][q.Name]; ok {
		return ErrAlreadyExists
	}
	q.ProjectID = projectID
	q.Location = location
	s.queues[key][q.Name] = q
	return nil
}

func (s *MemoryStore) GetQueue(_ context.Context, projectID, location, name string) (Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.queues[queueScope(projectID, location)][name]
	if !ok {
		return Queue{}, ErrNoSuchQueue
	}
	return q, nil
}

func (s *MemoryStore) UpdateQueue(_ context.Context, projectID, location string, q Queue) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := queueScope(projectID, location)
	if _, ok := s.queues[key][q.Name]; !ok {
		return ErrNoSuchQueue
	}
	q.ProjectID = projectID
	q.Location = location
	s.queues[key][q.Name] = q
	return nil
}

func (s *MemoryStore) UpdateQueueAtomic(_ context.Context, projectID, location, name string, mutate func(Queue) (Queue, error)) (Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := queueScope(projectID, location)
	current, ok := s.queues[key][name]
	if !ok {
		return Queue{}, ErrNoSuchQueue
	}
	next, err := mutate(current)
	if err != nil {
		return Queue{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.Name = name
	s.queues[key][name] = next
	return next, nil
}

func (s *MemoryStore) DeleteQueue(_ context.Context, projectID, location, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := queueScope(projectID, location)
	if _, ok := s.queues[key][name]; !ok {
		return ErrNoSuchQueue
	}
	delete(s.queues[key], name)
	delete(s.tasks, taskScope(projectID, location, name))
	return nil
}

func (s *MemoryStore) ListQueues(_ context.Context, projectID, location string) ([]Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.queues[queueScope(projectID, location)]
	result := make([]Queue, 0, len(m))
	for _, q := range m {
		result = append(result, q)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) ListQueuesByProject(_ context.Context, projectID string) ([]Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []Queue
	for key, byName := range s.queues {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, q := range byName {
			result = append(result, q)
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

func (s *MemoryStore) ListAllQueues(_ context.Context) ([]Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Queue, 0)
	for _, byName := range s.queues {
		for _, q := range byName {
			result = append(result, q)
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

func (s *MemoryStore) CreateTask(_ context.Context, projectID, location, queue string, t Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := taskScope(projectID, location, queue)
	if s.tasks[key] == nil {
		s.tasks[key] = make(map[string]Task)
	}
	if _, ok := s.tasks[key][t.Name]; ok {
		return ErrAlreadyExists
	}
	t.ProjectID = projectID
	t.Location = location
	t.Queue = queue
	s.tasks[key][t.Name] = t
	return nil
}

func (s *MemoryStore) GetTask(_ context.Context, projectID, location, queue, name string) (Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[taskScope(projectID, location, queue)][name]
	if !ok {
		return Task{}, ErrNoSuchTask
	}
	return t, nil
}

func (s *MemoryStore) UpdateTaskAtomic(_ context.Context, projectID, location, queue, name string, mutate func(Task) (Task, error)) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := taskScope(projectID, location, queue)
	current, ok := s.tasks[key][name]
	if !ok {
		return Task{}, ErrNoSuchTask
	}
	next, err := mutate(current)
	if err != nil {
		return Task{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.Queue = queue
	next.Name = name
	s.tasks[key][name] = next
	return next, nil
}

func (s *MemoryStore) DeleteTask(_ context.Context, projectID, location, queue, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := taskScope(projectID, location, queue)
	if _, ok := s.tasks[key][name]; !ok {
		return ErrNoSuchTask
	}
	delete(s.tasks[key], name)
	return nil
}

func (s *MemoryStore) ListTasks(_ context.Context, projectID, location, queue string) ([]Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.tasks[taskScope(projectID, location, queue)]
	result := make([]Task, 0, len(m))
	for _, t := range m {
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) DeleteTasks(_ context.Context, projectID, location, queue string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := taskScope(projectID, location, queue)
	n := len(s.tasks[key])
	delete(s.tasks, key)
	return n, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = make(map[string]map[string]Queue)
	s.tasks = make(map[string]map[string]Task)
}
