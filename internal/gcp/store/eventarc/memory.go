package eventarc

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu       sync.RWMutex
	triggers map[string]map[string]Trigger // projectID+"/"+location → id → trigger
	channels map[string]map[string]Channel // projectID+"/"+location → id → channel
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		triggers: make(map[string]map[string]Trigger),
		channels: make(map[string]map[string]Channel),
	}
}

func scope(projectID, location string) string { return projectID + "/" + location }

// --- Triggers ---

func (s *MemoryStore) CreateTrigger(_ context.Context, projectID, location string, t Trigger) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if s.triggers[key] == nil {
		s.triggers[key] = make(map[string]Trigger)
	}
	if _, ok := s.triggers[key][t.Name]; ok {
		return ErrAlreadyExists
	}
	t.ProjectID = projectID
	t.Location = location
	s.triggers[key][t.Name] = t
	return nil
}

func (s *MemoryStore) GetTrigger(_ context.Context, projectID, location, id string) (Trigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.triggers[scope(projectID, location)][id]
	if !ok {
		return Trigger{}, ErrNoSuchTrigger
	}
	return t, nil
}

func (s *MemoryStore) UpdateTriggerAtomic(_ context.Context, projectID, location, id string, mutate func(Trigger) (Trigger, error)) (Trigger, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	current, ok := s.triggers[key][id]
	if !ok {
		return Trigger{}, ErrNoSuchTrigger
	}
	next, err := mutate(current)
	if err != nil {
		return Trigger{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	s.triggers[key][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteTrigger(_ context.Context, projectID, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if _, ok := s.triggers[key][id]; !ok {
		return ErrNoSuchTrigger
	}
	delete(s.triggers[key], id)
	return nil
}

func (s *MemoryStore) DeleteTriggerAtomic(_ context.Context, projectID, location, id string, guard func(Trigger) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	current, ok := s.triggers[key][id]
	if !ok {
		return ErrNoSuchTrigger
	}
	if err := guard(current); err != nil {
		return err
	}
	delete(s.triggers[key], id)
	return nil
}

func (s *MemoryStore) ListTriggers(_ context.Context, projectID, location string) ([]Trigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.triggers[scope(projectID, location)]
	result := make([]Trigger, 0, len(m))
	for _, t := range m {
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) ListTriggersAllLocations(_ context.Context, projectID string) ([]Trigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	var result []Trigger
	for key, m := range s.triggers {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, t := range m {
			result = append(result, t)
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

// --- Channels ---

func (s *MemoryStore) CreateChannel(_ context.Context, projectID, location string, c Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if s.channels[key] == nil {
		s.channels[key] = make(map[string]Channel)
	}
	if _, ok := s.channels[key][c.Name]; ok {
		return ErrAlreadyExists
	}
	c.ProjectID = projectID
	c.Location = location
	s.channels[key][c.Name] = c
	return nil
}

func (s *MemoryStore) GetChannel(_ context.Context, projectID, location, id string) (Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[scope(projectID, location)][id]
	if !ok {
		return Channel{}, ErrNoSuchChannel
	}
	return c, nil
}

func (s *MemoryStore) UpdateChannelAtomic(_ context.Context, projectID, location, id string, mutate func(Channel) (Channel, error)) (Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	current, ok := s.channels[key][id]
	if !ok {
		return Channel{}, ErrNoSuchChannel
	}
	next, err := mutate(current)
	if err != nil {
		return Channel{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	s.channels[key][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteChannel(_ context.Context, projectID, location, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if _, ok := s.channels[key][id]; !ok {
		return ErrNoSuchChannel
	}
	delete(s.channels[key], id)
	return nil
}

func (s *MemoryStore) DeleteChannelAtomic(_ context.Context, projectID, location, id string, guard func(Channel) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	current, ok := s.channels[key][id]
	if !ok {
		return ErrNoSuchChannel
	}
	if err := guard(current); err != nil {
		return err
	}
	delete(s.channels[key], id)
	return nil
}

func (s *MemoryStore) ListChannels(_ context.Context, projectID, location string) ([]Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.channels[scope(projectID, location)]
	result := make([]Channel, 0, len(m))
	for _, c := range m {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.triggers = make(map[string]map[string]Trigger)
	s.channels = make(map[string]map[string]Channel)
}
