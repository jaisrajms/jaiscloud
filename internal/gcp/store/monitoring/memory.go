package monitoring

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu          sync.RWMutex
	descriptors map[string]map[string]MetricDescriptor // project → type → descriptor
	series      map[string]map[string]*TimeSeries      // project → seriesKey → series
	policies    map[string]map[string]AlertPolicy      // project → id → policy
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		descriptors: make(map[string]map[string]MetricDescriptor),
		series:      make(map[string]map[string]*TimeSeries),
		policies:    make(map[string]map[string]AlertPolicy),
	}
}

func (s *MemoryStore) CreateMetricDescriptor(_ context.Context, project string, d MetricDescriptor) (MetricDescriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.descriptors[project] == nil {
		s.descriptors[project] = make(map[string]MetricDescriptor)
	}
	if existing, ok := s.descriptors[project][d.Type]; ok {
		d = mergeMetricDescriptor(existing, d)
	}
	s.descriptors[project][d.Type] = d
	return d, nil
}

func (s *MemoryStore) GetMetricDescriptor(_ context.Context, project, metricType string) (MetricDescriptor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.descriptors[project][metricType]
	if !ok {
		return MetricDescriptor{}, ErrMetricDescriptorNotFound
	}
	return d, nil
}

func (s *MemoryStore) ListMetricDescriptors(_ context.Context, project string) ([]MetricDescriptor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]MetricDescriptor, 0, len(s.descriptors[project]))
	for _, d := range s.descriptors[project] {
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Type < result[j].Type })
	return result, nil
}

func (s *MemoryStore) DeleteMetricDescriptor(_ context.Context, project, metricType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.descriptors[project][metricType]; !ok {
		return ErrMetricDescriptorNotFound
	}
	delete(s.descriptors[project], metricType)
	return nil
}

func (s *MemoryStore) CreateTimeSeries(_ context.Context, project string, ts TimeSeries) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := seriesKey(ts)
	if s.series[project] == nil {
		s.series[project] = make(map[string]*TimeSeries)
	}
	if existing, ok := s.series[project][key]; ok {
		existing.Points = append(existing.Points, ts.Points...)
		return nil
	}
	cp := ts
	s.series[project][key] = &cp
	return nil
}

func (s *MemoryStore) ListTimeSeries(_ context.Context, project string) ([]TimeSeries, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]TimeSeries, 0, len(s.series[project]))
	for _, ts := range s.series[project] {
		result = append(result, *ts)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].MetricType != result[j].MetricType {
			return result[i].MetricType < result[j].MetricType
		}
		return result[i].ResourceType < result[j].ResourceType
	})
	return result, nil
}

func (s *MemoryStore) CreateAlertPolicy(_ context.Context, project string, p AlertPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.policies[project][p.ID]; ok {
		return ErrAlertPolicyExists
	}
	if s.policies[project] == nil {
		s.policies[project] = make(map[string]AlertPolicy)
	}
	s.policies[project][p.ID] = p
	return nil
}

func (s *MemoryStore) GetAlertPolicy(_ context.Context, project, id string) (AlertPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[project][id]
	if !ok {
		return AlertPolicy{}, ErrAlertPolicyNotFound
	}
	return p, nil
}

func (s *MemoryStore) ListAlertPolicies(_ context.Context, project string) ([]AlertPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]AlertPolicy, 0, len(s.policies[project]))
	for _, p := range s.policies[project] {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) UpdateAlertPolicy(_ context.Context, project string, p AlertPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.policies[project][p.ID]; !ok {
		return ErrAlertPolicyNotFound
	}
	s.policies[project][p.ID] = p
	return nil
}

func (s *MemoryStore) UpdateAlertPolicyAtomic(_ context.Context, project, id string, mutate func(AlertPolicy) (AlertPolicy, error)) (AlertPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.policies[project][id]
	if !ok {
		return AlertPolicy{}, ErrAlertPolicyNotFound
	}
	next, err := mutate(current)
	if err != nil {
		return AlertPolicy{}, err
	}
	next.ID = id
	s.policies[project][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteAlertPolicy(_ context.Context, project, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.policies[project][id]; !ok {
		return ErrAlertPolicyNotFound
	}
	delete(s.policies[project], id)
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.descriptors = make(map[string]map[string]MetricDescriptor)
	s.series = make(map[string]map[string]*TimeSeries)
	s.policies = make(map[string]map[string]AlertPolicy)
}

// seriesKey returns a stable identity key for a time series, derived from its
// fully-specified metric and monitored resource. It is JSON (deterministic map
// ordering) so it can double as a TEXT column value in the Postgres store.
func seriesKey(ts TimeSeries) string {
	b, _ := json.Marshal(struct {
		MetricType     string            `json:"mt"`
		MetricLabels   map[string]string `json:"ml,omitempty"`
		ResourceType   string            `json:"rt"`
		ResourceLabels map[string]string `json:"rl,omitempty"`
	}{
		MetricType:     ts.MetricType,
		MetricLabels:   ts.MetricLabels,
		ResourceType:   ts.ResourceType,
		ResourceLabels: ts.ResourceLabels,
	})
	return string(b)
}
