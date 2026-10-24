package logging

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	nextID     int64
	entries    map[string][]LogEntry              // scope → entries
	sinks      map[string]map[string]LogSink      // scope → name → sink
	exclusions map[string]map[string]LogExclusion // scope → name → exclusion
	metrics    map[string]map[string]LogMetric    // scope → name → metric
	// admin holds the Admin v2 registries (buckets/views/links/logScopes/
	// settings/cmek): "<collection>\x00<scope>" → id → record.
	admin        map[string]map[string]adminRecord
	adminRecords // shared registry semantics over the memory backend
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		entries:    make(map[string][]LogEntry),
		sinks:      make(map[string]map[string]LogSink),
		exclusions: make(map[string]map[string]LogExclusion),
		metrics:    make(map[string]map[string]LogMetric),
		admin:      make(map[string]map[string]adminRecord),
	}
	s.backend = s
	return s
}

// ─── admin backend ────────────────────────────────────────────────────────────

func adminKey(collection, scope string) string { return collection + "\x00" + scope }

func (s *MemoryStore) putAdmin(_ context.Context, collection, scope, id string, rec adminRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := adminKey(collection, scope)
	if s.admin[key] == nil {
		s.admin[key] = make(map[string]adminRecord)
	}
	s.admin[key][id] = rec
	return nil
}

func (s *MemoryStore) getAdmin(_ context.Context, collection, scope, id string) (adminRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.admin[adminKey(collection, scope)][id]
	return rec, ok, nil
}

func (s *MemoryStore) listAdmin(_ context.Context, collection, scope string) (map[string]adminRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.admin[adminKey(collection, scope)]
	out := make(map[string]adminRecord, len(src))
	for id, rec := range src {
		out[id] = rec
	}
	return out, nil
}

func (s *MemoryStore) deleteAdmin(_ context.Context, collection, scope, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := adminKey(collection, scope)
	if _, ok := s.admin[key][id]; !ok {
		return false, nil
	}
	delete(s.admin[key], id)
	return true, nil
}

func (s *MemoryStore) Write(_ context.Context, scope string, e LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.ID = s.nextID
	s.nextID++
	s.entries[scope] = append(s.entries[scope], e)
	return nil
}

func (s *MemoryStore) List(_ context.Context, scope string) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.entries[scope]
	result := make([]LogEntry, len(src))
	copy(result, src)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Timestamp.Equal(result[j].Timestamp) {
			return result[i].ID < result[j].ID
		}
		return result[i].Timestamp.Before(result[j].Timestamp)
	})
	return result, nil
}

func (s *MemoryStore) ListLogs(_ context.Context, scope string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[string]struct{})
	for _, e := range s.entries[scope] {
		if e.LogName != "" {
			seen[e.LogName] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func (s *MemoryStore) DeleteLog(_ context.Context, scope, logName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.entries[scope]
	kept := src[:0]
	for _, e := range src {
		if e.LogName != logName {
			kept = append(kept, e)
		}
	}
	s.entries[scope] = kept
	return nil
}

// ─── sinks ────────────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateSink(_ context.Context, scope string, sink LogSink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sinks[scope] == nil {
		s.sinks[scope] = make(map[string]LogSink)
	}
	if _, ok := s.sinks[scope][sink.Name]; ok {
		return ErrSinkExists
	}
	s.sinks[scope][sink.Name] = sink
	return nil
}

func (s *MemoryStore) GetSink(_ context.Context, scope, name string) (LogSink, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sink, ok := s.sinks[scope][name]
	if !ok {
		return LogSink{}, ErrSinkNotFound
	}
	return sink, nil
}

func (s *MemoryStore) ListSinks(_ context.Context, scope string) ([]LogSink, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]LogSink, 0, len(s.sinks[scope]))
	for _, sink := range s.sinks[scope] {
		result = append(result, sink)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) UpdateSink(_ context.Context, scope string, sink LogSink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sinks[scope][sink.Name]; !ok {
		return ErrSinkNotFound
	}
	s.sinks[scope][sink.Name] = sink
	return nil
}

func (s *MemoryStore) DeleteSink(_ context.Context, scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sinks[scope][name]; !ok {
		return ErrSinkNotFound
	}
	delete(s.sinks[scope], name)
	return nil
}

// ─── exclusions ───────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateExclusion(_ context.Context, scope string, e LogExclusion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exclusions[scope] == nil {
		s.exclusions[scope] = make(map[string]LogExclusion)
	}
	if _, ok := s.exclusions[scope][e.Name]; ok {
		return ErrExclusionExists
	}
	s.exclusions[scope][e.Name] = e
	return nil
}

func (s *MemoryStore) GetExclusion(_ context.Context, scope, name string) (LogExclusion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.exclusions[scope][name]
	if !ok {
		return LogExclusion{}, ErrExclusionNotFound
	}
	return e, nil
}

func (s *MemoryStore) ListExclusions(_ context.Context, scope string) ([]LogExclusion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]LogExclusion, 0, len(s.exclusions[scope]))
	for _, e := range s.exclusions[scope] {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) UpdateExclusion(_ context.Context, scope string, e LogExclusion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.exclusions[scope][e.Name]; !ok {
		return ErrExclusionNotFound
	}
	s.exclusions[scope][e.Name] = e
	return nil
}

func (s *MemoryStore) DeleteExclusion(_ context.Context, scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.exclusions[scope][name]; !ok {
		return ErrExclusionNotFound
	}
	delete(s.exclusions[scope], name)
	return nil
}

// ─── metrics ──────────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateMetric(_ context.Context, scope string, m LogMetric) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics[scope] == nil {
		s.metrics[scope] = make(map[string]LogMetric)
	}
	if _, ok := s.metrics[scope][m.Name]; ok {
		return ErrMetricExists
	}
	s.metrics[scope][m.Name] = cloneMetric(m)
	return nil
}

func (s *MemoryStore) GetMetric(_ context.Context, scope, name string) (LogMetric, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.metrics[scope][name]
	if !ok {
		return LogMetric{}, ErrMetricNotFound
	}
	return cloneMetric(m), nil
}

func (s *MemoryStore) ListMetrics(_ context.Context, scope string) ([]LogMetric, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]LogMetric, 0, len(s.metrics[scope]))
	for _, m := range s.metrics[scope] {
		result = append(result, cloneMetric(m))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) UpdateMetric(_ context.Context, scope string, m LogMetric) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.metrics[scope][m.Name]; !ok {
		return ErrMetricNotFound
	}
	s.metrics[scope][m.Name] = cloneMetric(m)
	return nil
}

func (s *MemoryStore) DeleteMetric(_ context.Context, scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.metrics[scope][name]; !ok {
		return ErrMetricNotFound
	}
	delete(s.metrics[scope], name)
	return nil
}

// cloneMetric deep-copies the slice-bearing fields so a stored metric cannot be
// mutated through the caller's maps/slices.
func cloneMetric(m LogMetric) LogMetric {
	if m.LabelExtractors != nil {
		labels := make(map[string]string, len(m.LabelExtractors))
		for k, v := range m.LabelExtractors {
			labels[k] = v
		}
		m.LabelExtractors = labels
	}
	if m.BucketOptions != nil {
		m.BucketOptions = append([]byte(nil), m.BucketOptions...)
	}
	if m.Descriptor.Labels != nil {
		m.Descriptor.Labels = append([]LogMetricLabel(nil), m.Descriptor.Labels...)
	}
	return m
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string][]LogEntry)
	s.sinks = make(map[string]map[string]LogSink)
	s.exclusions = make(map[string]map[string]LogExclusion)
	s.metrics = make(map[string]map[string]LogMetric)
	s.admin = make(map[string]map[string]adminRecord)
	s.nextID = 0
}
