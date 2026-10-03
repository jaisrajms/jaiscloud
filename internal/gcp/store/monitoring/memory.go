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
	descriptors map[string]map[string]MetricDescriptor      // project → type → descriptor
	series      map[string]map[string]*TimeSeries           // project → seriesKey → series
	policies    map[string]map[string]AlertPolicy           // project → id → policy
	channels    map[string]map[string]NotificationChannel   // project → id → channel
	incidents   map[string]map[string]Incident              // project → id → incident
	services    map[string]map[string]Service               // project → id → service
	slos        map[string]map[string]ServiceLevelObjective // project → "serviceID/id" → SLO
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		descriptors: make(map[string]map[string]MetricDescriptor),
		series:      make(map[string]map[string]*TimeSeries),
		policies:    make(map[string]map[string]AlertPolicy),
		channels:    make(map[string]map[string]NotificationChannel),
		incidents:   make(map[string]map[string]Incident),
		services:    make(map[string]map[string]Service),
		slos:        make(map[string]map[string]ServiceLevelObjective),
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

// ─── notification channels ────────────────────────────────────────────────────

func (s *MemoryStore) CreateNotificationChannel(_ context.Context, project string, c NotificationChannel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.channels[project][c.ID]; ok {
		return ErrNotificationChannelExists
	}
	if s.channels[project] == nil {
		s.channels[project] = make(map[string]NotificationChannel)
	}
	s.channels[project][c.ID] = c
	return nil
}

func (s *MemoryStore) GetNotificationChannel(_ context.Context, project, id string) (NotificationChannel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[project][id]
	if !ok {
		return NotificationChannel{}, ErrNotificationChannelNotFound
	}
	return c, nil
}

func (s *MemoryStore) ListNotificationChannels(_ context.Context, project string) ([]NotificationChannel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]NotificationChannel, 0, len(s.channels[project]))
	for _, c := range s.channels[project] {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) UpdateNotificationChannelAtomic(_ context.Context, project, id string, mutate func(NotificationChannel) (NotificationChannel, error)) (NotificationChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.channels[project][id]
	if !ok {
		return NotificationChannel{}, ErrNotificationChannelNotFound
	}
	next, err := mutate(current)
	if err != nil {
		return NotificationChannel{}, err
	}
	next.ID = id
	s.channels[project][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteNotificationChannel(_ context.Context, project, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.channels[project][id]; !ok {
		return ErrNotificationChannelNotFound
	}
	delete(s.channels[project], id)
	return nil
}

// ─── incidents ────────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateIncident(_ context.Context, inc Incident) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.incidents[inc.ProjectID][inc.ID]; ok && existing.State == IncidentOpen {
		return ErrIncidentExists
	}
	for _, existing := range s.incidents[inc.ProjectID] {
		if existing.State == IncidentOpen && existing.PolicyID == inc.PolicyID {
			return ErrIncidentExists
		}
	}
	if s.incidents[inc.ProjectID] == nil {
		s.incidents[inc.ProjectID] = make(map[string]Incident)
	}
	s.incidents[inc.ProjectID][inc.ID] = inc
	return nil
}

func (s *MemoryStore) GetIncident(_ context.Context, project, id string) (Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inc, ok := s.incidents[project][id]
	if !ok {
		return Incident{}, ErrIncidentNotFound
	}
	return inc, nil
}

func (s *MemoryStore) ListIncidents(_ context.Context, project string) ([]Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Incident, 0, len(s.incidents[project]))
	for _, inc := range s.incidents[project] {
		result = append(result, inc)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].StartedAt.Equal(result[j].StartedAt) {
			return result[i].StartedAt.Before(result[j].StartedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (s *MemoryStore) FindOpenIncident(_ context.Context, project, policyID string) (Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, inc := range s.incidents[project] {
		if inc.State == IncidentOpen && inc.PolicyID == policyID {
			return inc, nil
		}
	}
	return Incident{}, ErrIncidentNotFound
}

func (s *MemoryStore) UpdateIncidentAtomic(_ context.Context, project, id string, mutate func(Incident) (Incident, error)) (Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.incidents[project][id]
	if !ok {
		return Incident{}, ErrIncidentNotFound
	}
	next, err := mutate(current)
	if err != nil {
		return Incident{}, err
	}
	next.ID = id
	s.incidents[project][id] = next
	return next, nil
}

// ─── services ─────────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateService(_ context.Context, project string, svc Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[project][svc.ID]; ok {
		return ErrServiceExists
	}
	if s.services[project] == nil {
		s.services[project] = make(map[string]Service)
	}
	s.services[project][svc.ID] = svc
	return nil
}

func (s *MemoryStore) GetService(_ context.Context, project, id string) (Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	svc, ok := s.services[project][id]
	if !ok {
		return Service{}, ErrServiceNotFound
	}
	return svc, nil
}

func (s *MemoryStore) ListServices(_ context.Context, project string) ([]Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Service, 0, len(s.services[project]))
	for _, svc := range s.services[project] {
		result = append(result, svc)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) UpdateServiceAtomic(_ context.Context, project, id string, mutate func(Service) (Service, error)) (Service, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.services[project][id]
	if !ok {
		return Service{}, ErrServiceNotFound
	}
	next, err := mutate(current)
	if err != nil {
		return Service{}, err
	}
	next.ID = id
	s.services[project][id] = next
	return next, nil
}

func (s *MemoryStore) DeleteService(_ context.Context, project, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[project][id]; !ok {
		return ErrServiceNotFound
	}
	delete(s.services[project], id)
	for key, slo := range s.slos[project] {
		if slo.ServiceID == id {
			delete(s.slos[project], key)
		}
	}
	return nil
}

// ─── service level objectives ─────────────────────────────────────────────────

// sloKey is the in-memory map key for SLOs, namespacing the SLO id under its
// parent service id.
func sloKey(serviceID, id string) string { return serviceID + "/" + id }

func (s *MemoryStore) CreateServiceLevelObjective(_ context.Context, project string, slo ServiceLevelObjective) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sloKey(slo.ServiceID, slo.ID)
	if _, ok := s.slos[project][key]; ok {
		return ErrServiceLevelObjectiveExists
	}
	if s.slos[project] == nil {
		s.slos[project] = make(map[string]ServiceLevelObjective)
	}
	s.slos[project][key] = slo
	return nil
}

func (s *MemoryStore) GetServiceLevelObjective(_ context.Context, project, serviceID, id string) (ServiceLevelObjective, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slo, ok := s.slos[project][sloKey(serviceID, id)]
	if !ok {
		return ServiceLevelObjective{}, ErrServiceLevelObjectiveNotFound
	}
	return slo, nil
}

func (s *MemoryStore) ListServiceLevelObjectives(_ context.Context, project, serviceID string) ([]ServiceLevelObjective, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]ServiceLevelObjective, 0, len(s.slos[project]))
	for _, slo := range s.slos[project] {
		if slo.ServiceID == serviceID {
			result = append(result, slo)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) UpdateServiceLevelObjectiveAtomic(_ context.Context, project, serviceID, id string, mutate func(ServiceLevelObjective) (ServiceLevelObjective, error)) (ServiceLevelObjective, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sloKey(serviceID, id)
	current, ok := s.slos[project][key]
	if !ok {
		return ServiceLevelObjective{}, ErrServiceLevelObjectiveNotFound
	}
	next, err := mutate(current)
	if err != nil {
		return ServiceLevelObjective{}, err
	}
	next.ID = id
	next.ServiceID = serviceID
	s.slos[project][key] = next
	return next, nil
}

func (s *MemoryStore) DeleteServiceLevelObjective(_ context.Context, project, serviceID, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sloKey(serviceID, id)
	if _, ok := s.slos[project][key]; !ok {
		return ErrServiceLevelObjectiveNotFound
	}
	delete(s.slos[project], key)
	return nil
}

// ListProjects returns the distinct projects that hold any monitoring state.
func (s *MemoryStore) ListProjects(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[string]struct{})
	for p := range s.descriptors {
		seen[p] = struct{}{}
	}
	for p := range s.series {
		seen[p] = struct{}{}
	}
	for p := range s.policies {
		seen[p] = struct{}{}
	}
	for p := range s.channels {
		seen[p] = struct{}{}
	}
	for p := range s.incidents {
		seen[p] = struct{}{}
	}
	for p := range s.services {
		seen[p] = struct{}{}
	}
	for p := range s.slos {
		seen[p] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for p := range seen {
		result = append(result, p)
	}
	sort.Strings(result)
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.descriptors = make(map[string]map[string]MetricDescriptor)
	s.series = make(map[string]map[string]*TimeSeries)
	s.policies = make(map[string]map[string]AlertPolicy)
	s.channels = make(map[string]map[string]NotificationChannel)
	s.incidents = make(map[string]map[string]Incident)
	s.services = make(map[string]map[string]Service)
	s.slos = make(map[string]map[string]ServiceLevelObjective)
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
