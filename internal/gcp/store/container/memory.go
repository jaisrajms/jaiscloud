package container

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	clusters   map[string]map[string]Cluster   // projectID+"/"+location → cluster name → cluster
	operations map[string]map[string]Operation // projectID+"/"+location → operation id → operation
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		clusters:   make(map[string]map[string]Cluster),
		operations: make(map[string]map[string]Operation),
	}
}

func scope(projectID, location string) string { return projectID + "/" + location }

func (s *MemoryStore) CreateCluster(_ context.Context, projectID, location string, c Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if s.clusters[key] == nil {
		s.clusters[key] = make(map[string]Cluster)
	}
	if _, ok := s.clusters[key][c.Name]; ok {
		return ErrAlreadyExists
	}
	c.ProjectID = projectID
	c.Location = location
	s.clusters[key][c.Name] = cloneCluster(c)
	return nil
}

func (s *MemoryStore) GetCluster(_ context.Context, projectID, location, name string) (Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clusters[scope(projectID, location)][name]
	if !ok {
		return Cluster{}, ErrNoSuchCluster
	}
	return cloneCluster(c), nil
}

func (s *MemoryStore) DeleteCluster(_ context.Context, projectID, location, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if _, ok := s.clusters[key][name]; !ok {
		return ErrNoSuchCluster
	}
	delete(s.clusters[key], name)
	return nil
}

func (s *MemoryStore) ListClusters(_ context.Context, projectID, location string) ([]Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.clusters[scope(projectID, location)]
	result := make([]Cluster, 0, len(m))
	for _, c := range m {
		result = append(result, cloneCluster(c))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) MutateCluster(_ context.Context, projectID, location, cluster string, fn func(*Cluster) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	stored, ok := s.clusters[key][cluster]
	if !ok {
		return ErrNoSuchCluster
	}
	// Operate on a deep copy so fn's in-place mutations (node pools and their
	// nested configs) never alias a slice/map already handed to a reader.
	c := cloneCluster(stored)
	if err := fn(&c); err != nil {
		return err
	}
	c.ProjectID = projectID
	c.Location = location
	c.Name = cluster
	s.clusters[key][cluster] = c
	return nil
}

// cloneCluster deep-copies a Cluster and its node pools so the memory store
// never hands out (or mutates in place) a shared backing slice/map.
func cloneCluster(c Cluster) Cluster {
	if c.NodePools != nil {
		pools := make([]NodePool, len(c.NodePools))
		for i, p := range c.NodePools {
			pools[i] = cloneNodePool(p)
		}
		c.NodePools = pools
	}
	c.ResourceLabels = cloneStringMap(c.ResourceLabels)
	if c.Locations != nil {
		c.Locations = append([]string(nil), c.Locations...)
	}
	if c.AddonsConfig != nil {
		a := *c.AddonsConfig
		if c.AddonsConfig.HttpLoadBalancing != nil {
			v := *c.AddonsConfig.HttpLoadBalancing
			a.HttpLoadBalancing = &v
		}
		if c.AddonsConfig.HorizontalPodAutoscaling != nil {
			v := *c.AddonsConfig.HorizontalPodAutoscaling
			a.HorizontalPodAutoscaling = &v
		}
		if c.AddonsConfig.NetworkPolicyConfig != nil {
			v := *c.AddonsConfig.NetworkPolicyConfig
			a.NetworkPolicyConfig = &v
		}
		c.AddonsConfig = &a
	}
	if c.LegacyAbac != nil {
		v := *c.LegacyAbac
		c.LegacyAbac = &v
	}
	if c.NetworkPolicy != nil {
		v := *c.NetworkPolicy
		c.NetworkPolicy = &v
	}
	if c.MaintenancePolicy != nil {
		mp := *c.MaintenancePolicy
		if c.MaintenancePolicy.Window != nil {
			w := *c.MaintenancePolicy.Window
			if c.MaintenancePolicy.Window.DailyMaintenanceWindow != nil {
				d := *c.MaintenancePolicy.Window.DailyMaintenanceWindow
				w.DailyMaintenanceWindow = &d
			}
			if c.MaintenancePolicy.Window.RecurringWindow != nil {
				r := *c.MaintenancePolicy.Window.RecurringWindow
				if c.MaintenancePolicy.Window.RecurringWindow.Window != nil {
					t := *c.MaintenancePolicy.Window.RecurringWindow.Window
					r.Window = &t
				}
				w.RecurringWindow = &r
			}
			w.MaintenanceExclusions = cloneTimeWindowMap(c.MaintenancePolicy.Window.MaintenanceExclusions)
			mp.Window = &w
		}
		c.MaintenancePolicy = &mp
	}
	return c
}

func cloneNodePool(p NodePool) NodePool {
	if p.Locations != nil {
		p.Locations = append([]string(nil), p.Locations...)
	}
	if p.Config != nil {
		n := *p.Config
		if p.Config.OauthScopes != nil {
			n.OauthScopes = append([]string(nil), p.Config.OauthScopes...)
		}
		if p.Config.Tags != nil {
			n.Tags = append([]string(nil), p.Config.Tags...)
		}
		n.Metadata = cloneStringMap(p.Config.Metadata)
		n.Labels = cloneStringMap(p.Config.Labels)
		n.ResourceLabels = cloneStringMap(p.Config.ResourceLabels)
		p.Config = &n
	}
	if p.Autoscaling != nil {
		v := *p.Autoscaling
		p.Autoscaling = &v
	}
	if p.Management != nil {
		v := *p.Management
		p.Management = &v
	}
	if p.UpgradeSettings != nil {
		v := *p.UpgradeSettings
		p.UpgradeSettings = &v
	}
	return p
}

func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneTimeWindowMap(m map[string]*TimeWindow) map[string]*TimeWindow {
	if m == nil {
		return nil
	}
	out := make(map[string]*TimeWindow, len(m))
	for k, v := range m {
		if v == nil {
			out[k] = nil
			continue
		}
		t := *v
		out[k] = &t
	}
	return out
}

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scope(projectID, location)
	if s.operations[key] == nil {
		s.operations[key] = make(map[string]Operation)
	}
	if _, ok := s.operations[key][op.Name]; ok {
		return ErrAlreadyExists
	}
	op.ProjectID = projectID
	op.Location = location
	s.operations[key][op.Name] = op
	return nil
}

func (s *MemoryStore) GetOperation(_ context.Context, projectID, location, name string) (Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[scope(projectID, location)][name]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) ListOperations(_ context.Context, projectID, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[scope(projectID, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = make(map[string]map[string]Cluster)
	s.operations = make(map[string]map[string]Operation)
}
