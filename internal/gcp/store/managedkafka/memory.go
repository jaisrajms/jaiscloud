package managedkafka

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	clusters   map[string]map[string]Cluster   // projectID+"/"+location → name → cluster
	topics     map[string]map[string]Topic     // projectID+"/"+location+"/"+cluster → name → topic
	operations map[string]map[string]Operation // projectID+"/"+location → id → operation
	acls       map[string]map[string]Acl       // projectID+"/"+location+"/"+cluster → aclId → acl
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		clusters:   make(map[string]map[string]Cluster),
		topics:     make(map[string]map[string]Topic),
		operations: make(map[string]map[string]Operation),
		acls:       make(map[string]map[string]Acl),
	}
}

func clusterScope(projectID, location string) string { return projectID + "/" + location }

func topicScope(projectID, location, clusterName string) string {
	return projectID + "/" + location + "/" + clusterName
}

func (s *MemoryStore) CreateCluster(_ context.Context, projectID, location string, c Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := clusterScope(projectID, location)
	if s.clusters[key] == nil {
		s.clusters[key] = make(map[string]Cluster)
	}
	if _, ok := s.clusters[key][c.Name]; ok {
		return ErrAlreadyExists
	}
	c.ProjectID = projectID
	c.Location = location
	s.clusters[key][c.Name] = c
	return nil
}

func (s *MemoryStore) GetCluster(_ context.Context, projectID, location, name string) (Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clusters[clusterScope(projectID, location)][name]
	if !ok {
		return Cluster{}, ErrNoSuchCluster
	}
	return c, nil
}

func (s *MemoryStore) UpdateCluster(_ context.Context, projectID, location string, c Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := clusterScope(projectID, location)
	if _, ok := s.clusters[key][c.Name]; !ok {
		return ErrNoSuchCluster
	}
	c.ProjectID = projectID
	c.Location = location
	s.clusters[key][c.Name] = c
	return nil
}

func (s *MemoryStore) UpdateClusterAtomic(_ context.Context, projectID, location, name string, mutate func(Cluster) (Cluster, error)) (Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := clusterScope(projectID, location)
	current, ok := s.clusters[key][name]
	if !ok {
		return Cluster{}, ErrNoSuchCluster
	}
	next, err := mutate(current)
	if err != nil {
		return Cluster{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	s.clusters[key][name] = next
	return next, nil
}

func (s *MemoryStore) DeleteCluster(_ context.Context, projectID, location, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := clusterScope(projectID, location)
	if _, ok := s.clusters[key][name]; !ok {
		return ErrNoSuchCluster
	}
	delete(s.clusters[key], name)
	delete(s.topics, topicScope(projectID, location, name))
	delete(s.acls, topicScope(projectID, location, name))
	return nil
}

func (s *MemoryStore) ListClusters(_ context.Context, projectID, location string) ([]Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.clusters[clusterScope(projectID, location)]
	result := make([]Cluster, 0, len(m))
	for _, c := range m {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) CreateTopic(_ context.Context, projectID, location, clusterName string, t Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	if s.topics[key] == nil {
		s.topics[key] = make(map[string]Topic)
	}
	if _, ok := s.topics[key][t.Name]; ok {
		return ErrAlreadyExists
	}
	t.ProjectID = projectID
	t.Location = location
	t.ClusterName = clusterName
	s.topics[key][t.Name] = t
	return nil
}

func (s *MemoryStore) GetTopic(_ context.Context, projectID, location, clusterName, topicName string) (Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[topicScope(projectID, location, clusterName)][topicName]
	if !ok {
		return Topic{}, ErrNoSuchTopic
	}
	return t, nil
}

func (s *MemoryStore) UpdateTopic(_ context.Context, projectID, location, clusterName string, t Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	if _, ok := s.topics[key][t.Name]; !ok {
		return ErrNoSuchTopic
	}
	t.ProjectID = projectID
	t.Location = location
	t.ClusterName = clusterName
	s.topics[key][t.Name] = t
	return nil
}

func (s *MemoryStore) UpdateTopicAtomic(_ context.Context, projectID, location, clusterName, topicName string, mutate func(Topic) (Topic, error)) (Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	current, ok := s.topics[key][topicName]
	if !ok {
		return Topic{}, ErrNoSuchTopic
	}
	next, err := mutate(current)
	if err != nil {
		return Topic{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.ClusterName = clusterName
	s.topics[key][topicName] = next
	return next, nil
}

func (s *MemoryStore) DeleteTopic(_ context.Context, projectID, location, clusterName, topicName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	if _, ok := s.topics[key][topicName]; !ok {
		return ErrNoSuchTopic
	}
	delete(s.topics[key], topicName)
	return nil
}

func (s *MemoryStore) ListTopics(_ context.Context, projectID, location, clusterName string) ([]Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.topics[topicScope(projectID, location, clusterName)]
	result := make([]Topic, 0, len(m))
	for _, t := range m {
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := clusterScope(projectID, location)
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
	op, ok := s.operations[clusterScope(projectID, location)][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) ListOperations(_ context.Context, projectID, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[clusterScope(projectID, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// --- ACLs ---

func (s *MemoryStore) CreateAcl(_ context.Context, projectID, location, clusterName string, a Acl) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	if s.acls[key] == nil {
		s.acls[key] = make(map[string]Acl)
	}
	if _, ok := s.acls[key][a.Name]; ok {
		return ErrAlreadyExists
	}
	a.ProjectID = projectID
	a.Location = location
	a.ClusterName = clusterName
	s.acls[key][a.Name] = a
	return nil
}

func (s *MemoryStore) GetAcl(_ context.Context, projectID, location, clusterName, name string) (Acl, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.acls[topicScope(projectID, location, clusterName)][name]
	if !ok {
		return Acl{}, ErrNoSuchAcl
	}
	return a, nil
}

func (s *MemoryStore) UpdateAcl(_ context.Context, projectID, location, clusterName string, a Acl) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	if _, ok := s.acls[key][a.Name]; !ok {
		return ErrNoSuchAcl
	}
	a.ProjectID = projectID
	a.Location = location
	a.ClusterName = clusterName
	s.acls[key][a.Name] = a
	return nil
}

func (s *MemoryStore) UpdateAclAtomic(_ context.Context, projectID, location, clusterName, name string, mutate func(Acl) (Acl, error)) (Acl, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	current, ok := s.acls[key][name]
	if !ok {
		return Acl{}, ErrNoSuchAcl
	}
	next, err := mutate(current)
	if err != nil {
		return Acl{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.ClusterName = clusterName
	s.acls[key][name] = next
	return next, nil
}

func (s *MemoryStore) DeleteAcl(_ context.Context, projectID, location, clusterName, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicScope(projectID, location, clusterName)
	if _, ok := s.acls[key][name]; !ok {
		return ErrNoSuchAcl
	}
	delete(s.acls[key], name)
	return nil
}

func (s *MemoryStore) ListAcls(_ context.Context, projectID, location, clusterName string) ([]Acl, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.acls[topicScope(projectID, location, clusterName)]
	result := make([]Acl, 0, len(m))
	for _, a := range m {
		result = append(result, a)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = make(map[string]map[string]Cluster)
	s.topics = make(map[string]map[string]Topic)
	s.operations = make(map[string]map[string]Operation)
	s.acls = make(map[string]map[string]Acl)
}
