package metastore

import (
	"context"
	"sort"
	"sync"

	"jaiscloud/internal/gcp/storeutil"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu              sync.RWMutex
	services        map[string]map[string]Service        // project+"/"+location → name → service
	backups         map[string]map[string]Backup         // project+"/"+location+"/"+service → name → backup
	metadataImports map[string]map[string]MetadataImport // project+"/"+location+"/"+service → name → import
	operations      map[string]map[string]Operation      // project+"/"+location → id → operation
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		services:        make(map[string]map[string]Service),
		backups:         make(map[string]map[string]Backup),
		metadataImports: make(map[string]map[string]MetadataImport),
		operations:      make(map[string]map[string]Operation),
	}
}

func serviceScope(projectID, location string) string { return projectID + "/" + location }

func childScope(projectID, location, serviceName string) string {
	return projectID + "/" + location + "/" + serviceName
}

func (s *MemoryStore) CreateService(_ context.Context, projectID, location string, svc Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := serviceScope(projectID, location)
	if s.services[key] == nil {
		s.services[key] = make(map[string]Service)
	}
	if _, ok := s.services[key][svc.Name]; ok {
		return ErrAlreadyExists
	}
	svc.ProjectID = projectID
	svc.Location = location
	s.services[key][svc.Name] = svc
	return nil
}

func (s *MemoryStore) GetService(_ context.Context, projectID, location, name string) (Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	svc, ok := s.services[serviceScope(projectID, location)][name]
	if !ok {
		return Service{}, ErrNoSuchService
	}
	return svc, nil
}

func (s *MemoryStore) UpdateService(_ context.Context, projectID, location string, svc Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := serviceScope(projectID, location)
	if _, ok := s.services[key][svc.Name]; !ok {
		return ErrNoSuchService
	}
	svc.ProjectID = projectID
	svc.Location = location
	s.services[key][svc.Name] = svc
	return nil
}

func (s *MemoryStore) UpdateServiceAtomic(_ context.Context, projectID, location, name string, mutate func(Service) (Service, error)) (Service, error) {
	key := serviceScope(projectID, location)
	return storeutil.AtomicUpdate(&s.mu,
		func() (Service, bool) { svc, ok := s.services[key][name]; return svc, ok },
		func(current Service, exists bool) (Service, error) {
			if !exists {
				return Service{}, ErrNoSuchService
			}
			return mutate(current)
		},
		func(svc Service) {
			svc.ProjectID = projectID
			svc.Location = location
			s.services[key][name] = svc
		},
	)
}

func (s *MemoryStore) DeleteService(_ context.Context, projectID, location, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := serviceScope(projectID, location)
	if _, ok := s.services[key][name]; !ok {
		return ErrNoSuchService
	}
	delete(s.services[key], name)
	delete(s.backups, childScope(projectID, location, name))
	delete(s.metadataImports, childScope(projectID, location, name))
	return nil
}

func (s *MemoryStore) ListServices(_ context.Context, projectID, location string) ([]Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.services[serviceScope(projectID, location)]
	result := make([]Service, 0, len(m))
	for _, svc := range m {
		result = append(result, svc)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) CreateBackup(_ context.Context, projectID, location, serviceName string, b Backup) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := childScope(projectID, location, serviceName)
	if s.backups[key] == nil {
		s.backups[key] = make(map[string]Backup)
	}
	if _, ok := s.backups[key][b.Name]; ok {
		return ErrAlreadyExists
	}
	b.ProjectID = projectID
	b.Location = location
	b.ServiceName = serviceName
	s.backups[key][b.Name] = b
	return nil
}

func (s *MemoryStore) GetBackup(_ context.Context, projectID, location, serviceName, name string) (Backup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.backups[childScope(projectID, location, serviceName)][name]
	if !ok {
		return Backup{}, ErrNoSuchBackup
	}
	return b, nil
}

func (s *MemoryStore) DeleteBackup(_ context.Context, projectID, location, serviceName, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := childScope(projectID, location, serviceName)
	if _, ok := s.backups[key][name]; !ok {
		return ErrNoSuchBackup
	}
	delete(s.backups[key], name)
	return nil
}

func (s *MemoryStore) ListBackups(_ context.Context, projectID, location, serviceName string) ([]Backup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.backups[childScope(projectID, location, serviceName)]
	result := make([]Backup, 0, len(m))
	for _, b := range m {
		result = append(result, b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) CreateMetadataImport(_ context.Context, projectID, location, serviceName string, mi MetadataImport) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := childScope(projectID, location, serviceName)
	if s.metadataImports[key] == nil {
		s.metadataImports[key] = make(map[string]MetadataImport)
	}
	if _, ok := s.metadataImports[key][mi.Name]; ok {
		return ErrAlreadyExists
	}
	mi.ProjectID = projectID
	mi.Location = location
	mi.ServiceName = serviceName
	s.metadataImports[key][mi.Name] = mi
	return nil
}

func (s *MemoryStore) GetMetadataImport(_ context.Context, projectID, location, serviceName, name string) (MetadataImport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mi, ok := s.metadataImports[childScope(projectID, location, serviceName)][name]
	if !ok {
		return MetadataImport{}, ErrNoSuchMetadataImport
	}
	return mi, nil
}

func (s *MemoryStore) UpdateMetadataImport(_ context.Context, projectID, location, serviceName string, mi MetadataImport) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := childScope(projectID, location, serviceName)
	if _, ok := s.metadataImports[key][mi.Name]; !ok {
		return ErrNoSuchMetadataImport
	}
	mi.ProjectID = projectID
	mi.Location = location
	mi.ServiceName = serviceName
	s.metadataImports[key][mi.Name] = mi
	return nil
}

func (s *MemoryStore) UpdateMetadataImportAtomic(_ context.Context, projectID, location, serviceName, name string, mutate func(MetadataImport) (MetadataImport, error)) (MetadataImport, error) {
	key := childScope(projectID, location, serviceName)
	return storeutil.AtomicUpdate(&s.mu,
		func() (MetadataImport, bool) { mi, ok := s.metadataImports[key][name]; return mi, ok },
		func(current MetadataImport, exists bool) (MetadataImport, error) {
			if !exists {
				return MetadataImport{}, ErrNoSuchMetadataImport
			}
			return mutate(current)
		},
		func(mi MetadataImport) {
			mi.ProjectID = projectID
			mi.Location = location
			mi.ServiceName = serviceName
			s.metadataImports[key][name] = mi
		},
	)
}

func (s *MemoryStore) ListMetadataImports(_ context.Context, projectID, location, serviceName string) ([]MetadataImport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.metadataImports[childScope(projectID, location, serviceName)]
	result := make([]MetadataImport, 0, len(m))
	for _, mi := range m {
		result = append(result, mi)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) CreateOperation(_ context.Context, projectID, location string, op Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := serviceScope(projectID, location)
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
	op, ok := s.operations[serviceScope(projectID, location)][id]
	if !ok {
		return Operation{}, ErrNoSuchOperation
	}
	return op, nil
}

func (s *MemoryStore) ListOperations(_ context.Context, projectID, location string) ([]Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.operations[serviceScope(projectID, location)]
	result := make([]Operation, 0, len(m))
	for _, op := range m {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = make(map[string]map[string]Service)
	s.backups = make(map[string]map[string]Backup)
	s.metadataImports = make(map[string]map[string]MetadataImport)
	s.operations = make(map[string]map[string]Operation)
}
