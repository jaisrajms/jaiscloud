package iceberg

import (
	"context"
	"sort"
	"sync"

	"jaiscloud/internal/gcp/storeutil"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	namespaces map[string]map[string]string // namespace → property map
	tables     map[string]map[string]Table  // namespace → name → table
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		namespaces: make(map[string]map[string]string),
		tables:     make(map[string]map[string]Table),
	}
}

func (s *MemoryStore) CreateNamespace(_ context.Context, namespace string, properties map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.namespaces[namespace]; ok {
		return ErrNamespaceExists
	}
	if properties == nil {
		properties = map[string]string{}
	}
	s.namespaces[namespace] = properties
	return nil
}

func (s *MemoryStore) GetNamespace(_ context.Context, namespace string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	props, ok := s.namespaces[namespace]
	if !ok {
		return nil, ErrNamespaceNotFound
	}
	return props, nil
}

func (s *MemoryStore) ListNamespaces(_ context.Context) ([]Namespace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Namespace, 0, len(s.namespaces))
	for ns, props := range s.namespaces {
		result = append(result, Namespace{Namespace: ns, Properties: props})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Namespace < result[j].Namespace })
	return result, nil
}

func (s *MemoryStore) NamespaceExists(_ context.Context, namespace string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.namespaces[namespace]
	return ok, nil
}

func (s *MemoryStore) UpdateNamespaceProperties(_ context.Context, namespace string, removals []string, updates map[string]string) (NamespacePropertiesUpdate, error) {
	var before map[string]string
	after, err := storeutil.AtomicUpdate(&s.mu,
		func() (map[string]string, bool) { p, ok := s.namespaces[namespace]; return p, ok },
		func(current map[string]string, exists bool) (map[string]string, error) {
			if !exists {
				return nil, ErrNamespaceNotFound
			}
			before = current
			next := make(map[string]string, len(current)+len(updates))
			for k, v := range current {
				next[k] = v
			}
			for _, r := range removals {
				delete(next, r)
			}
			for k, v := range updates {
				next[k] = v
			}
			return next, nil
		},
		func(next map[string]string) { s.namespaces[namespace] = next },
	)
	if err != nil {
		return NamespacePropertiesUpdate{}, err
	}
	return NamespacePropertiesUpdate{Before: before, After: after}, nil
}

func (s *MemoryStore) DropNamespace(_ context.Context, namespace string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.namespaces[namespace]; !ok {
		return ErrNamespaceNotFound
	}
	if m := s.tables[namespace]; len(m) > 0 {
		return ErrNamespaceNotEmpty
	}
	delete(s.namespaces, namespace)
	return nil
}

func (s *MemoryStore) CreateTable(_ context.Context, namespace, name string, t Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.namespaces[namespace]; !ok {
		return ErrNamespaceNotFound
	}
	if s.tables[namespace] == nil {
		s.tables[namespace] = make(map[string]Table)
	}
	if _, ok := s.tables[namespace][name]; ok {
		return ErrTableExists
	}
	t.Namespace = namespace
	t.Name = name
	s.tables[namespace][name] = t
	return nil
}

func (s *MemoryStore) GetTable(_ context.Context, namespace, name string) (Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[namespace][name]
	if !ok {
		return Table{}, ErrTableNotFound
	}
	return t, nil
}

func (s *MemoryStore) ListTables(_ context.Context, namespace string) ([]Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.tables[namespace]
	result := make([]Table, 0, len(m))
	for _, t := range m {
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) DropTable(_ context.Context, namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[namespace][name]; !ok {
		return ErrTableNotFound
	}
	delete(s.tables[namespace], name)
	return nil
}

func (s *MemoryStore) CommitTable(_ context.Context, namespace, name string, mutate func(Table) (Table, error)) (Table, error) {
	return storeutil.AtomicUpdate(&s.mu,
		func() (Table, bool) { t, ok := s.tables[namespace][name]; return t, ok },
		func(current Table, exists bool) (Table, error) {
			if !exists {
				return Table{}, ErrTableNotFound
			}
			return mutate(current)
		},
		func(next Table) {
			next.Namespace = namespace
			next.Name = name
			s.tables[namespace][name] = next
		},
	)
}

func (s *MemoryStore) RenameTable(_ context.Context, srcNamespace, srcName, dstNamespace, dstName string) (Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.tables[srcNamespace][srcName]
	if !ok {
		return Table{}, ErrTableNotFound
	}
	if _, ok := s.namespaces[dstNamespace]; !ok {
		return Table{}, ErrNamespaceNotFound
	}
	if _, exists := s.tables[dstNamespace][dstName]; exists {
		return Table{}, ErrTableExists
	}
	delete(s.tables[srcNamespace], srcName)
	if s.tables[dstNamespace] == nil {
		s.tables[dstNamespace] = make(map[string]Table)
	}
	src.Namespace = dstNamespace
	src.Name = dstName
	s.tables[dstNamespace][dstName] = src
	return src, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.namespaces = make(map[string]map[string]string)
	s.tables = make(map[string]map[string]Table)
}
