package secretmanager

import (
	"context"
	"sort"
	"sync"

	"jaiscloud/internal/gcp/storeutil"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu       sync.RWMutex
	secrets  map[string]map[string]Secret  // projectID → secretID → secret
	versions map[string]map[string]Version // projectID+"/"+secretID → versionID → version
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		secrets:  make(map[string]map[string]Secret),
		versions: make(map[string]map[string]Version),
	}
}

func vkey(projectID, secretID string) string { return projectID + "/" + secretID }

func (s *MemoryStore) CreateSecret(_ context.Context, projectID, id string, sec Secret) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.secrets[projectID] == nil {
		s.secrets[projectID] = make(map[string]Secret)
	}
	if _, ok := s.secrets[projectID][id]; ok {
		return ErrAlreadyExists
	}
	s.secrets[projectID][id] = sec
	return nil
}

func (s *MemoryStore) GetSecret(_ context.Context, projectID, id string) (Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.secrets[projectID][id]
	if !ok {
		return Secret{}, ErrNoSuchSecret
	}
	return sec, nil
}

func (s *MemoryStore) UpdateSecret(_ context.Context, projectID, id string, sec Secret) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.secrets[projectID][id]; !ok {
		return ErrNoSuchSecret
	}
	s.secrets[projectID][id] = sec
	return nil
}

func (s *MemoryStore) UpdateSecretAtomic(_ context.Context, projectID, id string, mutate func(Secret) (Secret, error)) (Secret, error) {
	return storeutil.AtomicUpdate(&s.mu,
		func() (Secret, bool) { sec, ok := s.secrets[projectID][id]; return sec, ok },
		func(current Secret, exists bool) (Secret, error) {
			if !exists {
				return Secret{}, ErrNoSuchSecret
			}
			return mutate(current)
		},
		func(sec Secret) { s.secrets[projectID][id] = sec },
	)
}

func (s *MemoryStore) DeleteSecret(_ context.Context, projectID, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.secrets[projectID][id]; !ok {
		return ErrNoSuchSecret
	}
	delete(s.secrets[projectID], id)
	delete(s.versions, vkey(projectID, id))
	return nil
}

// DeleteSecretIf deletes under the store lock, running cond against the value
// the delete replaces, so a concurrent UpdateSecret/NextVersion cannot land
// between the precondition check and the delete.
func (s *MemoryStore) DeleteSecretIf(_ context.Context, projectID, id string, cond func(Secret) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, ok := s.secrets[projectID][id]
	if !ok {
		return ErrNoSuchSecret
	}
	if cond != nil {
		if err := cond(sec); err != nil {
			return err
		}
	}
	delete(s.secrets[projectID], id)
	delete(s.versions, vkey(projectID, id))
	return nil
}

func (s *MemoryStore) ListSecrets(_ context.Context, projectID string) ([]Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.secrets[projectID]
	result := make([]Secret, 0, len(m))
	for _, sec := range m {
		result = append(result, sec)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) CreateVersion(_ context.Context, projectID string, v Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := vkey(projectID, v.SecretID)
	if s.versions[key] == nil {
		s.versions[key] = make(map[string]Version)
	}
	s.versions[key][v.VersionID] = v
	return nil
}

func (s *MemoryStore) GetVersion(_ context.Context, projectID, secretID, versionID string) (Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.versions[vkey(projectID, secretID)][versionID]
	if !ok {
		return Version{}, ErrNoSuchVersion
	}
	return v, nil
}

func (s *MemoryStore) ListVersions(_ context.Context, projectID, secretID string) ([]Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.versions[vkey(projectID, secretID)]
	result := make([]Version, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].VersionID < result[j].VersionID })
	return result, nil
}

func (s *MemoryStore) UpdateVersion(_ context.Context, projectID string, v Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := vkey(projectID, v.SecretID)
	if _, ok := s.versions[key][v.VersionID]; !ok {
		return ErrNoSuchVersion
	}
	s.versions[key][v.VersionID] = v
	return nil
}

// UpdateVersionIf applies a lifecycle transition under the store lock, running
// mutate against the value the write replaces, so a concurrent transition
// cannot land between the precondition check and the update.
func (s *MemoryStore) UpdateVersionIf(_ context.Context, projectID, secretID, versionID string, mutate func(Version) (Version, error)) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := vkey(projectID, secretID)
	cur, ok := s.versions[key][versionID]
	if !ok {
		return Version{}, ErrNoSuchVersion
	}
	next, err := mutate(cur)
	if err != nil {
		return Version{}, err
	}
	// Identity is the address, not a mutation: never let mutate move the record.
	next.SecretID, next.VersionID = cur.SecretID, cur.VersionID
	s.versions[key][versionID] = next
	return next, nil
}

// NextVersion atomically allocates and advances a secret's version counter.
func (s *MemoryStore) NextVersion(_ context.Context, projectID, secretID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, ok := s.secrets[projectID][secretID]
	if !ok {
		return 0, ErrNoSuchSecret
	}
	v := sec.NextVer
	sec.NextVer++
	s.secrets[projectID][secretID] = sec
	return v, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets = make(map[string]map[string]Secret)
	s.versions = make(map[string]map[string]Version)
}
