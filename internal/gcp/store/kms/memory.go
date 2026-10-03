package kms

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"jaiscloud/internal/gcp/storeutil"
)

const defaultAlgorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	keyrings   map[string]map[string]KeyRing   // projectID+"/"+location → keyringID → KeyRing
	cryptokeys map[string]map[string]CryptoKey // projectID+"/"+location+"/"+keyringID → keyID → CryptoKey
	versions   map[string]map[string]Version   // projectID+"/"+location+"/"+keyringID+"/"+keyID → version → Version
	serverDEK  []byte                          // server DEK protecting key material at rest
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		keyrings:   make(map[string]map[string]KeyRing),
		cryptokeys: make(map[string]map[string]CryptoKey),
		versions:   make(map[string]map[string]Version),
	}
}

func krKey(projectID, location string) string { return projectID + "/" + location }
func ckKey(projectID, location, keyringID string) string {
	return projectID + "/" + location + "/" + keyringID
}
func vKey(projectID, location, keyringID, keyID string) string {
	return ckKey(projectID, location, keyringID) + "/" + keyID
}

// dek lazily generates and caches the server DEK.
func (s *MemoryStore) dek() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverDEK == nil {
		dek, err := Generate32()
		if err != nil {
			return nil, err
		}
		s.serverDEK = dek
	}
	return s.serverDEK, nil
}

func (s *MemoryStore) ServerDEK(ctx context.Context) ([]byte, error) {
	return s.dek()
}

func (s *MemoryStore) CreateKeyRing(_ context.Context, projectID, location, id string, kr KeyRing) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := krKey(projectID, location)
	if s.keyrings[key] == nil {
		s.keyrings[key] = make(map[string]KeyRing)
	}
	if _, ok := s.keyrings[key][id]; ok {
		return ErrAlreadyExists
	}
	s.keyrings[key][id] = kr
	return nil
}

func (s *MemoryStore) GetKeyRing(_ context.Context, projectID, location, id string) (KeyRing, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	kr, ok := s.keyrings[krKey(projectID, location)][id]
	if !ok {
		return KeyRing{}, ErrNoSuchKeyRing
	}
	return kr, nil
}

func (s *MemoryStore) ListKeyRings(_ context.Context, projectID, location string) ([]KeyRing, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.keyrings[krKey(projectID, location)]
	result := make([]KeyRing, 0, len(m))
	for _, kr := range m {
		result = append(result, kr)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *MemoryStore) CreateCryptoKey(_ context.Context, projectID, location, keyringID, id string, ck CryptoKey) error {
	dek, err := s.dek()
	if err != nil {
		return err
	}
	v, err := wrapVersionMaterial(dek, id, ck.Algorithm)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := ckKey(projectID, location, keyringID)
	if s.cryptokeys[key] == nil {
		s.cryptokeys[key] = make(map[string]CryptoKey)
	}
	if _, ok := s.cryptokeys[key][id]; ok {
		return ErrAlreadyExists
	}
	ck.PrimaryVersion = "1"
	if ck.Algorithm == "" {
		ck.Algorithm = v.Algorithm
	}
	s.cryptokeys[key][id] = ck

	vk := vKey(projectID, location, keyringID, id)
	v.KeyID = id
	v.Version = "1"
	v.State = "ENABLED"
	v.CreateTime = ck.CreateTime
	s.versions[vk] = map[string]Version{"1": v}
	return nil
}

func (s *MemoryStore) GetCryptoKey(_ context.Context, projectID, location, keyringID, id string) (CryptoKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ck, ok := s.cryptokeys[ckKey(projectID, location, keyringID)][id]
	if !ok {
		return CryptoKey{}, ErrNoSuchCryptoKey
	}
	return ck, nil
}

func (s *MemoryStore) ListCryptoKeys(_ context.Context, projectID, location, keyringID string) ([]CryptoKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.cryptokeys[ckKey(projectID, location, keyringID)]
	result := make([]CryptoKey, 0, len(m))
	for _, ck := range m {
		result = append(result, ck)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// UpdateCryptoKeyAtomic holds the store lock across the read-mutate-write so a
// concurrent update on the same key cannot be lost.
func (s *MemoryStore) UpdateCryptoKeyAtomic(_ context.Context, projectID, location, keyringID, id string, mutate func(CryptoKey) (CryptoKey, error)) (CryptoKey, error) {
	return storeutil.AtomicUpdate(&s.mu,
		func() (CryptoKey, bool) {
			ck, ok := s.cryptokeys[ckKey(projectID, location, keyringID)][id]
			return ck, ok
		},
		func(current CryptoKey, exists bool) (CryptoKey, error) {
			if !exists {
				return CryptoKey{}, ErrNoSuchCryptoKey
			}
			return mutate(current)
		},
		func(ck CryptoKey) { s.cryptokeys[ckKey(projectID, location, keyringID)][id] = ck },
	)
}

func (s *MemoryStore) CreateVersion(_ context.Context, projectID, location, keyringID, keyID string, v Version) (string, error) {
	dek, err := s.dek()
	if err != nil {
		return "", err
	}
	wrapped, err := wrapVersionMaterial(dek, keyID, v.Algorithm)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := ckKey(projectID, location, keyringID)
	if _, ok := s.cryptokeys[key][keyID]; !ok {
		return "", ErrNoSuchCryptoKey
	}
	vk := vKey(projectID, location, keyringID, keyID)
	if s.versions[vk] == nil {
		s.versions[vk] = make(map[string]Version)
	}
	next := 0
	for ver := range s.versions[vk] {
		if n, err := strconv.Atoi(ver); err == nil && n > next {
			next = n
		}
	}
	next++
	version := strconv.Itoa(next)
	v.KeyID = keyID
	v.Version = version
	if v.State == "" {
		v.State = "ENABLED"
	}
	v.Algorithm = wrapped.Algorithm
	v.KeyMaterial = wrapped.KeyMaterial
	v.PrivateKey = wrapped.PrivateKey
	v.PublicKey = wrapped.PublicKey
	s.versions[vk][version] = v
	return version, nil
}

func (s *MemoryStore) GetVersion(_ context.Context, projectID, location, keyringID, keyID, version string) (Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.versions[vKey(projectID, location, keyringID, keyID)][version]
	if !ok {
		return Version{}, ErrNoSuchVersion
	}
	return v, nil
}

func (s *MemoryStore) ListVersions(_ context.Context, projectID, location, keyringID, keyID string) ([]Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.versions[vKey(projectID, location, keyringID, keyID)]
	result := make([]Version, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return atoi(result[i].Version) < atoi(result[j].Version) })
	return result, nil
}

// UpdateVersionState applies an ENABLED/DISABLED transition and clears any
// destruction timestamps so the version carries no stale destroy_time.
func (s *MemoryStore) UpdateVersionState(_ context.Context, projectID, location, keyringID, keyID, version, state string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	vk := vKey(projectID, location, keyringID, keyID)
	v, ok := s.versions[vk][version]
	if !ok {
		return ErrNoSuchVersion
	}
	v.State = state
	v.DestroyTime = time.Time{}
	v.DestroyEventTime = time.Time{}
	s.versions[vk][version] = v
	return nil
}

// DestroyVersion schedules a version for destruction. DESTROY_SCHEDULED and
// DESTROYED are idempotent; only ENABLED and DISABLED may be destroyed.
func (s *MemoryStore) DestroyVersion(_ context.Context, projectID, location, keyringID, keyID, version string, destroyTime time.Time) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vk := vKey(projectID, location, keyringID, keyID)
	v, ok := s.versions[vk][version]
	if !ok {
		return Version{}, ErrNoSuchVersion
	}
	switch v.State {
	case "DESTROY_SCHEDULED", "DESTROYED":
		return v, nil
	case "ENABLED", "DISABLED":
		v.State = "DESTROY_SCHEDULED"
		v.DestroyTime = destroyTime
		v.DestroyEventTime = time.Time{}
		s.versions[vk][version] = v
		return v, nil
	default:
		return Version{}, ErrNotDestroyable
	}
}

// RestoreVersion reverses a scheduled destruction, moving DESTROY_SCHEDULED to
// DISABLED and clearing destroy_time.
func (s *MemoryStore) RestoreVersion(_ context.Context, projectID, location, keyringID, keyID, version string) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vk := vKey(projectID, location, keyringID, keyID)
	v, ok := s.versions[vk][version]
	if !ok {
		return Version{}, ErrNoSuchVersion
	}
	if v.State != "DESTROY_SCHEDULED" {
		return Version{}, ErrNotRestorable
	}
	v.State = "DISABLED"
	v.DestroyTime = time.Time{}
	v.DestroyEventTime = time.Time{}
	s.versions[vk][version] = v
	return v, nil
}

// PromoteDestroyed moves every DESTROY_SCHEDULED version of the key whose
// destroy window has elapsed to DESTROYED.
func (s *MemoryStore) PromoteDestroyed(_ context.Context, projectID, location, keyringID, keyID string, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.versions[vKey(projectID, location, keyringID, keyID)]
	promoted := 0
	for version, v := range m {
		if v.State == "DESTROY_SCHEDULED" && !v.DestroyTime.IsZero() && !now.Before(v.DestroyTime) {
			v.State = "DESTROYED"
			v.DestroyEventTime = v.DestroyTime
			v.DestroyTime = time.Time{}
			m[version] = v
			promoted++
		}
	}
	return promoted, nil
}

func (s *MemoryStore) UpdatePrimaryVersion(_ context.Context, projectID, location, keyringID, keyID, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ckKey(projectID, location, keyringID)
	ck, ok := s.cryptokeys[key][keyID]
	if !ok {
		return ErrNoSuchCryptoKey
	}
	if _, ok := s.versions[vKey(projectID, location, keyringID, keyID)][version]; !ok {
		return ErrNoSuchVersion
	}
	ck.PrimaryVersion = version
	s.cryptokeys[key][keyID] = ck
	return nil
}

func (s *MemoryStore) DeleteVersion(_ context.Context, projectID, location, keyringID, keyID, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	vk := vKey(projectID, location, keyringID, keyID)
	if _, ok := s.versions[vk][version]; !ok {
		return ErrNoSuchVersion
	}
	delete(s.versions[vk], version)
	return nil
}

func (s *MemoryStore) DeleteCryptoKey(_ context.Context, projectID, location, keyringID, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ckKey(projectID, location, keyringID)
	if _, ok := s.cryptokeys[key][id]; !ok {
		return ErrNoSuchCryptoKey
	}
	delete(s.cryptokeys[key], id)
	delete(s.versions, vKey(projectID, location, keyringID, id))
	return nil
}

func (s *MemoryStore) KeyMaterial(_ context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error) {
	dek, err := s.dek()
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	v, ok := s.versions[vKey(projectID, location, keyringID, keyID)][version]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrNoSuchVersion
	}
	if len(v.KeyMaterial) == 0 {
		return nil, ErrNoSuchVersion
	}
	return DecryptData(dek, v.KeyMaterial, []byte(keyID))
}

func (s *MemoryStore) PrivateKey(_ context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error) {
	dek, err := s.dek()
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	v, ok := s.versions[vKey(projectID, location, keyringID, keyID)][version]
	s.mu.RUnlock()
	if !ok || len(v.PrivateKey) == 0 {
		return nil, ErrNoSuchVersion
	}
	return DecryptData(dek, v.PrivateKey, []byte(keyID))
}

func (s *MemoryStore) PublicKey(_ context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error) {
	dek, err := s.dek()
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	v, ok := s.versions[vKey(projectID, location, keyringID, keyID)][version]
	s.mu.RUnlock()
	if !ok || len(v.PublicKey) == 0 {
		return nil, ErrNoSuchVersion
	}
	return DecryptData(dek, v.PublicKey, []byte(keyID))
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keyrings = make(map[string]map[string]KeyRing)
	s.cryptokeys = make(map[string]map[string]CryptoKey)
	s.versions = make(map[string]map[string]Version)
	s.serverDEK = nil
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
