package gcs

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"jaiscloud/internal/clock"
)

// MemoryObjectStore is an in-memory ObjectStore.
type MemoryObjectStore struct {
	mu      sync.RWMutex
	buckets map[string]map[string]any          // bucket name → meta
	objects map[string]map[string][]ObjectMeta // bucket → name → generations
	uploads map[string]ResumableSession
}

// NewMemoryObjectStore returns an empty in-memory store.
func NewMemoryObjectStore() *MemoryObjectStore {
	return &MemoryObjectStore{
		buckets: make(map[string]map[string]any),
		objects: make(map[string]map[string][]ObjectMeta),
		uploads: make(map[string]ResumableSession),
	}
}

func (s *MemoryObjectStore) CreateBucket(_ context.Context, projectID, name string, meta map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[name]; exists {
		return ErrAlreadyExists
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	meta["projectId"] = projectID
	s.buckets[name] = meta
	return nil
}

func (s *MemoryObjectStore) GetBucket(_ context.Context, name string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.buckets[name]
	if !ok {
		return nil, ErrNoSuchBucket
	}
	// Return a shallow copy so callers can't mutate the stored map.
	out := make(map[string]any, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out, nil
}

func (s *MemoryObjectStore) UpdateBucketMeta(_ context.Context, name string, meta map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[name]; !ok {
		return ErrNoSuchBucket
	}
	s.buckets[name] = meta
	return nil
}

func (s *MemoryObjectStore) DeleteBucket(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[name]; !ok {
		return ErrNoSuchBucket
	}
	if len(s.objects[name]) > 0 {
		return ErrBucketNotEmpty
	}
	delete(s.buckets, name)
	return nil
}

func (s *MemoryObjectStore) ListBuckets(_ context.Context, projectID string) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []map[string]any
	for name, meta := range s.buckets {
		if projectID != "" {
			if pid, _ := meta["projectId"].(string); pid != projectID {
				continue
			}
		}
		out := make(map[string]any, len(meta))
		for k, v := range meta {
			out[k] = v
		}
		out["name"] = name
		result = append(result, out)
	}
	sort.Slice(result, func(i, j int) bool { return result[i]["name"].(string) < result[j]["name"].(string) })
	return result, nil
}

func (s *MemoryObjectStore) PutObjectMeta(_ context.Context, bucket, name string, meta ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[bucket]; !ok {
		return ErrNoSuchBucket
	}
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	if s.objects[bucket] == nil {
		s.objects[bucket] = make(map[string][]ObjectMeta)
	}
	s.objects[bucket][name] = []ObjectMeta{meta}
	return nil
}

// liveGeneration returns the live (non-deleted) generation for a name, or
// (ObjectMeta{}, false). The caller must hold at least the read lock.
func liveGeneration(gens []ObjectMeta) (ObjectMeta, bool) {
	// The live generation is the last appended one without a TimeDeleted mark.
	for i := len(gens) - 1; i >= 0; i-- {
		if gens[i].TimeDeleted == nil {
			return gens[i], true
		}
	}
	return ObjectMeta{}, false
}

func (s *MemoryObjectStore) PutObjectGeneration(_ context.Context, bucket, name string, meta ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[bucket]; !ok {
		return ErrNoSuchBucket
	}
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	if s.objects[bucket] == nil {
		s.objects[bucket] = make(map[string][]ObjectMeta)
	}
	gens := s.objects[bucket][name]
	// Mark any prior live generation non-live (Object.timeDeleted).
	now := clock.Now()
	for i := range gens {
		if gens[i].TimeDeleted == nil {
			t := now
			gens[i].TimeDeleted = &t
		}
	}
	s.objects[bucket][name] = append(gens, meta)
	return nil
}

func (s *MemoryObjectStore) PutObjectMetaChecked(_ context.Context, bucket, name string, meta ObjectMeta, precondition *Precondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[bucket]; !ok {
		return ErrNoSuchBucket
	}
	current, exists := liveGeneration(s.objects[bucket][name])
	if !objectPreconditionMatches(current, exists, precondition) {
		return ErrPreconditionFailed
	}
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	if s.objects[bucket] == nil {
		s.objects[bucket] = make(map[string][]ObjectMeta)
	}
	s.objects[bucket][name] = []ObjectMeta{meta}
	return nil
}

func (s *MemoryObjectStore) PutObjectGenerationChecked(_ context.Context, bucket, name string, meta ObjectMeta, precondition *Precondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[bucket]; !ok {
		return ErrNoSuchBucket
	}
	current, exists := liveGeneration(s.objects[bucket][name])
	if !objectPreconditionMatches(current, exists, precondition) {
		return ErrPreconditionFailed
	}
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	if s.objects[bucket] == nil {
		s.objects[bucket] = make(map[string][]ObjectMeta)
	}
	gens := s.objects[bucket][name]
	now := clock.Now()
	for i := range gens {
		if gens[i].TimeDeleted == nil {
			t := now
			gens[i].TimeDeleted = &t
		}
	}
	s.objects[bucket][name] = append(gens, meta)
	return nil
}

func (s *MemoryObjectStore) DeleteObjectMetaChecked(_ context.Context, bucket, name string, precondition *Precondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ErrNoSuchObject
	}
	gens, ok := objs[name]
	if !ok {
		return ErrNoSuchObject
	}
	current, exists := liveGeneration(gens)
	if !objectPreconditionMatches(current, exists, precondition) {
		return ErrPreconditionFailed
	}
	delete(objs, name)
	return nil
}

func (s *MemoryObjectStore) TombstoneObjectMetaChecked(_ context.Context, bucket, name string, precondition *Precondition) (ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	gens, ok := objs[name]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	current, exists := liveGeneration(gens)
	if !exists {
		return ObjectMeta{}, ErrNoSuchObject
	}
	if !objectPreconditionMatches(current, exists, precondition) {
		return ObjectMeta{}, ErrPreconditionFailed
	}
	for i := len(gens) - 1; i >= 0; i-- {
		if gens[i].TimeDeleted == nil {
			now := clock.Now()
			gens[i].TimeDeleted = &now
			objs[name] = gens
			return gens[i], nil
		}
	}
	return ObjectMeta{}, ErrNoSuchObject
}

func (s *MemoryObjectStore) GetObjectMeta(_ context.Context, bucket, name string) (ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	gens, ok := objs[name]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	if meta, ok := liveGeneration(gens); ok {
		return meta, nil
	}
	return ObjectMeta{}, ErrNoSuchObject
}

func (s *MemoryObjectStore) GetObjectGeneration(_ context.Context, bucket, name, generation string) (ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	for _, m := range objs[name] {
		if m.Generation == generation {
			return m, nil
		}
	}
	return ObjectMeta{}, ErrNoSuchObject
}

func (s *MemoryObjectStore) DeleteObjectMeta(_ context.Context, bucket, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ErrNoSuchObject
	}
	if _, ok := objs[name]; !ok {
		return ErrNoSuchObject
	}
	delete(objs, name)
	return nil
}

func (s *MemoryObjectStore) TombstoneObjectMeta(_ context.Context, bucket, name string) (ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	gens, ok := objs[name]
	if !ok {
		return ObjectMeta{}, ErrNoSuchObject
	}
	// The live generation is the last appended one without a TimeDeleted mark.
	for i := len(gens) - 1; i >= 0; i-- {
		if gens[i].TimeDeleted == nil {
			now := clock.Now()
			gens[i].TimeDeleted = &now
			objs[name] = gens
			return gens[i], nil
		}
	}
	return ObjectMeta{}, ErrNoSuchObject
}

func (s *MemoryObjectStore) ListObjects(_ context.Context, bucket string) ([]ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.buckets[bucket]; !ok {
		return nil, ErrNoSuchBucket
	}
	objs := s.objects[bucket]
	result := make([]ObjectMeta, 0, len(objs))
	for _, gens := range objs {
		if m, ok := liveGeneration(gens); ok {
			result = append(result, m)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryObjectStore) ListObjectVersions(_ context.Context, bucket string) ([]ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.buckets[bucket]; !ok {
		return nil, ErrNoSuchBucket
	}
	objs := s.objects[bucket]
	var result []ObjectMeta
	for _, gens := range objs {
		result = append(result, gens...)
	}
	// Sort by name ascending, then generation descending (newest first).
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		gi, _ := strconv.ParseInt(result[i].Generation, 10, 64)
		gj, _ := strconv.ParseInt(result[j].Generation, 10, 64)
		return gi > gj
	})
	return result, nil
}

func (s *MemoryObjectStore) InitResumable(_ context.Context, sess ResumableSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.uploads[sess.UploadID]; ok {
		return ErrAlreadyExists
	}
	s.uploads[sess.UploadID] = sess
	return nil
}

func (s *MemoryObjectStore) GetResumable(_ context.Context, uploadID string) (ResumableSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.uploads[uploadID]
	if !ok {
		return ResumableSession{}, ErrNoSuchUpload
	}
	return sess, nil
}

func (s *MemoryObjectStore) UpdateResumable(_ context.Context, sess ResumableSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.uploads[sess.UploadID]; !ok {
		return ErrNoSuchUpload
	}
	s.uploads[sess.UploadID] = sess
	return nil
}

func (s *MemoryObjectStore) DeleteResumable(_ context.Context, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.uploads[uploadID]; !ok {
		return ErrNoSuchUpload
	}
	delete(s.uploads, uploadID)
	return nil
}

func (s *MemoryObjectStore) ListStaleResumable(_ context.Context, cutoff time.Time) ([]ResumableSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []ResumableSession
	for _, sess := range s.uploads {
		if sess.LastAccess.Before(cutoff) {
			result = append(result, sess)
		}
	}
	return result, nil
}

func (s *MemoryObjectStore) MaxGeneration(_ context.Context) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var max int64 = -1
	for _, objs := range s.objects {
		for _, gens := range objs {
			for _, m := range gens {
				g, err := strconv.ParseInt(m.Generation, 10, 64)
				if err == nil && g > max {
					max = g
				}
			}
		}
	}
	if max < 0 {
		return "", nil
	}
	return strconv.FormatInt(max, 10), nil
}

func (s *MemoryObjectStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = make(map[string]map[string]any)
	s.objects = make(map[string]map[string][]ObjectMeta)
	s.uploads = make(map[string]ResumableSession)
}
