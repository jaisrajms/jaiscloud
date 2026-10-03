package gcs

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/storeutil"
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
	normalizeBucketMeta(meta)
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
	normalizeBucketMeta(out)
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

func (s *MemoryObjectStore) UpdateBucketMetaAtomic(_ context.Context, name string, mutate func(meta map[string]any) (map[string]any, error)) (map[string]any, error) {
	return storeutil.AtomicUpdate(&s.mu,
		func() (map[string]any, bool) { meta, ok := s.buckets[name]; return meta, ok },
		func(current map[string]any, exists bool) (map[string]any, error) {
			if !exists {
				return nil, ErrNoSuchBucket
			}
			// Mutate a copy so a precondition failure (or any error) leaves the
			// stored map untouched rather than partially modified.
			next := make(map[string]any, len(current))
			for k, v := range current {
				next[k] = v
			}
			next, err := mutate(next)
			if err != nil {
				return nil, err
			}
			if next == nil {
				next = map[string]any{}
			}
			next["name"] = name
			normalizeBucketMeta(next)
			return next, nil
		},
		func(next map[string]any) { s.buckets[name] = next },
	)
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
		normalizeBucketMeta(out)
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

// UpdateObjectMetaChecked updates the live generation's metadata in place,
// preserving every other (noncurrent) generation and the live generation's
// immutable fields (id, creation time, size, checksums, encryption material —
// including WrappedDEK, which has no wire representation). The precondition is
// validated against the current live generation under the same write lock.
func (s *MemoryObjectStore) UpdateObjectMetaChecked(_ context.Context, bucket, name string, meta ObjectMeta, precondition *Precondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	gens := s.objects[bucket][name]
	idx := -1
	for i := len(gens) - 1; i >= 0; i-- {
		if gens[i].TimeDeleted == nil {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ErrNoSuchObject
	}
	if !objectPreconditionMatches(gens[idx], true, precondition) {
		return ErrPreconditionFailed
	}
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	// Mutate only the mutable metadata fields on the stored live generation,
	// taking the immutables (generation, size, checksums, creation time, and
	// key material such as WrappedDEK) from storage — the caller's meta is
	// built from the wire object, which omits WrappedDEK. Replacing the whole
	// struct would zero the wrapped DEK and make a CMEK object unreadable.
	cur := gens[idx]
	cur.ContentType = meta.ContentType
	cur.ContentEncoding = meta.ContentEncoding
	cur.StorageClass = meta.StorageClass
	cur.Metadata = meta.Metadata
	cur.TemporaryHold = meta.TemporaryHold
	cur.EventBasedHold = meta.EventBasedHold
	cur.Retention = meta.Retention
	cur.Metageneration = meta.Metageneration
	cur.Updated = meta.Updated
	gens[idx] = cur
	s.objects[bucket][name] = gens
	return nil
}

// UpdateObjectGenerationMetaChecked updates one specific generation's mutable
// metadata in place, mirroring UpdateObjectMetaChecked but targeting the
// revision named by generation (live or non-current). The selected generation's
// immutable fields are preserved; the precondition is validated against that
// generation under the same write lock.
func (s *MemoryObjectStore) UpdateObjectGenerationMetaChecked(_ context.Context, bucket, name, generation string, meta ObjectMeta, precondition *Precondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	gens, ok := s.objects[bucket][name]
	if !ok {
		return ErrNoSuchObject
	}
	idx := -1
	for i, g := range gens {
		if g.Generation == generation {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ErrNoSuchObject
	}
	if !objectPreconditionMatches(gens[idx], true, precondition) {
		return ErrPreconditionFailed
	}
	meta.Bucket = bucket
	meta.Name = name
	meta.Generation = generation
	normalizeMeta(&meta)
	cur := gens[idx]
	cur.ContentType = meta.ContentType
	cur.ContentEncoding = meta.ContentEncoding
	cur.StorageClass = meta.StorageClass
	cur.Metadata = meta.Metadata
	cur.TemporaryHold = meta.TemporaryHold
	cur.EventBasedHold = meta.EventBasedHold
	cur.Retention = meta.Retention
	cur.Metageneration = meta.Metageneration
	cur.Updated = meta.Updated
	gens[idx] = cur
	s.objects[bucket][name] = gens
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

// RestoreObjectGeneration makes generation live again: the target's timeDeleted
// is cleared and metageneration bumped, and any other live generation is marked
// non-live (mirroring real GCS, where restoring a soft-deleted generation
// supersedes the current live one). The precondition is validated against the
// current live state under the same write lock.
func (s *MemoryObjectStore) RestoreObjectGeneration(_ context.Context, bucket, name, generation string, precondition *Precondition) (ObjectMeta, error) {
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
	if !objectPreconditionMatches(current, exists, precondition) {
		return ObjectMeta{}, ErrPreconditionFailed
	}
	idx := -1
	for i := range gens {
		if gens[i].Generation == generation {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ObjectMeta{}, ErrNoSuchObject
	}
	if gens[idx].TimeDeleted == nil {
		// The target is already live: restore is a no-op.
		return gens[idx], nil
	}
	now := clock.Now()
	for i := range gens {
		if gens[i].TimeDeleted == nil && gens[i].Generation != generation {
			t := now
			gens[i].TimeDeleted = &t
		}
	}
	g := gens[idx]
	g.TimeDeleted = nil
	g.Metageneration = nextMetageneration(g.Metageneration)
	g.Updated = now
	gens[idx] = g
	objs[name] = gens
	return g, nil
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

// DeleteObjectGeneration removes one specific generation of an object. Remaining
// revisions stay noncurrent (GCS does not promote a noncurrent version to live),
// so removing the live generation leaves the name unresolvable by bare lookup;
// when no generations remain the name is forgotten so the bucket can become
// empty. The precondition is validated against the current live state under the
// same lock.
func (s *MemoryObjectStore) DeleteObjectGeneration(_ context.Context, bucket, name, generation string, precondition *Precondition) (ObjectMeta, error) {
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
	if !objectPreconditionMatches(current, exists, precondition) {
		return ObjectMeta{}, ErrPreconditionFailed
	}
	idx := -1
	for i := range gens {
		if gens[i].Generation == generation {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ObjectMeta{}, ErrNoSuchObject
	}
	removed := gens[idx]
	gens = append(gens[:idx], gens[idx+1:]...)
	if len(gens) == 0 {
		delete(objs, name)
	} else {
		objs[name] = gens
	}
	return removed, nil
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
