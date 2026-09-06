package gcs

import (
	"context"
	"encoding/json"
	"io"
)

// gcsSnap is the JSON snapshot shape for both memory and postgres backends.
type gcsSnap struct {
	Buckets []bucketRow        `json:"buckets"`
	Objects []ObjectMeta       `json:"objects"`
	Uploads []ResumableSession `json:"uploads"`
}

// bucketRow is a serializable bucket record (mirrors jc_gcs_buckets).
type bucketRow struct {
	Name         string         `json:"name"`
	ProjectID    string         `json:"projectId"`
	Location     string         `json:"location"`
	StorageClass string         `json:"storageClass"`
	Meta         map[string]any `json:"meta"`
}

// MemoryObjectStore Snapshot/Restore/IsEmpty.

func (s *MemoryObjectStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.buckets) == 0 && len(s.objects) == 0 && len(s.uploads) == 0, nil
}

func (s *MemoryObjectStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	buckets := make([]bucketRow, 0, len(s.buckets))
	for name, meta := range s.buckets {
		row := bucketRow{Name: name, Meta: meta}
		if v, ok := meta["projectId"].(string); ok {
			row.ProjectID = v
		}
		if v, ok := meta["location"].(string); ok {
			row.Location = v
		}
		if v, ok := meta["storageClass"].(string); ok {
			row.StorageClass = v
		}
		buckets = append(buckets, row)
	}
	objects := make([]ObjectMeta, 0)
	for _, byName := range s.objects {
		for _, gens := range byName {
			objects = append(objects, gens...)
		}
	}
	uploads := make([]ResumableSession, 0, len(s.uploads))
	for _, u := range s.uploads {
		uploads = append(uploads, u)
	}
	return json.NewEncoder(w).Encode(gcsSnap{Buckets: buckets, Objects: objects, Uploads: uploads})
}

func (s *MemoryObjectStore) Restore(_ context.Context, r io.Reader) error {
	var snap gcsSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	buckets := make(map[string]map[string]any, len(snap.Buckets))
	for _, b := range snap.Buckets {
		meta := b.Meta
		if meta == nil {
			meta = map[string]any{}
		}
		if b.ProjectID != "" {
			meta["projectId"] = b.ProjectID
		}
		if b.Location != "" {
			meta["location"] = b.Location
		}
		if b.StorageClass != "" {
			meta["storageClass"] = b.StorageClass
		}
		buckets[b.Name] = meta
	}
	objects := make(map[string]map[string][]ObjectMeta)
	for _, o := range snap.Objects {
		if objects[o.Bucket] == nil {
			objects[o.Bucket] = make(map[string][]ObjectMeta)
		}
		objects[o.Bucket][o.Name] = append(objects[o.Bucket][o.Name], o)
	}
	uploads := make(map[string]ResumableSession, len(snap.Uploads))
	for _, u := range snap.Uploads {
		uploads[u.UploadID] = u
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = buckets
	s.objects = objects
	s.uploads = uploads
	return nil
}

// PostgresObjectStore Snapshot/Restore/IsEmpty.

func (s *PostgresObjectStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jc_gcs_buckets) + (SELECT count(*) FROM jc_gcs_objects) + (SELECT count(*) FROM jc_gcs_resumable_sessions)`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresObjectStore) Snapshot(ctx context.Context, w io.Writer) error {
	var snap gcsSnap
	rows, err := s.pool.Query(ctx, `SELECT name, project_id, location, storage_class, meta FROM jc_gcs_buckets ORDER BY name`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b bucketRow
		var meta []byte
		if err := rows.Scan(&b.Name, &b.ProjectID, &b.Location, &b.StorageClass, &meta); err != nil {
			rows.Close()
			return err
		}
		json.Unmarshal(meta, &b.Meta)
		snap.Buckets = append(snap.Buckets, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	orows, err := s.pool.Query(ctx, `SELECT `+objectCols+` FROM jc_gcs_objects ORDER BY bucket, name, generation`)
	if err != nil {
		return err
	}
	for orows.Next() {
		o, err := scanObject(orows.Scan)
		if err != nil {
			orows.Close()
			return err
		}
		snap.Objects = append(snap.Objects, o)
	}
	orows.Close()
	if err := orows.Err(); err != nil {
		return err
	}

	urows, err := s.pool.Query(ctx, `SELECT upload_id, bucket, name, content_type, length, tmp_path, last_access FROM jc_gcs_resumable_sessions`)
	if err != nil {
		return err
	}
	for urows.Next() {
		var u ResumableSession
		if err := urows.Scan(&u.UploadID, &u.Bucket, &u.Name, &u.ContentType, &u.Length, &u.TmpPath, &u.LastAccess); err != nil {
			urows.Close()
			return err
		}
		snap.Uploads = append(snap.Uploads, u)
	}
	urows.Close()
	if err := urows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresObjectStore) Restore(ctx context.Context, r io.Reader) error {
	var snap gcsSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_gcs_resumable_sessions`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_gcs_objects`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_gcs_buckets`); err != nil {
		return err
	}
	for _, b := range snap.Buckets {
		meta := b.Meta
		if meta == nil {
			meta = map[string]any{}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO jc_gcs_buckets (name, project_id, location, storage_class, meta) VALUES ($1,$2,$3,$4,$5)`,
			b.Name, b.ProjectID, b.Location, b.StorageClass, json.RawMessage(mustJSON(meta))); err != nil {
			return err
		}
	}
	for _, o := range snap.Objects {
		metadata := o.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		retainUntil, retentionMode := retentionArgs(&o)
		timeDeleted := nullableTime(o.TimeDeleted)
		if _, err := tx.Exec(ctx, `INSERT INTO jc_gcs_objects (bucket, name, generation, metageneration, content_type, size, md5_hash, crc32c, storage_class, metadata, time_created, updated, retain_until, retention_mode, temporary_hold, event_based_hold, time_deleted, kms_key_name, wrapped_dek, cse_key_sha256, component_count) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`,
			o.Bucket, o.Name, o.Generation, o.Metageneration, o.ContentType, o.Size, o.MD5Hash, o.CRC32C, o.StorageClass, json.RawMessage(mustJSON(metadata)), o.TimeCreated, o.Updated, retainUntil, retentionMode, o.TemporaryHold, o.EventBasedHold, timeDeleted, o.KmsKeyName, o.WrappedDEK, o.CSEKeySHA256, o.ComponentCount); err != nil {
			return err
		}
	}
	for _, u := range snap.Uploads {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_gcs_resumable_sessions (upload_id, bucket, name, content_type, length, tmp_path, last_access) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			u.UploadID, u.Bucket, u.Name, u.ContentType, u.Length, u.TmpPath, u.LastAccess); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
