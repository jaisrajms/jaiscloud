package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresObjectStore implements ObjectStore against the jc_gcs_* tables.
type PostgresObjectStore struct {
	pool *pgxpool.Pool
}

// NewPostgresObjectStore returns a Postgres-backed ObjectStore. The GCS
// migrations (gcpstore.MigrationFS) must have run before use.
func NewPostgresObjectStore(pool *pgxpool.Pool) *PostgresObjectStore {
	return &PostgresObjectStore{pool: pool}
}

func (s *PostgresObjectStore) CreateBucket(ctx context.Context, projectID, name string, meta map[string]any) error {
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	meta["projectId"] = projectID
	loc, _ := meta["location"].(string)
	sc, _ := meta["storageClass"].(string)
	raw, _ := json.Marshal(meta)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_gcs_buckets (name, project_id, location, storage_class, meta)
		VALUES ($1, $2, $3, $4, $5)
	`, name, projectID, loc, sc, json.RawMessage(raw))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return fmt.Errorf("CreateBucket: %w", err)
	}
	return nil
}

func (s *PostgresObjectStore) GetBucket(ctx context.Context, name string) (map[string]any, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT meta FROM jc_gcs_buckets WHERE name=$1`, name).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *PostgresObjectStore) UpdateBucketMeta(ctx context.Context, name string, meta map[string]any) error {
	raw, _ := json.Marshal(meta)
	tag, err := s.pool.Exec(ctx, `UPDATE jc_gcs_buckets SET meta=$2 WHERE name=$1`, name, json.RawMessage(raw))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchBucket
	}
	return nil
}

func (s *PostgresObjectStore) DeleteBucket(ctx context.Context, name string) error {
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_gcs_objects WHERE bucket=$1`, name).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return ErrBucketNotEmpty
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_gcs_buckets WHERE name=$1`, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchBucket
	}
	return nil
}

// ensureBucketExists returns ErrNoSuchBucket when the named bucket is absent,
// guarding PutObjectMeta/PutObjectGeneration against orphan object rows.
func ensureBucketExists(ctx context.Context, tx pgx.Tx, name string) error {
	var one int
	err := tx.QueryRow(ctx, `SELECT 1 FROM jc_gcs_buckets WHERE name=$1`, name).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoSuchBucket
	}
	return err
}

func (s *PostgresObjectStore) ListBuckets(ctx context.Context, projectID string) ([]map[string]any, error) {
	var rows pgx.Rows
	var err error
	if projectID != "" {
		rows, err = s.pool.Query(ctx, `SELECT meta FROM jc_gcs_buckets WHERE project_id=$1 ORDER BY name`, projectID)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT meta FROM jc_gcs_buckets ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var meta map[string]any
		if json.Unmarshal(raw, &meta) == nil {
			result = append(result, meta)
		}
	}
	return result, rows.Err()
}

func (s *PostgresObjectStore) PutObjectMeta(ctx context.Context, bucket, name string, meta ObjectMeta) error {
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	// Replace semantics: drop prior generations, insert one live generation.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := ensureBucketExists(ctx, tx, bucket); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_gcs_objects WHERE bucket=$1 AND name=$2`, bucket, name); err != nil {
		return err
	}
	if err := insertObjectGeneration(ctx, tx, meta); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PutObjectGeneration appends a new live generation, marking any prior live
// generation non-live (time_deleted set) so prior generations are retained.
func (s *PostgresObjectStore) PutObjectGeneration(ctx context.Context, bucket, name string, meta ObjectMeta) error {
	meta.Bucket = bucket
	meta.Name = name
	normalizeMeta(&meta)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := ensureBucketExists(ctx, tx, bucket); err != nil {
		return err
	}
	now := clock.Now()
	if _, err := tx.Exec(ctx, `
		UPDATE jc_gcs_objects SET time_deleted=$3 WHERE bucket=$1 AND name=$2 AND time_deleted IS NULL
	`, bucket, name, now); err != nil {
		return err
	}
	if err := insertObjectGeneration(ctx, tx, meta); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// insertObjectGeneration inserts a single object generation row within tx.
// The caller owns the transaction and commits it.
func insertObjectGeneration(ctx context.Context, tx pgx.Tx, meta ObjectMeta) error {
	metaRaw, _ := json.Marshal(meta.Metadata)
	retainUntil, retentionMode := retentionArgs(&meta)
	timeDeleted := nullableTime(meta.TimeDeleted)
	_, err := tx.Exec(ctx, `
		INSERT INTO jc_gcs_objects
		  (bucket, name, generation, metageneration, content_type, size, md5_hash, crc32c, storage_class, metadata, time_created, updated, retain_until, retention_mode, temporary_hold, event_based_hold, time_deleted, kms_key_name, wrapped_dek, cse_key_sha256)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
	`, meta.Bucket, meta.Name, meta.Generation, meta.Metageneration, meta.ContentType, meta.Size, meta.MD5Hash, meta.CRC32C,
		meta.StorageClass, json.RawMessage(metaRaw), meta.TimeCreated, meta.Updated, retainUntil, retentionMode,
		meta.TemporaryHold, meta.EventBasedHold, timeDeleted, meta.KmsKeyName, meta.WrappedDEK, meta.CSEKeySHA256)
	if err != nil {
		return err
	}
	return nil
}

// objectCols is the shared SELECT column list for object rows.
const objectCols = "bucket, name, generation, metageneration, content_type, size, md5_hash, crc32c, storage_class, metadata, time_created, updated, retain_until, retention_mode, temporary_hold, event_based_hold, time_deleted, kms_key_name, wrapped_dek, cse_key_sha256"

// scanObject scans the objectCols columns into an ObjectMeta.
func scanObject(scan func(...any) error) (ObjectMeta, error) {
	var m ObjectMeta
	var metaRaw []byte
	var retentionMode string
	var retainUntil pgtype.Timestamptz
	var timeDeleted pgtype.Timestamptz
	err := scan(&m.Bucket, &m.Name, &m.Generation, &m.Metageneration, &m.ContentType, &m.Size, &m.MD5Hash, &m.CRC32C,
		&m.StorageClass, &metaRaw, &m.TimeCreated, &m.Updated, &retainUntil, &retentionMode, &m.TemporaryHold, &m.EventBasedHold, &timeDeleted, &m.KmsKeyName, &m.WrappedDEK, &m.CSEKeySHA256)
	if err != nil {
		return ObjectMeta{}, err
	}
	_ = json.Unmarshal(metaRaw, &m.Metadata)
	if retainUntil.Valid || retentionMode != "" {
		m.Retention = &ObjectRetention{RetainUntilTime: retainUntil.Time, Mode: retentionMode}
	}
	if timeDeleted.Valid {
		t := timeDeleted.Time
		m.TimeDeleted = &t
	}
	return m, nil
}

// retentionArgs converts ObjectMeta.Retention into (retain_until, retention_mode).
func retentionArgs(m *ObjectMeta) (pgtype.Timestamptz, string) {
	if m.Retention == nil {
		return pgtype.Timestamptz{}, ""
	}
	return nullableTimePtr(&m.Retention.RetainUntilTime), m.Retention.Mode
}

// nullableTime converts a *time.Time into a pgtype.Timestamptz.
func nullableTime(t *time.Time) pgtype.Timestamptz {
	if t == nil || t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

func nullableTimePtr(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return nullableTime(t)
}

func (s *PostgresObjectStore) GetObjectMeta(ctx context.Context, bucket, name string) (ObjectMeta, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+objectCols+`
		FROM jc_gcs_objects WHERE bucket=$1 AND name=$2 AND time_deleted IS NULL
		ORDER BY generation::bigint DESC LIMIT 1
	`, bucket, name)
	m, err := scanObject(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObjectMeta{}, ErrNoSuchObject
	}
	return m, err
}

func (s *PostgresObjectStore) GetObjectGeneration(ctx context.Context, bucket, name, generation string) (ObjectMeta, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+objectCols+`
		FROM jc_gcs_objects WHERE bucket=$1 AND name=$2 AND generation=$3
	`, bucket, name, generation)
	m, err := scanObject(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObjectMeta{}, ErrNoSuchObject
	}
	return m, err
}

func (s *PostgresObjectStore) DeleteObjectMeta(ctx context.Context, bucket, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_gcs_objects WHERE bucket=$1 AND name=$2`, bucket, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchObject
	}
	return nil
}

func (s *PostgresObjectStore) TombstoneObjectMeta(ctx context.Context, bucket, name string) (ObjectMeta, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ObjectMeta{}, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `
		SELECT `+objectCols+`
		FROM jc_gcs_objects WHERE bucket=$1 AND name=$2 AND time_deleted IS NULL
		ORDER BY generation::bigint DESC LIMIT 1
	`, bucket, name)
	m, err := scanObject(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObjectMeta{}, ErrNoSuchObject
	}
	if err != nil {
		return ObjectMeta{}, err
	}
	now := clock.Now()
	tag, err := tx.Exec(ctx, `
		UPDATE jc_gcs_objects SET time_deleted=$3 WHERE bucket=$1 AND name=$2 AND generation=$4
	`, bucket, name, now, m.Generation)
	if err != nil {
		return ObjectMeta{}, err
	}
	if tag.RowsAffected() == 0 {
		return ObjectMeta{}, ErrNoSuchObject
	}
	if err := tx.Commit(ctx); err != nil {
		return ObjectMeta{}, err
	}
	m.TimeDeleted = &now
	return m, nil
}

func (s *PostgresObjectStore) ListObjects(ctx context.Context, bucket string) ([]ObjectMeta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+objectCols+`
		FROM jc_gcs_objects WHERE bucket=$1 AND time_deleted IS NULL
		ORDER BY name, generation::bigint DESC
	`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ObjectMeta
	for rows.Next() {
		m, err := scanObject(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

func (s *PostgresObjectStore) ListObjectVersions(ctx context.Context, bucket string) ([]ObjectMeta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+objectCols+`
		FROM jc_gcs_objects WHERE bucket=$1
		ORDER BY name, generation::bigint DESC
	`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ObjectMeta
	for rows.Next() {
		m, err := scanObject(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (s *PostgresObjectStore) InitResumable(ctx context.Context, sess ResumableSession) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_gcs_resumable_sessions (upload_id, bucket, name, content_type, length, tmp_path, last_access)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, sess.UploadID, sess.Bucket, sess.Name, sess.ContentType, sess.Length, sess.TmpPath, sess.LastAccess)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresObjectStore) GetResumable(ctx context.Context, uploadID string) (ResumableSession, error) {
	var sess ResumableSession
	err := s.pool.QueryRow(ctx, `
		SELECT upload_id, bucket, name, content_type, length, tmp_path, last_access
		FROM jc_gcs_resumable_sessions WHERE upload_id=$1
	`, uploadID).Scan(&sess.UploadID, &sess.Bucket, &sess.Name, &sess.ContentType, &sess.Length, &sess.TmpPath, &sess.LastAccess)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResumableSession{}, ErrNoSuchUpload
	}
	if err != nil {
		return ResumableSession{}, err
	}
	return sess, nil
}

func (s *PostgresObjectStore) UpdateResumable(ctx context.Context, sess ResumableSession) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_gcs_resumable_sessions
		SET content_type=$2, length=$3, tmp_path=$4, last_access=$5
		WHERE upload_id=$1
	`, sess.UploadID, sess.ContentType, sess.Length, sess.TmpPath, sess.LastAccess)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchUpload
	}
	return nil
}

func (s *PostgresObjectStore) DeleteResumable(ctx context.Context, uploadID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_gcs_resumable_sessions WHERE upload_id=$1`, uploadID)
	return err
}

func (s *PostgresObjectStore) ListStaleResumable(ctx context.Context, cutoff time.Time) ([]ResumableSession, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT upload_id, bucket, name, content_type, length, tmp_path, last_access
		FROM jc_gcs_resumable_sessions WHERE last_access < $1
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ResumableSession
	for rows.Next() {
		var sess ResumableSession
		if err := rows.Scan(&sess.UploadID, &sess.Bucket, &sess.Name, &sess.ContentType, &sess.Length, &sess.TmpPath, &sess.LastAccess); err != nil {
			return nil, err
		}
		result = append(result, sess)
	}
	return result, rows.Err()
}

func (s *PostgresObjectStore) MaxGeneration(ctx context.Context) (string, error) {
	var max *string
	err := s.pool.QueryRow(ctx, `
		SELECT (SELECT max(generation::bigint)::text FROM jc_gcs_objects WHERE generation ~ '^[0-9]+$')
	`).Scan(&max)
	if err != nil {
		return "", err
	}
	if max == nil {
		return "", nil
	}
	return *max, nil
}

func (s *PostgresObjectStore) Reset(ctx context.Context) {
	for _, stmt := range []string{
		`DELETE FROM jc_gcs_resumable_sessions`,
		`DELETE FROM jc_gcs_objects`,
		`DELETE FROM jc_gcs_buckets`,
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			slog.Warn("gcs reset: exec failed", "stmt", stmt, "err", err)
			return
		}
	}
}
