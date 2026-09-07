package secretmanager

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_sm_secrets / jc_sm_versions.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// nullableJSON marshals v to JSONB, or returns nil (SQL NULL) when v is nil.
func nullableJSON(v any) any {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func (s *PostgresStore) CreateSecret(ctx context.Context, projectID, id string, sec Secret) error {
	if sec.CreateTime.IsZero() {
		sec.CreateTime = clock.Now()
	}
	labels, _ := json.Marshal(sec.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_sm_secrets (project_id, secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, projectID, id, json.RawMessage(labels), sec.CreateTime, sec.NextVer, nullableJSON(sec.Rotation), nullableJSON(sec.VersionAliases), sec.KmsKeyName)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetSecret(ctx context.Context, projectID, id string) (Secret, error) {
	var sec Secret
	var labels, rotation, aliases []byte
	err := s.pool.QueryRow(ctx, `
		SELECT secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name
		FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2
	`, projectID, id).Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName)
	if errors.Is(err, pgx.ErrNoRows) {
		return Secret{}, ErrNoSuchSecret
	}
	if err != nil {
		return Secret{}, err
	}
	json.Unmarshal(labels, &sec.Labels)
	if len(rotation) > 0 {
		json.Unmarshal(rotation, &sec.Rotation)
	}
	if len(aliases) > 0 {
		json.Unmarshal(aliases, &sec.VersionAliases)
	}
	return sec, nil
}

func (s *PostgresStore) UpdateSecret(ctx context.Context, projectID, id string, sec Secret) error {
	labels, _ := json.Marshal(sec.Labels)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_sm_secrets SET labels=$3, next_ver=$4, rotation=$5, version_aliases=$6, kms_key_name=$7
		WHERE project_id=$1 AND secret_id=$2
	`, projectID, id, json.RawMessage(labels), sec.NextVer, nullableJSON(sec.Rotation), nullableJSON(sec.VersionAliases), sec.KmsKeyName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchSecret
	}
	return nil
}

// UpdateSecretAtomic mirrors MemoryStore's version: a Serializable transaction
// with SELECT ... FOR UPDATE row-locks the secret for the duration of
// mutate, so a concurrent NextVersion() (or another UpdateSecretAtomic) call
// on the same secret blocks until this transaction commits or rolls back,
// instead of racing to silently roll back the other's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateSecretAtomic(ctx context.Context, projectID, id string, mutate func(Secret) (Secret, error)) (Secret, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Secret{}, err
	}
	defer tx.Rollback(ctx)

	var sec Secret
	var labels, rotation, aliases []byte
	err = tx.QueryRow(ctx, `
		SELECT secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name
		FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2 FOR UPDATE
	`, projectID, id).Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName)
	if errors.Is(err, pgx.ErrNoRows) {
		return Secret{}, ErrNoSuchSecret
	}
	if err != nil {
		return Secret{}, err
	}
	json.Unmarshal(labels, &sec.Labels)
	if len(rotation) > 0 {
		json.Unmarshal(rotation, &sec.Rotation)
	}
	if len(aliases) > 0 {
		json.Unmarshal(aliases, &sec.VersionAliases)
	}

	next, err := mutate(sec)
	if err != nil {
		return Secret{}, err
	}

	newLabels, _ := json.Marshal(next.Labels)
	if _, err := tx.Exec(ctx, `
		UPDATE jc_sm_secrets SET labels=$3, next_ver=$4, rotation=$5, version_aliases=$6, kms_key_name=$7
		WHERE project_id=$1 AND secret_id=$2
	`, projectID, id, json.RawMessage(newLabels), next.NextVer, nullableJSON(next.Rotation), nullableJSON(next.VersionAliases), next.KmsKeyName); err != nil {
		return Secret{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Secret{}, err
	}
	return next, nil
}

func (s *PostgresStore) DeleteSecret(ctx context.Context, projectID, id string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2`, projectID, id); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2`, projectID, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchSecret
	}
	return nil
}

func (s *PostgresStore) ListSecrets(ctx context.Context, projectID string) ([]Secret, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name
		FROM jc_sm_secrets WHERE project_id=$1 ORDER BY secret_id
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Secret
	for rows.Next() {
		var sec Secret
		var labels, rotation, aliases []byte
		if err := rows.Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName); err != nil {
			return nil, err
		}
		json.Unmarshal(labels, &sec.Labels)
		if len(rotation) > 0 {
			json.Unmarshal(rotation, &sec.Rotation)
		}
		if len(aliases) > 0 {
			json.Unmarshal(aliases, &sec.VersionAliases)
		}
		result = append(result, sec)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) CreateVersion(ctx context.Context, projectID string, v Version) error {
	if v.CreateTime.IsZero() {
		v.CreateTime = clock.Now()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_sm_versions (project_id, secret_id, version_id, state, create_time, data, kms_key_name, wrapped_dek)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, projectID, v.SecretID, v.VersionID, v.State, v.CreateTime, v.Data, v.KmsKeyName, v.WrappedDEK)
	return err
}

func (s *PostgresStore) GetVersion(ctx context.Context, projectID, secretID, versionID string) (Version, error) {
	var v Version
	err := s.pool.QueryRow(ctx, `
		SELECT secret_id, version_id, state, create_time, data, kms_key_name, wrapped_dek
		FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2 AND version_id=$3
	`, projectID, secretID, versionID).Scan(&v.SecretID, &v.VersionID, &v.State, &v.CreateTime, &v.Data, &v.KmsKeyName, &v.WrappedDEK)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNoSuchVersion
	}
	if err != nil {
		return Version{}, err
	}
	return v, nil
}

func (s *PostgresStore) ListVersions(ctx context.Context, projectID, secretID string) ([]Version, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT secret_id, version_id, state, create_time, data, kms_key_name, wrapped_dek
		FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2 ORDER BY version_id
	`, projectID, secretID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Version
	for rows.Next() {
		var v Version
		if err := rows.Scan(&v.SecretID, &v.VersionID, &v.State, &v.CreateTime, &v.Data, &v.KmsKeyName, &v.WrappedDEK); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].VersionID < result[j].VersionID })
	return result, rows.Err()
}

func (s *PostgresStore) UpdateVersion(ctx context.Context, projectID string, v Version) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_sm_versions SET state=$4 WHERE project_id=$1 AND secret_id=$2 AND version_id=$3
	`, projectID, v.SecretID, v.VersionID, v.State)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchVersion
	}
	return nil
}

// NextVersion atomically allocates and advances a secret's version counter via
// UPDATE ... RETURNING, so concurrent AddVersion calls never collide.
func (s *PostgresStore) NextVersion(ctx context.Context, projectID, secretID string) (int, error) {
	var next int
	err := s.pool.QueryRow(ctx, `
		UPDATE jc_sm_secrets SET next_ver = next_ver + 1
		WHERE project_id=$1 AND secret_id=$2
		RETURNING next_ver - 1
	`, projectID, secretID).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNoSuchSecret
	}
	if err != nil {
		return 0, err
	}
	return next, nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_sm_versions`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_sm_secrets`)
}
