package secretmanager

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/storeutil"

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

// decodeSecretJSON fills the JSONB-backed fields of sec from the raw column
// values every jc_sm_secrets SELECT returns.
func decodeSecretJSON(sec *Secret, labels, annotations, topics, rotation, aliases, replication []byte) {
	json.Unmarshal(labels, &sec.Labels)
	json.Unmarshal(annotations, &sec.Annotations)
	if len(topics) > 0 {
		json.Unmarshal(topics, &sec.Topics)
	}
	if len(rotation) > 0 {
		json.Unmarshal(rotation, &sec.Rotation)
	}
	if len(aliases) > 0 {
		json.Unmarshal(aliases, &sec.VersionAliases)
	}
	sec.Replication = nil
	if len(replication) > 0 {
		json.Unmarshal(replication, &sec.Replication)
	}
}

const secretColumns = `secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name, annotations, replication, topics, version_destroy_ttl, expire_time`

func (s *PostgresStore) CreateSecret(ctx context.Context, projectID, id string, sec Secret) error {
	if sec.CreateTime.IsZero() {
		sec.CreateTime = clock.Now()
	}
	labels, _ := json.Marshal(sec.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_sm_secrets (project_id, secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name, annotations, replication, topics, version_destroy_ttl, expire_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`, projectID, id, json.RawMessage(labels), sec.CreateTime, sec.NextVer, nullableJSON(sec.Rotation), nullableJSON(sec.VersionAliases), sec.KmsKeyName, nullableJSON(sec.Annotations), nullableJSON(sec.Replication), nullableJSON(sec.Topics), sec.VersionDestroyTTL, nullableTime(sec.ExpireTime))
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
	var labels, annotations, topics, rotation, aliases, replication []byte
	var expireTime *time.Time
	err := s.pool.QueryRow(ctx, `SELECT `+secretColumns+` FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2`,
		projectID, id).Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName, &annotations, &replication, &topics, &sec.VersionDestroyTTL, &expireTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return Secret{}, ErrNoSuchSecret
	}
	if err != nil {
		return Secret{}, err
	}
	if expireTime != nil {
		sec.ExpireTime = *expireTime
	}
	decodeSecretJSON(&sec, labels, annotations, topics, rotation, aliases, replication)
	return sec, nil
}

func (s *PostgresStore) UpdateSecret(ctx context.Context, projectID, id string, sec Secret) error {
	labels, _ := json.Marshal(sec.Labels)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_sm_secrets SET labels=$3, next_ver=$4, rotation=$5, version_aliases=$6, kms_key_name=$7, annotations=$8, replication=$9, topics=$10, version_destroy_ttl=$11, expire_time=$12
		WHERE project_id=$1 AND secret_id=$2
	`, projectID, id, json.RawMessage(labels), sec.NextVer, nullableJSON(sec.Rotation), nullableJSON(sec.VersionAliases), sec.KmsKeyName, nullableJSON(sec.Annotations), nullableJSON(sec.Replication), nullableJSON(sec.Topics), sec.VersionDestroyTTL, nullableTime(sec.ExpireTime))
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
	// Retry a transient SERIALIZABLE conflict (40001/40P01) rather than
	// surfacing a benign race as a 500.
	return storeutil.RetrySerializable(ctx, func() (Secret, error) {
		return s.updateSecretAtomicOnce(ctx, projectID, id, mutate)
	})
}

func (s *PostgresStore) updateSecretAtomicOnce(ctx context.Context, projectID, id string, mutate func(Secret) (Secret, error)) (Secret, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Secret{}, err
	}
	defer tx.Rollback(ctx)

	var sec Secret
	var labels, annotations, topics, rotation, aliases, replication []byte
	var expireTime *time.Time
	err = tx.QueryRow(ctx, `SELECT `+secretColumns+` FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2 FOR UPDATE`,
		projectID, id).Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName, &annotations, &replication, &topics, &sec.VersionDestroyTTL, &expireTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return Secret{}, ErrNoSuchSecret
	}
	if err != nil {
		return Secret{}, err
	}
	if expireTime != nil {
		sec.ExpireTime = *expireTime
	}
	decodeSecretJSON(&sec, labels, annotations, topics, rotation, aliases, replication)

	next, err := mutate(sec)
	if err != nil {
		return Secret{}, err
	}

	newLabels, _ := json.Marshal(next.Labels)
	if _, err := tx.Exec(ctx, `
		UPDATE jc_sm_secrets SET labels=$3, next_ver=$4, rotation=$5, version_aliases=$6, kms_key_name=$7, annotations=$8, replication=$9, topics=$10, version_destroy_ttl=$11, expire_time=$12
		WHERE project_id=$1 AND secret_id=$2
	`, projectID, id, json.RawMessage(newLabels), next.NextVer, nullableJSON(next.Rotation), nullableJSON(next.VersionAliases), next.KmsKeyName, nullableJSON(next.Annotations), nullableJSON(next.Replication), nullableJSON(next.Topics), next.VersionDestroyTTL, nullableTime(next.ExpireTime)); err != nil {
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

// DeleteSecretIf runs the precondition against a row-locked snapshot and
// deletes in the same Serializable transaction, so a concurrent mutation
// cannot land between the check and the delete (the same convention as
// UpdateSecretAtomic).
func (s *PostgresStore) DeleteSecretIf(ctx context.Context, projectID, id string, cond func(Secret) error) error {
	return storeutil.RetrySerializableErr(ctx, func() error {
		return s.deleteSecretIfOnce(ctx, projectID, id, cond)
	})
}

func (s *PostgresStore) deleteSecretIfOnce(ctx context.Context, projectID, id string, cond func(Secret) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var sec Secret
	var labels, annotations, topics, rotation, aliases, replication []byte
	var expireTime *time.Time
	err = tx.QueryRow(ctx, `SELECT `+secretColumns+` FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2 FOR UPDATE`,
		projectID, id).Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName, &annotations, &replication, &topics, &sec.VersionDestroyTTL, &expireTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoSuchSecret
	}
	if err != nil {
		return err
	}
	if expireTime != nil {
		sec.ExpireTime = *expireTime
	}
	decodeSecretJSON(&sec, labels, annotations, topics, rotation, aliases, replication)

	if cond != nil {
		if err := cond(sec); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2`, projectID, id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_sm_secrets WHERE project_id=$1 AND secret_id=$2`, projectID, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListSecrets(ctx context.Context, projectID string) ([]Secret, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+secretColumns+` FROM jc_sm_secrets WHERE project_id=$1 ORDER BY secret_id`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Secret
	for rows.Next() {
		var sec Secret
		var labels, annotations, topics, rotation, aliases, replication []byte
		var expireTime *time.Time
		if err := rows.Scan(&sec.ID, &labels, &sec.CreateTime, &sec.NextVer, &rotation, &aliases, &sec.KmsKeyName, &annotations, &replication, &topics, &sec.VersionDestroyTTL, &expireTime); err != nil {
			return nil, err
		}
		if expireTime != nil {
			sec.ExpireTime = *expireTime
		}
		decodeSecretJSON(&sec, labels, annotations, topics, rotation, aliases, replication)
		result = append(result, sec)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) CreateVersion(ctx context.Context, projectID string, v Version) error {
	if v.CreateTime.IsZero() {
		v.CreateTime = clock.Now()
	}
	replicaDEKs, err := marshalReplicaDEKs(v.ReplicaDEKs)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_sm_versions (project_id, secret_id, version_id, state, create_time, destroy_time, scheduled_destroy_time, data, kms_key_name, wrapped_dek, replica_deks)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, projectID, v.SecretID, v.VersionID, v.State, v.CreateTime, nullableTime(v.DestroyTime), nullableTime(v.ScheduledDestroyTime), v.Data, v.KmsKeyName, v.WrappedDEK, replicaDEKs)
	return err
}

func (s *PostgresStore) GetVersion(ctx context.Context, projectID, secretID, versionID string) (Version, error) {
	var v Version
	var destroyTime, scheduledDestroyTime *time.Time
	var replicaDEKs []byte
	err := s.pool.QueryRow(ctx, `
		SELECT secret_id, version_id, state, create_time, destroy_time, scheduled_destroy_time, data, kms_key_name, wrapped_dek, replica_deks
		FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2 AND version_id=$3
	`, projectID, secretID, versionID).Scan(&v.SecretID, &v.VersionID, &v.State, &v.CreateTime, &destroyTime, &scheduledDestroyTime, &v.Data, &v.KmsKeyName, &v.WrappedDEK, &replicaDEKs)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNoSuchVersion
	}
	if err != nil {
		return Version{}, err
	}
	if destroyTime != nil {
		v.DestroyTime = *destroyTime
	}
	if scheduledDestroyTime != nil {
		v.ScheduledDestroyTime = *scheduledDestroyTime
	}
	if err := decodeReplicaDEKs(replicaDEKs, &v); err != nil {
		return Version{}, err
	}
	return v, nil
}

func (s *PostgresStore) ListVersions(ctx context.Context, projectID, secretID string) ([]Version, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT secret_id, version_id, state, create_time, destroy_time, scheduled_destroy_time, data, kms_key_name, wrapped_dek, replica_deks
		FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2 ORDER BY version_id
	`, projectID, secretID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Version
	for rows.Next() {
		var v Version
		var destroyTime, scheduledDestroyTime *time.Time
		var replicaDEKs []byte
		if err := rows.Scan(&v.SecretID, &v.VersionID, &v.State, &v.CreateTime, &destroyTime, &scheduledDestroyTime, &v.Data, &v.KmsKeyName, &v.WrappedDEK, &replicaDEKs); err != nil {
			return nil, err
		}
		if destroyTime != nil {
			v.DestroyTime = *destroyTime
		}
		if scheduledDestroyTime != nil {
			v.ScheduledDestroyTime = *scheduledDestroyTime
		}
		if err := decodeReplicaDEKs(replicaDEKs, &v); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].VersionID < result[j].VersionID })
	return result, rows.Err()
}

func (s *PostgresStore) UpdateVersion(ctx context.Context, projectID string, v Version) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_sm_versions SET state=$4, destroy_time=$5, scheduled_destroy_time=$6 WHERE project_id=$1 AND secret_id=$2 AND version_id=$3
	`, projectID, v.SecretID, v.VersionID, v.State, nullableTime(v.DestroyTime), nullableTime(v.ScheduledDestroyTime))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchVersion
	}
	return nil
}

// UpdateVersionIf reads the version FOR UPDATE, applies mutate, and writes it
// back in one Serializable transaction, so a concurrent transition cannot land
// between the precondition check and the update.
func (s *PostgresStore) UpdateVersionIf(ctx context.Context, projectID, secretID, versionID string, mutate func(Version) (Version, error)) (Version, error) {
	return storeutil.RetrySerializable(ctx, func() (Version, error) {
		return s.updateVersionIfOnce(ctx, projectID, secretID, versionID, mutate)
	})
}

func (s *PostgresStore) updateVersionIfOnce(ctx context.Context, projectID, secretID, versionID string, mutate func(Version) (Version, error)) (Version, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Version{}, err
	}
	defer tx.Rollback(ctx)

	var v Version
	var destroyTime, scheduledDestroyTime *time.Time
	var replicaDEKs []byte
	err = tx.QueryRow(ctx, `
		SELECT secret_id, version_id, state, create_time, destroy_time, scheduled_destroy_time, data, kms_key_name, wrapped_dek, replica_deks
		FROM jc_sm_versions WHERE project_id=$1 AND secret_id=$2 AND version_id=$3 FOR UPDATE
	`, projectID, secretID, versionID).Scan(&v.SecretID, &v.VersionID, &v.State, &v.CreateTime, &destroyTime, &scheduledDestroyTime, &v.Data, &v.KmsKeyName, &v.WrappedDEK, &replicaDEKs)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNoSuchVersion
	}
	if err != nil {
		return Version{}, err
	}
	if destroyTime != nil {
		v.DestroyTime = *destroyTime
	}
	if scheduledDestroyTime != nil {
		v.ScheduledDestroyTime = *scheduledDestroyTime
	}
	if err := decodeReplicaDEKs(replicaDEKs, &v); err != nil {
		return Version{}, err
	}

	next, err := mutate(v)
	if err != nil {
		return Version{}, err
	}
	// Identity is the address, not a mutation: never let mutate move the record.
	next.SecretID, next.VersionID = v.SecretID, v.VersionID
	if _, err := tx.Exec(ctx, `
		UPDATE jc_sm_versions SET state=$4, destroy_time=$5, scheduled_destroy_time=$6 WHERE project_id=$1 AND secret_id=$2 AND version_id=$3
	`, projectID, next.SecretID, next.VersionID, next.State, nullableTime(next.DestroyTime), nullableTime(next.ScheduledDestroyTime)); err != nil {
		return Version{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Version{}, err
	}
	return next, nil
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

// marshalReplicaDEKs encodes a version's per-replica wraps for the replica_deks
// JSONB column, or nil (SQL NULL) when there are none.
func marshalReplicaDEKs(deks []ReplicaDEK) (any, error) {
	if len(deks) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(deks)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// decodeReplicaDEKs fills v.ReplicaDEKs from the replica_deks column (nil and
// empty both mean "no per-replica wraps": an automatic-replication version or
// one written before the column existed).
func decodeReplicaDEKs(raw []byte, v *Version) error {
	v.ReplicaDEKs = nil
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, &v.ReplicaDEKs)
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
