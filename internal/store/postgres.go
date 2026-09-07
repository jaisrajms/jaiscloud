// PostgresResourceStore is a PostgreSQL-backed implementation of ResourceStore using pgx/v5.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresResourceStore implements ResourceStore against PostgreSQL.
type PostgresResourceStore struct {
	pool *pgxpool.Pool
}

// poolConfig returns a pgxpool.Config with sensible connection-pool defaults
// similar to HikariCP: bounded pool size, health checks, and fast connection
// acquisition timeouts.
//
// cloud sets the PostgreSQL search_path so every connection in the pool
// automatically resolves unqualified table names to the cloud-specific schema
// (e.g. "aws", "azure", "gcp"). The value is allowlist-validated by
// config.Load() before it reaches here, so no sanitisation is needed.
func poolConfig(dsn, cloud string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 40
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 10 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	if cfg.ConnConfig.Config.RuntimeParams == nil {
		cfg.ConnConfig.Config.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.Config.RuntimeParams["search_path"] = cloud
	return cfg, nil
}

// NewPostgresResourceStore opens a connection pool with startup retry, runs
// migrations, and returns a ready store.  It retries the initial ping up to
// maxAttempts times with exponential backoff so that the server can be started
// before the database is ready (e.g. docker-compose spin-up order).
func NewPostgresResourceStore(ctx context.Context, dsn, cloud string) (*PostgresResourceStore, error) {
	cfg, err := poolConfig(dsn, cloud)
	if err != nil {
		return nil, fmt.Errorf("pgxpool config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}

	const maxAttempts = 10
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pingErr := pool.Ping(ctx)
		if pingErr == nil {
			break
		}
		if attempt == maxAttempts {
			pool.Close()
			return nil, fmt.Errorf("postgres ping: %w", pingErr)
		}
		slog.Warn("postgres not ready, retrying",
			"attempt", attempt, "max", maxAttempts, "backoff", backoff, "err", pingErr)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		// Real wall time: connection retry backoff is an actual sleep duration,
		// not a simulated timestamp — must fire after real elapsed time.
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}

	if err := RunMigrations(ctx, pool, cloud, SharedMigrationFS, "shared"); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}
	return &PostgresResourceStore{pool: pool}, nil
}

// Pool exposes the underlying pool so service-specific stores can share it.
func (s *PostgresResourceStore) Pool() *pgxpool.Pool { return s.pool }

// Close shuts down the connection pool.
func (s *PostgresResourceStore) Close() { s.pool.Close() }

func (s *PostgresResourceStore) Create(ctx context.Context, account, region string, entry ResourceEntry) error {
	if region == "" {
		return fmt.Errorf("store: region must not be empty (type=%s id=%s); use store.GlobalRegion for global services", entry.Type, entry.ID)
	}
	now := clock.Now()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_resources (account_id, region, resource_type, id, data, created_at, updated_at, seeded)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, account, region, entry.Type, entry.ID, json.RawMessage(entry.Data), now, now, entry.Seeded)
	return wrapPgError("Create", err)
}

func (s *PostgresResourceStore) Upsert(ctx context.Context, account, region string, entry ResourceEntry) error {
	if region == "" {
		return fmt.Errorf("store: region must not be empty (type=%s id=%s); use store.GlobalRegion for global services", entry.Type, entry.ID)
	}
	now := clock.Now()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_resources (account_id, region, resource_type, id, data, created_at, updated_at, seeded)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (account_id, region, resource_type, id)
		DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at, seeded = EXCLUDED.seeded
	`, account, region, entry.Type, entry.ID, json.RawMessage(entry.Data), now, now, entry.Seeded)
	return wrapPgError("Upsert", err)
}

func (s *PostgresResourceStore) Get(ctx context.Context, account, region, resourceType, id string) (ResourceEntry, error) {
	var e ResourceEntry
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT resource_type, id, data, created_at, updated_at, seeded
		FROM jc_resources
		WHERE account_id=$1 AND region=$2 AND resource_type=$3 AND id=$4
	`, account, region, resourceType, id).Scan(&e.Type, &e.ID, &data, &e.CreatedAt, &e.UpdatedAt, &e.Seeded)
	if err != nil {
		return ResourceEntry{}, wrapPgError("Get", err)
	}
	e.Data = json.RawMessage(data)
	return e, nil
}

func (s *PostgresResourceStore) Update(ctx context.Context, account, region string, entry ResourceEntry) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_resources
		SET data=$1, updated_at=now()
		WHERE account_id=$2 AND region=$3 AND resource_type=$4 AND id=$5
	`, json.RawMessage(entry.Data), account, region, entry.Type, entry.ID)
	if err != nil {
		return wrapPgError("Update", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UpsertAtomic mirrors MemoryResourceStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the entry for the
// duration of mutate, so a concurrent writer on the same key blocks (or, for
// the narrow case of two concurrent first-writes racing on a not-yet-existing
// row, is retried) instead of silently overwriting this call's result. See
// gcp/store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresResourceStore) UpsertAtomic(ctx context.Context, account, region, resourceType, id string, mutate func(current ResourceEntry, exists bool) (ResourceEntry, error)) (ResourceEntry, error) {
	if region == "" {
		return ResourceEntry{}, fmt.Errorf("store: region must not be empty (type=%s id=%s); use store.GlobalRegion for global services", resourceType, id)
	}
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		entry, err := s.upsertAtomicOnce(ctx, account, region, resourceType, id, mutate)
		if err == nil {
			return entry, nil
		}
		if !isSerializationFailure(err) {
			return ResourceEntry{}, err
		}
		lastErr = err
	}
	return ResourceEntry{}, lastErr
}

func (s *PostgresResourceStore) upsertAtomicOnce(ctx context.Context, account, region, resourceType, id string, mutate func(current ResourceEntry, exists bool) (ResourceEntry, error)) (ResourceEntry, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return ResourceEntry{}, err
	}
	defer tx.Rollback(ctx)

	var current ResourceEntry
	var data []byte
	err = tx.QueryRow(ctx, `
		SELECT resource_type, id, data, created_at, updated_at, seeded
		FROM jc_resources
		WHERE account_id=$1 AND region=$2 AND resource_type=$3 AND id=$4
		FOR UPDATE
	`, account, region, resourceType, id).Scan(&current.Type, &current.ID, &data, &current.CreatedAt, &current.UpdatedAt, &current.Seeded)
	exists := true
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		exists = false
	case err != nil:
		return ResourceEntry{}, wrapPgError("UpsertAtomic", err)
	default:
		current.Data = json.RawMessage(data)
	}

	next, err := mutate(current, exists)
	if err != nil {
		return ResourceEntry{}, err
	}

	now := clock.Now()
	if exists {
		next.CreatedAt = current.CreatedAt
	} else {
		next.CreatedAt = now
	}
	next.UpdatedAt = now
	next.Type = resourceType
	next.ID = id

	_, err = tx.Exec(ctx, `
		INSERT INTO jc_resources (account_id, region, resource_type, id, data, created_at, updated_at, seeded)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (account_id, region, resource_type, id)
		DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at, seeded = EXCLUDED.seeded
	`, account, region, resourceType, id, json.RawMessage(next.Data), next.CreatedAt, next.UpdatedAt, next.Seeded)
	if err != nil {
		return ResourceEntry{}, wrapPgError("UpsertAtomic", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ResourceEntry{}, wrapPgError("UpsertAtomic commit", err)
	}
	return next, nil
}

// isSerializationFailure returns true when err is a PostgreSQL serialization
// failure (SQLSTATE 40001 — could not serialize access), which can occur when
// two UpsertAtomic calls race to create the same not-yet-existing key (no row
// exists for SELECT ... FOR UPDATE to lock, so both transactions proceed to
// INSERT and Postgres's serializable snapshot isolation aborts one of them).
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001"
	}
	return false
}

func (s *PostgresResourceStore) Delete(ctx context.Context, account, region, resourceType, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_resources WHERE account_id=$1 AND region=$2 AND resource_type=$3 AND id=$4
	`, account, region, resourceType, id)
	if err != nil {
		return wrapPgError("Delete", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresResourceStore) List(ctx context.Context, account, region, resourceType, prefix string) ([]ResourceEntry, error) {
	var rows pgx.Rows
	var err error
	if account == "" && region == "" {
		// Cross-scope scan: match all accounts/regions for this resource type.
		// Mirrors MemoryResourceStore behaviour when both account and region are "".
		rows, err = s.pool.Query(ctx, `
			SELECT account_id, region, resource_type, id, data, seeded, created_at, updated_at
			FROM jc_resources
			WHERE resource_type=$1 AND ($2 = '' OR id LIKE '%' || $2 || '%')
			ORDER BY created_at
		`, resourceType, prefix)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT account_id, region, resource_type, id, data, seeded, created_at, updated_at
			FROM jc_resources
			WHERE account_id=$1 AND region=$2 AND resource_type=$3 AND ($4 = '' OR id LIKE '%' || $4 || '%')
			ORDER BY created_at
		`, account, region, resourceType, prefix)
	}
	if err != nil {
		return nil, wrapPgError("List", err)
	}
	defer rows.Close()

	var results []ResourceEntry
	for rows.Next() {
		var e ResourceEntry
		var data []byte
		if err := rows.Scan(&e.Account, &e.Region, &e.Type, &e.ID, &data, &e.Seeded, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, wrapPgError("List scan", err)
		}
		e.Data = json.RawMessage(data)
		results = append(results, e)
	}
	return results, wrapPgError("List rows", rows.Err())
}

func (s *PostgresResourceStore) Purge(ctx context.Context, account, region, resourceType string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_resources WHERE account_id=$1 AND region=$2 AND resource_type=$3`, account, region, resourceType)
	return wrapPgError("Purge", err)
}

func (s *PostgresResourceStore) Reset(ctx context.Context) {
	s.pool.Exec(ctx, `DELETE FROM jc_resources`)
}

func (s *PostgresResourceStore) ResetScope(account, region string) {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_resources WHERE account_id=$1 AND region=$2`, account, region)
}

func (s *PostgresResourceStore) ResetAccount(account string) {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_resources WHERE account_id=$1`, account)
}

func (s *PostgresResourceStore) IsEmpty(ctx context.Context) (bool, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jc_resources WHERE seeded = false`).Scan(&count)
	return count == 0, err

}

func (s *PostgresResourceStore) Snapshot(ctx context.Context, w io.Writer) error {
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, region, resource_type, id, data, seeded, created_at, updated_at
		FROM jc_resources ORDER BY account_id, region, resource_type, id
		`)
	if err != nil {
		return wrapPgError("PostgresResourceStore.Snapshot query: %w", err)
	}
	defer rows.Close()
	entries := make(map[string]ResourceEntry)
	for rows.Next() {
		var e ResourceEntry
		var data []byte
		if err := rows.Scan(&e.Account, &e.Region, &e.Type, &e.ID, &data, &e.Seeded, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return wrapPgError("PostgresResourceStore.Snapshot scan: %w", err)
		}
		e.Data = json.RawMessage(data)
		entries[e.Account+":"+e.Region+":"+e.Type+":"+e.ID] = e
	}
	if err := rows.Err(); err != nil {
		return wrapPgError("PostgresResourceStore.Snapshot rows: %w", err)
	}

	return json.NewEncoder(w).Encode(entries)
}

func (s *PostgresResourceStore) Restore(ctx context.Context, r io.Reader) error {
	var entries map[string]ResourceEntry
	if err := json.NewDecoder(r).Decode(&entries); err != nil {
		return fmt.Errorf("PostgresResourceStore.Restore decode: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("PostgresResourceStore.Restore begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, e := range entries {
		_, err := tx.Exec(ctx, `
			INSERT INTO jc_resources (account_id, region, resource_type, id, data, created_at, updated_at, seeded)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (account_id, region, resource_type, id)
			DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at, seeded = EXCLUDED.seeded
		`, e.Account, e.Region, e.Type, e.ID, json.RawMessage(e.Data), e.CreatedAt, e.UpdatedAt, e.Seeded)
		if err != nil {
			return fmt.Errorf("PostgresResourceStore.Restore insert %s/%s/%s/%s: %w", e.Account, e.Region, e.Type, e.ID, err)
		}
	}

	return tx.Commit(ctx)
}

// wrapPgError classifies a pgx error:
//   - pgx.ErrNoRows          → ErrNotFound
//   - unique-violation (23505) → ErrAlreadyExists
//   - network / connectivity  → ErrStorageUnavailable (wraps original)
//   - anything else           → fmt.Errorf wrapping original
//
// Callers should use this instead of raw error returns so that providers can
// distinguish "not found" from "database is down" without importing pgx.
func wrapPgError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		// Class 08 = connection errors; class 57 = operator intervention
		if len(pgErr.Code) >= 2 && (pgErr.Code[:2] == "08" || pgErr.Code[:2] == "57") {
			return fmt.Errorf("%s: %w: %w", op, ErrStorageUnavailable, err)
		}
		return fmt.Errorf("%s: %w", op, err)
	}
	// pgxpool returns net.Error or context errors when the DB is unreachable
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w: %w", op, ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}
