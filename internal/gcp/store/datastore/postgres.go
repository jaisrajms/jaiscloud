package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against the jc_datastore_entities table plus
// the jc_datastore_id_allocator counter. The Datastore migration
// (gcpstore.MigrationFS, migration 022) must have run before use.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed Store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// entityCols is the SELECT column list for jc_datastore_entities (the project
// and name_or_id columns are re-derivable from Key and scanned but discarded).
const entityCols = "project, kind, name_or_id, properties, version, update_time"

func scanEntity(scan func(...any) error) (Entity, error) {
	var e Entity
	var project, nameOrID string
	var props []byte
	if err := scan(&project, &e.Kind, &nameOrID, &props, &e.Version, &e.UpdateTime); err != nil {
		return Entity{}, err
	}
	if len(props) > 0 {
		_ = json.Unmarshal(props, &e.Properties)
	}
	if e.Properties == nil {
		e.Properties = map[string]Value{}
	}
	e.Key = e.Kind + "/" + nameOrID
	return e, nil
}

func propertiesJSON(props map[string]Value) []byte {
	if props == nil {
		props = map[string]Value{}
	}
	b, err := json.Marshal(props)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// splitKeyCols decomposes an entity's canonical key into the kind and tagged
// id-or-name columns.
func splitKeyCols(e Entity) (kind, nameOrID string, err error) {
	kind, nameOrID, ok := SplitKey(e.Key)
	if !ok {
		return "", "", ErrInvalidKey
	}
	return kind, nameOrID, nil
}

func (s *PostgresStore) Get(ctx context.Context, project, key string) (Entity, error) {
	kind, nameOrID, ok := SplitKey(key)
	if !ok {
		return Entity{}, ErrInvalidKey
	}
	row := s.pool.QueryRow(ctx, `SELECT `+entityCols+` FROM jc_datastore_entities WHERE project=$1 AND kind=$2 AND name_or_id=$3`, project, kind, nameOrID)
	e, err := scanEntity(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entity{}, ErrEntityNotFound
	}
	return e, err
}

func (s *PostgresStore) Insert(ctx context.Context, project string, e Entity) error {
	kind, nameOrID, err := splitKeyCols(e)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_datastore_entities (project, kind, name_or_id, properties)
		VALUES ($1,$2,$3,$4)
	`, project, kind, nameOrID, propertiesJSON(e.Properties))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrEntityExists
		}
		return fmt.Errorf("datastore Insert: %w", err)
	}
	return nil
}

func (s *PostgresStore) Upsert(ctx context.Context, project string, e Entity) error {
	kind, nameOrID, err := splitKeyCols(e)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_datastore_entities (project, kind, name_or_id, properties)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (project, kind, name_or_id) DO UPDATE SET properties = EXCLUDED.properties
	`, project, kind, nameOrID, propertiesJSON(e.Properties))
	if err != nil {
		return fmt.Errorf("datastore Upsert: %w", err)
	}
	return nil
}

func (s *PostgresStore) Update(ctx context.Context, project string, e Entity) error {
	kind, nameOrID, err := splitKeyCols(e)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_datastore_entities SET properties=$4
		WHERE project=$1 AND kind=$2 AND name_or_id=$3
	`, project, kind, nameOrID, propertiesJSON(e.Properties))
	if err != nil {
		return fmt.Errorf("datastore Update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrEntityNotFound
	}
	return nil
}

func (s *PostgresStore) Delete(ctx context.Context, project, key string) error {
	kind, nameOrID, ok := SplitKey(key)
	if !ok {
		return ErrInvalidKey
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_datastore_entities WHERE project=$1 AND kind=$2 AND name_or_id=$3`, project, kind, nameOrID)
	return err
}

// ApplyMutation mirrors MemoryStore's: a Serializable transaction with
// SELECT ... FOR UPDATE row-locks the entity (or its absence — a locked scan
// of zero rows still participates in the transaction's serialization) for
// the duration of the precondition check and apply, so a concurrent mutation
// on the same entity can't land in between. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) ApplyMutation(ctx context.Context, project string, kind MutationKind, e Entity, precondition *Precondition) (Entity, error) {
	entKind, nameOrID, err := splitKeyCols(e)
	if err != nil {
		return Entity{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Entity{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT `+entityCols+` FROM jc_datastore_entities
		WHERE project=$1 AND kind=$2 AND name_or_id=$3 FOR UPDATE
	`, project, entKind, nameOrID)
	current, err := scanEntity(row.Scan)
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		exists = false
	} else if err != nil {
		return Entity{}, err
	}

	if !preconditionMatches(current, exists, precondition) {
		// Report the entity's actual current (unchanged) state — see
		// MemoryStore.ApplyMutation's doc comment on this same return.
		return current, ErrConflict
	}
	switch kind {
	case MutationInsert:
		if exists {
			return Entity{}, ErrEntityExists
		}
		e.Version = 1
	case MutationUpdate:
		if !exists {
			return Entity{}, ErrEntityNotFound
		}
		e.Version = current.Version + 1
	case MutationUpsert:
		e.Version = current.Version + 1
	}
	e.UpdateTime = clock.Now()

	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_datastore_entities (project, kind, name_or_id, properties, version, update_time)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (project, kind, name_or_id) DO UPDATE
			SET properties=EXCLUDED.properties, version=EXCLUDED.version, update_time=EXCLUDED.update_time
	`, project, entKind, nameOrID, propertiesJSON(e.Properties), e.Version, e.UpdateTime); err != nil {
		return Entity{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Entity{}, err
	}
	return e, nil
}

// DeleteConflictChecked mirrors ApplyMutation's locking convention.
func (s *PostgresStore) DeleteConflictChecked(ctx context.Context, project, key string, precondition *Precondition) error {
	kind, nameOrID, ok := SplitKey(key)
	if !ok {
		return ErrInvalidKey
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT `+entityCols+` FROM jc_datastore_entities
		WHERE project=$1 AND kind=$2 AND name_or_id=$3 FOR UPDATE
	`, project, kind, nameOrID)
	current, err := scanEntity(row.Scan)
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		exists = false
	} else if err != nil {
		return err
	}

	if !preconditionMatches(current, exists, precondition) {
		return ErrConflict
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_datastore_entities WHERE project=$1 AND kind=$2 AND name_or_id=$3`, project, kind, nameOrID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListKind(ctx context.Context, project, kind string) ([]Entity, error) {
	var rows pgx.Rows
	var err error
	if kind == "" {
		rows, err = s.pool.Query(ctx, `SELECT `+entityCols+` FROM jc_datastore_entities WHERE project=$1 ORDER BY kind, name_or_id`, project)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT `+entityCols+` FROM jc_datastore_entities WHERE project=$1 AND kind=$2 ORDER BY name_or_id`, project, kind)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Entity, 0)
	for rows.Next() {
		e, err := scanEntity(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *PostgresStore) AllocateIDs(ctx context.Context, project string, n int) ([]int64, error) {
	if n <= 0 {
		return []int64{}, nil
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO jc_datastore_id_allocator (project_id, next_id)
		VALUES ($1, 1)
		ON CONFLICT (project_id) DO NOTHING
	`, project); err != nil {
		return nil, err
	}
	var next int64
	if err := s.pool.QueryRow(ctx, `
		UPDATE jc_datastore_id_allocator SET next_id = next_id + $2
		WHERE project_id = $1
		RETURNING next_id
	`, project, int64(n)).Scan(&next); err != nil {
		return nil, err
	}
	start := next - int64(n)
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = start + int64(i)
	}
	return ids, nil
}

func (s *PostgresStore) AdvanceIDs(ctx context.Context, project string, max int64) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO jc_datastore_id_allocator (project_id, next_id)
		VALUES ($1, 1)
		ON CONFLICT (project_id) DO NOTHING
	`, project); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE jc_datastore_id_allocator SET next_id = GREATEST(next_id, $2)
		WHERE project_id = $1
	`, project, max+1)
	return err
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_datastore_entities`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_datastore_id_allocator`)
}
