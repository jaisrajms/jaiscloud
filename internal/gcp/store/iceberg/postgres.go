package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_iceberg_namespaces and
// jc_iceberg_tables.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func jsonObj(v any) any {
	if v == nil {
		return json.RawMessage("{}")
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

// --- Namespaces ---

func (s *PostgresStore) CreateNamespace(ctx context.Context, namespace string, properties map[string]string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_iceberg_namespaces (namespace, properties) VALUES ($1, $2)
	`, namespace, jsonObj(properties))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrNamespaceExists
		}
		return err
	}
	return nil
}

func scanNamespaceProps(b []byte) map[string]string {
	if len(b) == 0 {
		return map[string]string{}
	}
	m := map[string]string{}
	_ = json.Unmarshal(b, &m)
	if m == nil {
		m = map[string]string{}
	}
	return m
}

func (s *PostgresStore) GetNamespace(ctx context.Context, namespace string) (map[string]string, error) {
	var b []byte
	err := s.pool.QueryRow(ctx, `
		SELECT properties FROM jc_iceberg_namespaces WHERE namespace=$1
	`, namespace).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNamespaceNotFound
	}
	if err != nil {
		return nil, err
	}
	return scanNamespaceProps(b), nil
}

func (s *PostgresStore) ListNamespaces(ctx context.Context) ([]Namespace, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT namespace, properties FROM jc_iceberg_namespaces ORDER BY namespace
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Namespace
	for rows.Next() {
		var ns string
		var b []byte
		if err := rows.Scan(&ns, &b); err != nil {
			return nil, err
		}
		result = append(result, Namespace{Namespace: ns, Properties: scanNamespaceProps(b)})
	}
	return result, rows.Err()
}

func (s *PostgresStore) NamespaceExists(ctx context.Context, namespace string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx, `SELECT 1 FROM jc_iceberg_namespaces WHERE namespace=$1`, namespace).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) UpdateNamespaceProperties(ctx context.Context, namespace string, removals []string, updates map[string]string) (NamespacePropertiesUpdate, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return NamespacePropertiesUpdate{}, err
	}
	defer tx.Rollback(ctx)

	var b []byte
	err = tx.QueryRow(ctx, `
		SELECT properties FROM jc_iceberg_namespaces WHERE namespace=$1 FOR UPDATE
	`, namespace).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return NamespacePropertiesUpdate{}, ErrNamespaceNotFound
	}
	if err != nil {
		return NamespacePropertiesUpdate{}, err
	}
	before := scanNamespaceProps(b)
	next := make(map[string]string, len(before)+len(updates))
	for k, v := range before {
		next[k] = v
	}
	for _, r := range removals {
		delete(next, r)
	}
	for k, v := range updates {
		next[k] = v
	}
	if _, err := tx.Exec(ctx, `UPDATE jc_iceberg_namespaces SET properties=$2 WHERE namespace=$1`, namespace, jsonObj(next)); err != nil {
		return NamespacePropertiesUpdate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return NamespacePropertiesUpdate{}, err
	}
	return NamespacePropertiesUpdate{Before: before, After: next}, nil
}

func (s *PostgresStore) DropNamespace(ctx context.Context, namespace string) error {
	// The emptiness check is enforced atomically by the FK (ON DELETE RESTRICT):
	// a DELETE on a namespace still referenced by a table fails with 23503
	// (foreign_key_violation), closing the check-then-act race where a table
	// could be created between a count probe and the delete.
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_iceberg_namespaces WHERE namespace=$1`, namespace)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrNamespaceNotEmpty
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNamespaceNotFound
	}
	return nil
}

// --- Tables ---

func (s *PostgresStore) CreateTable(ctx context.Context, namespace, name string, t Table) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_iceberg_tables (namespace, table_name, metadata, metadata_location, table_uuid, version)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, namespace, name, jsonObj(t.Metadata), t.MetadataLocation, t.UUID, t.Version)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrTableExists
		}
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrNamespaceNotFound
		}
		return err
	}
	return nil
}

func scanTable(row pgx.Row) (Table, error) {
	var t Table
	var meta []byte
	err := row.Scan(&t.Namespace, &t.Name, &meta, &t.MetadataLocation, &t.UUID, &t.Version)
	if err != nil {
		return Table{}, err
	}
	t.Metadata = json.RawMessage(meta)
	return t, nil
}

const tableColumns = `namespace, table_name, metadata, metadata_location, table_uuid, version`

func (s *PostgresStore) GetTable(ctx context.Context, namespace, name string) (Table, error) {
	t, err := scanTable(s.pool.QueryRow(ctx, `
		SELECT `+tableColumns+` FROM jc_iceberg_tables WHERE namespace=$1 AND table_name=$2
	`, namespace, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrTableNotFound
	}
	return t, err
}

func (s *PostgresStore) ListTables(ctx context.Context, namespace string) ([]Table, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+tableColumns+` FROM jc_iceberg_tables WHERE namespace=$1 ORDER BY table_name
	`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Table
	for rows.Next() {
		t, err := scanTable(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

func (s *PostgresStore) DropTable(ctx context.Context, namespace, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_iceberg_tables WHERE namespace=$1 AND table_name=$2`, namespace, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrTableNotFound
	}
	return nil
}

// CommitTable mirrors MemoryStore's version: a Serializable transaction with
// SELECT ... FOR UPDATE row-locks the table for the duration of mutate, so a
// concurrent commit to the same table blocks instead of silently losing the
// other's update.
func (s *PostgresStore) CommitTable(ctx context.Context, namespace, name string, mutate func(Table) (Table, error)) (Table, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Table{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanTable(tx.QueryRow(ctx, `
		SELECT `+tableColumns+` FROM jc_iceberg_tables WHERE namespace=$1 AND table_name=$2 FOR UPDATE
	`, namespace, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrTableNotFound
	}
	if err != nil {
		return Table{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Table{}, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jc_iceberg_tables SET metadata=$3, metadata_location=$4, table_uuid=$5, version=$6
		WHERE namespace=$1 AND table_name=$2
	`, namespace, name, jsonObj(next.Metadata), next.MetadataLocation, next.UUID, next.Version); err != nil {
		return Table{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Table{}, err
	}
	next.Namespace = namespace
	next.Name = name
	return next, nil
}

// RenameTable is atomic: a Serializable transaction row-locks the source (and
// probes the destination) so a concurrent commit/rename can't race it.
func (s *PostgresStore) RenameTable(ctx context.Context, srcNamespace, srcName, dstNamespace, dstName string) (Table, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Table{}, err
	}
	defer tx.Rollback(ctx)

	src, err := scanTable(tx.QueryRow(ctx, `
		SELECT `+tableColumns+` FROM jc_iceberg_tables WHERE namespace=$1 AND table_name=$2 FOR UPDATE
	`, srcNamespace, srcName))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrTableNotFound
	}
	if err != nil {
		return Table{}, err
	}

	var one int
	err = tx.QueryRow(ctx, `SELECT 1 FROM jc_iceberg_tables WHERE namespace=$1 AND table_name=$2 FOR UPDATE`, dstNamespace, dstName).Scan(&one)
	if err == nil {
		return Table{}, ErrTableExists
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Table{}, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM jc_iceberg_tables WHERE namespace=$1 AND table_name=$2`, srcNamespace, srcName); err != nil {
		return Table{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_iceberg_tables (namespace, table_name, metadata, metadata_location, table_uuid, version)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, dstNamespace, dstName, jsonObj(src.Metadata), src.MetadataLocation, src.UUID, src.Version); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return Table{}, ErrNamespaceNotFound
		}
		// 23505 (unique_violation) closes the race where two concurrent renames
		// target the same destination: the second INSERT collides on the
		// (namespace, table_name) primary key.
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Table{}, ErrTableExists
		}
		return Table{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Table{}, err
	}
	src.Namespace = dstNamespace
	src.Name = dstName
	return src, nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_iceberg_tables`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_iceberg_namespaces`)
}
