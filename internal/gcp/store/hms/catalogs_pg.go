package hms

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCatalogs is the Postgres-backed multi-catalog store: one
// PostgresStore handle per catalog, all sharing a pool. Catalog rows live in the
// same jc_hms_* tables, discriminated by the catalog column, so memory and
// --dsn behaviour stay identical.
type PostgresCatalogs struct {
	pool *pgxpool.Pool
}

// NewPostgresCatalogs returns a Postgres-backed catalog set.
func NewPostgresCatalogs(pool *pgxpool.Pool) *PostgresCatalogs {
	return &PostgresCatalogs{pool: pool}
}

// Catalog implements Catalogs.
func (c *PostgresCatalogs) Catalog(id string) Store {
	return &PostgresStore{pool: c.pool, catalog: id}
}

// DropCatalog deletes id's catalog rows.
func (c *PostgresCatalogs) DropCatalog(ctx context.Context, id string) {
	for _, tbl := range []string{"jc_hms_partitions", "jc_hms_tables", "jc_hms_databases", "jc_hms_locks"} {
		_, _ = c.pool.Exec(ctx, `DELETE FROM `+tbl+` WHERE catalog=$1`, id)
	}
}

// Reset deletes every catalog's rows.
func (c *PostgresCatalogs) Reset(ctx context.Context) {
	for _, tbl := range []string{"jc_hms_partitions", "jc_hms_tables", "jc_hms_databases", "jc_hms_locks"} {
		_, _ = c.pool.Exec(ctx, `DELETE FROM `+tbl)
	}
}

// IsEmpty reports whether every catalog is empty.
func (c *PostgresCatalogs) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	for _, tbl := range []string{"jc_hms_databases", "jc_hms_tables", "jc_hms_partitions"} {
		if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return false, nil
		}
	}
	return true, nil
}

// catalogIDs returns every catalog id that has any stored row.
func (c *PostgresCatalogs) catalogIDs(ctx context.Context) ([]string, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT DISTINCT catalog FROM jc_hms_databases
		UNION SELECT DISTINCT catalog FROM jc_hms_tables
		UNION SELECT DISTINCT catalog FROM jc_hms_partitions
		UNION SELECT DISTINCT catalog FROM jc_hms_locks
		ORDER BY catalog
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Snapshot writes the v2 envelope: each catalog's flat snapshot keyed by
// catalog id (the legacy catalog is "").
func (c *PostgresCatalogs) Snapshot(ctx context.Context, w io.Writer) error {
	ids, err := c.catalogIDs(ctx)
	if err != nil {
		return err
	}
	env := catalogsSnapshot{Version: catalogsSnapshotVersion, Catalogs: map[string]json.RawMessage{}}
	for _, id := range ids {
		var buf bytes.Buffer
		if err := (&PostgresStore{pool: c.pool, catalog: id}).Snapshot(ctx, &buf); err != nil {
			return err
		}
		env.Catalogs[id] = buf.Bytes()
	}
	return json.NewEncoder(w).Encode(env)
}

// Restore replaces every catalog from a snapshot. A v2 envelope restores each
// catalog it names and drops the rest; a version 1 flat snapshot restores into
// the legacy catalog.
func (c *PostgresCatalogs) Restore(ctx context.Context, r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	env, ok, err := decodeCatalogsSnapshot(raw)
	if err != nil {
		return err
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_hms_partitions", "jc_hms_tables", "jc_hms_databases"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	if !ok {
		var snap postgresSnapshot
		if err := json.Unmarshal(raw, &snap); err != nil {
			return err
		}
		if err := restorePostgresCatalogRows(ctx, tx, LegacyCatalogID, snap); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	for id, data := range env.Catalogs {
		var snap postgresSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
		if err := restorePostgresCatalogRows(ctx, tx, id, snap); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
