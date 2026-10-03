package iceberg

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.namespaces) == 0 && len(s.tables) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"namespaces": s.namespaces,
		"tables":     s.tables,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Namespaces map[string]map[string]string `json:"namespaces"`
		Tables     map[string]map[string]Table  `json:"tables"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Namespaces == nil {
		snap.Namespaces = map[string]map[string]string{}
	}
	if snap.Tables == nil {
		snap.Tables = map[string]map[string]Table{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.namespaces = snap.Namespaces
	s.tables = snap.Tables
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	for _, tbl := range []string{"jc_iceberg_namespaces", "jc_iceberg_tables"} {
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type namespaceRow struct {
		Namespace  string            `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	type tableRow struct {
		Namespace string `json:"namespace"`
		Table     Table  `json:"table"`
	}

	namespaces := make([]namespaceRow, 0)
	tables := make([]tableRow, 0)

	nrows, err := s.pool.Query(ctx, `SELECT namespace, properties FROM jc_iceberg_namespaces ORDER BY namespace`)
	if err != nil {
		return err
	}
	for nrows.Next() {
		var r namespaceRow
		var b []byte
		if err := nrows.Scan(&r.Namespace, &b); err != nil {
			nrows.Close()
			return err
		}
		r.Properties = scanNamespaceProps(b)
		namespaces = append(namespaces, r)
	}
	nrows.Close()
	if err := nrows.Err(); err != nil {
		return err
	}

	trows, err := s.pool.Query(ctx, `
		SELECT namespace, table_name, metadata, metadata_location, table_uuid, version
		FROM jc_iceberg_tables ORDER BY namespace, table_name
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r tableRow
		var meta []byte
		if err := trows.Scan(&r.Namespace, &r.Table.Name, &meta, &r.Table.MetadataLocation, &r.Table.UUID, &r.Table.Version); err != nil {
			trows.Close()
			return err
		}
		r.Table.Namespace = r.Namespace
		r.Table.Metadata = json.RawMessage(meta)
		tables = append(tables, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"namespaces": namespaces,
		"tables":     tables,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Namespaces []struct {
			Namespace  string            `json:"namespace"`
			Properties map[string]string `json:"properties"`
		} `json:"namespaces"`
		Tables []struct {
			Namespace string `json:"namespace"`
			Table     Table  `json:"table"`
		} `json:"tables"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_iceberg_tables", "jc_iceberg_namespaces"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, ns := range snap.Namespaces {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_iceberg_namespaces (namespace, properties) VALUES ($1,$2)`, ns.Namespace, jsonObj(ns.Properties)); err != nil {
			return err
		}
	}
	for _, tr := range snap.Tables {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_iceberg_tables (namespace, table_name, metadata, metadata_location, table_uuid, version)
			VALUES ($1,$2,$3,$4,$5,$6)
		`, tr.Namespace, tr.Table.Name, jsonObj(tr.Table.Metadata), tr.Table.MetadataLocation, tr.Table.UUID, tr.Table.Version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
