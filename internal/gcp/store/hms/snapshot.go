package hms

import (
	"context"
	"encoding/json"
	"io"
)

// Snapshot/restore covers only catalog metadata (databases + tables +
// partitions). Locks are transient concurrency state — like the GCS generation
// counter — and are deliberately excluded so a periodic snapshot never persists
// a held lock.

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.databases) == 0 && len(s.tables) == 0 && len(s.partitions) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"databases":  s.databases,
		"tables":     s.tables,
		"partitions": s.partitions,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Databases  map[string]Database                        `json:"databases"`
		Tables     map[string]map[string]Table                `json:"tables"`
		Partitions map[string]map[string]map[string]Partition `json:"partitions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Databases == nil {
		snap.Databases = map[string]Database{}
	}
	if snap.Tables == nil {
		snap.Tables = map[string]map[string]Table{}
	}
	if snap.Partitions == nil {
		snap.Partitions = map[string]map[string]map[string]Partition{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databases = snap.Databases
	s.tables = snap.Tables
	s.partitions = snap.Partitions
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	for _, tbl := range []string{"jc_hms_databases", "jc_hms_tables", "jc_hms_partitions"} {
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
	type databaseRow struct {
		Name     string   `json:"name"`
		Database Database `json:"database"`
	}
	type tableRow struct {
		DBName    string `json:"dbName"`
		TableName string `json:"tableName"`
		TableJSON []byte `json:"table"`
	}
	type partitionRow struct {
		DBName    string   `json:"dbName"`
		TableName string   `json:"tableName"`
		Values    []string `json:"values"`
		PartJSON  []byte   `json:"partition"`
	}

	databases := make([]databaseRow, 0)
	tables := make([]tableRow, 0)
	partitions := make([]partitionRow, 0)

	drows, err := s.pool.Query(ctx, `
		SELECT name, location_uri, parameters, description, owner
		FROM jc_hms_databases ORDER BY name
	`)
	if err != nil {
		return err
	}
	for drows.Next() {
		var r databaseRow
		var params []byte
		if err := drows.Scan(&r.Name, &r.Database.LocationURI, &params, &r.Database.Description, &r.Database.Owner); err != nil {
			drows.Close()
			return err
		}
		r.Database.Name = r.Name
		r.Database.Parameters = scanStrMap(params)
		databases = append(databases, r)
	}
	drows.Close()
	if err := drows.Err(); err != nil {
		return err
	}

	trows, err := s.pool.Query(ctx, `
		SELECT db_name, table_name, table_json FROM jc_hms_tables ORDER BY db_name, table_name
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r tableRow
		if err := trows.Scan(&r.DBName, &r.TableName, &r.TableJSON); err != nil {
			trows.Close()
			return err
		}
		tables = append(tables, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	prows, err := s.pool.Query(ctx, `
		SELECT db_name, table_name, values_json, part_json
		FROM jc_hms_partitions ORDER BY db_name, table_name, part_key
	`)
	if err != nil {
		return err
	}
	for prows.Next() {
		var r partitionRow
		var values []byte
		if err := prows.Scan(&r.DBName, &r.TableName, &values, &r.PartJSON); err != nil {
			prows.Close()
			return err
		}
		if len(values) > 0 {
			_ = json.Unmarshal(values, &r.Values)
		}
		if r.Values == nil {
			r.Values = []string{}
		}
		partitions = append(partitions, r)
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"databases":  databases,
		"tables":     tables,
		"partitions": partitions,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Databases []struct {
			Name     string   `json:"name"`
			Database Database `json:"database"`
		} `json:"databases"`
		Tables []struct {
			DBName    string `json:"dbName"`
			TableName string `json:"tableName"`
			TableJSON []byte `json:"table"`
		} `json:"tables"`
		Partitions []struct {
			DBName    string   `json:"dbName"`
			TableName string   `json:"tableName"`
			Values    []string `json:"values"`
			PartJSON  []byte   `json:"partition"`
		} `json:"partitions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_hms_partitions", "jc_hms_tables", "jc_hms_databases"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, dr := range snap.Databases {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_hms_databases (name, location_uri, parameters, description, owner)
			VALUES ($1,$2,$3,$4,$5)
		`, dr.Database.Name, dr.Database.LocationURI, jsonObj(dr.Database.Parameters), dr.Database.Description, dr.Database.Owner); err != nil {
			return err
		}
	}
	for _, tr := range snap.Tables {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_hms_tables (db_name, table_name, table_json)
			VALUES ($1,$2,$3)
		`, tr.DBName, tr.TableName, jsonObj(json.RawMessage(tr.TableJSON))); err != nil {
			return err
		}
	}
	for _, pr := range snap.Partitions {
		values := pr.Values
		if values == nil {
			values = []string{}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_hms_partitions (db_name, table_name, part_key, values_json, part_json)
			VALUES ($1,$2,$3,$4,$5)
		`, pr.DBName, pr.TableName, PartitionKey(values), jsonObj(values), jsonObj(json.RawMessage(pr.PartJSON))); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
