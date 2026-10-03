package metastore

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.services) == 0 && len(s.backups) == 0 && len(s.metadataImports) == 0 && len(s.operations) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"services":        s.services,
		"backups":         s.backups,
		"metadataImports": s.metadataImports,
		"operations":      s.operations,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Services        map[string]map[string]Service        `json:"services"`
		Backups         map[string]map[string]Backup         `json:"backups"`
		MetadataImports map[string]map[string]MetadataImport `json:"metadataImports"`
		Operations      map[string]map[string]Operation      `json:"operations"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Services == nil {
		snap.Services = map[string]map[string]Service{}
	}
	if snap.Backups == nil {
		snap.Backups = map[string]map[string]Backup{}
	}
	if snap.MetadataImports == nil {
		snap.MetadataImports = map[string]map[string]MetadataImport{}
	}
	if snap.Operations == nil {
		snap.Operations = map[string]map[string]Operation{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = snap.Services
	s.backups = snap.Backups
	s.metadataImports = snap.MetadataImports
	s.operations = snap.Operations
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	for _, tbl := range []string{"jc_metastore_services", "jc_metastore_backups", "jc_metastore_metadata_imports", "jc_metastore_operations"} {
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
	type serviceRow struct {
		ProjectID string  `json:"projectId"`
		Service   Service `json:"service"`
	}
	type backupRow struct {
		ProjectID string `json:"projectId"`
		Backup    Backup `json:"backup"`
	}
	type importRow struct {
		ProjectID string         `json:"projectId"`
		Import    MetadataImport `json:"import"`
	}
	type operationRow struct {
		ProjectID string    `json:"projectId"`
		Operation Operation `json:"operation"`
	}

	services := make([]serviceRow, 0)
	backups := make([]backupRow, 0)
	imports := make([]importRow, 0)
	operations := make([]operationRow, 0)

	srows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_name, config, labels, state, state_history, create_time, update_time
		FROM jc_metastore_services ORDER BY project_id, location, service_name
	`)
	if err != nil {
		return err
	}
	for srows.Next() {
		var r serviceRow
		var config, labels, history []byte
		if err := srows.Scan(&r.ProjectID, &r.Service.Location, &r.Service.Name, &config, &labels, &r.Service.State, &history,
			&r.Service.CreateTime, &r.Service.UpdateTime); err != nil {
			srows.Close()
			return err
		}
		r.Service.Config = json.RawMessage(config)
		json.Unmarshal(labels, &r.Service.Labels)
		json.Unmarshal(history, &r.Service.StateHistory)
		services = append(services, r)
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return err
	}

	brows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_name, backup_name, config, description, state, create_time, end_time
		FROM jc_metastore_backups ORDER BY project_id, location, service_name, backup_name
	`)
	if err != nil {
		return err
	}
	for brows.Next() {
		var r backupRow
		var config []byte
		if err := brows.Scan(&r.ProjectID, &r.Backup.Location, &r.Backup.ServiceName, &r.Backup.Name, &config, &r.Backup.Description,
			&r.Backup.State, &r.Backup.CreateTime, &r.Backup.EndTime); err != nil {
			brows.Close()
			return err
		}
		r.Backup.Config = json.RawMessage(config)
		backups = append(backups, r)
	}
	brows.Close()
	if err := brows.Err(); err != nil {
		return err
	}

	irows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_name, import_name, config, description, state, create_time, update_time, end_time
		FROM jc_metastore_metadata_imports ORDER BY project_id, location, service_name, import_name
	`)
	if err != nil {
		return err
	}
	for irows.Next() {
		var r importRow
		var config []byte
		if err := irows.Scan(&r.ProjectID, &r.Import.Location, &r.Import.ServiceName, &r.Import.Name, &config, &r.Import.Description,
			&r.Import.State, &r.Import.CreateTime, &r.Import.UpdateTime, &r.Import.EndTime); err != nil {
			irows.Close()
			return err
		}
		r.Import.Config = json.RawMessage(config)
		imports = append(imports, r)
	}
	irows.Close()
	if err := irows.Err(); err != nil {
		return err
	}

	orows, err := s.pool.Query(ctx, `
		SELECT project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_metastore_operations ORDER BY project_id, location, operation_id
	`)
	if err != nil {
		return err
	}
	for orows.Next() {
		var r operationRow
		if err := orows.Scan(&r.ProjectID, &r.Operation.Location, &r.Operation.ID, &r.Operation.Done, &r.Operation.Metadata,
			&r.Operation.Response, &r.Operation.Verb, &r.Operation.Target, &r.Operation.CreateTime, &r.Operation.EndTime); err != nil {
			orows.Close()
			return err
		}
		operations = append(operations, r)
	}
	orows.Close()
	if err := orows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"services":        services,
		"backups":         backups,
		"metadataImports": imports,
		"operations":      operations,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Services []struct {
			ProjectID string  `json:"projectId"`
			Service   Service `json:"service"`
		} `json:"services"`
		Backups []struct {
			ProjectID string `json:"projectId"`
			Backup    Backup `json:"backup"`
		} `json:"backups"`
		MetadataImports []struct {
			ProjectID string         `json:"projectId"`
			Import    MetadataImport `json:"import"`
		} `json:"metadataImports"`
		Operations []struct {
			ProjectID string    `json:"projectId"`
			Operation Operation `json:"operation"`
		} `json:"operations"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_metastore_services", "jc_metastore_backups", "jc_metastore_metadata_imports", "jc_metastore_operations"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Services {
		labels, _ := json.Marshal(r.Service.Labels)
		history, _ := json.Marshal(r.Service.StateHistory)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_metastore_services
				(project_id, location, service_name, config, labels, state, state_history, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Service.Location, r.Service.Name, nullableJSONRaw(r.Service.Config, "{}"), nullableJSONRaw(labels, "{}"),
			r.Service.State, nullableJSONRaw(history, "[]"), r.Service.CreateTime, r.Service.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Backups {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_metastore_backups
				(project_id, location, service_name, backup_name, config, description, state, create_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Backup.Location, r.Backup.ServiceName, r.Backup.Name, nullableJSONRaw(r.Backup.Config, "{}"),
			r.Backup.Description, r.Backup.State, r.Backup.CreateTime, r.Backup.EndTime); err != nil {
			return err
		}
	}
	for _, r := range snap.MetadataImports {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_metastore_metadata_imports
				(project_id, location, service_name, import_name, config, description, state, create_time, update_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, r.ProjectID, r.Import.Location, r.Import.ServiceName, r.Import.Name, nullableJSONRaw(r.Import.Config, "{}"),
			r.Import.Description, r.Import.State, r.Import.CreateTime, r.Import.UpdateTime, r.Import.EndTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Operations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_metastore_operations
				(project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, r.ProjectID, r.Operation.Location, r.Operation.ID, r.Operation.Done, r.Operation.Metadata, r.Operation.Response,
			r.Operation.Verb, r.Operation.Target, r.Operation.CreateTime, r.Operation.EndTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
