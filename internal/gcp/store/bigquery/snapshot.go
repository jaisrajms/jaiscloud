package bigquery

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.datasets) == 0 && len(s.tables) == 0 && len(s.jobs) == 0 &&
		len(s.routines) == 0 && len(s.policies) == 0 && len(s.rows) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"datasets":          s.datasets,
		"tables":            s.tables,
		"jobs":              s.jobs,
		"routines":          s.routines,
		"rowAccessPolicies": s.policies,
		"rows":              s.rows,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Datasets          map[string]Dataset         `json:"datasets"`
		Tables            map[string]Table           `json:"tables"`
		Jobs              map[string]Job             `json:"jobs"`
		Routines          map[string]Routine         `json:"routines"`
		RowAccessPolicies map[string]RowAccessPolicy `json:"rowAccessPolicies"`
		Rows              map[string][]Row           `json:"rows"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Datasets == nil {
		snap.Datasets = map[string]Dataset{}
	}
	if snap.Tables == nil {
		snap.Tables = map[string]Table{}
	}
	if snap.Jobs == nil {
		snap.Jobs = map[string]Job{}
	}
	if snap.Routines == nil {
		snap.Routines = map[string]Routine{}
	}
	if snap.RowAccessPolicies == nil {
		snap.RowAccessPolicies = map[string]RowAccessPolicy{}
	}
	if snap.Rows == nil {
		snap.Rows = map[string][]Row{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.datasets = snap.Datasets
	s.tables = snap.Tables
	s.jobs = snap.Jobs
	s.routines = snap.Routines
	s.policies = snap.RowAccessPolicies
	s.rows = snap.Rows
	s.dedup.reset()
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	for _, tbl := range []string{"jc_bq_datasets", "jc_bq_tables", "jc_bq_jobs", "jc_bq_routines", "jc_bq_row_access_policies", "jc_bq_rows"} {
		var n int
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
	type datasetRow struct {
		ProjectID string  `json:"projectId"`
		Dataset   Dataset `json:"dataset"`
	}
	type tableRow struct {
		ProjectID string `json:"projectId"`
		Table     Table  `json:"table"`
	}
	type jobRow struct {
		ProjectID string `json:"projectId"`
		Job       Job    `json:"job"`
	}
	type routineRow struct {
		ProjectID string  `json:"projectId"`
		Routine   Routine `json:"routine"`
	}
	type policyRow struct {
		ProjectID string          `json:"projectId"`
		Policy    RowAccessPolicy `json:"policy"`
	}

	datasets := make([]datasetRow, 0)
	tables := make([]tableRow, 0)
	jobs := make([]jobRow, 0)
	routines := make([]routineRow, 0)
	policies := make([]policyRow, 0)
	rows := make([]Row, 0)

	drows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, config, labels, create_time, update_time
		FROM jc_bq_datasets ORDER BY project_id, dataset_id
	`)
	if err != nil {
		return err
	}
	for drows.Next() {
		var r datasetRow
		var config, labels []byte
		if err := drows.Scan(&r.ProjectID, &r.Dataset.DatasetID, &config, &labels, &r.Dataset.CreateTime, &r.Dataset.UpdateTime); err != nil {
			drows.Close()
			return err
		}
		r.Dataset.Config = json.RawMessage(config)
		json.Unmarshal(labels, &r.Dataset.Labels)
		datasets = append(datasets, r)
	}
	drows.Close()
	if err := drows.Err(); err != nil {
		return err
	}

	trows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time
		FROM jc_bq_tables ORDER BY project_id, dataset_id, table_id
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r tableRow
		var config, schema, labels []byte
		if err := trows.Scan(&r.ProjectID, &r.Table.DatasetID, &r.Table.TableID, &config, &schema, &labels, &r.Table.NumRows, &r.Table.CreateTime, &r.Table.UpdateTime); err != nil {
			trows.Close()
			return err
		}
		r.Table.Config = json.RawMessage(config)
		r.Table.Schema = json.RawMessage(schema)
		json.Unmarshal(labels, &r.Table.Labels)
		tables = append(tables, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	jrows, err := s.pool.Query(ctx, `
		SELECT project_id, job_id, config, create_time
		FROM jc_bq_jobs ORDER BY project_id, job_id
	`)
	if err != nil {
		return err
	}
	for jrows.Next() {
		var r jobRow
		var config []byte
		if err := jrows.Scan(&r.ProjectID, &r.Job.JobID, &config, &r.Job.CreateTime); err != nil {
			jrows.Close()
			return err
		}
		r.Job.Config = json.RawMessage(config)
		jobs = append(jobs, r)
	}
	jrows.Close()
	if err := jrows.Err(); err != nil {
		return err
	}

	rtrows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, routine_id, config, create_time, update_time
		FROM jc_bq_routines ORDER BY project_id, dataset_id, routine_id
	`)
	if err != nil {
		return err
	}
	for rtrows.Next() {
		var r routineRow
		var config []byte
		if err := rtrows.Scan(&r.ProjectID, &r.Routine.DatasetID, &r.Routine.RoutineID, &config, &r.Routine.CreateTime, &r.Routine.UpdateTime); err != nil {
			rtrows.Close()
			return err
		}
		r.Routine.Config = json.RawMessage(config)
		routines = append(routines, r)
	}
	rtrows.Close()
	if err := rtrows.Err(); err != nil {
		return err
	}

	prows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, table_id, policy_id, config, create_time, update_time
		FROM jc_bq_row_access_policies ORDER BY project_id, dataset_id, table_id, policy_id
	`)
	if err != nil {
		return err
	}
	for prows.Next() {
		var r policyRow
		var config []byte
		if err := prows.Scan(&r.ProjectID, &r.Policy.DatasetID, &r.Policy.TableID, &r.Policy.PolicyID, &config, &r.Policy.CreateTime, &r.Policy.UpdateTime); err != nil {
			prows.Close()
			return err
		}
		r.Policy.Config = json.RawMessage(config)
		policies = append(policies, r)
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return err
	}

	rrows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, table_id, seq, data
		FROM jc_bq_rows ORDER BY project_id, dataset_id, table_id, seq
	`)
	if err != nil {
		return err
	}
	for rrows.Next() {
		var r Row
		var data []byte
		if err := rrows.Scan(&r.ProjectID, &r.DatasetID, &r.TableID, &r.Seq, &data); err != nil {
			rrows.Close()
			return err
		}
		r.Data = json.RawMessage(data)
		rows = append(rows, r)
	}
	rrows.Close()
	if err := rrows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"datasets":          datasets,
		"tables":            tables,
		"jobs":              jobs,
		"routines":          routines,
		"rowAccessPolicies": policies,
		"rows":              rows,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Datasets []struct {
			ProjectID string  `json:"projectId"`
			Dataset   Dataset `json:"dataset"`
		} `json:"datasets"`
		Tables []struct {
			ProjectID string `json:"projectId"`
			Table     Table  `json:"table"`
		} `json:"tables"`
		Jobs []struct {
			ProjectID string `json:"projectId"`
			Job       Job    `json:"job"`
		} `json:"jobs"`
		Routines []struct {
			ProjectID string  `json:"projectId"`
			Routine   Routine `json:"routine"`
		} `json:"routines"`
		RowAccessPolicies []struct {
			ProjectID string          `json:"projectId"`
			Policy    RowAccessPolicy `json:"policy"`
		} `json:"rowAccessPolicies"`
		Rows []Row `json:"rows"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_bq_datasets", "jc_bq_tables", "jc_bq_jobs", "jc_bq_routines", "jc_bq_row_access_policies", "jc_bq_rows"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Datasets {
		labels, _ := json.Marshal(r.Dataset.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_datasets (project_id, dataset_id, config, labels, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6)
		`, r.ProjectID, r.Dataset.DatasetID, nullableJSONRaw(r.Dataset.Config, "{}"), nullableJSONRaw(labels, "{}"), r.Dataset.CreateTime, r.Dataset.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Tables {
		labels, _ := json.Marshal(r.Table.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_tables (project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Table.DatasetID, r.Table.TableID, nullableJSONRaw(r.Table.Config, "{}"), nullableJSONRaw(r.Table.Schema, "{}"), nullableJSONRaw(labels, "{}"), r.Table.NumRows, r.Table.CreateTime, r.Table.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Jobs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_jobs (project_id, job_id, config, create_time)
			VALUES ($1,$2,$3,$4)
		`, r.ProjectID, r.Job.JobID, nullableJSONRaw(r.Job.Config, "{}"), r.Job.CreateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Routines {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_routines (project_id, dataset_id, routine_id, config, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6)
		`, r.ProjectID, r.Routine.DatasetID, r.Routine.RoutineID, nullableJSONRaw(r.Routine.Config, "{}"), r.Routine.CreateTime, r.Routine.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.RowAccessPolicies {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_row_access_policies (project_id, dataset_id, table_id, policy_id, config, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, r.ProjectID, r.Policy.DatasetID, r.Policy.TableID, r.Policy.PolicyID, nullableJSONRaw(r.Policy.Config, "{}"), r.Policy.CreateTime, r.Policy.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Rows {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_rows (project_id, dataset_id, table_id, seq, data)
			VALUES ($1,$2,$3,$4,$5)
		`, r.ProjectID, r.DatasetID, r.TableID, r.Seq, nullableJSONRaw(r.Data, "{}")); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
