package dataproc

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clusters) == 0 && len(s.jobs) == 0 && len(s.operations) == 0 && len(s.templates) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"clusters":          s.clusters,
		"jobs":              s.jobs,
		"operations":        s.operations,
		"workflowTemplates": s.templates,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Clusters          map[string]map[string]Cluster          `json:"clusters"`
		Jobs              map[string]map[string]Job              `json:"jobs"`
		Operations        map[string]map[string]Operation        `json:"operations"`
		WorkflowTemplates map[string]map[string]WorkflowTemplate `json:"workflowTemplates"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Clusters == nil {
		snap.Clusters = map[string]map[string]Cluster{}
	}
	if snap.Jobs == nil {
		snap.Jobs = map[string]map[string]Job{}
	}
	if snap.Operations == nil {
		snap.Operations = map[string]map[string]Operation{}
	}
	if snap.WorkflowTemplates == nil {
		snap.WorkflowTemplates = map[string]map[string]WorkflowTemplate{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = snap.Clusters
	s.jobs = snap.Jobs
	s.operations = snap.Operations
	s.templates = snap.WorkflowTemplates
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_dataproc_clusters`).Scan(&n); err != nil {
		return false, err
	}
	if n > 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_dataproc_jobs`).Scan(&n); err != nil {
		return false, err
	}
	if n > 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_dataproc_operations`).Scan(&n); err != nil {
		return false, err
	}
	if n > 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_dataproc_workflow_templates`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type clusterRow struct {
		ProjectID string  `json:"projectId"`
		Cluster   Cluster `json:"cluster"`
	}
	type jobRow struct {
		ProjectID string `json:"projectId"`
		Job       Job    `json:"job"`
	}
	type operationRow struct {
		ProjectID string    `json:"projectId"`
		Operation Operation `json:"operation"`
	}
	type templateRow struct {
		ProjectID string           `json:"projectId"`
		Template  WorkflowTemplate `json:"template"`
	}

	clusters := make([]clusterRow, 0)
	jobs := make([]jobRow, 0)
	operations := make([]operationRow, 0)
	templates := make([]templateRow, 0)

	crows, err := s.pool.Query(ctx, `
		SELECT project_id, region, cluster_name, config, virtual_cluster_config, labels, status, status_history, cluster_uuid, create_time, update_time
		FROM jc_dataproc_clusters ORDER BY project_id, region, cluster_name
	`)
	if err != nil {
		return err
	}
	for crows.Next() {
		var r clusterRow
		var config, vcc, labels, status, history []byte
		if err := crows.Scan(&r.ProjectID, &r.Cluster.Region, &r.Cluster.Name, &config, &vcc, &labels, &status, &history,
			&r.Cluster.ClusterUUID, &r.Cluster.CreateTime, &r.Cluster.UpdateTime); err != nil {
			crows.Close()
			return err
		}
		r.Cluster.Config = normalizeOptionalJSON(config)
		r.Cluster.VirtualClusterConfig = normalizeOptionalJSON(vcc)
		json.Unmarshal(labels, &r.Cluster.Labels)
		json.Unmarshal(status, &r.Cluster.Status)
		json.Unmarshal(history, &r.Cluster.StatusHistory)
		clusters = append(clusters, r)
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return err
	}

	jrows, err := s.pool.Query(ctx, `
		SELECT project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time, placement_cluster_uuid,
		       scheduling, long_running
		FROM jc_dataproc_jobs ORDER BY project_id, region, job_id
	`)
	if err != nil {
		return err
	}
	for jrows.Next() {
		var r jobRow
		var typeJob, labels, status, history, scheduling []byte
		var longRunning bool
		if err := jrows.Scan(&r.ProjectID, &r.Job.Region, &r.Job.JobID, &r.Job.PlacementClusterName, &r.Job.Type, &typeJob,
			&labels, &status, &history, &r.Job.DriverOutputResourceURI, &r.Job.DriverControlFilesURI,
			&r.Job.JobUUID, &r.Job.CreateTime, &r.Job.PlacementClusterUUID, &scheduling, &longRunning); err != nil {
			jrows.Close()
			return err
		}
		r.Job.TypeJob = json.RawMessage(typeJob)
		json.Unmarshal(labels, &r.Job.Labels)
		json.Unmarshal(status, &r.Job.Status)
		json.Unmarshal(history, &r.Job.StatusHistory)
		r.Job.LongRunning = longRunning
		var sched JobScheduling
		if json.Unmarshal(scheduling, &sched) == nil && (sched.MaxFailuresPerHour != 0 || sched.MaxFailuresTotal != 0) {
			r.Job.Scheduling = &sched
		}
		jobs = append(jobs, r)
	}
	jrows.Close()
	if err := jrows.Err(); err != nil {
		return err
	}

	orows, err := s.pool.Query(ctx, `
		SELECT project_id, region, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_dataproc_operations ORDER BY project_id, region, operation_id
	`)
	if err != nil {
		return err
	}
	for orows.Next() {
		var r operationRow
		if err := orows.Scan(&r.ProjectID, &r.Operation.Region, &r.Operation.ID, &r.Operation.Done, &r.Operation.Metadata,
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

	trows, err := s.pool.Query(ctx, `
		SELECT project_id, region, template_id, version, definition, create_time, update_time
		FROM jc_dataproc_workflow_templates ORDER BY project_id, region, template_id
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r templateRow
		var definition []byte
		if err := trows.Scan(&r.ProjectID, &r.Template.Region, &r.Template.TemplateID, &r.Template.Version,
			&definition, &r.Template.CreateTime, &r.Template.UpdateTime); err != nil {
			trows.Close()
			return err
		}
		r.Template.Definition = normalizeOptionalJSON(definition)
		if r.Template.Definition == nil {
			r.Template.Definition = json.RawMessage("{}")
		}
		templates = append(templates, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"clusters":          clusters,
		"jobs":              jobs,
		"operations":        operations,
		"workflowTemplates": templates,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Clusters []struct {
			ProjectID string  `json:"projectId"`
			Cluster   Cluster `json:"cluster"`
		} `json:"clusters"`
		Jobs []struct {
			ProjectID string `json:"projectId"`
			Job       Job    `json:"job"`
		} `json:"jobs"`
		Operations []struct {
			ProjectID string    `json:"projectId"`
			Operation Operation `json:"operation"`
		} `json:"operations"`
		WorkflowTemplates []struct {
			ProjectID string           `json:"projectId"`
			Template  WorkflowTemplate `json:"template"`
		} `json:"workflowTemplates"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_dataproc_clusters", "jc_dataproc_jobs", "jc_dataproc_operations", "jc_dataproc_workflow_templates"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Clusters {
		labels, _ := json.Marshal(r.Cluster.Labels)
		status, _ := json.Marshal(r.Cluster.Status)
		history, _ := json.Marshal(r.Cluster.StatusHistory)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_dataproc_clusters
				(project_id, region, cluster_name, config, virtual_cluster_config, labels, status, status_history, cluster_uuid, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		`, r.ProjectID, r.Cluster.Region, r.Cluster.Name, nullableJSONRaw(r.Cluster.Config, "{}"), nullableJSONRaw(r.Cluster.VirtualClusterConfig, "{}"),
			nullableJSONRaw(labels, "{}"), nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), r.Cluster.ClusterUUID, r.Cluster.CreateTime, r.Cluster.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Jobs {
		labels, _ := json.Marshal(r.Job.Labels)
		status, _ := json.Marshal(r.Job.Status)
		history, _ := json.Marshal(r.Job.StatusHistory)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_dataproc_jobs
				(project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
				 driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time, placement_cluster_uuid,
				 scheduling, long_running)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		`, r.ProjectID, r.Job.Region, r.Job.JobID, r.Job.PlacementClusterName, r.Job.Type, nullableJSONRaw(r.Job.TypeJob, "{}"),
			nullableJSONRaw(labels, "{}"), nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), r.Job.DriverOutputResourceURI,
			r.Job.DriverControlFilesURI, r.Job.JobUUID, r.Job.CreateTime, r.Job.PlacementClusterUUID,
			nullableJSONRaw(jobSchedulingJSON(r.Job.Scheduling), "{}"), r.Job.LongRunning); err != nil {
			return err
		}
	}
	for _, r := range snap.Operations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_dataproc_operations
				(project_id, region, operation_id, done, metadata, response, verb, target, create_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, r.ProjectID, r.Operation.Region, r.Operation.ID, r.Operation.Done, r.Operation.Metadata, r.Operation.Response,
			r.Operation.Verb, r.Operation.Target, r.Operation.CreateTime, r.Operation.EndTime); err != nil {
			return err
		}
	}
	for _, r := range snap.WorkflowTemplates {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_dataproc_workflow_templates
				(project_id, region, template_id, version, definition, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, r.ProjectID, r.Template.Region, r.Template.TemplateID, r.Template.Version,
			nullableJSONRaw(r.Template.Definition, "{}"), r.Template.CreateTime, r.Template.UpdateTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
