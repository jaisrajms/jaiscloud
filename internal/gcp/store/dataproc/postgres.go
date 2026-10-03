package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_dataproc_clusters / jc_dataproc_jobs /
// jc_dataproc_operations.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// --- Clusters ---

func (s *PostgresStore) CreateCluster(ctx context.Context, projectID, region string, c Cluster) error {
	if c.CreateTime.IsZero() {
		c.CreateTime = clock.Now()
	}
	if c.UpdateTime.IsZero() {
		c.UpdateTime = c.CreateTime
	}
	labels, _ := json.Marshal(c.Labels)
	status, _ := json.Marshal(c.Status)
	history, _ := json.Marshal(c.StatusHistory)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_dataproc_clusters
			(project_id, region, cluster_name, config, virtual_cluster_config, labels, status, status_history, cluster_uuid, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, projectID, region, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(c.VirtualClusterConfig, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), c.ClusterUUID, c.CreateTime, c.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanCluster(row pgx.Row) (Cluster, error) {
	var c Cluster
	var config, vcc, labels, status, history []byte
	err := row.Scan(&c.ProjectID, &c.Region, &c.Name, &config, &vcc, &labels, &status, &history, &c.ClusterUUID, &c.CreateTime, &c.UpdateTime)
	if err != nil {
		return Cluster{}, err
	}
	c.Config = normalizeOptionalJSON(config)
	c.VirtualClusterConfig = normalizeOptionalJSON(vcc)
	json.Unmarshal(labels, &c.Labels)
	json.Unmarshal(status, &c.Status)
	json.Unmarshal(history, &c.StatusHistory)
	return c, nil
}

// normalizeOptionalJSON maps the "unset" sentinels (empty object / SQL null)
// back to nil so a cluster created without a virtualClusterConfig does not
// render an empty `virtualClusterConfig` object.
func normalizeOptionalJSON(b []byte) json.RawMessage {
	if len(b) == 0 || string(b) == "{}" || string(b) == "null" {
		return nil
	}
	return json.RawMessage(b)
}

func (s *PostgresStore) GetCluster(ctx context.Context, projectID, region, name string) (Cluster, error) {
	c, err := scanCluster(s.pool.QueryRow(ctx, `
		SELECT project_id, region, cluster_name, config, virtual_cluster_config, labels, status, status_history, cluster_uuid, create_time, update_time
		FROM jc_dataproc_clusters WHERE project_id=$1 AND region=$2 AND cluster_name=$3
	`, projectID, region, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Cluster{}, ErrNoSuchCluster
	}
	return c, err
}

func (s *PostgresStore) UpdateCluster(ctx context.Context, projectID, region string, c Cluster) error {
	labels, _ := json.Marshal(c.Labels)
	status, _ := json.Marshal(c.Status)
	history, _ := json.Marshal(c.StatusHistory)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_dataproc_clusters SET config=$4, virtual_cluster_config=$5, labels=$6, status=$7, status_history=$8, cluster_uuid=$9, update_time=$10
		WHERE project_id=$1 AND region=$2 AND cluster_name=$3
	`, projectID, region, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(c.VirtualClusterConfig, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), c.ClusterUUID, c.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchCluster
	}
	return nil
}

// UpdateClusterAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the cluster for the
// duration of mutate, so a concurrent UpdateClusterAtomic on the same
// cluster (e.g. a labels PATCH racing a StartCluster/StopCluster status
// transition) blocks until this transaction commits or rolls back, instead
// of racing to silently overwrite this call's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateClusterAtomic(ctx context.Context, projectID, region, name string, mutate func(Cluster) (Cluster, error)) (Cluster, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Cluster{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanCluster(tx.QueryRow(ctx, `
		SELECT project_id, region, cluster_name, config, virtual_cluster_config, labels, status, status_history, cluster_uuid, create_time, update_time
		FROM jc_dataproc_clusters WHERE project_id=$1 AND region=$2 AND cluster_name=$3 FOR UPDATE
	`, projectID, region, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Cluster{}, ErrNoSuchCluster
	}
	if err != nil {
		return Cluster{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Cluster{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	status, _ := json.Marshal(next.Status)
	history, _ := json.Marshal(next.StatusHistory)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_dataproc_clusters SET config=$4, virtual_cluster_config=$5, labels=$6, status=$7, status_history=$8, cluster_uuid=$9, update_time=$10
		WHERE project_id=$1 AND region=$2 AND cluster_name=$3
	`, projectID, region, name, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(next.VirtualClusterConfig, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), next.ClusterUUID, next.UpdateTime)
	if err != nil {
		return Cluster{}, err
	}
	if tag.RowsAffected() == 0 {
		return Cluster{}, ErrNoSuchCluster
	}
	if err := tx.Commit(ctx); err != nil {
		return Cluster{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	return next, nil
}

func (s *PostgresStore) DeleteCluster(ctx context.Context, projectID, region, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_dataproc_clusters WHERE project_id=$1 AND region=$2 AND cluster_name=$3`, projectID, region, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchCluster
	}
	return nil
}

func (s *PostgresStore) ListClusters(ctx context.Context, projectID, region string) ([]Cluster, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, region, cluster_name, config, virtual_cluster_config, labels, status, status_history, cluster_uuid, create_time, update_time
		FROM jc_dataproc_clusters WHERE project_id=$1 AND region=$2 ORDER BY cluster_name
	`, projectID, region)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

// --- Jobs ---

func (s *PostgresStore) CreateJob(ctx context.Context, projectID, region string, j Job) error {
	if j.CreateTime.IsZero() {
		j.CreateTime = clock.Now()
	}
	labels, _ := json.Marshal(j.Labels)
	status, _ := json.Marshal(j.Status)
	history, _ := json.Marshal(j.StatusHistory)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_dataproc_jobs
			(project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
			 driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time, placement_cluster_uuid,
			 scheduling, long_running)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
	`, projectID, region, j.JobID, j.PlacementClusterName, j.Type, nullableJSONRaw(j.TypeJob, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), j.DriverOutputResourceURI, j.DriverControlFilesURI,
		j.JobUUID, j.CreateTime, j.PlacementClusterUUID, nullableJSONRaw(jobSchedulingJSON(j.Scheduling), "{}"), j.LongRunning)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// jobSchedulingJSON marshals a job's restart policy for the scheduling JSONB
// column, returning nil for an unset policy (persisted as "{}").
func jobSchedulingJSON(s *JobScheduling) []byte {
	if s == nil {
		return nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil
	}
	return b
}

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var typeJob, labels, status, history, scheduling []byte
	var longRunning bool
	err := row.Scan(&j.ProjectID, &j.Region, &j.JobID, &j.PlacementClusterName, &j.Type, &typeJob, &labels, &status, &history,
		&j.DriverOutputResourceURI, &j.DriverControlFilesURI, &j.JobUUID, &j.CreateTime, &j.PlacementClusterUUID,
		&scheduling, &longRunning)
	if err != nil {
		return Job{}, err
	}
	j.TypeJob = json.RawMessage(typeJob)
	json.Unmarshal(labels, &j.Labels)
	json.Unmarshal(status, &j.Status)
	json.Unmarshal(history, &j.StatusHistory)
	j.LongRunning = longRunning
	// An all-zero policy is the API default "no restarts" and is persisted as
	// "{}", so scan it back as unset rather than an empty object.
	var sched JobScheduling
	if json.Unmarshal(scheduling, &sched) == nil && (sched.MaxFailuresPerHour != 0 || sched.MaxFailuresTotal != 0) {
		j.Scheduling = &sched
	}
	return j, nil
}

func (s *PostgresStore) GetJob(ctx context.Context, projectID, region, jobID string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time, placement_cluster_uuid,
		       scheduling, long_running
		FROM jc_dataproc_jobs WHERE project_id=$1 AND region=$2 AND job_id=$3
	`, projectID, region, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoSuchJob
	}
	return j, err
}

func (s *PostgresStore) UpdateJob(ctx context.Context, projectID, region string, j Job) error {
	labels, _ := json.Marshal(j.Labels)
	status, _ := json.Marshal(j.Status)
	history, _ := json.Marshal(j.StatusHistory)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_dataproc_jobs SET placement_cluster_name=$4, job_type=$5, type_job=$6, labels=$7, status=$8,
		       status_history=$9, driver_output_resource_uri=$10, driver_control_files_uri=$11, job_uuid=$12, placement_cluster_uuid=$13,
		       scheduling=$14, long_running=$15
		WHERE project_id=$1 AND region=$2 AND job_id=$3
	`, projectID, region, j.JobID, j.PlacementClusterName, j.Type, nullableJSONRaw(j.TypeJob, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), j.DriverOutputResourceURI, j.DriverControlFilesURI, j.JobUUID, j.PlacementClusterUUID,
		nullableJSONRaw(jobSchedulingJSON(j.Scheduling), "{}"), j.LongRunning)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchJob
	}
	return nil
}

// UpdateJobAtomic mirrors MemoryStore's version: a Serializable transaction
// with SELECT ... FOR UPDATE row-locks the job for the duration of mutate,
// so CancelJob and finishJob (which both call this) can't race to overwrite
// each other's terminal-state transition. See store/firestore/postgres.go's
// Commit for the same convention.
func (s *PostgresStore) UpdateJobAtomic(ctx context.Context, projectID, region, jobID string, mutate func(Job) (Job, error)) (Job, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanJob(tx.QueryRow(ctx, `
		SELECT project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time, placement_cluster_uuid,
		       scheduling, long_running
		FROM jc_dataproc_jobs WHERE project_id=$1 AND region=$2 AND job_id=$3 FOR UPDATE
	`, projectID, region, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoSuchJob
	}
	if err != nil {
		return Job{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Job{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	status, _ := json.Marshal(next.Status)
	history, _ := json.Marshal(next.StatusHistory)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_dataproc_jobs SET placement_cluster_name=$4, job_type=$5, type_job=$6, labels=$7, status=$8,
		       status_history=$9, driver_output_resource_uri=$10, driver_control_files_uri=$11, job_uuid=$12, placement_cluster_uuid=$13,
		       scheduling=$14, long_running=$15
		WHERE project_id=$1 AND region=$2 AND job_id=$3
	`, projectID, region, jobID, next.PlacementClusterName, next.Type, nullableJSONRaw(next.TypeJob, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), next.DriverOutputResourceURI, next.DriverControlFilesURI, next.JobUUID, next.PlacementClusterUUID,
		nullableJSONRaw(jobSchedulingJSON(next.Scheduling), "{}"), next.LongRunning)
	if err != nil {
		return Job{}, err
	}
	if tag.RowsAffected() == 0 {
		return Job{}, ErrNoSuchJob
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	return next, nil
}

func (s *PostgresStore) DeleteJob(ctx context.Context, projectID, region, jobID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_dataproc_jobs WHERE project_id=$1 AND region=$2 AND job_id=$3`, projectID, region, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchJob
	}
	return nil
}

func (s *PostgresStore) ListJobs(ctx context.Context, projectID, region string) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time, placement_cluster_uuid,
		       scheduling, long_running
		FROM jc_dataproc_jobs WHERE project_id=$1 AND region=$2 ORDER BY job_id
	`, projectID, region)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].JobID < result[j].JobID })
	return result, rows.Err()
}

// --- Workflow templates ---

func (s *PostgresStore) CreateWorkflowTemplate(ctx context.Context, projectID, region string, t WorkflowTemplate) error {
	if t.CreateTime.IsZero() {
		t.CreateTime = clock.Now()
	}
	if t.UpdateTime.IsZero() {
		t.UpdateTime = t.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_dataproc_workflow_templates
			(project_id, region, template_id, version, definition, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, projectID, region, t.TemplateID, t.Version, nullableJSONRaw(t.Definition, "{}"), t.CreateTime, t.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanWorkflowTemplate(row pgx.Row) (WorkflowTemplate, error) {
	var t WorkflowTemplate
	var definition []byte
	err := row.Scan(&t.ProjectID, &t.Region, &t.TemplateID, &t.Version, &definition, &t.CreateTime, &t.UpdateTime)
	if err != nil {
		return WorkflowTemplate{}, err
	}
	t.Definition = normalizeOptionalJSON(definition)
	if t.Definition == nil {
		t.Definition = json.RawMessage("{}")
	}
	return t, nil
}

func (s *PostgresStore) GetWorkflowTemplate(ctx context.Context, projectID, region, templateID string) (WorkflowTemplate, error) {
	t, err := scanWorkflowTemplate(s.pool.QueryRow(ctx, `
		SELECT project_id, region, template_id, version, definition, create_time, update_time
		FROM jc_dataproc_workflow_templates WHERE project_id=$1 AND region=$2 AND template_id=$3
	`, projectID, region, templateID))
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowTemplate{}, ErrNoSuchWorkflowTemplate
	}
	return t, err
}

// UpdateWorkflowTemplateAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the template for the
// duration of mutate, so concurrent version bumps serialize.
func (s *PostgresStore) UpdateWorkflowTemplateAtomic(ctx context.Context, projectID, region, templateID string, mutate func(WorkflowTemplate) (WorkflowTemplate, error)) (WorkflowTemplate, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return WorkflowTemplate{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanWorkflowTemplate(tx.QueryRow(ctx, `
		SELECT project_id, region, template_id, version, definition, create_time, update_time
		FROM jc_dataproc_workflow_templates WHERE project_id=$1 AND region=$2 AND template_id=$3 FOR UPDATE
	`, projectID, region, templateID))
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowTemplate{}, ErrNoSuchWorkflowTemplate
	}
	if err != nil {
		return WorkflowTemplate{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return WorkflowTemplate{}, err
	}
	if next.UpdateTime.IsZero() {
		next.UpdateTime = clock.Now()
	}
	tag, err := tx.Exec(ctx, `
		UPDATE jc_dataproc_workflow_templates SET version=$4, definition=$5, update_time=$6
		WHERE project_id=$1 AND region=$2 AND template_id=$3
	`, projectID, region, templateID, next.Version, nullableJSONRaw(next.Definition, "{}"), next.UpdateTime)
	if err != nil {
		return WorkflowTemplate{}, err
	}
	if tag.RowsAffected() == 0 {
		return WorkflowTemplate{}, ErrNoSuchWorkflowTemplate
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkflowTemplate{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	return next, nil
}

func (s *PostgresStore) DeleteWorkflowTemplate(ctx context.Context, projectID, region, templateID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_dataproc_workflow_templates WHERE project_id=$1 AND region=$2 AND template_id=$3`, projectID, region, templateID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchWorkflowTemplate
	}
	return nil
}

func (s *PostgresStore) ListWorkflowTemplates(ctx context.Context, projectID, region string) ([]WorkflowTemplate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, region, template_id, version, definition, create_time, update_time
		FROM jc_dataproc_workflow_templates WHERE project_id=$1 AND region=$2 ORDER BY template_id
	`, projectID, region)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []WorkflowTemplate
	for rows.Next() {
		t, err := scanWorkflowTemplate(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TemplateID < result[j].TemplateID })
	return result, rows.Err()
}

// --- Operations ---

func (s *PostgresStore) CreateOperation(ctx context.Context, projectID, region string, op Operation) error {
	if op.CreateTime.IsZero() {
		op.CreateTime = clock.Now()
	}
	if op.EndTime.IsZero() {
		op.EndTime = op.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_dataproc_operations
			(project_id, region, operation_id, done, metadata, response, verb, target, create_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, projectID, region, op.ID, op.Done, op.Metadata, op.Response, op.Verb, op.Target, op.CreateTime, op.EndTime)
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) GetOperation(ctx context.Context, projectID, region, id string) (Operation, error) {
	var op Operation
	err := s.pool.QueryRow(ctx, `
		SELECT project_id, region, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_dataproc_operations WHERE project_id=$1 AND region=$2 AND operation_id=$3
	`, projectID, region, id).Scan(&op.ProjectID, &op.Region, &op.ID, &op.Done, &op.Metadata, &op.Response, &op.Verb, &op.Target,
		&op.CreateTime, &op.EndTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	return op, err
}

func (s *PostgresStore) UpdateOperation(ctx context.Context, projectID, region string, op Operation) error {
	if op.EndTime.IsZero() {
		op.EndTime = clock.Now()
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_dataproc_operations SET done=$4, metadata=$5, response=$6, verb=$7, target=$8, end_time=$9
		WHERE project_id=$1 AND region=$2 AND operation_id=$3
	`, projectID, region, op.ID, op.Done, op.Metadata, op.Response, op.Verb, op.Target, op.EndTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchOperation
	}
	return nil
}

// UpdateOperationAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the operation for the
// duration of mutate, so a metadata refresh cannot race a concurrent
// completion into a last-write-wins regression.
func (s *PostgresStore) UpdateOperationAtomic(ctx context.Context, projectID, region, id string, mutate func(Operation) (Operation, error)) (Operation, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback(ctx)

	var current Operation
	err = tx.QueryRow(ctx, `
		SELECT project_id, region, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_dataproc_operations WHERE project_id=$1 AND region=$2 AND operation_id=$3 FOR UPDATE
	`, projectID, region, id).Scan(&current.ProjectID, &current.Region, &current.ID, &current.Done, &current.Metadata, &current.Response,
		&current.Verb, &current.Target, &current.CreateTime, &current.EndTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	if err != nil {
		return Operation{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Operation{}, err
	}
	if next.EndTime.IsZero() {
		next.EndTime = clock.Now()
	}
	tag, err := tx.Exec(ctx, `
		UPDATE jc_dataproc_operations SET done=$4, metadata=$5, response=$6, verb=$7, target=$8, end_time=$9
		WHERE project_id=$1 AND region=$2 AND operation_id=$3
	`, projectID, region, id, next.Done, next.Metadata, next.Response, next.Verb, next.Target, next.EndTime)
	if err != nil {
		return Operation{}, err
	}
	if tag.RowsAffected() == 0 {
		return Operation{}, ErrNoSuchOperation
	}
	if err := tx.Commit(ctx); err != nil {
		return Operation{}, err
	}
	next.ProjectID = projectID
	next.Region = region
	return next, nil
}

// DeleteStaleOperations removes completed operations older than cutoff (all
// scopes). In-flight operations (done=false) are retained.
func (s *PostgresStore) DeleteStaleOperations(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_dataproc_operations WHERE create_time < $1 AND done = true`, cutoff)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_clusters`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_jobs`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_operations`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_workflow_templates`)
}

// nullableJSONRaw renders nil config/type_job as SQL NULL, otherwise the raw
// JSON bytes (kept verbatim).
// nullableJSONRaw returns a json.RawMessage for a JSONB column, substituting the
// given empty default ("{}" or "[]") for a nil/null value so NOT NULL columns
// receive valid JSON instead of SQL NULL.
func nullableJSONRaw(b []byte, empty string) any {
	if len(b) == 0 || string(b) == "null" {
		return json.RawMessage(empty)
	}
	return json.RawMessage(b)
}
