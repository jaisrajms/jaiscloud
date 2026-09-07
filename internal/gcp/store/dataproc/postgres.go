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
			(project_id, region, cluster_name, config, labels, status, status_history, cluster_uuid, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, projectID, region, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(labels, "{}"), nullableJSONRaw(status, "{}"),
		nullableJSONRaw(history, "[]"), c.ClusterUUID, c.CreateTime, c.UpdateTime)
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
	var config, labels, status, history []byte
	err := row.Scan(&c.ProjectID, &c.Region, &c.Name, &config, &labels, &status, &history, &c.ClusterUUID, &c.CreateTime, &c.UpdateTime)
	if err != nil {
		return Cluster{}, err
	}
	c.Config = json.RawMessage(config)
	json.Unmarshal(labels, &c.Labels)
	json.Unmarshal(status, &c.Status)
	json.Unmarshal(history, &c.StatusHistory)
	return c, nil
}

func (s *PostgresStore) GetCluster(ctx context.Context, projectID, region, name string) (Cluster, error) {
	c, err := scanCluster(s.pool.QueryRow(ctx, `
		SELECT project_id, region, cluster_name, config, labels, status, status_history, cluster_uuid, create_time, update_time
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
		UPDATE jc_dataproc_clusters SET config=$4, labels=$5, status=$6, status_history=$7, cluster_uuid=$8, update_time=$9
		WHERE project_id=$1 AND region=$2 AND cluster_name=$3
	`, projectID, region, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(labels, "{}"), nullableJSONRaw(status, "{}"),
		nullableJSONRaw(history, "[]"), c.ClusterUUID, c.UpdateTime)
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
		SELECT project_id, region, cluster_name, config, labels, status, status_history, cluster_uuid, create_time, update_time
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
		UPDATE jc_dataproc_clusters SET config=$4, labels=$5, status=$6, status_history=$7, cluster_uuid=$8, update_time=$9
		WHERE project_id=$1 AND region=$2 AND cluster_name=$3
	`, projectID, region, name, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(labels, "{}"), nullableJSONRaw(status, "{}"),
		nullableJSONRaw(history, "[]"), next.ClusterUUID, next.UpdateTime)
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
		SELECT project_id, region, cluster_name, config, labels, status, status_history, cluster_uuid, create_time, update_time
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
			 driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`, projectID, region, j.JobID, j.PlacementClusterName, j.Type, nullableJSONRaw(j.TypeJob, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), j.DriverOutputResourceURI, j.DriverControlFilesURI,
		j.JobUUID, j.CreateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var typeJob, labels, status, history []byte
	err := row.Scan(&j.ProjectID, &j.Region, &j.JobID, &j.PlacementClusterName, &j.Type, &typeJob, &labels, &status, &history,
		&j.DriverOutputResourceURI, &j.DriverControlFilesURI, &j.JobUUID, &j.CreateTime)
	if err != nil {
		return Job{}, err
	}
	j.TypeJob = json.RawMessage(typeJob)
	json.Unmarshal(labels, &j.Labels)
	json.Unmarshal(status, &j.Status)
	json.Unmarshal(history, &j.StatusHistory)
	return j, nil
}

func (s *PostgresStore) GetJob(ctx context.Context, projectID, region, jobID string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT project_id, region, job_id, placement_cluster_name, job_type, type_job, labels, status, status_history,
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time
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
		       status_history=$9, driver_output_resource_uri=$10, driver_control_files_uri=$11, job_uuid=$12
		WHERE project_id=$1 AND region=$2 AND job_id=$3
	`, projectID, region, j.JobID, j.PlacementClusterName, j.Type, nullableJSONRaw(j.TypeJob, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), j.DriverOutputResourceURI, j.DriverControlFilesURI, j.JobUUID)
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
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time
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
		       status_history=$9, driver_output_resource_uri=$10, driver_control_files_uri=$11, job_uuid=$12
		WHERE project_id=$1 AND region=$2 AND job_id=$3
	`, projectID, region, jobID, next.PlacementClusterName, next.Type, nullableJSONRaw(next.TypeJob, "{}"), nullableJSONRaw(labels, "{}"),
		nullableJSONRaw(status, "{}"), nullableJSONRaw(history, "[]"), next.DriverOutputResourceURI, next.DriverControlFilesURI, next.JobUUID)
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
		       driver_output_resource_uri, driver_control_files_uri, job_uuid, create_time
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

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_clusters`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_jobs`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_dataproc_operations`)
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
