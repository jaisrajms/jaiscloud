package scheduler

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_scheduler_jobs. Each job is stored
// as a JSONB document keyed by (project_id, location, job_name).
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func encodeJob(j Job) ([]byte, error) {
	j.ProjectID = ""
	j.Location = ""
	return json.Marshal(j)
}

func decodeJob(data []byte) (Job, error) {
	var j Job
	if err := json.Unmarshal(data, &j); err != nil {
		return Job{}, err
	}
	return j, nil
}

func (s *PostgresStore) CreateJob(ctx context.Context, projectID, location string, j Job) error {
	data, err := encodeJob(j)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_scheduler_jobs (project_id, location, job_name, data)
		VALUES ($1,$2,$3,$4)
	`, projectID, location, j.Name, data)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetJob(ctx context.Context, projectID, location, name string) (Job, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_scheduler_jobs
		WHERE project_id=$1 AND location=$2 AND job_name=$3
	`, projectID, location, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoSuchJob
	}
	if err != nil {
		return Job{}, err
	}
	j, err := decodeJob(data)
	if err != nil {
		return Job{}, err
	}
	j.ProjectID = projectID
	j.Location = location
	j.Name = name
	return j, nil
}

func (s *PostgresStore) UpdateJob(ctx context.Context, projectID, location string, j Job) error {
	data, err := encodeJob(j)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_scheduler_jobs SET data=$4
		WHERE project_id=$1 AND location=$2 AND job_name=$3
	`, projectID, location, j.Name, data)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchJob
	}
	return nil
}

func (s *PostgresStore) UpdateJobAtomic(ctx context.Context, projectID, location, name string, mutate func(Job) (Job, error)) (Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)

	var data []byte
	err = tx.QueryRow(ctx, `
		SELECT data FROM jc_scheduler_jobs
		WHERE project_id=$1 AND location=$2 AND job_name=$3
		FOR UPDATE
	`, projectID, location, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoSuchJob
	}
	if err != nil {
		return Job{}, err
	}
	current, err := decodeJob(data)
	if err != nil {
		return Job{}, err
	}
	current.ProjectID = projectID
	current.Location = location
	current.Name = name
	next, err := mutate(current)
	if err != nil {
		return Job{}, err
	}
	encoded, err := encodeJob(next)
	if err != nil {
		return Job{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jc_scheduler_jobs SET data=$4
		WHERE project_id=$1 AND location=$2 AND job_name=$3
	`, projectID, location, name, encoded); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.Name = name
	return next, nil
}

func (s *PostgresStore) DeleteJob(ctx context.Context, projectID, location, name string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_scheduler_jobs
		WHERE project_id=$1 AND location=$2 AND job_name=$3
	`, projectID, location, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchJob
	}
	return nil
}

func (s *PostgresStore) ListJobs(ctx context.Context, projectID, location string) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_name, data FROM jc_scheduler_jobs
		WHERE project_id=$1 AND location=$2
		ORDER BY job_name
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Job
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		j, err := decodeJob(data)
		if err != nil {
			return nil, err
		}
		j.ProjectID = projectID
		j.Location = location
		j.Name = name
		result = append(result, j)
	}
	return result, rows.Err()
}

func (s *PostgresStore) ListAllJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, job_name, data FROM jc_scheduler_jobs
		ORDER BY project_id, location, job_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Job
	for rows.Next() {
		var projectID, location, name string
		var data []byte
		if err := rows.Scan(&projectID, &location, &name, &data); err != nil {
			return nil, err
		}
		j, err := decodeJob(data)
		if err != nil {
			return nil, err
		}
		j.ProjectID = projectID
		j.Location = location
		j.Name = name
		result = append(result, j)
	}
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_scheduler_jobs`)
}
