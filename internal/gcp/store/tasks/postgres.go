package tasks

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/gcp/storeutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_tasks_queues and jc_tasks_tasks.
// Each queue/task is stored as a JSONB document keyed by its name parts.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func encodeQueue(q Queue) ([]byte, error) {
	q.ProjectID = ""
	q.Location = ""
	return json.Marshal(q)
}

func decodeQueue(data []byte) (Queue, error) {
	var q Queue
	if err := json.Unmarshal(data, &q); err != nil {
		return Queue{}, err
	}
	return q, nil
}

func encodeTask(t Task) ([]byte, error) {
	t.ProjectID = ""
	t.Location = ""
	t.Queue = ""
	return json.Marshal(t)
}

func decodeTask(data []byte) (Task, error) {
	var t Task
	if err := json.Unmarshal(data, &t); err != nil {
		return Task{}, err
	}
	return t, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *PostgresStore) CreateQueue(ctx context.Context, projectID, location string, q Queue) error {
	data, err := encodeQueue(q)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_tasks_queues (project_id, location, queue_name, data)
		VALUES ($1,$2,$3,$4)
	`, projectID, location, q.Name, data)
	if isUniqueViolation(err) {
		return ErrAlreadyExists
	}
	return err
}

func (s *PostgresStore) GetQueue(ctx context.Context, projectID, location, name string) (Queue, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_tasks_queues
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
	`, projectID, location, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Queue{}, ErrNoSuchQueue
	}
	if err != nil {
		return Queue{}, err
	}
	q, err := decodeQueue(data)
	if err != nil {
		return Queue{}, err
	}
	q.ProjectID = projectID
	q.Location = location
	q.Name = name
	return q, nil
}

func (s *PostgresStore) UpdateQueue(ctx context.Context, projectID, location string, q Queue) error {
	data, err := encodeQueue(q)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_tasks_queues SET data=$4
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
	`, projectID, location, q.Name, data)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchQueue
	}
	return nil
}

func (s *PostgresStore) UpdateQueueAtomic(ctx context.Context, projectID, location, name string, mutate func(Queue) (Queue, error)) (Queue, error) {
	// Retry a transient transaction conflict (40001/40P01) rather than
	// surfacing a benign race as a 500.
	return storeutil.RetrySerializable(ctx, func() (Queue, error) {
		return s.updateQueueAtomicOnce(ctx, projectID, location, name, mutate)
	})
}

func (s *PostgresStore) updateQueueAtomicOnce(ctx context.Context, projectID, location, name string, mutate func(Queue) (Queue, error)) (Queue, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Queue{}, err
	}
	defer tx.Rollback(ctx)

	var data []byte
	err = tx.QueryRow(ctx, `
		SELECT data FROM jc_tasks_queues
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
		FOR UPDATE
	`, projectID, location, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Queue{}, ErrNoSuchQueue
	}
	if err != nil {
		return Queue{}, err
	}
	current, err := decodeQueue(data)
	if err != nil {
		return Queue{}, err
	}
	current.ProjectID = projectID
	current.Location = location
	current.Name = name
	next, err := mutate(current)
	if err != nil {
		return Queue{}, err
	}
	encoded, err := encodeQueue(next)
	if err != nil {
		return Queue{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jc_tasks_queues SET data=$4
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
	`, projectID, location, name, encoded); err != nil {
		return Queue{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Queue{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.Name = name
	return next, nil
}

func (s *PostgresStore) DeleteQueue(ctx context.Context, projectID, location, name string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		DELETE FROM jc_tasks_queues
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
	`, projectID, location, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchQueue
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM jc_tasks_tasks
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
	`, projectID, location, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListQueues(ctx context.Context, projectID, location string) ([]Queue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue_name, data FROM jc_tasks_queues
		WHERE project_id=$1 AND location=$2
		ORDER BY queue_name
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Queue
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		q, err := decodeQueue(data)
		if err != nil {
			return nil, err
		}
		q.ProjectID = projectID
		q.Location = location
		q.Name = name
		result = append(result, q)
	}
	return result, rows.Err()
}

func (s *PostgresStore) ListQueuesByProject(ctx context.Context, projectID string) ([]Queue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue_name, location, data FROM jc_tasks_queues
		WHERE project_id=$1
		ORDER BY location, queue_name
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Queue
	for rows.Next() {
		var name, location string
		var data []byte
		if err := rows.Scan(&name, &location, &data); err != nil {
			return nil, err
		}
		q, err := decodeQueue(data)
		if err != nil {
			return nil, err
		}
		q.ProjectID = projectID
		q.Location = location
		q.Name = name
		result = append(result, q)
	}
	return result, rows.Err()
}

func (s *PostgresStore) ListAllQueues(ctx context.Context) ([]Queue, error) {
	rows, err := s.listAllQueues(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Queue, 0, len(rows))
	for _, r := range rows {
		result = append(result, r.Queue)
	}
	return result, nil
}

func (s *PostgresStore) CreateTask(ctx context.Context, projectID, location, queue string, t Task) error {
	data, err := encodeTask(t)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_tasks_tasks (project_id, location, queue_name, task_name, data)
		VALUES ($1,$2,$3,$4,$5)
	`, projectID, location, queue, t.Name, data)
	if isUniqueViolation(err) {
		return ErrAlreadyExists
	}
	return err
}

func (s *PostgresStore) GetTask(ctx context.Context, projectID, location, queue, name string) (Task, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_tasks_tasks
		WHERE project_id=$1 AND location=$2 AND queue_name=$3 AND task_name=$4
	`, projectID, location, queue, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrNoSuchTask
	}
	if err != nil {
		return Task{}, err
	}
	t, err := decodeTask(data)
	if err != nil {
		return Task{}, err
	}
	t.ProjectID = projectID
	t.Location = location
	t.Queue = queue
	t.Name = name
	return t, nil
}

func (s *PostgresStore) UpdateTaskAtomic(ctx context.Context, projectID, location, queue, name string, mutate func(Task) (Task, error)) (Task, error) {
	return storeutil.RetrySerializable(ctx, func() (Task, error) {
		return s.updateTaskAtomicOnce(ctx, projectID, location, queue, name, mutate)
	})
}

func (s *PostgresStore) updateTaskAtomicOnce(ctx context.Context, projectID, location, queue, name string, mutate func(Task) (Task, error)) (Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback(ctx)

	var data []byte
	err = tx.QueryRow(ctx, `
		SELECT data FROM jc_tasks_tasks
		WHERE project_id=$1 AND location=$2 AND queue_name=$3 AND task_name=$4
		FOR UPDATE
	`, projectID, location, queue, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrNoSuchTask
	}
	if err != nil {
		return Task{}, err
	}
	current, err := decodeTask(data)
	if err != nil {
		return Task{}, err
	}
	current.ProjectID = projectID
	current.Location = location
	current.Queue = queue
	current.Name = name
	next, err := mutate(current)
	if err != nil {
		return Task{}, err
	}
	encoded, err := encodeTask(next)
	if err != nil {
		return Task{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jc_tasks_tasks SET data=$5
		WHERE project_id=$1 AND location=$2 AND queue_name=$3 AND task_name=$4
	`, projectID, location, queue, name, encoded); err != nil {
		return Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Task{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.Queue = queue
	next.Name = name
	return next, nil
}

func (s *PostgresStore) DeleteTask(ctx context.Context, projectID, location, queue, name string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_tasks_tasks
		WHERE project_id=$1 AND location=$2 AND queue_name=$3 AND task_name=$4
	`, projectID, location, queue, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTask
	}
	return nil
}

func (s *PostgresStore) ListTasks(ctx context.Context, projectID, location, queue string) ([]Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT task_name, data FROM jc_tasks_tasks
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
		ORDER BY task_name
	`, projectID, location, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Task
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		t, err := decodeTask(data)
		if err != nil {
			return nil, err
		}
		t.ProjectID = projectID
		t.Location = location
		t.Queue = queue
		t.Name = name
		result = append(result, t)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteTasks(ctx context.Context, projectID, location, queue string) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_tasks_tasks
		WHERE project_id=$1 AND location=$2 AND queue_name=$3
	`, projectID, location, queue)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_tasks_tasks`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_tasks_queues`)
}
