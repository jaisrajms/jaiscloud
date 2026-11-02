package tasks

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queues) == 0 && len(s.tasks) == 0 && len(s.cmek) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{"queues": s.queues, "tasks": s.tasks, "cmekConfigs": s.cmek})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Queues      map[string]map[string]Queue `json:"queues"`
		Tasks       map[string]map[string]Task  `json:"tasks"`
		CmekConfigs map[string]CmekConfig       `json:"cmekConfigs"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Queues == nil {
		snap.Queues = map[string]map[string]Queue{}
	}
	if snap.Tasks == nil {
		snap.Tasks = map[string]map[string]Task{}
	}
	if snap.CmekConfigs == nil {
		snap.CmekConfigs = map[string]CmekConfig{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = snap.Queues
	s.tasks = snap.Tasks
	s.cmek = snap.CmekConfigs
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_tasks_queues`).Scan(&n); err != nil {
		return false, err
	}
	if n != 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_tasks_tasks`).Scan(&n); err != nil {
		return false, err
	}
	if n != 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_tasks_cmek_configs`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

type queueSnapshotRow struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Queue     Queue  `json:"queue"`
}

type taskSnapshotRow struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Queue     string `json:"queue"`
	Task      Task   `json:"task"`
}

type cmekSnapshotRow struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	KmsKey    string `json:"kmsKey"`
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	queues, err := s.listAllQueues(ctx)
	if err != nil {
		return err
	}
	tasks, err := s.listAllTasks(ctx)
	if err != nil {
		return err
	}
	cmek, err := s.listAllCmekConfigs(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(map[string]any{"queues": queues, "tasks": tasks, "cmekConfigs": cmek})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Queues      []queueSnapshotRow `json:"queues"`
		Tasks       []taskSnapshotRow  `json:"tasks"`
		CmekConfigs []cmekSnapshotRow  `json:"cmekConfigs"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_tasks_tasks`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_tasks_queues`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_tasks_cmek_configs`); err != nil {
		return err
	}
	for _, row := range snap.Queues {
		data, err := encodeQueue(row.Queue)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_tasks_queues (project_id, location, queue_name, data)
			VALUES ($1,$2,$3,$4)
		`, row.ProjectID, row.Location, row.Queue.Name, data); err != nil {
			return err
		}
	}
	for _, row := range snap.Tasks {
		data, err := encodeTask(row.Task)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_tasks_tasks (project_id, location, queue_name, task_name, data)
			VALUES ($1,$2,$3,$4,$5)
		`, row.ProjectID, row.Location, row.Queue, row.Task.Name, data); err != nil {
			return err
		}
	}
	for _, row := range snap.CmekConfigs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_tasks_cmek_configs (project_id, location, kms_key)
			VALUES ($1,$2,$3)
		`, row.ProjectID, row.Location, row.KmsKey); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) listAllQueues(ctx context.Context) ([]queueSnapshotRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, queue_name, data FROM jc_tasks_queues
		ORDER BY project_id, location, queue_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []queueSnapshotRow
	for rows.Next() {
		var projectID, location, name string
		var data []byte
		if err := rows.Scan(&projectID, &location, &name, &data); err != nil {
			return nil, err
		}
		q, err := decodeQueue(data)
		if err != nil {
			return nil, err
		}
		q.ProjectID = projectID
		q.Location = location
		q.Name = name
		result = append(result, queueSnapshotRow{ProjectID: projectID, Location: location, Queue: q})
	}
	return result, rows.Err()
}

func (s *PostgresStore) listAllTasks(ctx context.Context) ([]taskSnapshotRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, queue_name, task_name, data FROM jc_tasks_tasks
		ORDER BY project_id, location, queue_name, task_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []taskSnapshotRow
	for rows.Next() {
		var projectID, location, queue, name string
		var data []byte
		if err := rows.Scan(&projectID, &location, &queue, &name, &data); err != nil {
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
		result = append(result, taskSnapshotRow{ProjectID: projectID, Location: location, Queue: queue, Task: t})
	}
	return result, rows.Err()
}

func (s *PostgresStore) listAllCmekConfigs(ctx context.Context) ([]cmekSnapshotRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, kms_key FROM jc_tasks_cmek_configs
		ORDER BY project_id, location
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []cmekSnapshotRow
	for rows.Next() {
		var row cmekSnapshotRow
		if err := rows.Scan(&row.ProjectID, &row.Location, &row.KmsKey); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
