package scheduler

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{"jobs": s.jobs})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Jobs map[string]map[string]Job `json:"jobs"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Jobs == nil {
		snap.Jobs = map[string]map[string]Job{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = snap.Jobs
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_scheduler_jobs`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

type jobSnapshotRow struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Job       Job    `json:"job"`
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	jobs, err := s.ListAllJobs(ctx)
	if err != nil {
		return err
	}
	rows := make([]jobSnapshotRow, 0, len(jobs))
	for _, j := range jobs {
		rows = append(rows, jobSnapshotRow{ProjectID: j.ProjectID, Location: j.Location, Job: j})
	}
	return json.NewEncoder(w).Encode(map[string]any{"jobs": rows})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Jobs []jobSnapshotRow `json:"jobs"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_scheduler_jobs`); err != nil {
		return err
	}
	for _, row := range snap.Jobs {
		data, err := encodeJob(row.Job)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_scheduler_jobs (project_id, location, job_name, data)
			VALUES ($1,$2,$3,$4)
		`, row.ProjectID, row.Location, row.Job.Name, data); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
