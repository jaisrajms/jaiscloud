package workflows

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.workflows) == 0 && len(s.executions) == 0 && len(s.operations) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"workflows":  s.workflows,
		"executions": s.executions,
		"operations": s.operations,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Workflows  map[string]map[string]Workflow  `json:"workflows"`
		Executions map[string]map[string]Execution `json:"executions"`
		Operations map[string]map[string]Operation `json:"operations"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Workflows == nil {
		snap.Workflows = map[string]map[string]Workflow{}
	}
	if snap.Executions == nil {
		snap.Executions = map[string]map[string]Execution{}
	}
	if snap.Operations == nil {
		snap.Operations = map[string]map[string]Operation{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows = snap.Workflows
	s.executions = snap.Executions
	s.operations = snap.Operations
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_workflows`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type workflowRow struct {
		ProjectID string   `json:"projectId"`
		Workflow  Workflow `json:"workflow"`
	}
	type executionRow struct {
		ProjectID string    `json:"projectId"`
		Execution Execution `json:"execution"`
	}
	type operationRow struct {
		ProjectID string    `json:"projectId"`
		Operation Operation `json:"operation"`
	}

	workflows := make([]workflowRow, 0)
	executions := make([]executionRow, 0)
	operations := make([]operationRow, 0)

	wrows, err := s.pool.Query(ctx, `
		SELECT project_id, workflow_id, location, description, labels, service_account, source_contents,
		       state, revision_id, create_time, update_time, call_log_level, user_env_vars, tags
		FROM jc_workflows ORDER BY project_id, location, workflow_id
	`)
	if err != nil {
		return err
	}
	for wrows.Next() {
		var r workflowRow
		var labels, userEnvVars, tags []byte
		if err := wrows.Scan(&r.ProjectID, &r.Workflow.ID, &r.Workflow.Location, &r.Workflow.Description, &labels,
			&r.Workflow.ServiceAccount, &r.Workflow.SourceContents, &r.Workflow.State, &r.Workflow.RevisionID,
			&r.Workflow.CreateTime, &r.Workflow.UpdateTime, &r.Workflow.CallLogLevel, &userEnvVars, &tags); err != nil {
			wrows.Close()
			return err
		}
		json.Unmarshal(labels, &r.Workflow.Labels)
		json.Unmarshal(userEnvVars, &r.Workflow.UserEnvVars)
		json.Unmarshal(tags, &r.Workflow.Tags)
		workflows = append(workflows, r)
	}
	wrows.Close()
	if err := wrows.Err(); err != nil {
		return err
	}

	erows, err := s.pool.Query(ctx, `
		SELECT project_id, execution_id, workflow_id, location, state, argument, result, error,
		       start_time, end_time, duration, workflow_revision_id, call_log_level, labels, current_steps
		FROM jc_workflow_executions ORDER BY project_id, location, workflow_id, execution_id
	`)
	if err != nil {
		return err
	}
	for erows.Next() {
		var r executionRow
		var errJSON, labels, steps []byte
		var start, end *time.Time
		if err := erows.Scan(&r.ProjectID, &r.Execution.ID, &r.Execution.WorkflowID, &r.Execution.Location,
			&r.Execution.State, &r.Execution.Argument, &r.Execution.Result, &errJSON,
			&start, &end, &r.Execution.Duration, &r.Execution.WorkflowRevisionID, &r.Execution.CallLogLevel,
			&labels, &steps); err != nil {
			erows.Close()
			return err
		}
		if start != nil {
			r.Execution.StartTime = *start
		}
		if end != nil {
			r.Execution.EndTime = *end
		}
		if len(errJSON) > 0 {
			json.Unmarshal(errJSON, &r.Execution.Error)
		}
		json.Unmarshal(labels, &r.Execution.Labels)
		json.Unmarshal(steps, &r.Execution.CurrentSteps)
		executions = append(executions, r)
	}
	erows.Close()
	if err := erows.Err(); err != nil {
		return err
	}

	orows, err := s.pool.Query(ctx, `
		SELECT project_id, operation_id, location, done, response, verb, target, create_time, end_time
		FROM jc_workflow_operations ORDER BY project_id, location, operation_id
	`)
	if err != nil {
		return err
	}
	for orows.Next() {
		var r operationRow
		if err := orows.Scan(&r.ProjectID, &r.Operation.ID, &r.Operation.Location, &r.Operation.Done,
			&r.Operation.Response, &r.Operation.Verb, &r.Operation.Target, &r.Operation.CreateTime,
			&r.Operation.EndTime); err != nil {
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
		"workflows":  workflows,
		"executions": executions,
		"operations": operations,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Workflows []struct {
			ProjectID string   `json:"projectId"`
			Workflow  Workflow `json:"workflow"`
		} `json:"workflows"`
		Executions []struct {
			ProjectID string    `json:"projectId"`
			Execution Execution `json:"execution"`
		} `json:"executions"`
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
	for _, tbl := range []string{"jc_workflows", "jc_workflow_executions", "jc_workflow_operations"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Workflows {
		labels, _ := json.Marshal(r.Workflow.Labels)
		userEnvVars, _ := json.Marshal(r.Workflow.UserEnvVars)
		tags, _ := json.Marshal(r.Workflow.Tags)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_workflows
				(project_id, location, workflow_id, description, labels, service_account, source_contents,
				 state, revision_id, create_time, update_time, call_log_level, user_env_vars, tags)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		`, r.ProjectID, r.Workflow.Location, r.Workflow.ID, r.Workflow.Description, json.RawMessage(labels),
			r.Workflow.ServiceAccount, r.Workflow.SourceContents, r.Workflow.State, r.Workflow.RevisionID,
			r.Workflow.CreateTime, r.Workflow.UpdateTime, r.Workflow.CallLogLevel, json.RawMessage(userEnvVars),
			json.RawMessage(tags)); err != nil {
			return err
		}
	}
	for _, r := range snap.Executions {
		labels, _ := json.Marshal(r.Execution.Labels)
		steps, _ := json.Marshal(r.Execution.CurrentSteps)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_workflow_executions
				(project_id, location, workflow_id, execution_id, state, argument, result, error,
				 start_time, end_time, duration, workflow_revision_id, call_log_level, labels, current_steps)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		`, r.ProjectID, r.Execution.Location, r.Execution.WorkflowID, r.Execution.ID, r.Execution.State,
			r.Execution.Argument, r.Execution.Result, nullableJSON(r.Execution.Error),
			nullableTime(r.Execution.StartTime), nullableTime(r.Execution.EndTime), r.Execution.Duration,
			r.Execution.WorkflowRevisionID, r.Execution.CallLogLevel, json.RawMessage(labels),
			json.RawMessage(steps)); err != nil {
			return err
		}
	}
	for _, r := range snap.Operations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_workflow_operations
				(project_id, location, operation_id, done, response, verb, target, create_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Operation.Location, r.Operation.ID, r.Operation.Done, r.Operation.Response,
			r.Operation.Verb, r.Operation.Target, r.Operation.CreateTime, r.Operation.EndTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
