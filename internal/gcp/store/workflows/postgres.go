package workflows

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

// PostgresStore implements Store against jc_workflows / jc_workflow_executions /
// jc_workflow_operations.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// nullableJSON marshals v to JSONB, or returns nil (SQL NULL) when v is nil.
func nullableJSON(v any) any {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func (s *PostgresStore) CreateWorkflow(ctx context.Context, projectID, location, id string, w Workflow) error {
	if w.CreateTime.IsZero() {
		w.CreateTime = clock.Now()
	}
	if w.UpdateTime.IsZero() {
		w.UpdateTime = w.CreateTime
	}
	labels, _ := json.Marshal(w.Labels)
	userEnvVars, _ := json.Marshal(w.UserEnvVars)
	tags, _ := json.Marshal(w.Tags)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_workflows
			(project_id, location, workflow_id, description, labels, service_account, source_contents,
			 state, revision_id, create_time, update_time, call_log_level, user_env_vars, tags)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`, projectID, location, id, w.Description, json.RawMessage(labels), w.ServiceAccount, w.SourceContents,
		w.State, w.RevisionID, w.CreateTime, w.UpdateTime, w.CallLogLevel, json.RawMessage(userEnvVars),
		json.RawMessage(tags))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanWorkflow(row pgx.Row) (Workflow, error) {
	var w Workflow
	var labels, userEnvVars, tags []byte
	err := row.Scan(&w.ID, &w.Location, &w.Description, &labels, &w.ServiceAccount, &w.SourceContents,
		&w.State, &w.RevisionID, &w.CreateTime, &w.UpdateTime, &w.CallLogLevel, &userEnvVars, &tags)
	if err != nil {
		return Workflow{}, err
	}
	json.Unmarshal(labels, &w.Labels)
	json.Unmarshal(userEnvVars, &w.UserEnvVars)
	json.Unmarshal(tags, &w.Tags)
	return w, nil
}

func (s *PostgresStore) GetWorkflow(ctx context.Context, projectID, location, id string) (Workflow, error) {
	w, err := scanWorkflow(s.pool.QueryRow(ctx, `
		SELECT workflow_id, location, description, labels, service_account, source_contents,
		       state, revision_id, create_time, update_time, call_log_level, user_env_vars, tags
		FROM jc_workflows WHERE project_id=$1 AND location=$2 AND workflow_id=$3
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Workflow{}, ErrNoSuchWorkflow
	}
	return w, err
}

func (s *PostgresStore) UpdateWorkflow(ctx context.Context, projectID, location, id string, w Workflow) error {
	labels, _ := json.Marshal(w.Labels)
	userEnvVars, _ := json.Marshal(w.UserEnvVars)
	tags, _ := json.Marshal(w.Tags)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_workflows SET description=$4, labels=$5, service_account=$6, source_contents=$7,
		       state=$8, revision_id=$9, update_time=$10, call_log_level=$11, user_env_vars=$12, tags=$13
		WHERE project_id=$1 AND location=$2 AND workflow_id=$3
	`, projectID, location, id, w.Description, json.RawMessage(labels), w.ServiceAccount, w.SourceContents,
		w.State, w.RevisionID, w.UpdateTime, w.CallLogLevel, json.RawMessage(userEnvVars), json.RawMessage(tags))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchWorkflow
	}
	return nil
}

// UpdateWorkflowAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the workflow for the
// duration of mutate, so a concurrent UpdateWorkflowAtomic on the same
// workflow blocks until this transaction commits or rolls back, instead of
// racing to silently overwrite this call's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateWorkflowAtomic(ctx context.Context, projectID, location, id string, mutate func(Workflow) (Workflow, error)) (Workflow, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Workflow{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanWorkflow(tx.QueryRow(ctx, `
		SELECT workflow_id, location, description, labels, service_account, source_contents,
		       state, revision_id, create_time, update_time, call_log_level, user_env_vars, tags
		FROM jc_workflows WHERE project_id=$1 AND location=$2 AND workflow_id=$3 FOR UPDATE
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Workflow{}, ErrNoSuchWorkflow
	}
	if err != nil {
		return Workflow{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Workflow{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	userEnvVars, _ := json.Marshal(next.UserEnvVars)
	tags, _ := json.Marshal(next.Tags)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_workflows SET description=$4, labels=$5, service_account=$6, source_contents=$7,
		       state=$8, revision_id=$9, update_time=$10, call_log_level=$11, user_env_vars=$12, tags=$13
		WHERE project_id=$1 AND location=$2 AND workflow_id=$3
	`, projectID, location, id, next.Description, json.RawMessage(labels), next.ServiceAccount, next.SourceContents,
		next.State, next.RevisionID, next.UpdateTime, next.CallLogLevel, json.RawMessage(userEnvVars), json.RawMessage(tags))
	if err != nil {
		return Workflow{}, err
	}
	if tag.RowsAffected() == 0 {
		return Workflow{}, ErrNoSuchWorkflow
	}
	if err := tx.Commit(ctx); err != nil {
		return Workflow{}, err
	}
	next.ID = id
	next.Location = location
	return next, nil
}

func (s *PostgresStore) DeleteWorkflow(ctx context.Context, projectID, location, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `DELETE FROM jc_workflows WHERE project_id=$1 AND location=$2 AND workflow_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchWorkflow
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_workflow_executions WHERE project_id=$1 AND location=$2 AND workflow_id=$3`, projectID, location, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListWorkflows(ctx context.Context, projectID, location string) ([]Workflow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id, location, description, labels, service_account, source_contents,
		       state, revision_id, create_time, update_time, call_log_level, user_env_vars, tags
		FROM jc_workflows WHERE project_id=$1 AND location=$2 ORDER BY workflow_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Workflow
	for rows.Next() {
		w, err := scanWorkflow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, w)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) CreateExecution(ctx context.Context, projectID, location, workflowID, id string, e Execution) error {
	errJSON := nullableJSON(e.Error)
	labels, _ := json.Marshal(e.Labels)
	steps, _ := json.Marshal(e.CurrentSteps)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_workflow_executions
			(project_id, location, workflow_id, execution_id, state, argument, result, error,
			 start_time, end_time, duration, workflow_revision_id, call_log_level, labels, current_steps)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	`, projectID, location, workflowID, id, e.State, e.Argument, e.Result, errJSON,
		nullableTime(e.StartTime), nullableTime(e.EndTime), e.Duration, e.WorkflowRevisionID, e.CallLogLevel,
		json.RawMessage(labels), json.RawMessage(steps))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// nullableTime renders a zero time.Time as SQL NULL.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func scanExecution(row pgx.Row) (Execution, error) {
	var e Execution
	var errJSON, labels, steps []byte
	var start, end *time.Time
	err := row.Scan(&e.ID, &e.WorkflowID, &e.Location, &e.State, &e.Argument, &e.Result, &errJSON,
		&start, &end, &e.Duration, &e.WorkflowRevisionID, &e.CallLogLevel, &labels, &steps)
	if err != nil {
		return Execution{}, err
	}
	if start != nil {
		e.StartTime = *start
	}
	if end != nil {
		e.EndTime = *end
	}
	if len(errJSON) > 0 {
		json.Unmarshal(errJSON, &e.Error)
	}
	json.Unmarshal(labels, &e.Labels)
	json.Unmarshal(steps, &e.CurrentSteps)
	return e, nil
}

func (s *PostgresStore) GetExecution(ctx context.Context, projectID, location, workflowID, id string) (Execution, error) {
	e, err := scanExecution(s.pool.QueryRow(ctx, `
		SELECT execution_id, workflow_id, location, state, argument, result, error,
		       start_time, end_time, duration, workflow_revision_id, call_log_level, labels, current_steps
		FROM jc_workflow_executions WHERE project_id=$1 AND location=$2 AND workflow_id=$3 AND execution_id=$4
	`, projectID, location, workflowID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Execution{}, ErrNoSuchExecution
	}
	return e, err
}

func (s *PostgresStore) UpdateExecution(ctx context.Context, projectID, location, workflowID, id string, e Execution) error {
	errJSON := nullableJSON(e.Error)
	labels, _ := json.Marshal(e.Labels)
	steps, _ := json.Marshal(e.CurrentSteps)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_workflow_executions SET state=$5, argument=$6, result=$7, error=$8,
		       start_time=$9, end_time=$10, duration=$11, workflow_revision_id=$12, call_log_level=$13,
		       labels=$14, current_steps=$15
		WHERE project_id=$1 AND location=$2 AND workflow_id=$3 AND execution_id=$4
	`, projectID, location, workflowID, id, e.State, e.Argument, e.Result, errJSON,
		nullableTime(e.StartTime), nullableTime(e.EndTime), e.Duration, e.WorkflowRevisionID, e.CallLogLevel,
		json.RawMessage(labels), json.RawMessage(steps))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchExecution
	}
	return nil
}

func (s *PostgresStore) ListExecutions(ctx context.Context, projectID, location, workflowID string) ([]Execution, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT execution_id, workflow_id, location, state, argument, result, error,
		       start_time, end_time, duration, workflow_revision_id, call_log_level, labels, current_steps
		FROM jc_workflow_executions WHERE project_id=$1 AND location=$2 AND workflow_id=$3
		ORDER BY start_time DESC NULLS LAST, execution_id
	`, projectID, location, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Execution
	for rows.Next() {
		e, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *PostgresStore) CreateOperation(ctx context.Context, projectID, location string, op Operation) error {
	if op.CreateTime.IsZero() {
		op.CreateTime = clock.Now()
	}
	if op.EndTime.IsZero() {
		op.EndTime = op.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_workflow_operations
			(project_id, location, operation_id, done, response, verb, target, create_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, op.ID, op.Done, op.Response, op.Verb, op.Target, op.CreateTime, op.EndTime)
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) GetOperation(ctx context.Context, projectID, location, id string) (Operation, error) {
	var op Operation
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, project_id, location, done, response, verb, target, create_time, end_time
		FROM jc_workflow_operations WHERE project_id=$1 AND location=$2 AND operation_id=$3
	`, projectID, location, id).Scan(&op.ID, &op.ProjectID, &op.Location, &op.Done, &op.Response,
		&op.Verb, &op.Target, &op.CreateTime, &op.EndTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	return op, err
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_workflows`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_workflow_executions`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_workflow_operations`)
}
