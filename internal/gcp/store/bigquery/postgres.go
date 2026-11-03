package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/storeutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_bq_datasets / jc_bq_tables /
// jc_bq_jobs / jc_bq_rows.
type PostgresStore struct {
	pool  *pgxpool.Pool
	dedup *insertDedupTracker
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool, dedup: newInsertDedupTracker()}
}

// --- Datasets ---

func (s *PostgresStore) CreateDataset(ctx context.Context, projectID string, d Dataset) error {
	if d.CreateTime.IsZero() {
		d.CreateTime = clock.Now()
	}
	if d.UpdateTime.IsZero() {
		d.UpdateTime = d.CreateTime
	}
	labels, _ := json.Marshal(d.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_bq_datasets (project_id, dataset_id, config, labels, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, projectID, d.DatasetID, nullableJSONRaw(d.Config, "{}"), nullableJSONRaw(labels, "{}"), d.CreateTime, d.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanDataset(row pgx.Row) (Dataset, error) {
	var d Dataset
	var config, labels []byte
	err := row.Scan(&d.ProjectID, &d.DatasetID, &config, &labels, &d.CreateTime, &d.UpdateTime)
	if err != nil {
		return Dataset{}, err
	}
	d.Config = json.RawMessage(config)
	json.Unmarshal(labels, &d.Labels)
	return d, nil
}

func (s *PostgresStore) GetDataset(ctx context.Context, projectID, datasetID string) (Dataset, error) {
	d, err := scanDataset(s.pool.QueryRow(ctx, `
		SELECT project_id, dataset_id, config, labels, create_time, update_time
		FROM jc_bq_datasets WHERE project_id=$1 AND dataset_id=$2
	`, projectID, datasetID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Dataset{}, ErrNoSuchDataset
	}
	return d, err
}

func (s *PostgresStore) UpdateDataset(ctx context.Context, projectID string, d Dataset) error {
	labels, _ := json.Marshal(d.Labels)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_bq_datasets SET config=$3, labels=$4, update_time=$5
		WHERE project_id=$1 AND dataset_id=$2
	`, projectID, d.DatasetID, nullableJSONRaw(d.Config, "{}"), nullableJSONRaw(labels, "{}"), d.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchDataset
	}
	return nil
}

// UpdateDatasetAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the dataset for the
// duration of mutate, so a concurrent UpdateDatasetAtomic on the same
// dataset blocks until this transaction commits or rolls back, instead of
// racing to silently overwrite this call's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateDatasetAtomic(ctx context.Context, projectID, datasetID string, mutate func(Dataset) (Dataset, error)) (Dataset, error) {
	// Retry a transient SERIALIZABLE conflict (40001/40P01) rather than
	// surfacing a benign race as a 500.
	return storeutil.RetrySerializable(ctx, func() (Dataset, error) {
		return s.updateDatasetAtomicOnce(ctx, projectID, datasetID, mutate)
	})
}

func (s *PostgresStore) updateDatasetAtomicOnce(ctx context.Context, projectID, datasetID string, mutate func(Dataset) (Dataset, error)) (Dataset, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Dataset{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanDataset(tx.QueryRow(ctx, `
		SELECT project_id, dataset_id, config, labels, create_time, update_time
		FROM jc_bq_datasets WHERE project_id=$1 AND dataset_id=$2 FOR UPDATE
	`, projectID, datasetID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Dataset{}, ErrNoSuchDataset
	}
	if err != nil {
		return Dataset{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Dataset{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_bq_datasets SET config=$3, labels=$4, update_time=$5
		WHERE project_id=$1 AND dataset_id=$2
	`, projectID, datasetID, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(labels, "{}"), next.UpdateTime)
	if err != nil {
		return Dataset{}, err
	}
	if tag.RowsAffected() == 0 {
		return Dataset{}, ErrNoSuchDataset
	}
	if err := tx.Commit(ctx); err != nil {
		return Dataset{}, err
	}
	next.ProjectID = projectID
	return next, nil
}

func (s *PostgresStore) DeleteDataset(ctx context.Context, projectID, datasetID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_bq_rows WHERE project_id=$1 AND dataset_id=$2`, projectID, datasetID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_bq_row_access_policies WHERE project_id=$1 AND dataset_id=$2`, projectID, datasetID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_bq_routines WHERE project_id=$1 AND dataset_id=$2`, projectID, datasetID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_bq_tables WHERE project_id=$1 AND dataset_id=$2`, projectID, datasetID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM jc_bq_datasets WHERE project_id=$1 AND dataset_id=$2`, projectID, datasetID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchDataset
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.dedup.forgetPrefix(datasetScope(projectID, datasetID) + "/")
	return nil
}

func (s *PostgresStore) ListDatasets(ctx context.Context, projectID string) ([]Dataset, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, config, labels, create_time, update_time
		FROM jc_bq_datasets WHERE project_id=$1 ORDER BY dataset_id
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Dataset
	for rows.Next() {
		d, err := scanDataset(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DatasetID < result[j].DatasetID })
	return result, rows.Err()
}

// --- Tables ---

func (s *PostgresStore) CreateTable(ctx context.Context, projectID, datasetID string, t Table) error {
	if t.CreateTime.IsZero() {
		t.CreateTime = clock.Now()
	}
	if t.UpdateTime.IsZero() {
		t.UpdateTime = t.CreateTime
	}
	labels, _ := json.Marshal(t.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_bq_tables (project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, datasetID, t.TableID, nullableJSONRaw(t.Config, "{}"), nullableJSONRaw(t.Schema, "{}"), nullableJSONRaw(labels, "{}"), t.NumRows, t.CreateTime, t.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) CreateTableWithRows(ctx context.Context, projectID, datasetID string, t Table, rows []Row) error {
	if t.CreateTime.IsZero() {
		t.CreateTime = clock.Now()
	}
	if t.UpdateTime.IsZero() {
		t.UpdateTime = t.CreateTime
	}
	t.NumRows = int64(len(rows))
	if len(rows) > 0 {
		t.UpdateTime = clock.Now().UTC()
	}
	labels, _ := json.Marshal(t.Labels)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_bq_tables (project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, datasetID, t.TableID, nullableJSONRaw(t.Config, "{}"), nullableJSONRaw(t.Schema, "{}"), nullableJSONRaw(labels, "{}"), t.NumRows, t.CreateTime, t.UpdateTime); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	for i := range rows {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_rows (project_id, dataset_id, table_id, seq, data)
			VALUES ($1,$2,$3,$4,$5)
		`, projectID, datasetID, t.TableID, int64(i+1), nullableJSONRaw(rows[i].Data, "{}")); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.dedup.forget(tableScope(projectID, datasetID, t.TableID))
	return nil
}

func scanTable(row pgx.Row) (Table, error) {
	var t Table
	var config, schema, labels []byte
	err := row.Scan(&t.ProjectID, &t.DatasetID, &t.TableID, &config, &schema, &labels, &t.NumRows, &t.CreateTime, &t.UpdateTime)
	if err != nil {
		return Table{}, err
	}
	t.Config = json.RawMessage(config)
	t.Schema = json.RawMessage(schema)
	json.Unmarshal(labels, &t.Labels)
	return t, nil
}

func (s *PostgresStore) GetTable(ctx context.Context, projectID, datasetID, tableID string) (Table, error) {
	t, err := scanTable(s.pool.QueryRow(ctx, `
		SELECT project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time
		FROM jc_bq_tables WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, tableID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrNoSuchTable
	}
	return t, err
}

func (s *PostgresStore) UpdateTable(ctx context.Context, projectID, datasetID string, t Table) error {
	labels, _ := json.Marshal(t.Labels)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_bq_tables SET config=$4, schema=$5, labels=$6, update_time=$7
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, t.TableID, nullableJSONRaw(t.Config, "{}"), nullableJSONRaw(t.Schema, "{}"), nullableJSONRaw(labels, "{}"), t.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTable
	}
	return nil
}

// UpdateTableAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the table for the
// duration of mutate, so a concurrent UpdateTableAtomic on the same table
// blocks until this transaction commits or rolls back, instead of racing to
// silently overwrite this call's write. See store/firestore/postgres.go's
// Commit for the same convention.
func (s *PostgresStore) UpdateTableAtomic(ctx context.Context, projectID, datasetID, tableID string, mutate func(Table) (Table, error)) (Table, error) {
	return storeutil.RetrySerializable(ctx, func() (Table, error) {
		return s.updateTableAtomicOnce(ctx, projectID, datasetID, tableID, mutate)
	})
}

func (s *PostgresStore) updateTableAtomicOnce(ctx context.Context, projectID, datasetID, tableID string, mutate func(Table) (Table, error)) (Table, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Table{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanTable(tx.QueryRow(ctx, `
		SELECT project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time
		FROM jc_bq_tables WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 FOR UPDATE
	`, projectID, datasetID, tableID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrNoSuchTable
	}
	if err != nil {
		return Table{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Table{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_bq_tables SET config=$4, schema=$5, labels=$6, update_time=$7
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, tableID, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(next.Schema, "{}"), nullableJSONRaw(labels, "{}"), next.UpdateTime)
	if err != nil {
		return Table{}, err
	}
	if tag.RowsAffected() == 0 {
		return Table{}, ErrNoSuchTable
	}
	if err := tx.Commit(ctx); err != nil {
		return Table{}, err
	}
	next.ProjectID = projectID
	next.DatasetID = datasetID
	return next, nil
}

func (s *PostgresStore) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_bq_rows WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3`, projectID, datasetID, tableID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_bq_row_access_policies WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3`, projectID, datasetID, tableID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM jc_bq_tables WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3`, projectID, datasetID, tableID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTable
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.dedup.forget(tableScope(projectID, datasetID, tableID))
	return nil
}

func (s *PostgresStore) ListTables(ctx context.Context, projectID, datasetID string) ([]Table, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, table_id, config, schema, labels, num_rows, create_time, update_time
		FROM jc_bq_tables WHERE project_id=$1 AND dataset_id=$2 ORDER BY table_id
	`, projectID, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Table
	for rows.Next() {
		t, err := scanTable(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TableID < result[j].TableID })
	return result, rows.Err()
}

// --- Jobs ---

func (s *PostgresStore) CreateJob(ctx context.Context, projectID string, j Job) error {
	if j.CreateTime.IsZero() {
		j.CreateTime = clock.Now()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_bq_jobs (project_id, job_id, config, create_time)
		VALUES ($1,$2,$3,$4)
	`, projectID, j.JobID, nullableJSONRaw(j.Config, "{}"), j.CreateTime)
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
	var config []byte
	err := row.Scan(&j.ProjectID, &j.JobID, &config, &j.CreateTime)
	if err != nil {
		return Job{}, err
	}
	j.Config = json.RawMessage(config)
	return j, nil
}

func (s *PostgresStore) GetJob(ctx context.Context, projectID, jobID string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT project_id, job_id, config, create_time
		FROM jc_bq_jobs WHERE project_id=$1 AND job_id=$2
	`, projectID, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoSuchJob
	}
	return j, err
}

func (s *PostgresStore) DeleteJob(ctx context.Context, projectID, jobID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_bq_jobs WHERE project_id=$1 AND job_id=$2`, projectID, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchJob
	}
	return nil
}

func (s *PostgresStore) ListJobs(ctx context.Context, projectID string) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, job_id, config, create_time
		FROM jc_bq_jobs WHERE project_id=$1 ORDER BY job_id
	`, projectID)
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

// --- Rows ---

func (s *PostgresStore) InsertRows(ctx context.Context, projectID, datasetID, tableID string, rows []Row) ([]int, error) {
	// Resolve the table up front so a missing table never records insertIds.
	if _, err := s.GetTable(ctx, projectID, datasetID, tableID); err != nil {
		return nil, err
	}
	key := tableScope(projectID, datasetID, tableID)
	dups := s.dedup.filter(key, rows, clock.Now())
	dupSet := make(map[int]bool, len(dups))
	for _, i := range dups {
		dupSet[i] = true
	}
	keep := make([]Row, 0, len(rows))
	for i := range rows {
		if dupSet[i] {
			continue
		}
		keep = append(keep, rows[i])
	}
	if len(keep) == 0 {
		return dups, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var seq int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(seq), 0) FROM jc_bq_rows
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, tableID).Scan(&seq); err != nil {
		return nil, err
	}
	for i := range keep {
		seq++
		keep[i].ProjectID = projectID
		keep[i].DatasetID = datasetID
		keep[i].TableID = tableID
		keep[i].Seq = seq
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_rows (project_id, dataset_id, table_id, seq, data)
			VALUES ($1,$2,$3,$4,$5)
		`, projectID, datasetID, tableID, seq, nullableJSONRaw(keep[i].Data, "{}")); err != nil {
			return nil, err
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE jc_bq_tables SET num_rows = num_rows + $4, update_time = $5
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, tableID, len(keep), clock.Now().UTC())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNoSuchTable
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return dups, nil
}

func (s *PostgresStore) ReplaceRows(ctx context.Context, projectID, datasetID, tableID string, rows []Row) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		DELETE FROM jc_bq_rows WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, tableID); err != nil {
		return err
	}
	for i := range rows {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_bq_rows (project_id, dataset_id, table_id, seq, data)
			VALUES ($1,$2,$3,$4,$5)
		`, projectID, datasetID, tableID, int64(i+1), nullableJSONRaw(rows[i].Data, "{}")); err != nil {
			return err
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE jc_bq_tables SET num_rows=$4, update_time=$5
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3
	`, projectID, datasetID, tableID, len(rows), clock.Now().UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTable
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.dedup.forget(tableScope(projectID, datasetID, tableID))
	return nil
}

func (s *PostgresStore) ListRows(ctx context.Context, projectID, datasetID, tableID string) ([]Row, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, table_id, seq, data
		FROM jc_bq_rows WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 ORDER BY seq
	`, projectID, datasetID, tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Row
	for rows.Next() {
		var r Row
		var data []byte
		if err := rows.Scan(&r.ProjectID, &r.DatasetID, &r.TableID, &r.Seq, &data); err != nil {
			return nil, err
		}
		r.Data = json.RawMessage(data)
		result = append(result, r)
	}
	return result, rows.Err()
}

// --- Routines ---

func (s *PostgresStore) CreateRoutine(ctx context.Context, projectID, datasetID string, r Routine) error {
	if r.CreateTime.IsZero() {
		r.CreateTime = clock.Now()
	}
	if r.UpdateTime.IsZero() {
		r.UpdateTime = r.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_bq_routines (project_id, dataset_id, routine_id, config, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, projectID, datasetID, r.RoutineID, nullableJSONRaw(r.Config, "{}"), r.CreateTime, r.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanRoutine(row pgx.Row) (Routine, error) {
	var r Routine
	var config []byte
	err := row.Scan(&r.ProjectID, &r.DatasetID, &r.RoutineID, &config, &r.CreateTime, &r.UpdateTime)
	if err != nil {
		return Routine{}, err
	}
	r.Config = json.RawMessage(config)
	return r, nil
}

func (s *PostgresStore) GetRoutine(ctx context.Context, projectID, datasetID, routineID string) (Routine, error) {
	r, err := scanRoutine(s.pool.QueryRow(ctx, `
		SELECT project_id, dataset_id, routine_id, config, create_time, update_time
		FROM jc_bq_routines WHERE project_id=$1 AND dataset_id=$2 AND routine_id=$3
	`, projectID, datasetID, routineID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Routine{}, ErrNoSuchRoutine
	}
	return r, err
}

// UpdateRoutineAtomic mirrors UpdateTableAtomic: a Serializable transaction with
// SELECT ... FOR UPDATE row-locks the routine for the duration of mutate.
func (s *PostgresStore) UpdateRoutineAtomic(ctx context.Context, projectID, datasetID, routineID string, mutate func(Routine) (Routine, error)) (Routine, error) {
	return storeutil.RetrySerializable(ctx, func() (Routine, error) {
		return s.updateRoutineAtomicOnce(ctx, projectID, datasetID, routineID, mutate)
	})
}

func (s *PostgresStore) updateRoutineAtomicOnce(ctx context.Context, projectID, datasetID, routineID string, mutate func(Routine) (Routine, error)) (Routine, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Routine{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanRoutine(tx.QueryRow(ctx, `
		SELECT project_id, dataset_id, routine_id, config, create_time, update_time
		FROM jc_bq_routines WHERE project_id=$1 AND dataset_id=$2 AND routine_id=$3 FOR UPDATE
	`, projectID, datasetID, routineID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Routine{}, ErrNoSuchRoutine
	}
	if err != nil {
		return Routine{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Routine{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_bq_routines SET config=$4, update_time=$5
		WHERE project_id=$1 AND dataset_id=$2 AND routine_id=$3
	`, projectID, datasetID, routineID, nullableJSONRaw(next.Config, "{}"), next.UpdateTime)
	if err != nil {
		return Routine{}, err
	}
	if tag.RowsAffected() == 0 {
		return Routine{}, ErrNoSuchRoutine
	}
	if err := tx.Commit(ctx); err != nil {
		return Routine{}, err
	}
	next.ProjectID = projectID
	next.DatasetID = datasetID
	next.RoutineID = routineID
	return next, nil
}

func (s *PostgresStore) DeleteRoutine(ctx context.Context, projectID, datasetID, routineID string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_bq_routines WHERE project_id=$1 AND dataset_id=$2 AND routine_id=$3
	`, projectID, datasetID, routineID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchRoutine
	}
	return nil
}

func (s *PostgresStore) ListRoutines(ctx context.Context, projectID, datasetID string) ([]Routine, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, routine_id, config, create_time, update_time
		FROM jc_bq_routines WHERE project_id=$1 AND dataset_id=$2 ORDER BY routine_id
	`, projectID, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Routine
	for rows.Next() {
		r, err := scanRoutine(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// --- Row access policies ---

func (s *PostgresStore) CreateRowAccessPolicy(ctx context.Context, projectID, datasetID, tableID string, p RowAccessPolicy) error {
	if p.CreateTime.IsZero() {
		p.CreateTime = clock.Now()
	}
	if p.UpdateTime.IsZero() {
		p.UpdateTime = p.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_bq_row_access_policies (project_id, dataset_id, table_id, policy_id, config, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, projectID, datasetID, tableID, p.PolicyID, nullableJSONRaw(p.Config, "{}"), p.CreateTime, p.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanRowAccessPolicy(row pgx.Row) (RowAccessPolicy, error) {
	var p RowAccessPolicy
	var config []byte
	err := row.Scan(&p.ProjectID, &p.DatasetID, &p.TableID, &p.PolicyID, &config, &p.CreateTime, &p.UpdateTime)
	if err != nil {
		return RowAccessPolicy{}, err
	}
	p.Config = json.RawMessage(config)
	return p, nil
}

func (s *PostgresStore) GetRowAccessPolicy(ctx context.Context, projectID, datasetID, tableID, policyID string) (RowAccessPolicy, error) {
	p, err := scanRowAccessPolicy(s.pool.QueryRow(ctx, `
		SELECT project_id, dataset_id, table_id, policy_id, config, create_time, update_time
		FROM jc_bq_row_access_policies
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 AND policy_id=$4
	`, projectID, datasetID, tableID, policyID))
	if errors.Is(err, pgx.ErrNoRows) {
		return RowAccessPolicy{}, ErrNoSuchRowAccessPolicy
	}
	return p, err
}

// UpdateRowAccessPolicyAtomic mirrors UpdateTableAtomic: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the policy for the duration
// of mutate.
func (s *PostgresStore) UpdateRowAccessPolicyAtomic(ctx context.Context, projectID, datasetID, tableID, policyID string, mutate func(RowAccessPolicy) (RowAccessPolicy, error)) (RowAccessPolicy, error) {
	return storeutil.RetrySerializable(ctx, func() (RowAccessPolicy, error) {
		return s.updateRowAccessPolicyAtomicOnce(ctx, projectID, datasetID, tableID, policyID, mutate)
	})
}

func (s *PostgresStore) updateRowAccessPolicyAtomicOnce(ctx context.Context, projectID, datasetID, tableID, policyID string, mutate func(RowAccessPolicy) (RowAccessPolicy, error)) (RowAccessPolicy, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return RowAccessPolicy{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanRowAccessPolicy(tx.QueryRow(ctx, `
		SELECT project_id, dataset_id, table_id, policy_id, config, create_time, update_time
		FROM jc_bq_row_access_policies
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 AND policy_id=$4 FOR UPDATE
	`, projectID, datasetID, tableID, policyID))
	if errors.Is(err, pgx.ErrNoRows) {
		return RowAccessPolicy{}, ErrNoSuchRowAccessPolicy
	}
	if err != nil {
		return RowAccessPolicy{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return RowAccessPolicy{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_bq_row_access_policies SET config=$5, update_time=$6
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 AND policy_id=$4
	`, projectID, datasetID, tableID, policyID, nullableJSONRaw(next.Config, "{}"), next.UpdateTime)
	if err != nil {
		return RowAccessPolicy{}, err
	}
	if tag.RowsAffected() == 0 {
		return RowAccessPolicy{}, ErrNoSuchRowAccessPolicy
	}
	if err := tx.Commit(ctx); err != nil {
		return RowAccessPolicy{}, err
	}
	next.ProjectID = projectID
	next.DatasetID = datasetID
	next.TableID = tableID
	next.PolicyID = policyID
	return next, nil
}

func (s *PostgresStore) DeleteRowAccessPolicy(ctx context.Context, projectID, datasetID, tableID, policyID string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_bq_row_access_policies
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 AND policy_id=$4
	`, projectID, datasetID, tableID, policyID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchRowAccessPolicy
	}
	return nil
}

func (s *PostgresStore) ListRowAccessPolicies(ctx context.Context, projectID, datasetID, tableID string) ([]RowAccessPolicy, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, dataset_id, table_id, policy_id, config, create_time, update_time
		FROM jc_bq_row_access_policies
		WHERE project_id=$1 AND dataset_id=$2 AND table_id=$3 ORDER BY policy_id
	`, projectID, datasetID, tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RowAccessPolicy
	for rows.Next() {
		p, err := scanRowAccessPolicy(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_bq_rows`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_bq_row_access_policies`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_bq_routines`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_bq_tables`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_bq_datasets`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_bq_jobs`)
	s.dedup.reset()
}

// nullableJSONRaw returns a json.RawMessage for a JSONB column, substituting the
// given empty default ("{}" or "[]") for a nil/null value so NOT NULL columns
// receive valid JSON instead of SQL NULL.
func nullableJSONRaw(b []byte, empty string) any {
	if len(b) == 0 || string(b) == "null" {
		return json.RawMessage(empty)
	}
	return json.RawMessage(b)
}
