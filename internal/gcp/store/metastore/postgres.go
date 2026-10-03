package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_metastore_services /
// jc_metastore_backups / jc_metastore_metadata_imports / jc_metastore_operations.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// --- Services ---

func (s *PostgresStore) CreateService(ctx context.Context, projectID, location string, svc Service) error {
	if svc.CreateTime.IsZero() {
		svc.CreateTime = clock.Now()
	}
	if svc.UpdateTime.IsZero() {
		svc.UpdateTime = svc.CreateTime
	}
	labels, _ := json.Marshal(svc.Labels)
	history, _ := json.Marshal(svc.StateHistory)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_metastore_services
			(project_id, location, service_name, config, labels, state, state_history, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, svc.Name, nullableJSONRaw(svc.Config, "{}"), nullableJSONRaw(labels, "{}"), svc.State,
		nullableJSONRaw(history, "[]"), svc.CreateTime, svc.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanService(row pgx.Row) (Service, error) {
	var svc Service
	var config, labels, history []byte
	err := row.Scan(&svc.ProjectID, &svc.Location, &svc.Name, &config, &labels, &svc.State, &history, &svc.CreateTime, &svc.UpdateTime)
	if err != nil {
		return Service{}, err
	}
	svc.Config = json.RawMessage(config)
	json.Unmarshal(labels, &svc.Labels)
	json.Unmarshal(history, &svc.StateHistory)
	return svc, nil
}

func (s *PostgresStore) GetService(ctx context.Context, projectID, location, name string) (Service, error) {
	svc, err := scanService(s.pool.QueryRow(ctx, `
		SELECT project_id, location, service_name, config, labels, state, state_history, create_time, update_time
		FROM jc_metastore_services WHERE project_id=$1 AND location=$2 AND service_name=$3
	`, projectID, location, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Service{}, ErrNoSuchService
	}
	return svc, err
}

func (s *PostgresStore) UpdateService(ctx context.Context, projectID, location string, svc Service) error {
	labels, _ := json.Marshal(svc.Labels)
	history, _ := json.Marshal(svc.StateHistory)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_metastore_services SET config=$4, labels=$5, state=$6, state_history=$7, update_time=$8
		WHERE project_id=$1 AND location=$2 AND service_name=$3
	`, projectID, location, svc.Name, nullableJSONRaw(svc.Config, "{}"), nullableJSONRaw(labels, "{}"), svc.State,
		nullableJSONRaw(history, "[]"), svc.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchService
	}
	return nil
}

// UpdateServiceAtomic mirrors MemoryStore's version: a Serializable transaction
// with SELECT ... FOR UPDATE row-locks the service for the duration of mutate,
// so a concurrent writer can't land between the read and the write. See
// store/secretmanager/postgres.go's UpdateSecretAtomic for the reference shape.
func (s *PostgresStore) UpdateServiceAtomic(ctx context.Context, projectID, location, name string, mutate func(Service) (Service, error)) (Service, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Service{}, err
	}
	defer tx.Rollback(ctx)

	svc, err := scanService(tx.QueryRow(ctx, `
		SELECT project_id, location, service_name, config, labels, state, state_history, create_time, update_time
		FROM jc_metastore_services WHERE project_id=$1 AND location=$2 AND service_name=$3 FOR UPDATE
	`, projectID, location, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Service{}, ErrNoSuchService
	}
	if err != nil {
		return Service{}, err
	}

	next, err := mutate(svc)
	if err != nil {
		return Service{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	history, _ := json.Marshal(next.StateHistory)
	if _, err := tx.Exec(ctx, `
		UPDATE jc_metastore_services SET config=$4, labels=$5, state=$6, state_history=$7, update_time=$8
		WHERE project_id=$1 AND location=$2 AND service_name=$3
	`, projectID, location, next.Name, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(labels, "{}"), next.State,
		nullableJSONRaw(history, "[]"), next.UpdateTime); err != nil {
		return Service{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Service{}, err
	}
	return next, nil
}

func (s *PostgresStore) DeleteService(ctx context.Context, projectID, location, name string) error {
	// Delete the service's backups and metadata imports first so no orphaned
	// rows remain, mirroring MemoryStore.DeleteService (which drops the child
	// scopes alongside the service). The explicit DELETEs keep the migration
	// additive-safe (no FK cascade required).
	if _, err := s.pool.Exec(ctx, `DELETE FROM jc_metastore_backups WHERE project_id=$1 AND location=$2 AND service_name=$3`, projectID, location, name); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM jc_metastore_metadata_imports WHERE project_id=$1 AND location=$2 AND service_name=$3`, projectID, location, name); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_metastore_services WHERE project_id=$1 AND location=$2 AND service_name=$3`, projectID, location, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchService
	}
	return nil
}

func (s *PostgresStore) ListServices(ctx context.Context, projectID, location string) ([]Service, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_name, config, labels, state, state_history, create_time, update_time
		FROM jc_metastore_services WHERE project_id=$1 AND location=$2 ORDER BY service_name
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Service
	for rows.Next() {
		svc, err := scanService(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, svc)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

// --- Backups ---

func (s *PostgresStore) CreateBackup(ctx context.Context, projectID, location, serviceName string, b Backup) error {
	if b.CreateTime.IsZero() {
		b.CreateTime = clock.Now()
	}
	if b.EndTime.IsZero() {
		b.EndTime = b.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_metastore_backups
			(project_id, location, service_name, backup_name, config, description, state, create_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, serviceName, b.Name, nullableJSONRaw(b.Config, "{}"), b.Description, b.State, b.CreateTime, b.EndTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanBackup(row pgx.Row) (Backup, error) {
	var b Backup
	var config []byte
	err := row.Scan(&b.ProjectID, &b.Location, &b.ServiceName, &b.Name, &config, &b.Description, &b.State, &b.CreateTime, &b.EndTime)
	if err != nil {
		return Backup{}, err
	}
	b.Config = json.RawMessage(config)
	return b, nil
}

func (s *PostgresStore) GetBackup(ctx context.Context, projectID, location, serviceName, name string) (Backup, error) {
	b, err := scanBackup(s.pool.QueryRow(ctx, `
		SELECT project_id, location, service_name, backup_name, config, description, state, create_time, end_time
		FROM jc_metastore_backups WHERE project_id=$1 AND location=$2 AND service_name=$3 AND backup_name=$4
	`, projectID, location, serviceName, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Backup{}, ErrNoSuchBackup
	}
	return b, err
}

func (s *PostgresStore) DeleteBackup(ctx context.Context, projectID, location, serviceName, name string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_metastore_backups WHERE project_id=$1 AND location=$2 AND service_name=$3 AND backup_name=$4
	`, projectID, location, serviceName, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchBackup
	}
	return nil
}

func (s *PostgresStore) ListBackups(ctx context.Context, projectID, location, serviceName string) ([]Backup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_name, backup_name, config, description, state, create_time, end_time
		FROM jc_metastore_backups WHERE project_id=$1 AND location=$2 AND service_name=$3 ORDER BY backup_name
	`, projectID, location, serviceName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Backup
	for rows.Next() {
		b, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

// --- Metadata imports ---

func (s *PostgresStore) CreateMetadataImport(ctx context.Context, projectID, location, serviceName string, mi MetadataImport) error {
	if mi.CreateTime.IsZero() {
		mi.CreateTime = clock.Now()
	}
	if mi.UpdateTime.IsZero() {
		mi.UpdateTime = mi.CreateTime
	}
	if mi.EndTime.IsZero() {
		mi.EndTime = mi.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_metastore_metadata_imports
			(project_id, location, service_name, import_name, config, description, state, create_time, update_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, projectID, location, serviceName, mi.Name, nullableJSONRaw(mi.Config, "{}"), mi.Description, mi.State, mi.CreateTime, mi.UpdateTime, mi.EndTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanMetadataImport(row pgx.Row) (MetadataImport, error) {
	var mi MetadataImport
	var config []byte
	err := row.Scan(&mi.ProjectID, &mi.Location, &mi.ServiceName, &mi.Name, &config, &mi.Description, &mi.State, &mi.CreateTime, &mi.UpdateTime, &mi.EndTime)
	if err != nil {
		return MetadataImport{}, err
	}
	mi.Config = json.RawMessage(config)
	return mi, nil
}

func (s *PostgresStore) GetMetadataImport(ctx context.Context, projectID, location, serviceName, name string) (MetadataImport, error) {
	mi, err := scanMetadataImport(s.pool.QueryRow(ctx, `
		SELECT project_id, location, service_name, import_name, config, description, state, create_time, update_time, end_time
		FROM jc_metastore_metadata_imports WHERE project_id=$1 AND location=$2 AND service_name=$3 AND import_name=$4
	`, projectID, location, serviceName, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return MetadataImport{}, ErrNoSuchMetadataImport
	}
	return mi, err
}

func (s *PostgresStore) UpdateMetadataImport(ctx context.Context, projectID, location, serviceName string, mi MetadataImport) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_metastore_metadata_imports SET config=$5, description=$6, state=$7, update_time=$8, end_time=$9
		WHERE project_id=$1 AND location=$2 AND service_name=$3 AND import_name=$4
	`, projectID, location, serviceName, mi.Name, nullableJSONRaw(mi.Config, "{}"), mi.Description, mi.State, mi.UpdateTime, mi.EndTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchMetadataImport
	}
	return nil
}

// UpdateMetadataImportAtomic mirrors UpdateServiceAtomic for a metadata import.
func (s *PostgresStore) UpdateMetadataImportAtomic(ctx context.Context, projectID, location, serviceName, name string, mutate func(MetadataImport) (MetadataImport, error)) (MetadataImport, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return MetadataImport{}, err
	}
	defer tx.Rollback(ctx)

	mi, err := scanMetadataImport(tx.QueryRow(ctx, `
		SELECT project_id, location, service_name, import_name, config, description, state, create_time, update_time, end_time
		FROM jc_metastore_metadata_imports WHERE project_id=$1 AND location=$2 AND service_name=$3 AND import_name=$4 FOR UPDATE
	`, projectID, location, serviceName, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return MetadataImport{}, ErrNoSuchMetadataImport
	}
	if err != nil {
		return MetadataImport{}, err
	}

	next, err := mutate(mi)
	if err != nil {
		return MetadataImport{}, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jc_metastore_metadata_imports SET config=$5, description=$6, state=$7, update_time=$8, end_time=$9
		WHERE project_id=$1 AND location=$2 AND service_name=$3 AND import_name=$4
	`, projectID, location, serviceName, next.Name, nullableJSONRaw(next.Config, "{}"), next.Description, next.State, next.UpdateTime, next.EndTime); err != nil {
		return MetadataImport{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MetadataImport{}, err
	}
	return next, nil
}

func (s *PostgresStore) ListMetadataImports(ctx context.Context, projectID, location, serviceName string) ([]MetadataImport, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_name, import_name, config, description, state, create_time, update_time, end_time
		FROM jc_metastore_metadata_imports WHERE project_id=$1 AND location=$2 AND service_name=$3 ORDER BY import_name
	`, projectID, location, serviceName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MetadataImport
	for rows.Next() {
		mi, err := scanMetadataImport(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, mi)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

// --- Operations ---

func (s *PostgresStore) CreateOperation(ctx context.Context, projectID, location string, op Operation) error {
	if op.CreateTime.IsZero() {
		op.CreateTime = clock.Now()
	}
	if op.EndTime.IsZero() {
		op.EndTime = op.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_metastore_operations
			(project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, projectID, location, op.ID, op.Done, op.Metadata, op.Response, op.Verb, op.Target, op.CreateTime, op.EndTime)
	return err
}

func (s *PostgresStore) GetOperation(ctx context.Context, projectID, location, id string) (Operation, error) {
	var op Operation
	err := s.pool.QueryRow(ctx, `
		SELECT project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_metastore_operations WHERE project_id=$1 AND location=$2 AND operation_id=$3
	`, projectID, location, id).Scan(&op.ProjectID, &op.Location, &op.ID, &op.Done, &op.Metadata, &op.Response, &op.Verb, &op.Target,
		&op.CreateTime, &op.EndTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	return op, err
}

func (s *PostgresStore) ListOperations(ctx context.Context, projectID, location string) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_metastore_operations WHERE project_id=$1 AND location=$2 ORDER BY operation_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		var op Operation
		if err := rows.Scan(&op.ProjectID, &op.Location, &op.ID, &op.Done, &op.Metadata, &op.Response, &op.Verb, &op.Target,
			&op.CreateTime, &op.EndTime); err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_metastore_services`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_metastore_backups`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_metastore_metadata_imports`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_metastore_operations`)
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
