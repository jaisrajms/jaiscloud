package run

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_run_services, jc_run_revisions and
// jc_run_operations. Each record is stored as a JSONB document keyed by its
// scope and name.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func encodeService(s Service) ([]byte, error) {
	s.ProjectID = ""
	s.Location = ""
	return json.Marshal(s)
}

func decodeService(data []byte) (Service, error) {
	var s Service
	if err := json.Unmarshal(data, &s); err != nil {
		return Service{}, err
	}
	return s, nil
}

func encodeRevision(r Revision) ([]byte, error) {
	r.ProjectID = ""
	r.Location = ""
	r.Service = ""
	return json.Marshal(r)
}

func decodeRevision(data []byte) (Revision, error) {
	var r Revision
	if err := json.Unmarshal(data, &r); err != nil {
		return Revision{}, err
	}
	return r, nil
}

func encodeOperation(op Operation) ([]byte, error) {
	op.ProjectID = ""
	op.Location = ""
	return json.Marshal(op)
}

func decodeOperation(data []byte) (Operation, error) {
	var op Operation
	if err := json.Unmarshal(data, &op); err != nil {
		return Operation{}, err
	}
	return op, nil
}

func (s *PostgresStore) CreateService(ctx context.Context, project, location string, svc Service) error {
	data, err := encodeService(svc)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_run_services (project_id, location, service_id, data)
		VALUES ($1,$2,$3,$4)
	`, project, location, svc.ID, data)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetService(ctx context.Context, project, location, id string) (Service, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_run_services
		WHERE project_id=$1 AND location=$2 AND service_id=$3
	`, project, location, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Service{}, ErrNoSuchService
	}
	if err != nil {
		return Service{}, err
	}
	svc, err := decodeService(data)
	if err != nil {
		return Service{}, err
	}
	svc.ProjectID = project
	svc.Location = location
	svc.ID = id
	return svc, nil
}

func (s *PostgresStore) UpdateService(ctx context.Context, project, location string, svc Service) error {
	data, err := encodeService(svc)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_run_services SET data=$4
		WHERE project_id=$1 AND location=$2 AND service_id=$3
	`, project, location, svc.ID, data)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchService
	}
	return nil
}

func (s *PostgresStore) DeleteService(ctx context.Context, project, location, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_run_services
		WHERE project_id=$1 AND location=$2 AND service_id=$3
	`, project, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchService
	}
	_, _ = s.pool.Exec(ctx, `
		DELETE FROM jc_run_revisions
		WHERE project_id=$1 AND location=$2 AND service_id=$3
	`, project, location, id)
	return nil
}

func (s *PostgresStore) ListServices(ctx context.Context, project, location string) ([]Service, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT service_id, data FROM jc_run_services
		WHERE project_id=$1 AND location=$2
		ORDER BY service_id
	`, project, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Service
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		svc, err := decodeService(data)
		if err != nil {
			return nil, err
		}
		svc.ProjectID = project
		svc.Location = location
		svc.ID = id
		result = append(result, svc)
	}
	return result, rows.Err()
}

// ListServicesByProject returns every service in a project across all
// locations, sorted by location then id.
func (s *PostgresStore) ListServicesByProject(ctx context.Context, project string) ([]Service, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT location, service_id, data FROM jc_run_services
		WHERE project_id=$1
		ORDER BY location, service_id
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Service
	for rows.Next() {
		var location, id string
		var data []byte
		if err := rows.Scan(&location, &id, &data); err != nil {
			return nil, err
		}
		svc, err := decodeService(data)
		if err != nil {
			return nil, err
		}
		svc.ProjectID = project
		svc.Location = location
		svc.ID = id
		result = append(result, svc)
	}
	return result, rows.Err()
}

func (s *PostgresStore) CreateRevision(ctx context.Context, project, location, service string, r Revision) error {
	data, err := encodeRevision(r)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_run_revisions (project_id, location, service_id, revision_id, data)
		VALUES ($1,$2,$3,$4,$5)
	`, project, location, service, r.ID, data)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetRevision(ctx context.Context, project, location, service, id string) (Revision, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_run_revisions
		WHERE project_id=$1 AND location=$2 AND service_id=$3 AND revision_id=$4
	`, project, location, service, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Revision{}, ErrNoSuchRevision
	}
	if err != nil {
		return Revision{}, err
	}
	r, err := decodeRevision(data)
	if err != nil {
		return Revision{}, err
	}
	r.ProjectID = project
	r.Location = location
	r.Service = service
	r.ID = id
	return r, nil
}

func (s *PostgresStore) ListRevisions(ctx context.Context, project, location, service string) ([]Revision, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT revision_id, data FROM jc_run_revisions
		WHERE project_id=$1 AND location=$2 AND service_id=$3
		ORDER BY revision_id
	`, project, location, service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Revision
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		r, err := decodeRevision(data)
		if err != nil {
			return nil, err
		}
		r.ProjectID = project
		r.Location = location
		r.Service = service
		r.ID = id
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteRevision(ctx context.Context, project, location, service, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_run_revisions
		WHERE project_id=$1 AND location=$2 AND service_id=$3 AND revision_id=$4
	`, project, location, service, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchRevision
	}
	return nil
}

func (s *PostgresStore) DeleteRevisions(ctx context.Context, project, location, service string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM jc_run_revisions
		WHERE project_id=$1 AND location=$2 AND service_id=$3
	`, project, location, service)
	return err
}

func (s *PostgresStore) CreateOperation(ctx context.Context, project, location string, op Operation) error {
	data, err := encodeOperation(op)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_run_operations (project_id, location, operation_id, data)
		VALUES ($1,$2,$3,$4)
	`, project, location, op.ID, data)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetOperation(ctx context.Context, project, location, id string) (Operation, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_run_operations
		WHERE project_id=$1 AND location=$2 AND operation_id=$3
	`, project, location, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	if err != nil {
		return Operation{}, err
	}
	op, err := decodeOperation(data)
	if err != nil {
		return Operation{}, err
	}
	op.ProjectID = project
	op.Location = location
	op.ID = id
	return op, nil
}

func (s *PostgresStore) ListOperations(ctx context.Context, project, location string) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT operation_id, data FROM jc_run_operations
		WHERE project_id=$1 AND location=$2
		ORDER BY operation_id
	`, project, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		op, err := decodeOperation(data)
		if err != nil {
			return nil, err
		}
		op.ProjectID = project
		op.Location = location
		op.ID = id
		result = append(result, op)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteOperation(ctx context.Context, project, location, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_run_operations
		WHERE project_id=$1 AND location=$2 AND operation_id=$3
	`, project, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchOperation
	}
	return nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_run_services`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_run_revisions`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_run_operations`)
}
