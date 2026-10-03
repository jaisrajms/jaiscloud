package container

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_container_clusters and
// jc_container_operations. Each record is stored as a JSONB document keyed by
// (project_id, location, name).
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func encodeCluster(c Cluster) ([]byte, error) {
	c.ProjectID = ""
	c.Location = ""
	return json.Marshal(c)
}

func decodeCluster(data []byte) (Cluster, error) {
	var c Cluster
	if err := json.Unmarshal(data, &c); err != nil {
		return Cluster{}, err
	}
	return c, nil
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *PostgresStore) CreateCluster(ctx context.Context, projectID, location string, c Cluster) error {
	data, err := encodeCluster(c)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_container_clusters (project_id, location, cluster_name, data)
		VALUES ($1,$2,$3,$4)
	`, projectID, location, c.Name, data)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetCluster(ctx context.Context, projectID, location, name string) (Cluster, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_container_clusters
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3
	`, projectID, location, name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return Cluster{}, ErrNoSuchCluster
	}
	if err != nil {
		return Cluster{}, err
	}
	c, err := decodeCluster(data)
	if err != nil {
		return Cluster{}, err
	}
	c.ProjectID = projectID
	c.Location = location
	c.Name = name
	return c, nil
}

func (s *PostgresStore) DeleteCluster(ctx context.Context, projectID, location, name string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_container_clusters
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3
	`, projectID, location, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchCluster
	}
	return nil
}

func (s *PostgresStore) ListClusters(ctx context.Context, projectID, location string) ([]Cluster, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT cluster_name, data FROM jc_container_clusters
		WHERE project_id=$1 AND location=$2
		ORDER BY cluster_name
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Cluster
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		c, err := decodeCluster(data)
		if err != nil {
			return nil, err
		}
		c.ProjectID = projectID
		c.Location = location
		c.Name = name
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *PostgresStore) CreateOperation(ctx context.Context, projectID, location string, op Operation) error {
	data, err := encodeOperation(op)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_container_operations (project_id, location, operation_name, data)
		VALUES ($1,$2,$3,$4)
	`, projectID, location, op.Name, data)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetOperation(ctx context.Context, projectID, location, name string) (Operation, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_container_operations
		WHERE project_id=$1 AND location=$2 AND operation_name=$3
	`, projectID, location, name).Scan(&data)
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
	op.ProjectID = projectID
	op.Location = location
	op.Name = name
	return op, nil
}

func (s *PostgresStore) ListOperations(ctx context.Context, projectID, location string) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT operation_name, data FROM jc_container_operations
		WHERE project_id=$1 AND location=$2
		ORDER BY operation_name
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return nil, err
		}
		op, err := decodeOperation(data)
		if err != nil {
			return nil, err
		}
		op.ProjectID = projectID
		op.Location = location
		op.Name = name
		result = append(result, op)
	}
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_container_clusters`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_container_operations`)
}
