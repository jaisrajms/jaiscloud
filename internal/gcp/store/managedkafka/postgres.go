package managedkafka

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

// PostgresStore implements Store against jc_mk_clusters / jc_mk_topics.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// --- Clusters ---

func (s *PostgresStore) CreateCluster(ctx context.Context, projectID, location string, c Cluster) error {
	if c.CreateTime.IsZero() {
		c.CreateTime = clock.Now()
	}
	if c.UpdateTime.IsZero() {
		c.UpdateTime = c.CreateTime
	}
	labels, _ := json.Marshal(c.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_mk_clusters
			(project_id, location, cluster_name, config, labels, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, projectID, location, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(labels, "{}"), c.CreateTime, c.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanCluster(row pgx.Row) (Cluster, error) {
	var c Cluster
	var config, labels []byte
	err := row.Scan(&c.ProjectID, &c.Location, &c.Name, &config, &labels, &c.CreateTime, &c.UpdateTime)
	if err != nil {
		return Cluster{}, err
	}
	c.Config = json.RawMessage(config)
	json.Unmarshal(labels, &c.Labels)
	return c, nil
}

func (s *PostgresStore) GetCluster(ctx context.Context, projectID, location, name string) (Cluster, error) {
	c, err := scanCluster(s.pool.QueryRow(ctx, `
		SELECT project_id, location, cluster_name, config, labels, create_time, update_time
		FROM jc_mk_clusters WHERE project_id=$1 AND location=$2 AND cluster_name=$3
	`, projectID, location, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Cluster{}, ErrNoSuchCluster
	}
	return c, err
}

func (s *PostgresStore) UpdateCluster(ctx context.Context, projectID, location string, c Cluster) error {
	labels, _ := json.Marshal(c.Labels)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_mk_clusters SET config=$4, labels=$5, update_time=$6
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3
	`, projectID, location, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(labels, "{}"), c.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchCluster
	}
	return nil
}

// UpdateClusterAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the cluster for the
// duration of mutate, so a concurrent UpdateClusterAtomic on the same
// cluster blocks until this transaction commits or rolls back, instead of
// racing to silently overwrite this call's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateClusterAtomic(ctx context.Context, projectID, location, name string, mutate func(Cluster) (Cluster, error)) (Cluster, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Cluster{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanCluster(tx.QueryRow(ctx, `
		SELECT project_id, location, cluster_name, config, labels, create_time, update_time
		FROM jc_mk_clusters WHERE project_id=$1 AND location=$2 AND cluster_name=$3 FOR UPDATE
	`, projectID, location, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Cluster{}, ErrNoSuchCluster
	}
	if err != nil {
		return Cluster{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Cluster{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_mk_clusters SET config=$4, labels=$5, update_time=$6
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3
	`, projectID, location, name, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(labels, "{}"), next.UpdateTime)
	if err != nil {
		return Cluster{}, err
	}
	if tag.RowsAffected() == 0 {
		return Cluster{}, ErrNoSuchCluster
	}
	if err := tx.Commit(ctx); err != nil {
		return Cluster{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	return next, nil
}

func (s *PostgresStore) DeleteCluster(ctx context.Context, projectID, location, name string) error {
	// Delete the cluster's topics first so no orphaned rows remain, mirroring
	// MemoryStore.DeleteCluster (which drops the topic scope alongside the
	// cluster). The explicit DELETE keeps the migration additive-safe (no
	// schema change / FK cascade required).
	if _, err := s.pool.Exec(ctx, `DELETE FROM jc_mk_topics WHERE project_id=$1 AND location=$2 AND cluster_name=$3`, projectID, location, name); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM jc_mk_acls WHERE project_id=$1 AND location=$2 AND cluster_name=$3`, projectID, location, name); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_mk_clusters WHERE project_id=$1 AND location=$2 AND cluster_name=$3`, projectID, location, name)
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
		SELECT project_id, location, cluster_name, config, labels, create_time, update_time
		FROM jc_mk_clusters WHERE project_id=$1 AND location=$2 ORDER BY cluster_name
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

// --- Topics ---

func (s *PostgresStore) CreateTopic(ctx context.Context, projectID, location, clusterName string, t Topic) error {
	if t.CreateTime.IsZero() {
		t.CreateTime = clock.Now()
	}
	if t.UpdateTime.IsZero() {
		t.UpdateTime = t.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_mk_topics
			(project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, clusterName, t.Name, t.PartitionCount, t.ReplicationFactor, nullableJSONRaw(t.Config, "{}"), t.CreateTime, t.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanTopic(row pgx.Row) (Topic, error) {
	var t Topic
	var config []byte
	err := row.Scan(&t.ProjectID, &t.Location, &t.ClusterName, &t.Name, &t.PartitionCount, &t.ReplicationFactor, &config, &t.CreateTime, &t.UpdateTime)
	if err != nil {
		return Topic{}, err
	}
	t.Config = json.RawMessage(config)
	return t, nil
}

func (s *PostgresStore) GetTopic(ctx context.Context, projectID, location, clusterName, topicName string) (Topic, error) {
	t, err := scanTopic(s.pool.QueryRow(ctx, `
		SELECT project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time
		FROM jc_mk_topics WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND topic_name=$4
	`, projectID, location, clusterName, topicName))
	if errors.Is(err, pgx.ErrNoRows) {
		return Topic{}, ErrNoSuchTopic
	}
	return t, err
}

func (s *PostgresStore) UpdateTopic(ctx context.Context, projectID, location, clusterName string, t Topic) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_mk_topics SET partition_count=$5, replication_factor=$6, config=$7, update_time=$8
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND topic_name=$4
	`, projectID, location, clusterName, t.Name, t.PartitionCount, t.ReplicationFactor, nullableJSONRaw(t.Config, "{}"), t.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTopic
	}
	return nil
}

// UpdateTopicAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the topic for the
// duration of mutate, so a concurrent UpdateTopicAtomic on the same topic
// blocks until this transaction commits or rolls back, instead of racing to
// silently overwrite this call's write. See store/firestore/postgres.go's
// Commit for the same convention.
func (s *PostgresStore) UpdateTopicAtomic(ctx context.Context, projectID, location, clusterName, topicName string, mutate func(Topic) (Topic, error)) (Topic, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Topic{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanTopic(tx.QueryRow(ctx, `
		SELECT project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time
		FROM jc_mk_topics WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND topic_name=$4 FOR UPDATE
	`, projectID, location, clusterName, topicName))
	if errors.Is(err, pgx.ErrNoRows) {
		return Topic{}, ErrNoSuchTopic
	}
	if err != nil {
		return Topic{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Topic{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_mk_topics SET partition_count=$5, replication_factor=$6, config=$7, update_time=$8
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND topic_name=$4
	`, projectID, location, clusterName, topicName, next.PartitionCount, next.ReplicationFactor, nullableJSONRaw(next.Config, "{}"), next.UpdateTime)
	if err != nil {
		return Topic{}, err
	}
	if tag.RowsAffected() == 0 {
		return Topic{}, ErrNoSuchTopic
	}
	if err := tx.Commit(ctx); err != nil {
		return Topic{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.ClusterName = clusterName
	return next, nil
}

func (s *PostgresStore) DeleteTopic(ctx context.Context, projectID, location, clusterName, topicName string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_mk_topics WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND topic_name=$4`, projectID, location, clusterName, topicName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTopic
	}
	return nil
}

func (s *PostgresStore) ListTopics(ctx context.Context, projectID, location, clusterName string) ([]Topic, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time
		FROM jc_mk_topics WHERE project_id=$1 AND location=$2 AND cluster_name=$3 ORDER BY topic_name
	`, projectID, location, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Topic
	for rows.Next() {
		t, err := scanTopic(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
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
		INSERT INTO jc_mk_operations
			(project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, projectID, location, op.ID, op.Done, op.Metadata, op.Response, op.Verb, op.Target, op.CreateTime, op.EndTime)
	return err
}

func (s *PostgresStore) GetOperation(ctx context.Context, projectID, location, id string) (Operation, error) {
	var op Operation
	err := s.pool.QueryRow(ctx, `
		SELECT project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_mk_operations WHERE project_id=$1 AND location=$2 AND operation_id=$3
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
		FROM jc_mk_operations WHERE project_id=$1 AND location=$2 ORDER BY operation_id
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
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_mk_clusters`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_mk_topics`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_mk_operations`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_mk_acls`)
}

// --- ACLs ---

func scanAcl(row pgx.Row) (Acl, error) {
	var a Acl
	var entries []byte
	err := row.Scan(&a.ProjectID, &a.Location, &a.ClusterName, &a.Name, &entries, &a.Etag,
		&a.ResourceType, &a.ResourceName, &a.PatternType)
	if err != nil {
		return Acl{}, err
	}
	_ = json.Unmarshal(entries, &a.AclEntries)
	return a, nil
}

func (s *PostgresStore) CreateAcl(ctx context.Context, projectID, location, clusterName string, a Acl) error {
	entries, _ := json.Marshal(a.AclEntries)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_mk_acls
			(project_id, location, cluster_name, acl_name, entries, etag, resource_type, resource_name, pattern_type)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, clusterName, a.Name, nullableJSONRaw(entries, "[]"), a.Etag, a.ResourceType, a.ResourceName, a.PatternType)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetAcl(ctx context.Context, projectID, location, clusterName, name string) (Acl, error) {
	a, err := scanAcl(s.pool.QueryRow(ctx, `
		SELECT project_id, location, cluster_name, acl_name, entries, etag, resource_type, resource_name, pattern_type
		FROM jc_mk_acls WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND acl_name=$4
	`, projectID, location, clusterName, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Acl{}, ErrNoSuchAcl
	}
	return a, err
}

func (s *PostgresStore) UpdateAcl(ctx context.Context, projectID, location, clusterName string, a Acl) error {
	entries, _ := json.Marshal(a.AclEntries)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_mk_acls SET entries=$5, etag=$6, resource_type=$7, resource_name=$8, pattern_type=$9
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND acl_name=$4
	`, projectID, location, clusterName, a.Name, nullableJSONRaw(entries, "[]"), a.Etag, a.ResourceType, a.ResourceName, a.PatternType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchAcl
	}
	return nil
}

func (s *PostgresStore) UpdateAclAtomic(ctx context.Context, projectID, location, clusterName, name string, mutate func(Acl) (Acl, error)) (Acl, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Acl{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanAcl(tx.QueryRow(ctx, `
		SELECT project_id, location, cluster_name, acl_name, entries, etag, resource_type, resource_name, pattern_type
		FROM jc_mk_acls WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND acl_name=$4 FOR UPDATE
	`, projectID, location, clusterName, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Acl{}, ErrNoSuchAcl
	}
	if err != nil {
		return Acl{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Acl{}, err
	}

	entries, _ := json.Marshal(next.AclEntries)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_mk_acls SET entries=$5, etag=$6, resource_type=$7, resource_name=$8, pattern_type=$9
		WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND acl_name=$4
	`, projectID, location, clusterName, name, nullableJSONRaw(entries, "[]"), next.Etag, next.ResourceType, next.ResourceName, next.PatternType)
	if err != nil {
		return Acl{}, err
	}
	if tag.RowsAffected() == 0 {
		return Acl{}, ErrNoSuchAcl
	}
	if err := tx.Commit(ctx); err != nil {
		return Acl{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	next.ClusterName = clusterName
	return next, nil
}

func (s *PostgresStore) DeleteAcl(ctx context.Context, projectID, location, clusterName, name string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_mk_acls WHERE project_id=$1 AND location=$2 AND cluster_name=$3 AND acl_name=$4
	`, projectID, location, clusterName, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchAcl
	}
	return nil
}

func (s *PostgresStore) ListAcls(ctx context.Context, projectID, location, clusterName string) ([]Acl, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, acl_name, entries, etag, resource_type, resource_name, pattern_type
		FROM jc_mk_acls WHERE project_id=$1 AND location=$2 AND cluster_name=$3 ORDER BY acl_name
	`, projectID, location, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Acl
	for rows.Next() {
		a, err := scanAcl(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
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
