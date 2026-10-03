package managedkafka

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

// Snapshot serializes the control-plane metadata only (clusters, topics,
// operations, ACLs). A cluster's live broker endpoint and the broker's own
// topic/group/ACL/message bytes are runtime state and are deliberately excluded
// (Cluster.BootstrapAddress is json:"-"): --dsn snapshots and import/export must
// not claim broker data is portable. On restore the broker is re-ensured lazily
// on the first read or data-plane call.
func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clusters) == 0 && len(s.topics) == 0 && len(s.operations) == 0 && len(s.acls) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"clusters":   s.clusters,
		"topics":     s.topics,
		"operations": s.operations,
		"acls":       s.acls,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Clusters   map[string]map[string]Cluster   `json:"clusters"`
		Topics     map[string]map[string]Topic     `json:"topics"`
		Operations map[string]map[string]Operation `json:"operations"`
		Acls       map[string]map[string]Acl       `json:"acls"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Clusters == nil {
		snap.Clusters = map[string]map[string]Cluster{}
	}
	if snap.Topics == nil {
		snap.Topics = map[string]map[string]Topic{}
	}
	if snap.Operations == nil {
		snap.Operations = map[string]map[string]Operation{}
	}
	if snap.Acls == nil {
		snap.Acls = map[string]map[string]Acl{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = snap.Clusters
	s.topics = snap.Topics
	s.operations = snap.Operations
	s.acls = snap.Acls
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	for _, tbl := range []string{"jc_mk_clusters", "jc_mk_topics", "jc_mk_operations", "jc_mk_acls"} {
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type clusterRow struct {
		ProjectID string  `json:"projectId"`
		Cluster   Cluster `json:"cluster"`
	}
	type topicRow struct {
		ProjectID string `json:"projectId"`
		Topic     Topic  `json:"topic"`
	}
	type operationRow struct {
		ProjectID string    `json:"projectId"`
		Operation Operation `json:"operation"`
	}
	type aclRow struct {
		ProjectID string `json:"projectId"`
		Acl       Acl    `json:"acl"`
	}

	clusters := make([]clusterRow, 0)
	topics := make([]topicRow, 0)
	operations := make([]operationRow, 0)
	acls := make([]aclRow, 0)

	crows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, config, labels, create_time, update_time
		FROM jc_mk_clusters ORDER BY project_id, location, cluster_name
	`)
	if err != nil {
		return err
	}
	for crows.Next() {
		var r clusterRow
		var config, labels []byte
		if err := crows.Scan(&r.ProjectID, &r.Cluster.Location, &r.Cluster.Name, &config, &labels, &r.Cluster.CreateTime, &r.Cluster.UpdateTime); err != nil {
			crows.Close()
			return err
		}
		r.Cluster.Config = json.RawMessage(config)
		json.Unmarshal(labels, &r.Cluster.Labels)
		clusters = append(clusters, r)
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return err
	}

	trows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time
		FROM jc_mk_topics ORDER BY project_id, location, cluster_name, topic_name
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r topicRow
		var config []byte
		if err := trows.Scan(&r.ProjectID, &r.Topic.Location, &r.Topic.ClusterName, &r.Topic.Name, &r.Topic.PartitionCount, &r.Topic.ReplicationFactor, &config, &r.Topic.CreateTime, &r.Topic.UpdateTime); err != nil {
			trows.Close()
			return err
		}
		r.Topic.Config = json.RawMessage(config)
		topics = append(topics, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	orows, err := s.pool.Query(ctx, `
		SELECT project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time
		FROM jc_mk_operations ORDER BY project_id, location, operation_id
	`)
	if err != nil {
		return err
	}
	for orows.Next() {
		var r operationRow
		if err := orows.Scan(&r.ProjectID, &r.Operation.Location, &r.Operation.ID, &r.Operation.Done, &r.Operation.Metadata,
			&r.Operation.Response, &r.Operation.Verb, &r.Operation.Target, &r.Operation.CreateTime, &r.Operation.EndTime); err != nil {
			orows.Close()
			return err
		}
		operations = append(operations, r)
	}
	orows.Close()
	if err := orows.Err(); err != nil {
		return err
	}

	arows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, acl_name, entries, etag, resource_type, resource_name, pattern_type
		FROM jc_mk_acls ORDER BY project_id, location, cluster_name, acl_name
	`)
	if err != nil {
		return err
	}
	for arows.Next() {
		var r aclRow
		var entries []byte
		if err := arows.Scan(&r.ProjectID, &r.Acl.Location, &r.Acl.ClusterName, &r.Acl.Name, &entries,
			&r.Acl.Etag, &r.Acl.ResourceType, &r.Acl.ResourceName, &r.Acl.PatternType); err != nil {
			arows.Close()
			return err
		}
		_ = json.Unmarshal(entries, &r.Acl.AclEntries)
		acls = append(acls, r)
	}
	arows.Close()
	if err := arows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"clusters":   clusters,
		"topics":     topics,
		"operations": operations,
		"acls":       acls,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Clusters []struct {
			ProjectID string  `json:"projectId"`
			Cluster   Cluster `json:"cluster"`
		} `json:"clusters"`
		Topics []struct {
			ProjectID string `json:"projectId"`
			Topic     Topic  `json:"topic"`
		} `json:"topics"`
		Operations []struct {
			ProjectID string    `json:"projectId"`
			Operation Operation `json:"operation"`
		} `json:"operations"`
		Acls []struct {
			ProjectID string `json:"projectId"`
			Acl       Acl    `json:"acl"`
		} `json:"acls"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_mk_clusters", "jc_mk_topics", "jc_mk_operations", "jc_mk_acls"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Clusters {
		labels, _ := json.Marshal(r.Cluster.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_mk_clusters
				(project_id, location, cluster_name, config, labels, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, r.ProjectID, r.Cluster.Location, r.Cluster.Name, nullableJSONRaw(r.Cluster.Config, "{}"), nullableJSONRaw(labels, "{}"), r.Cluster.CreateTime, r.Cluster.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Topics {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_mk_topics
				(project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Topic.Location, r.Topic.ClusterName, r.Topic.Name, r.Topic.PartitionCount, r.Topic.ReplicationFactor, nullableJSONRaw(r.Topic.Config, "{}"), r.Topic.CreateTime, r.Topic.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Operations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_mk_operations
				(project_id, location, operation_id, done, metadata, response, verb, target, create_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, r.ProjectID, r.Operation.Location, r.Operation.ID, r.Operation.Done, r.Operation.Metadata, r.Operation.Response,
			r.Operation.Verb, r.Operation.Target, r.Operation.CreateTime, r.Operation.EndTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Acls {
		entries, _ := json.Marshal(r.Acl.AclEntries)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_mk_acls
				(project_id, location, cluster_name, acl_name, entries, etag, resource_type, resource_name, pattern_type)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Acl.Location, r.Acl.ClusterName, r.Acl.Name, nullableJSONRaw(entries, "[]"),
			r.Acl.Etag, r.Acl.ResourceType, r.Acl.ResourceName, r.Acl.PatternType); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
