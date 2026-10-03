package container

import (
	"context"
	"encoding/json"
	"io"
	"sort"
)

// snapshotData is the export/import envelope for both stores.
type snapshotData struct {
	Clusters   []Cluster   `json:"clusters"`
	Operations []Operation `json:"operations"`
}

func sortedClusters(m map[string]map[string]Cluster) []Cluster {
	var out []Cluster
	for _, byName := range m {
		for _, c := range byName {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ProjectID != out[j].ProjectID {
			return out[i].ProjectID < out[j].ProjectID
		}
		if out[i].Location != out[j].Location {
			return out[i].Location < out[j].Location
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedOperations(m map[string]map[string]Operation) []Operation {
	var out []Operation
	for _, byName := range m {
		for _, op := range byName {
			out = append(out, op)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ProjectID != out[j].ProjectID {
			return out[i].ProjectID < out[j].ProjectID
		}
		if out[i].Location != out[j].Location {
			return out[i].Location < out[j].Location
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clusters) == 0 && len(s.operations) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(snapshotData{
		Clusters:   sortedClusters(s.clusters),
		Operations: sortedOperations(s.operations),
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap snapshotData
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	clusters := make(map[string]map[string]Cluster)
	operations := make(map[string]map[string]Operation)
	for _, c := range snap.Clusters {
		key := scope(c.ProjectID, c.Location)
		if clusters[key] == nil {
			clusters[key] = make(map[string]Cluster)
		}
		clusters[key][c.Name] = c
	}
	for _, op := range snap.Operations {
		key := scope(op.ProjectID, op.Location)
		if operations[key] == nil {
			operations[key] = make(map[string]Operation)
		}
		operations[key][op.Name] = op
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = clusters
	s.operations = operations
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var clusters, operations int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_container_clusters`).Scan(&clusters); err != nil {
		return false, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_container_operations`).Scan(&operations); err != nil {
		return false, err
	}
	return clusters == 0 && operations == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	clusters, err := s.listAllClusters(ctx)
	if err != nil {
		return err
	}
	operations, err := s.listAllOperations(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(snapshotData{Clusters: clusters, Operations: operations})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap snapshotData
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_container_clusters`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_container_operations`); err != nil {
		return err
	}
	for _, c := range snap.Clusters {
		data, err := encodeCluster(c)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_container_clusters (project_id, location, cluster_name, data)
			VALUES ($1,$2,$3,$4)
		`, c.ProjectID, c.Location, c.Name, data); err != nil {
			return err
		}
	}
	for _, op := range snap.Operations {
		data, err := encodeOperation(op)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_container_operations (project_id, location, operation_name, data)
			VALUES ($1,$2,$3,$4)
		`, op.ProjectID, op.Location, op.Name, data); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) listAllClusters(ctx context.Context) ([]Cluster, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, data FROM jc_container_clusters
		ORDER BY project_id, location, cluster_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Cluster
	for rows.Next() {
		var projectID, location, name string
		var data []byte
		if err := rows.Scan(&projectID, &location, &name, &data); err != nil {
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

func (s *PostgresStore) listAllOperations(ctx context.Context) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, operation_name, data FROM jc_container_operations
		ORDER BY project_id, location, operation_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		var projectID, location, name string
		var data []byte
		if err := rows.Scan(&projectID, &location, &name, &data); err != nil {
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
