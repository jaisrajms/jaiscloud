package run

import (
	"context"
	"encoding/json"
	"io"
	"sort"
)

// snapshotData is the export/import envelope for both stores.
type snapshotData struct {
	Services   []Service   `json:"services"`
	Revisions  []Revision  `json:"revisions"`
	Operations []Operation `json:"operations"`
}

func sortedServices(m map[string]map[string]Service) []Service {
	var out []Service
	for _, byID := range m {
		for _, s := range byID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return less(out[i].ProjectID, out[j].ProjectID, out[i].Location, out[j].Location, out[i].ID, out[j].ID)
	})
	return out
}

func sortedRevisions(m map[string]map[string]Revision) []Revision {
	var out []Revision
	for _, byID := range m {
		for _, r := range byID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return less(out[i].ProjectID, out[j].ProjectID, out[i].Location, out[j].Location, out[i].ID, out[j].ID)
	})
	return out
}

func sortedOperations(m map[string]map[string]Operation) []Operation {
	var out []Operation
	for _, byID := range m {
		for _, op := range byID {
			out = append(out, op)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return less(out[i].ProjectID, out[j].ProjectID, out[i].Location, out[j].Location, out[i].ID, out[j].ID)
	})
	return out
}

// less reports whether (a1,b1) sorts before (a2,b2) lexicographically.
func less(a1, a2, b1, b2, c1, c2 string) bool {
	if a1 != a2 {
		return a1 < a2
	}
	if b1 != b2 {
		return b1 < b2
	}
	return c1 < c2
}

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.services) == 0 && len(s.revisions) == 0 && len(s.operations) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(snapshotData{
		Services:   sortedServices(s.services),
		Revisions:  sortedRevisions(s.revisions),
		Operations: sortedOperations(s.operations),
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap snapshotData
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	services := make(map[string]map[string]Service)
	revisions := make(map[string]map[string]Revision)
	operations := make(map[string]map[string]Operation)
	for _, svc := range snap.Services {
		key := scope(svc.ProjectID, svc.Location)
		if services[key] == nil {
			services[key] = make(map[string]Service)
		}
		services[key][svc.ID] = svc
	}
	for _, rev := range snap.Revisions {
		key := revScope(rev.ProjectID, rev.Location, rev.Service)
		if revisions[key] == nil {
			revisions[key] = make(map[string]Revision)
		}
		revisions[key][rev.ID] = rev
	}
	for _, op := range snap.Operations {
		key := scope(op.ProjectID, op.Location)
		if operations[key] == nil {
			operations[key] = make(map[string]Operation)
		}
		operations[key][op.ID] = op
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = services
	s.revisions = revisions
	s.operations = operations
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var services, revisions, operations int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_run_services`).Scan(&services); err != nil {
		return false, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_run_revisions`).Scan(&revisions); err != nil {
		return false, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_run_operations`).Scan(&operations); err != nil {
		return false, err
	}
	return services == 0 && revisions == 0 && operations == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	services, err := s.listAllServices(ctx)
	if err != nil {
		return err
	}
	revisions, err := s.listAllRevisions(ctx)
	if err != nil {
		return err
	}
	operations, err := s.listAllOperations(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(snapshotData{Services: services, Revisions: revisions, Operations: operations})
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
	for _, table := range []string{"jc_run_services", "jc_run_revisions", "jc_run_operations"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+table); err != nil {
			return err
		}
	}
	for _, svc := range snap.Services {
		data, err := encodeService(svc)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_run_services (project_id, location, service_id, data)
			VALUES ($1,$2,$3,$4)
		`, svc.ProjectID, svc.Location, svc.ID, data); err != nil {
			return err
		}
	}
	for _, rev := range snap.Revisions {
		data, err := encodeRevision(rev)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_run_revisions (project_id, location, service_id, revision_id, data)
			VALUES ($1,$2,$3,$4,$5)
		`, rev.ProjectID, rev.Location, rev.Service, rev.ID, data); err != nil {
			return err
		}
	}
	for _, op := range snap.Operations {
		data, err := encodeOperation(op)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_run_operations (project_id, location, operation_id, data)
			VALUES ($1,$2,$3,$4)
		`, op.ProjectID, op.Location, op.ID, data); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) listAllServices(ctx context.Context) ([]Service, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_id, data FROM jc_run_services
		ORDER BY project_id, location, service_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Service
	for rows.Next() {
		var project, location, id string
		var data []byte
		if err := rows.Scan(&project, &location, &id, &data); err != nil {
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

func (s *PostgresStore) listAllRevisions(ctx context.Context) ([]Revision, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, service_id, revision_id, data FROM jc_run_revisions
		ORDER BY project_id, location, service_id, revision_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Revision
	for rows.Next() {
		var project, location, service, id string
		var data []byte
		if err := rows.Scan(&project, &location, &service, &id, &data); err != nil {
			return nil, err
		}
		rev, err := decodeRevision(data)
		if err != nil {
			return nil, err
		}
		rev.ProjectID = project
		rev.Location = location
		rev.Service = service
		rev.ID = id
		result = append(result, rev)
	}
	return result, rows.Err()
}

func (s *PostgresStore) listAllOperations(ctx context.Context) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, operation_id, data FROM jc_run_operations
		ORDER BY project_id, location, operation_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		var project, location, id string
		var data []byte
		if err := rows.Scan(&project, &location, &id, &data); err != nil {
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
