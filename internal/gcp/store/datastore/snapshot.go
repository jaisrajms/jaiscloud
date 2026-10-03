package datastore

import (
	"context"
	"encoding/json"
	"io"
	"sort"
)

// snapRow is one (project, entity) pair in the snapshot envelope.
type snapRow struct {
	Project string `json:"project"`
	Entity  Entity `json:"entity"`
}

// datastoreSnap is the JSON snapshot shape shared by both backends. The
// allocator counter is snapshotted so that IDs stay monotonic across restore
// (an AllocateIds result that was never stored as an entity still advances the
// counter).
type datastoreSnap struct {
	Entities  []snapRow        `json:"entities"`
	Allocator map[string]int64 `json:"allocator,omitempty"`
}

// MemoryStore Snapshot/Restore/IsEmpty.

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entities) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := datastoreSnap{Allocator: s.allocator}
	projects := make([]string, 0, len(s.entities))
	for p := range s.entities {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		keys := make([]string, 0, len(s.entities[p]))
		for k := range s.entities[p] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			snap.Entities = append(snap.Entities, snapRow{Project: p, Entity: s.entities[p][k]})
		}
	}
	return json.NewEncoder(w).Encode(snap)
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap datastoreSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	entities := make(map[string]map[string]Entity)
	for _, row := range snap.Entities {
		if entities[row.Project] == nil {
			entities[row.Project] = make(map[string]Entity)
		}
		entities[row.Project][row.Entity.Key] = row.Entity
	}
	allocator := snap.Allocator
	if allocator == nil {
		allocator = make(map[string]int64)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entities = entities
	s.allocator = allocator
	return nil
}

// PostgresStore Snapshot/Restore/IsEmpty.

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_datastore_entities`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	rows, err := s.pool.Query(ctx, `
		SELECT `+entityCols+` FROM jc_datastore_entities ORDER BY project, kind, name_or_id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()
	snap := datastoreSnap{Allocator: make(map[string]int64)}
	for rows.Next() {
		var row snapRow
		var nameOrID string
		var props []byte
		if err := rows.Scan(&row.Project, &row.Entity.Kind, &nameOrID, &props, &row.Entity.Version, &row.Entity.UpdateTime); err != nil {
			return err
		}
		if len(props) > 0 {
			_ = json.Unmarshal(props, &row.Entity.Properties)
		}
		if row.Entity.Properties == nil {
			row.Entity.Properties = map[string]Value{}
		}
		// name_or_id is the full canonical key.
		row.Entity.Key = nameOrID
		snap.Entities = append(snap.Entities, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	allocRows, err := s.pool.Query(ctx, `SELECT project_id, next_id FROM jc_datastore_id_allocator`)
	if err != nil {
		return err
	}
	defer allocRows.Close()
	for allocRows.Next() {
		var p string
		var n int64
		if err := allocRows.Scan(&p, &n); err != nil {
			return err
		}
		snap.Allocator[p] = n
	}
	if err := allocRows.Err(); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap datastoreSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_datastore_entities`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_datastore_id_allocator`); err != nil {
		return err
	}
	for _, row := range snap.Entities {
		kind, nameOrID, ok := keyCols(row.Entity.Key)
		if !ok {
			return ErrInvalidKey
		}
		version := row.Entity.Version
		if version == 0 {
			version = 1
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_datastore_entities (project, kind, name_or_id, properties, version, update_time)
			VALUES ($1,$2,$3,$4,$5,$6)
		`, row.Project, kind, nameOrID, propertiesJSON(row.Entity.Properties), version, row.Entity.UpdateTime); err != nil {
			return err
		}
	}
	for p, n := range snap.Allocator {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_datastore_id_allocator (project_id, next_id) VALUES ($1,$2)
		`, p, n); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
