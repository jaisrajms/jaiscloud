package firestore

import (
	"context"
	"encoding/json"
	"io"
	"sort"
)

// firestoreSnap is the JSON snapshot shape for both memory and postgres
// backends (mirrors the gcs snapshot envelope).
type firestoreSnap struct {
	Documents []Document `json:"documents"`
}

// MemoryStore Snapshot/Restore/IsEmpty.

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.docs) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	docs := make([]Document, 0, len(s.docs))
	for _, d := range s.docs {
		docs = append(docs, d)
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Name < docs[j].Name })
	return json.NewEncoder(w).Encode(firestoreSnap{Documents: docs})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap firestoreSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	docs := make(map[string]Document, len(snap.Documents))
	for _, d := range snap.Documents {
		normalizeDocument(&d)
		docs[d.Name] = d
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs = docs
	return nil
}

// PostgresStore Snapshot/Restore/IsEmpty.

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_firestore_documents`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	rows, err := s.pool.Query(ctx, `SELECT `+firestoreDocCols+` FROM jc_firestore_documents ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var snap firestoreSnap
	for rows.Next() {
		d, err := scanDocument(rows.Scan)
		if err != nil {
			return err
		}
		snap.Documents = append(snap.Documents, d)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap firestoreSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_firestore_documents`); err != nil {
		return err
	}
	for _, d := range snap.Documents {
		normalizeDocument(&d)
		project, database, _, ok := ParseDocumentName(d.Name)
		if !ok {
			return ErrInvalidArgument
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_firestore_documents (project, database, collection_id, parent_path, name, fields, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		`, project, database, d.CollectionID, d.ParentPath, d.Name, fieldsJSON(d.Fields), d.CreateTime, d.UpdateTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
