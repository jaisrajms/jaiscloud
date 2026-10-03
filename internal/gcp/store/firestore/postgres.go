package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements FirestoreStore against the jc_firestore_documents
// table. The Firestore migrations (gcpstore.MigrationFS, migration 017) must
// have run before use.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed FirestoreStore.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

const firestoreDocCols = "project, database, collection_id, parent_path, name, fields, create_time, update_time"

// scanDocument scans the firestoreDocCols columns into a Document. project and
// database are scanned but discarded (they are re-derivable from Name).
func scanDocument(scan func(...any) error) (Document, error) {
	var d Document
	var project, database string
	var fields []byte
	err := scan(&project, &database, &d.CollectionID, &d.ParentPath, &d.Name, &fields, &d.CreateTime, &d.UpdateTime)
	if err != nil {
		return Document{}, err
	}
	_ = json.Unmarshal(fields, &d.Fields)
	if d.Fields == nil {
		d.Fields = map[string]*Value{}
	}
	return d, nil
}

func (s *PostgresStore) GetDocument(ctx context.Context, name string) (Document, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+firestoreDocCols+` FROM jc_firestore_documents WHERE name=$1`, name)
	d, err := scanDocument(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Document{}, ErrDocumentNotFound
	}
	return d, err
}

func (s *PostgresStore) CreateDocument(ctx context.Context, doc Document) error {
	normalizeDocument(&doc)
	project, database, _, ok := ParseDocumentName(doc.Name)
	if !ok {
		return ErrInvalidArgument
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_firestore_documents (project, database, collection_id, parent_path, name, fields, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, project, database, doc.CollectionID, doc.ParentPath, doc.Name, fieldsJSON(doc.Fields), doc.CreateTime, doc.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDocumentExists
		}
		return fmt.Errorf("firestore CreateDocument: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateDocument(ctx context.Context, doc Document) error {
	normalizeDocument(&doc)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_firestore_documents
		SET fields=$2, update_time=$3, create_time=$4, collection_id=$5, parent_path=$6
		WHERE name=$1
	`, doc.Name, fieldsJSON(doc.Fields), doc.UpdateTime, doc.CreateTime, doc.CollectionID, doc.ParentPath)
	if err != nil {
		return fmt.Errorf("firestore UpdateDocument: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrDocumentNotFound
	}
	return nil
}

// DeleteDocument is idempotent.
func (s *PostgresStore) DeleteDocument(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_firestore_documents WHERE name=$1`, name)
	return err
}

func (s *PostgresStore) ListDocuments(ctx context.Context, project, database string) ([]Document, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+firestoreDocCols+` FROM jc_firestore_documents
		WHERE project=$1 AND database=$2 ORDER BY name
	`, project, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Document
	for rows.Next() {
		d, err := scanDocument(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// Commit applies a batch of writes atomically inside a serializable transaction
// with row locking, mirroring the memory store's read-set + precondition
// semantics.
func (s *PostgresStore) Commit(ctx context.Context, reads []ReadRef, writes []Write) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Re-validate the read-set under row locks.
	for _, r := range reads {
		var updateTime time.Time
		err := tx.QueryRow(ctx, `SELECT update_time FROM jc_firestore_documents WHERE name=$1 FOR UPDATE`, r.Name).Scan(&updateTime)
		exists := err == nil
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			exists = false
		case err != nil:
			return err
		}
		if r.Exists != exists {
			return ErrAborted
		}
		if exists && !updateTime.Equal(r.UpdateTime) {
			return ErrAborted
		}
	}

	// 2. Validate per-write preconditions (locking each touched row).
	for _, w := range writes {
		var cur *Document
		var project, database, collectionID, parentPath, name string
		var fields []byte
		var createTime, updateTime time.Time
		err := tx.QueryRow(ctx, `SELECT `+firestoreDocCols+` FROM jc_firestore_documents WHERE name=$1 FOR UPDATE`, w.Name).
			Scan(&project, &database, &collectionID, &parentPath, &name, &fields, &createTime, &updateTime)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			cur = nil
		case err != nil:
			return err
		default:
			d := Document{Name: name, CollectionID: collectionID, ParentPath: parentPath, CreateTime: createTime, UpdateTime: updateTime}
			cur = &d
		}
		if err := checkPreconditionDoc(w.Name, cur, w.Precondition); err != nil {
			return err
		}
	}

	// 3. Apply writes.
	for _, w := range writes {
		if w.Document == nil {
			if _, err := tx.Exec(ctx, `DELETE FROM jc_firestore_documents WHERE name=$1`, w.Name); err != nil {
				return err
			}
			continue
		}
		doc := *w.Document
		normalizeDocument(&doc)
		project, database, _, ok := ParseDocumentName(doc.Name)
		if !ok {
			return ErrInvalidArgument
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_firestore_documents (project, database, collection_id, parent_path, name, fields, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (name) DO UPDATE SET fields=$6, create_time=$7, update_time=$8, collection_id=$3, parent_path=$4
		`, project, database, doc.CollectionID, doc.ParentPath, doc.Name, fieldsJSON(doc.Fields), doc.CreateTime, doc.UpdateTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// checkPreconditionDoc validates a precondition against an optional current
// document (nil when the document does not exist). name is carried into the
// DocumentMissingError/DocumentExistsError so the provider can render the
// canonical Firestore message.
func checkPreconditionDoc(name string, cur *Document, pre *Precondition) error {
	if pre == nil {
		return nil
	}
	exists := cur != nil
	if pre.Exists != nil {
		if *pre.Exists {
			if !exists {
				return &DocumentMissingError{Name: name}
			}
		} else if exists {
			return &DocumentExistsError{Name: name}
		}
		return nil
	}
	if pre.UpdateTime != nil {
		if !exists || !cur.UpdateTime.Equal(*pre.UpdateTime) {
			return ErrPreconditionFailed
		}
	}
	return nil
}

// fieldsJSON marshals a document's fields map to JSONB-ready bytes.
func fieldsJSON(fields map[string]*Value) []byte {
	b, err := json.Marshal(fields)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func (s *PostgresStore) Reset(ctx context.Context) {
	if _, err := s.pool.Exec(ctx, `DELETE FROM jc_firestore_documents`); err != nil {
		slog.Warn("firestore reset: exec failed", "err", err)
	}
}
