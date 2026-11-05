//go:build gcp_persistence

package firestore

import (
	"context"
	"errors"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresDeleteDocuments runs the database-delete purge against the
// Postgres backend, so the store primitive the Firestore database cascade relies
// on is not memory-only. The gcp_persistence gate runs with -p 1 because these
// tests share one database.
func TestPostgresDeleteDocuments(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres store test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	if err := s.DeleteDocuments(ctx, "proj", "postgres-delete-db"); err != nil {
		t.Fatalf("DeleteDocuments(cleanup): %v", err)
	}

	docs := []string{
		"projects/proj/databases/postgres-delete-db/documents/c/one",
		"projects/proj/databases/postgres-delete-db/documents/c/two",
		"projects/proj/databases/postgres-delete-db2/documents/c/one",
		"projects/proj/databases/other/documents/c/one",
	}
	for _, name := range docs {
		if err := s.CreateDocument(ctx, Document{Name: name}); err != nil {
			t.Fatalf("CreateDocument(%s): %v", name, err)
		}
	}
	if err := s.DeleteDocuments(ctx, "proj", "postgres-delete-db"); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	for _, name := range docs[:2] {
		if _, err := s.GetDocument(ctx, name); !errors.Is(err, ErrDocumentNotFound) {
			t.Errorf("GetDocument(%s) after purge = %v, want ErrDocumentNotFound", name, err)
		}
	}
	for _, name := range docs[2:] {
		if _, err := s.GetDocument(ctx, name); err != nil {
			t.Errorf("purge removed a sibling document %q: %v", name, err)
		}
	}
	// Clean up the sibling rows so repeated runs stay isolated.
	for _, name := range docs[2:] {
		_ = s.DeleteDocument(ctx, name)
	}
}
