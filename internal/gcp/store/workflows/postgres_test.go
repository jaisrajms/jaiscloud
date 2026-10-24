//go:build gcp_persistence

package workflows

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStoreSnapshotVerbatim verifies that source_contents, argument and
// result survive a Postgres Snapshot/Restore round trip byte-for-byte (the
// hard verbatim-preservation requirement).
func TestPostgresStoreSnapshotVerbatim(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
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
	s.Reset(ctx)

	const source = "main:\n  params: [a]\n  steps:\n    - r:\n        return: ${a + 1}\n"
	w := Workflow{ID: "a", SourceContents: source, State: "ACTIVE", Labels: map[string]string{"k": "v"}}
	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "a", w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	start := time.Now().UTC().Truncate(time.Microsecond)
	e := Execution{ID: "e1", State: "SUCCEEDED", Argument: `{"a": 41}`, Result: `42`,
		StartTime: start, EndTime: start, Duration: "0.1s", WorkflowRevisionID: "000001-a4d",
		Error: &ExecutionError{Payload: `{"message":"x"}`}}
	if err := s.CreateExecution(ctx, "proj", "us-central1", "a", "e1", e); err != nil {
		t.Fatalf("create execution: %v", err)
	}
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op1", Done: true, Response: `{}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetWorkflow(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if got.SourceContents != source {
		t.Fatalf("source_contents not verbatim: %q", got.SourceContents)
	}
	if got.Labels["k"] != "v" {
		t.Fatalf("labels lost: %+v", got.Labels)
	}

	gex, err := s.GetExecution(ctx, "proj", "us-central1", "a", "e1")
	if err != nil {
		t.Fatalf("get execution: %v", err)
	}
	if gex.Argument != `{"a": 41}` || gex.Result != "42" {
		t.Fatalf("argument/result not verbatim: %+v", gex)
	}
	if gex.Error == nil || gex.Error.Payload != `{"message":"x"}` {
		t.Fatalf("error lost: %+v", gex.Error)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op1"); err != nil {
		t.Fatalf("get operation: %v", err)
	}

	// ListOperations is ID-ordered and location-scoped; DeleteOperation removes
	// a persisted operation and reports an unknown id.
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op2", Done: true, Response: `{}`})
	ops, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 2 || ops[0].ID != "op1" || ops[1].ID != "op2" {
		t.Fatalf("list operations = %+v, want op1,op2", ops)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != nil {
		t.Fatalf("delete operation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("get deleted operation = %v, want ErrNoSuchOperation", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("re-delete = %v, want ErrNoSuchOperation", err)
	}
}

// TestPostgresStoreRevisions verifies revision history persists, is ordered
// newest first, survives a snapshot/restore round trip, and is cascaded on
// workflow delete.
func TestPostgresStoreRevisions(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres revisions test")
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
	s.Reset(ctx)

	w := Workflow{ID: "a", State: "ACTIVE", SourceContents: "s1", RevisionID: "000001-aaa"}
	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "a", w); err != nil {
		t.Fatalf("create: %v", err)
	}
	w.UpdateTime = time.Now().UTC().Truncate(time.Microsecond)
	w.RevisionID = "000002-bbb"
	w.SourceContents = "s2"
	if _, err := s.UpdateWorkflowAtomic(ctx, "proj", "us-central1", "a", func(Workflow) (Workflow, error) {
		return w, nil
	}); err != nil {
		t.Fatalf("atomic update: %v", err)
	}

	revs, err := s.ListRevisions(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	if len(revs) != 2 || revs[0].RevisionID != "000002-bbb" || revs[1].RevisionID != "000001-aaa" {
		t.Fatalf("revisions = %+v", revs)
	}
	if got, err := s.GetRevision(ctx, "proj", "us-central1", "a", "000001-aaa"); err != nil || got.SourceContents != "s1" {
		t.Fatalf("get revision = %+v, %v", got, err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	revs, err = s.ListRevisions(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("list revisions after restore: %v", err)
	}
	if len(revs) != 2 || revs[0].RevisionID != "000002-bbb" || revs[1].SourceContents != "s1" {
		t.Fatalf("revisions after restore = %+v", revs)
	}

	if err := s.DeleteWorkflow(ctx, "proj", "us-central1", "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if revs, _ := s.ListRevisions(ctx, "proj", "us-central1", "a"); len(revs) != 0 {
		t.Fatalf("revisions survived delete: %+v", revs)
	}
}
