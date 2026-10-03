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
}
