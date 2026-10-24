//go:build gcp_persistence

package resourcemanager

import (
	"bytes"
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresProjectRegistryRoundTrip runs the project registry against the
// PostgreSQL backend so --dsn mode persists created projects and a
// snapshot/restore (the restart path) round-trips them.
func TestPostgresProjectRegistryRoundTrip(t *testing.T) {
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
	res := store.ResourceStore(pg)
	res.Reset(ctx)
	defer res.Reset(ctx)

	s := NewService(res, WithKnownProjects("default-proj", []string{"extra-proj1"}))

	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "pg-project-123", DisplayName: "PG Project"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "pg-project-456", DisplayName: "PG Admin"}); err != nil {
		t.Fatalf("create admin project: %v", err)
	}
	if _, _, err := s.UpdateProject(ctx, "pg-project-456", UpdateProjectInput{
		DisplayName: "PG Admin Updated", Labels: map[string]string{"env": "pg"}, UpdateMask: []string{"display_name", "labels"},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, _, err := s.MoveProject(ctx, "pg-project-456", "folders/777"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if _, _, err := s.DeleteProject(ctx, "extra-proj1"); err != nil {
		t.Fatalf("delete configured project: %v", err)
	}

	// Snapshot/restore (the --dsn restart path).
	var buf bytes.Buffer
	if err := pg.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	res.Reset(ctx)
	if err := pg.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetProject(ctx, "pg-project-123")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.DisplayName != "PG Project" || got.State != StateActive {
		t.Fatalf("restored project = %+v, want displayName PG Project ACTIVE", got)
	}
	admin, err := s.GetProject(ctx, "pg-project-456")
	if err != nil {
		t.Fatalf("get admin project after restore: %v", err)
	}
	if admin.DisplayName != "PG Admin Updated" || admin.Labels["env"] != "pg" || admin.Parent != "folders/777" {
		t.Fatalf("restored admin project = %+v, want updated displayName/labels/parent", admin)
	}
	deleted, err := s.GetProject(ctx, "extra-proj1")
	if err != nil {
		t.Fatalf("get deleted configured project: %v", err)
	}
	if deleted.State != StateDeleteRequested {
		t.Fatalf("restored configured project state = %q, want DELETE_REQUESTED", deleted.State)
	}
	page, _, err := s.ListProjects(ctx, 0, "", true, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	ids := map[string]bool{}
	for _, p := range page {
		ids[p.ProjectID] = true
	}
	for _, want := range []string{"pg-project-123", "pg-project-456", "default-proj", "extra-proj1"} {
		if !ids[want] {
			t.Errorf("restored list missing %s: %v", want, projectIDs(page))
		}
	}
}
