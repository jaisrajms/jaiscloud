//go:build gcp_persistence

package functions

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresOperationCRUD runs the operation store contract against the
// Postgres backend so --dsn mode persists pollable function operations (and
// survives a restart via snapshot/restore).
func TestPostgresOperationCRUD(t *testing.T) {
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
	s.Reset(ctx)

	f := &Function{ID: "a", Runtime: "nodejs20", EntryPoint: "hello"}
	op := Operation{ID: "op1", Location: "us-central1", Done: true, Verb: "create",
		Target: "projects/proj/locations/us-central1/functions/a", Function: f, CreateTime: time.Now(), EndTime: time.Now()}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}
	got, err := s.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if got.Verb != "create" || got.Function == nil || got.Function.Runtime != "nodejs20" {
		t.Fatalf("unexpected operation: %+v", got)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "missing"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}

	// A delete operation carries no Function snapshot; it must not read back as
	// a non-nil zero Function (which would render a Function response instead of
	// google.protobuf.Empty).
	if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op2", Done: true, Verb: "delete"}); err != nil {
		t.Fatalf("create delete op: %v", err)
	}
	del, err := s.GetOperation(ctx, "proj", "us-central1", "op2")
	if err != nil {
		t.Fatalf("get delete op: %v", err)
	}
	if del.Function != nil {
		t.Fatalf("delete operation Function = %+v, want nil", del.Function)
	}

	all, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list ops: %v", err)
	}
	if len(all) != 2 || all[0].ID != "op1" || all[1].ID != "op2" {
		t.Fatalf("unexpected op list: %+v", all)
	}

	// The v1 top-level operations collection spans every location in a project.
	if err := s.CreateOperation(ctx, "proj", "europe-west1", Operation{ID: "op3", Done: true, Verb: "create"}); err != nil {
		t.Fatalf("create other-location op: %v", err)
	}
	everywhere, err := s.ListOperationsAllLocations(ctx, "proj")
	if err != nil {
		t.Fatalf("list ops all locations: %v", err)
	}
	if len(everywhere) != 3 || everywhere[0].ID != "op1" || everywhere[2].ID != "op3" {
		t.Fatalf("unexpected all-locations op list: %+v", everywhere)
	}
	if err := s.DeleteOperation(ctx, "proj", "europe-west1", "op3"); err != nil {
		t.Fatalf("delete other-location op: %v", err)
	}

	// Snapshot/restore round-trip (the --dsn restart path).
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got, err := s.GetOperation(ctx, "proj", "us-central1", "op1"); err != nil || got.Function == nil {
		t.Fatalf("operation not restored: %+v err=%v", got, err)
	}

	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != nil {
		t.Fatalf("delete op: %v", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation on second delete, got %v", err)
	}

	s.Reset(ctx)
}

// TestPostgresFunctionInstanceConfig round-trips the FD6 v2 instance/concurrency
// columns through Postgres (insert, get, update, snapshot/restore) so --dsn mode
// persists them like the memory backend.
func TestPostgresFunctionInstanceConfig(t *testing.T) {
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
	s.Reset(ctx)

	f := Function{ID: "cfg", Runtime: "nodejs22", Status: "ACTIVE",
		MinInstanceCount: 2, MaxInstanceCount: 10, MaxInstanceRequestConcurrency: 80, AvailableCPU: "0.5"}
	if err := s.CreateFunction(ctx, "proj", "us-central1", "cfg", f); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetFunction(ctx, "proj", "us-central1", "cfg")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.MinInstanceCount != 2 || got.MaxInstanceCount != 10 || got.MaxInstanceRequestConcurrency != 80 || got.AvailableCPU != "0.5" {
		t.Fatalf("get config = %+v", got)
	}

	got.AvailableCPU = "1"
	got.MaxInstanceCount = 20
	if err := s.UpdateFunction(ctx, "proj", "us-central1", "cfg", got); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Snapshot/restore (the --dsn restart path) preserves the columns.
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err = s.GetFunction(ctx, "proj", "us-central1", "cfg")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.MinInstanceCount != 2 || got.MaxInstanceCount != 20 || got.MaxInstanceRequestConcurrency != 80 || got.AvailableCPU != "1" {
		t.Fatalf("restored config = %+v", got)
	}
	s.Reset(ctx)
}

// TestPostgresFunctionV1ModuleFields round-trips the TGM3 v1 columns
// (ingress/maxInstances/vpc/secret/docker/build/HTTPS security level) through
// Postgres (insert, get, update, snapshot/restore), so --dsn mode persists them
// exactly like the memory backend.
func TestPostgresFunctionV1ModuleFields(t *testing.T) {
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
	s.Reset(ctx)

	if err := s.CreateFunction(ctx, "proj", "us-central1", "v1", v1FieldsFunction()); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetFunction(ctx, "proj", "us-central1", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertV1Fields(t, got)

	// Clear-back update: every optional field is empty, ingress is ALLOW_ALL.
	got.IngressSettings = "ALLOW_ALL"
	got.MaxInstances = 0
	got.VPCConnector = ""
	got.VPCConnectorEgressSettings = ""
	got.SecretEnvironmentVariables = nil
	got.SecretVolumes = nil
	got.DockerRegistry = ""
	got.DockerRepository = ""
	got.BuildWorkerPool = ""
	got.HttpsTriggerSecurityLevel = "SECURE_ALWAYS"
	if err := s.UpdateFunction(ctx, "proj", "us-central1", "v1", got); err != nil {
		t.Fatalf("update: %v", err)
	}
	cleared, err := s.GetFunction(ctx, "proj", "us-central1", "v1")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if cleared.IngressSettings != "ALLOW_ALL" || cleared.MaxInstances != 0 || cleared.VPCConnector != "" ||
		len(cleared.SecretEnvironmentVariables) != 0 || len(cleared.SecretVolumes) != 0 ||
		cleared.DockerRegistry != "" || cleared.BuildWorkerPool != "" ||
		cleared.HttpsTriggerSecurityLevel != "SECURE_ALWAYS" {
		t.Fatalf("cleared function = %+v", cleared)
	}

	// Restore from the pre-clear snapshot and check the full field set again.
	if err := s.CreateFunction(ctx, "proj", "us-central1", "v2", v1FieldsFunction()); err != nil {
		t.Fatalf("create v2: %v", err)
	}
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := s.GetFunction(ctx, "proj", "us-central1", "v2")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	assertV1Fields(t, restored)
	s.Reset(ctx)
}
