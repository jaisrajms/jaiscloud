package metastore

import (
	"bytes"
	"context"
	"testing"
)

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	if _, err := s.GetService(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchService {
		t.Fatalf("expected ErrNoSuchService, got %v", err)
	}

	svc := Service{
		Name:         "my-service",
		Config:       []byte(`{"network":"default","hiveMetastoreConfig":{"version":"3.1.2"}}`),
		Labels:       map[string]string{"env": "dev"},
		State:        "ACTIVE",
		StateHistory: []ServiceState{{State: "CREATING"}},
	}
	if err := s.CreateService(ctx, "proj", "us-central1", svc); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := s.CreateService(ctx, "proj", "us-central1", svc); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetService(ctx, "proj", "us-central1", "my-service")
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if got.State != "ACTIVE" || got.Labels["env"] != "dev" {
		t.Fatalf("service fields lost: %+v", got)
	}
	if string(got.Config) != `{"network":"default","hiveMetastoreConfig":{"version":"3.1.2"}}` {
		t.Fatalf("config not verbatim: %s", got.Config)
	}
	if len(got.StateHistory) != 1 || got.StateHistory[0].State != "CREATING" {
		t.Fatalf("stateHistory lost: %+v", got.StateHistory)
	}

	got.State = "ACTIVE"
	got.UpdateTime = got.CreateTime
	if err := s.UpdateService(ctx, "proj", "us-central1", got); err != nil {
		t.Fatalf("update service: %v", err)
	}
	list, err := s.ListServices(ctx, "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list services: %v %d", err, len(list))
	}

	// Backups
	if _, err := s.GetBackup(ctx, "proj", "us-central1", "my-service", "nope"); err != ErrNoSuchBackup {
		t.Fatalf("expected ErrNoSuchBackup, got %v", err)
	}
	b := Backup{Name: "bk-1", Description: "nightly", State: "ACTIVE"}
	if err := s.CreateBackup(ctx, "proj", "us-central1", "my-service", b); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	if err := s.CreateBackup(ctx, "proj", "us-central1", "my-service", b); err != ErrAlreadyExists {
		t.Fatalf("expected backup ErrAlreadyExists, got %v", err)
	}
	gotBackup, err := s.GetBackup(ctx, "proj", "us-central1", "my-service", "bk-1")
	if err != nil || gotBackup.Description != "nightly" {
		t.Fatalf("get backup: %v %+v", err, gotBackup)
	}
	blist, err := s.ListBackups(ctx, "proj", "us-central1", "my-service")
	if err != nil || len(blist) != 1 {
		t.Fatalf("list backups: %v %d", err, len(blist))
	}
	if err := s.DeleteBackup(ctx, "proj", "us-central1", "my-service", "bk-1"); err != nil {
		t.Fatalf("delete backup: %v", err)
	}
	if _, err := s.GetBackup(ctx, "proj", "us-central1", "my-service", "bk-1"); err != ErrNoSuchBackup {
		t.Fatalf("expected ErrNoSuchBackup after delete, got %v", err)
	}

	// Metadata imports
	if _, err := s.GetMetadataImport(ctx, "proj", "us-central1", "my-service", "nope"); err != ErrNoSuchMetadataImport {
		t.Fatalf("expected ErrNoSuchMetadataImport, got %v", err)
	}
	mi := MetadataImport{Name: "imp-1", Description: "initial", State: "SUCCEEDED"}
	if err := s.CreateMetadataImport(ctx, "proj", "us-central1", "my-service", mi); err != nil {
		t.Fatalf("create import: %v", err)
	}
	gotImport, err := s.GetMetadataImport(ctx, "proj", "us-central1", "my-service", "imp-1")
	if err != nil || gotImport.State != "SUCCEEDED" {
		t.Fatalf("get import: %v %+v", err, gotImport)
	}
	gotImport.Description = "updated"
	if err := s.UpdateMetadataImport(ctx, "proj", "us-central1", "my-service", gotImport); err != nil {
		t.Fatalf("update import: %v", err)
	}
	ilist, err := s.ListMetadataImports(ctx, "proj", "us-central1", "my-service")
	if err != nil || len(ilist) != 1 || ilist[0].Description != "updated" {
		t.Fatalf("list imports: %v %+v", err, ilist)
	}

	// Operations
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}
	op := Operation{ID: "op-1", Done: true, Metadata: `{"@type":"x"}`, Response: `{}`}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, "proj", "us-central1", "op-1")
	if err != nil || gotOp.Metadata != `{"@type":"x"}` {
		t.Fatalf("get op: %v %+v", err, gotOp)
	}

	// Deleting the service cascades its backups and imports.
	if err := s.DeleteService(ctx, "proj", "us-central1", "my-service"); err != nil {
		t.Fatalf("delete service: %v", err)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "my-service"); err != ErrNoSuchService {
		t.Fatalf("expected ErrNoSuchService after delete, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "r", Service{Name: "s"})
	s.Reset(ctx)
	empty, _ := s.IsEmpty(ctx)
	if !empty {
		t.Fatal("expected empty after reset")
	}
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "r", Service{Name: "s", Labels: map[string]string{"k": "v"}, State: "ACTIVE", Config: []byte(`{"network":"n"}`)})
	_ = s.CreateBackup(ctx, "p", "r", "s", Backup{Name: "b", Description: "d", State: "ACTIVE"})
	_ = s.CreateMetadataImport(ctx, "p", "r", "s", MetadataImport{Name: "m", State: "SUCCEEDED"})
	_ = s.CreateOperation(ctx, "p", "r", Operation{ID: "op", Metadata: `{"@type":"m"}`, Response: `{"x":1}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := s2.GetService(ctx, "p", "r", "s")
	if err != nil || got.State != "ACTIVE" || string(got.Config) != `{"network":"n"}` {
		t.Fatalf("service lost after restore: %v %+v", err, got)
	}
	gotB, err := s2.GetBackup(ctx, "p", "r", "s", "b")
	if err != nil || gotB.Description != "d" {
		t.Fatalf("backup lost after restore: %v %+v", err, gotB)
	}
	gotM, err := s2.GetMetadataImport(ctx, "p", "r", "s", "m")
	if err != nil || gotM.State != "SUCCEEDED" {
		t.Fatalf("import lost after restore: %v %+v", err, gotM)
	}
	gotOp, err := s2.GetOperation(ctx, "p", "r", "op")
	if err != nil || gotOp.Metadata != `{"@type":"m"}` {
		t.Fatalf("op lost after restore: %v %+v", err, gotOp)
	}
}
