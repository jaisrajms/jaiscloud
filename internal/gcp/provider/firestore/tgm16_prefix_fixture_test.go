package firestore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/store"
)

// tgm16PrefixState is the persisted state captured from a pre-fix build (see
// testdata/tgm16_prefix_state.json). It is the exact shape the TGM16 report's
// "409 backup_schedule already exists" came from: the database delete
// tombstoned the database record but left backupSchedules/1 (and the rest of
// the database's children) behind, so re-creating the same id answered 409 on
// the first schedule create.
type tgm16PrefixState struct {
	CapturedFrom string `json:"capturedFrom"`
	Entries      []struct {
		Type string          `json:"type"`
		ID   string          `json:"id"`
		Data json.RawMessage `json:"data"`
	} `json:"entries"`
	Documents []string `json:"documents"`
}

const (
	tgm16FixtureDatabase = "lc-fixture"
	tgm16FixtureProject  = "proj"
)

// TestRecreateOverPreFixStateFixture pins the TGM16 lifecycle edge against the
// pre-fix persisted state instead of a hand-built approximation: seed the
// captured rows, re-create the database over its tombstone, and require the
// first backup-schedule create to succeed (the pre-fix code answers
// ALREADY_EXISTS because schedule id 1 is still occupied).
//
// The fixture is transport-neutral: it feeds the resource store the pre-fix
// build wrote, which both transports read.
func TestRecreateOverPreFixStateFixture(t *testing.T) {
	ctx := context.Background()
	state := loadTGM16PrefixState(t)

	resources := store.NewMemoryResourceStore()
	docs := firestorestore.NewMemoryStore()
	for _, e := range state.Entries {
		if err := resources.Upsert(ctx, tgm16FixtureProject, store.GlobalRegion, store.ResourceEntry{
			Type: e.Type,
			ID:   e.ID,
			Data: e.Data,
		}); err != nil {
			t.Fatalf("seed %s/%s: %v", e.Type, e.ID, err)
		}
	}
	for _, name := range state.Documents {
		if err := docs.CreateDocument(ctx, firestorestore.Document{
			Name:   name,
			Fields: map[string]*firestorestore.Value{},
		}); err != nil {
			t.Fatalf("seed document %s: %v", name, err)
		}
	}

	s := newService(docs, resources)

	// The fixture is a deleted database: it is not readable, but its id is
	// available for reuse (the pre-fix tombstone).
	if _, err := s.GetDatabase(ctx, tgm16FixtureProject, tgm16FixtureDatabase); err == nil {
		t.Fatal("fixture database is readable, want the pre-fix tombstone")
	}

	// A legal recreate must not answer ALREADY_EXISTS.
	if _, _, err := s.CreateDatabaseDef(ctx, tgm16FixtureProject, tgm16FixtureDatabase, databaseDef{LocationID: "nam5"}); err != nil {
		t.Fatalf("re-create over the pre-fix tombstone: %v", err)
	}

	// Re-creating purges the leftovers, so the first schedule create is free to
	// take id 1. Pre-fix this is the reported 409.
	if _, err := s.CreateBackupScheduleDef(ctx, tgm16FixtureProject, tgm16FixtureDatabase, "", backupScheduleDef{
		Retention:  24 * time.Hour,
		Recurrence: "DAILY",
	}); err != nil {
		t.Fatalf("first backup schedule on the re-created database = %v, want success (the TGM16 stale-schedule 409)", err)
	}

	// Every database-owned collection, and the documents, are gone.
	for _, typ := range []string{rtIndex, rtField, rtUserCreds, rtChangeStream} {
		rows, err := resources.List(ctx, tgm16FixtureProject, store.GlobalRegion, typ, "")
		if err != nil {
			t.Fatalf("list %s: %v", typ, err)
		}
		if len(rows) != 0 {
			t.Errorf("%s rows after recreate = %d, want the pre-fix leftovers purged", typ, len(rows))
		}
	}
	// The pre-fix schedule incarnation is gone: the only schedule left is the
	// one this test just created.
	schedules, err := resources.List(ctx, tgm16FixtureProject, store.GlobalRegion, rtBackupSchedule, "")
	if err != nil {
		t.Fatalf("list backup schedules: %v", err)
	}
	if len(schedules) != 1 {
		t.Fatalf("backup schedules after recreate = %d, want exactly the new one", len(schedules))
	}
	var schedule backupScheduleDef
	if err := json.Unmarshal(schedules[0].Data, &schedule); err != nil {
		t.Fatalf("parse schedule: %v", err)
	}
	if schedule.Uid == "pre-fix-schedule-incarnation" {
		t.Errorf("re-created database inherited the pre-fix backup schedule incarnation")
	}
	remaining, err := docs.ListDocuments(ctx, tgm16FixtureProject, tgm16FixtureDatabase)
	if err != nil {
		t.Fatalf("list documents: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("documents after recreate = %d, want the pre-fix document purged", len(remaining))
	}
}

func loadTGM16PrefixState(t *testing.T) tgm16PrefixState {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "tgm16_prefix_state.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var state tgm16PrefixState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if state.CapturedFrom == "" || len(state.Entries) == 0 {
		t.Fatalf("fixture is empty or does not record its pre-fix origin")
	}
	return state
}
