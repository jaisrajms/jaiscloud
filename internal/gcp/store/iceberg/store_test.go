package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// runStoreTests exercises a Store against the shared test matrix. Backend
// tests (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	// --- Namespace CRUD ---
	if _, err := s.GetNamespace(ctx, "db"); err != ErrNamespaceNotFound {
		t.Fatalf("expected ErrNamespaceNotFound, got %v", err)
	}
	if err := s.CreateNamespace(ctx, "db", map[string]string{"owner": "me"}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := s.CreateNamespace(ctx, "db", nil); err != ErrNamespaceExists {
		t.Fatalf("expected ErrNamespaceExists, got %v", err)
	}
	props, err := s.GetNamespace(ctx, "db")
	if err != nil || props["owner"] != "me" {
		t.Fatalf("get namespace: %v %v", err, props)
	}
	if ok, _ := s.NamespaceExists(ctx, "db"); !ok {
		t.Fatal("namespace should exist")
	}
	if ok, _ := s.NamespaceExists(ctx, "nope"); ok {
		t.Fatal("missing namespace reported as existing")
	}

	// Update properties (add + remove).
	upd, err := s.UpdateNamespaceProperties(ctx, "db", []string{"owner"}, map[string]string{"env": "prod"})
	if err != nil || upd.After["owner"] != "" || upd.After["env"] != "prod" {
		t.Fatalf("update properties: %v %v", err, upd.After)
	}
	if _, ok := upd.Before["owner"]; !ok {
		t.Fatalf("Before should carry the removed key owner: %v", upd.Before)
	}
	if _, err := s.UpdateNamespaceProperties(ctx, "nope", nil, nil); err != ErrNamespaceNotFound {
		t.Fatalf("expected ErrNamespaceNotFound on update, got %v", err)
	}

	list, err := s.ListNamespaces(ctx)
	if err != nil || len(list) != 1 || list[0].Namespace != "db" {
		t.Fatalf("list namespaces: %v %+v", err, list)
	}

	// --- Table CRUD ---
	if _, err := s.GetTable(ctx, "db", "t1"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	t0 := Table{
		Namespace:        "db",
		Name:             "t1",
		Metadata:         json.RawMessage(`{"format-version":2,"table-uuid":"u-1","location":"s3://w/db/t1"}`),
		MetadataLocation: "s3://w/db/t1/metadata/00000-u-1.metadata.json",
		UUID:             "u-1",
		Version:          0,
	}
	if err := s.CreateTable(ctx, "db", "t1", t0); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.CreateTable(ctx, "db", "t1", t0); err != ErrTableExists {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}
	if err := s.CreateTable(ctx, "missing", "t", t0); err != ErrNamespaceNotFound {
		t.Fatalf("expected ErrNamespaceNotFound creating table in missing namespace, got %v", err)
	}
	got, err := s.GetTable(ctx, "db", "t1")
	if err != nil || got.UUID != "u-1" || got.Version != 0 {
		t.Fatalf("get table: %v %+v", err, got)
	}
	tlist, err := s.ListTables(ctx, "db")
	if err != nil || len(tlist) != 1 || tlist[0].Name != "t1" {
		t.Fatalf("list tables: %v %+v", err, tlist)
	}

	// --- Commit atomicity: sequential commits both land ---
	for i := 1; i <= 2; i++ {
		committed, err := s.CommitTable(ctx, "db", "t1", func(cur Table) (Table, error) {
			var meta map[string]any
			if err := json.Unmarshal(cur.Metadata, &meta); err != nil {
				return Table{}, err
			}
			meta["last-updated-ms"] = i
			b, _ := json.Marshal(meta)
			return Table{
				Namespace:        cur.Namespace,
				Name:             cur.Name,
				Metadata:         b,
				MetadataLocation: cur.MetadataLocation,
				UUID:             cur.UUID,
				Version:          cur.Version + 1,
			}, nil
		})
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		if committed.Version != i {
			t.Fatalf("commit %d: version = %d, want %d", i, committed.Version, i)
		}
	}
	final, _ := s.GetTable(ctx, "db", "t1")
	if final.Version != 2 {
		t.Fatalf("final version = %d, want 2", final.Version)
	}
	var finalMeta map[string]any
	_ = json.Unmarshal(final.Metadata, &finalMeta)
	if finalMeta["last-updated-ms"] != float64(2) {
		t.Fatalf("final last-updated-ms = %v, want 2", finalMeta["last-updated-ms"])
	}

	// --- Commit rejected leaves state unchanged ---
	if _, err := s.CommitTable(ctx, "db", "t1", func(cur Table) (Table, error) {
		return Table{}, ErrTableNotFound // simulate a rejected requirement
	}); err == nil {
		t.Fatal("expected commit rejection to propagate")
	}
	afterReject, _ := s.GetTable(ctx, "db", "t1")
	if afterReject.Version != 2 {
		t.Fatalf("rejected commit mutated state: version = %d, want 2", afterReject.Version)
	}

	if _, err := s.CommitTable(ctx, "db", "missing", func(cur Table) (Table, error) { return cur, nil }); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound on commit, got %v", err)
	}

	// --- Concurrent commits: no lost update ---
	// N goroutines each commit a disjoint set-properties update to the same
	// table; the final metadata must reflect all N updates and land exactly N
	// versions later.
	const concurrency = 20
	conc := Table{
		Namespace:        "db",
		Name:             "t-conc",
		Metadata:         json.RawMessage(`{"format-version":2,"table-uuid":"u-conc","location":"s3://w/db/t-conc","properties":{}}`),
		MetadataLocation: "s3://w/db/t-conc/metadata/00000-u-conc.metadata.json",
		UUID:             "u-conc",
		Version:          0,
	}
	if err := s.CreateTable(ctx, "db", "t-conc", conc); err != nil {
		t.Fatalf("create concurrent table: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			_, err := s.CommitTable(ctx, "db", "t-conc", func(cur Table) (Table, error) {
				var meta map[string]any
				if err := json.Unmarshal(cur.Metadata, &meta); err != nil {
					return Table{}, err
				}
				props, _ := meta["properties"].(map[string]any)
				if props == nil {
					props = map[string]any{}
				}
				props[key] = "v"
				meta["properties"] = props
				b, _ := json.Marshal(meta)
				return Table{
					Namespace:        cur.Namespace,
					Name:             cur.Name,
					Metadata:         b,
					MetadataLocation: cur.MetadataLocation,
					UUID:             cur.UUID,
					Version:          cur.Version + 1,
				}, nil
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent commit: %v", err)
	}
	finalConc, err := s.GetTable(ctx, "db", "t-conc")
	if err != nil {
		t.Fatalf("get concurrent table: %v", err)
	}
	if finalConc.Version != concurrency {
		t.Fatalf("concurrent table version = %d, want %d", finalConc.Version, concurrency)
	}
	var concMeta map[string]any
	if err := json.Unmarshal(finalConc.Metadata, &concMeta); err != nil {
		t.Fatalf("unmarshal concurrent metadata: %v", err)
	}
	concProps := concMeta["properties"].(map[string]any)
	if len(concProps) != concurrency {
		t.Fatalf("concurrent properties = %d, want %d (lost update)", len(concProps), concurrency)
	}
	for i := 0; i < concurrency; i++ {
		if concProps[fmt.Sprintf("k%d", i)] != "v" {
			t.Fatalf("missing concurrent key k%d", i)
		}
	}

	// --- Large snapshot-id round-trips exactly through the commit path ---
	big := Table{
		Namespace:        "db",
		Name:             "t-big",
		Metadata:         json.RawMessage(`{"format-version":2,"table-uuid":"u-big","location":"s3://w/db/t-big","current-snapshot-id":-1,"snapshots":[]}`),
		MetadataLocation: "s3://w/db/t-big/metadata/00000-u-big.metadata.json",
		UUID:             "u-big",
		Version:          0,
	}
	if err := s.CreateTable(ctx, "db", "t-big", big); err != nil {
		t.Fatalf("create big-id table: %v", err)
	}
	const bigSnapshotID = int64(1000000000000000001) // > 2^53
	if _, err := s.CommitTable(ctx, "db", "t-big", func(cur Table) (Table, error) {
		var meta map[string]any
		if err := json.Unmarshal(cur.Metadata, &meta); err != nil {
			return Table{}, err
		}
		meta["current-snapshot-id"] = bigSnapshotID
		b, _ := json.Marshal(meta)
		return Table{
			Namespace:        cur.Namespace,
			Name:             cur.Name,
			Metadata:         b,
			MetadataLocation: cur.MetadataLocation,
			UUID:             cur.UUID,
			Version:          cur.Version + 1,
		}, nil
	}); err != nil {
		t.Fatalf("big-id commit: %v", err)
	}
	bigGot, err := s.GetTable(ctx, "db", "t-big")
	if err != nil {
		t.Fatalf("get big-id table: %v", err)
	}
	var bigMeta struct {
		CurrentSnapshotID int64 `json:"current-snapshot-id"`
	}
	if err := json.Unmarshal(bigGot.Metadata, &bigMeta); err != nil {
		t.Fatalf("unmarshal big-id metadata: %v", err)
	}
	if bigMeta.CurrentSnapshotID != bigSnapshotID {
		t.Fatalf("current-snapshot-id = %d, want %d", bigMeta.CurrentSnapshotID, bigSnapshotID)
	}

	// --- Rename ---
	if err := s.CreateNamespace(ctx, "db2", nil); err != nil {
		t.Fatalf("create db2 namespace: %v", err)
	}
	renamed, err := s.RenameTable(ctx, "db", "t1", "db2", "t2")
	if err != nil || renamed.Namespace != "db2" || renamed.Name != "t2" {
		t.Fatalf("rename: %v %+v", err, renamed)
	}
	if _, err := s.GetTable(ctx, "db", "t1"); err != ErrTableNotFound {
		t.Fatalf("source should be gone after rename, got %v", err)
	}
	if _, err := s.RenameTable(ctx, "db2", "missing", "db2", "x"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound renaming missing source, got %v", err)
	}
	if _, err := s.RenameTable(ctx, "db2", "t2", "missing", "x"); err != ErrNamespaceNotFound {
		t.Fatalf("expected ErrNamespaceNotFound renaming into missing namespace, got %v", err)
	}
	// Destination exists -> ErrTableExists.
	if err := s.CreateTable(ctx, "db2", "t3", t0); err != nil {
		t.Fatalf("create t3: %v", err)
	}
	if _, err := s.RenameTable(ctx, "db2", "t2", "db2", "t3"); err != ErrTableExists {
		t.Fatalf("expected ErrTableExists renaming onto existing table, got %v", err)
	}

	// --- Drop table + drop namespace (non-empty refusal) ---
	if err := s.DropTable(ctx, "db2", "t3"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	// db is empty now (its table moved out); db2 still has t2.
	if err := s.DropNamespace(ctx, "db2"); err != ErrNamespaceNotEmpty {
		t.Fatalf("expected ErrNamespaceNotEmpty, got %v", err)
	}
	if err := s.DropTable(ctx, "db2", "t2"); err != nil {
		t.Fatalf("drop t2: %v", err)
	}
	if err := s.DropNamespace(ctx, "db2"); err != nil {
		t.Fatalf("drop db2: %v", err)
	}
	if err := s.DropNamespace(ctx, "nope"); err != ErrNamespaceNotFound {
		t.Fatalf("expected ErrNamespaceNotFound dropping missing, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateNamespace(ctx, "db", nil)
	s.Reset(ctx)
	empty, _ := s.IsEmpty(ctx)
	if !empty {
		t.Fatal("expected empty after reset")
	}
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateNamespace(ctx, "db", map[string]string{"k": "v"})
	_ = s.CreateTable(ctx, "db", "t1", Table{
		Metadata:         json.RawMessage(`{"format-version":2,"table-uuid":"u-1"}`),
		MetadataLocation: "loc",
		UUID:             "u-1",
		Version:          3,
	})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	props, err := s2.GetNamespace(ctx, "db")
	if err != nil || props["k"] != "v" {
		t.Fatalf("namespace lost after restore: %v %v", err, props)
	}
	got, err := s2.GetTable(ctx, "db", "t1")
	if err != nil || got.UUID != "u-1" || got.Version != 3 || got.MetadataLocation != "loc" {
		t.Fatalf("table lost after restore: %v %+v", err, got)
	}
}
