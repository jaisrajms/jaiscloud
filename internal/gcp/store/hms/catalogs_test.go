package hms

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// TestCatalogsIsolation is the shared matrix for the multi-catalog store,
// exercised against both backends: two Services with the same database and table
// names never collide, each has its own `default`, and Iceberg commit locks are
// scoped per catalog.
func TestCatalogsIsolation(t *testing.T) {
	runCatalogIsolation(t, func() Catalogs { return NewMemoryCatalogs(nil) })
}

func runCatalogIsolation(t *testing.T, newCatalogs func() Catalogs) {
	t.Helper()
	ctx := context.Background()
	cats := newCatalogs()
	defer cats.Reset(ctx)

	a := cats.Catalog(CatalogID("proj-a", "us-central1", "hms"))
	b := cats.Catalog(CatalogID("proj-b", "europe-west1", "hms"))

	// Same names, different catalogs, different content.
	if err := a.CreateDatabase(ctx, Database{Name: "default", LocationURI: "gs://a/default"}); err != nil {
		t.Fatalf("a create default: %v", err)
	}
	if err := b.CreateDatabase(ctx, Database{Name: "default", LocationURI: "gs://b/default"}); err != nil {
		t.Fatalf("b create default: %v (a second Service's default must not collide)", err)
	}
	// Same database + table name in both catalogs.
	for _, s := range []struct {
		name  string
		store Store
		loc   string
	}{{"a", a, "gs://a/default/t1"}, {"b", b, "gs://b/default/t1"}} {
		tbl := testCatalogTable("default", "t1", s.loc)
		if err := s.store.CreateTable(ctx, "default", "t1", tbl); err != nil {
			t.Fatalf("%s create t1: %v", s.name, err)
		}
	}
	ga, err := a.GetTable(ctx, "default", "t1")
	if err != nil {
		t.Fatalf("a get t1: %v", err)
	}
	gb, err := b.GetTable(ctx, "default", "t1")
	if err != nil {
		t.Fatalf("b get t1: %v", err)
	}
	if string(ga.TableJSON) == string(gb.TableJSON) {
		t.Fatal("the same table name in two catalogs returned the same record")
	}
	if dba, _ := a.GetDatabase(ctx, "default"); dba.LocationURI != "gs://a/default" {
		t.Fatalf("catalog a default location = %q", dba.LocationURI)
	}
	if dbb, _ := b.GetDatabase(ctx, "default"); dbb.LocationURI != "gs://b/default" {
		t.Fatalf("catalog b default location = %q", dbb.LocationURI)
	}

	// Dropping in one catalog leaves the other untouched.
	if err := a.DropDatabase(ctx, "default", true); err != nil {
		t.Fatalf("a drop default: %v", err)
	}
	if _, err := a.GetDatabase(ctx, "default"); err != ErrDatabaseNotFound {
		t.Fatalf("a default still present: %v", err)
	}
	if _, err := b.GetDatabase(ctx, "default"); err != nil {
		t.Fatalf("b default vanished with a's drop: %v", err)
	}

	// Locks are per catalog: a lock taken in one catalog is not visible in the
	// other, so an Iceberg commit in one Service does not block another.
	id, err := a.Lock(ctx, Lock{DBName: "default", TableName: "t1", User: "spark"})
	if err != nil {
		t.Fatalf("a lock: %v", err)
	}
	if state, err := a.CheckLock(ctx, id); err != nil || state != LockStateAcquired {
		t.Fatalf("a check own lock = %v, %v; want ACQUIRED", state, err)
	}
	if state, err := b.CheckLock(ctx, id); err != nil || state != LockStateNotAcquired {
		t.Fatalf("b check a's lock = %v, %v; want NOT_ACQUIRED", state, err)
	}
	// Unlocking from the other catalog is a no-op, not a cross-catalog delete.
	if err := b.Unlock(ctx, id); err != nil {
		t.Fatalf("b unlock a's lock: %v", err)
	}
	if state, _ := a.CheckLock(ctx, id); state != LockStateAcquired {
		t.Fatalf("a's lock was deleted by b: %v", state)
	}

	// The legacy catalog is a separate catalog again.
	legacy := cats.Catalog(LegacyCatalogID)
	if names, err := legacy.ListDatabases(ctx); err != nil || len(names) != 0 {
		t.Fatalf("legacy catalog databases = %v, %v; want empty", names, err)
	}

	// DropCatalog removes the Service's data (a Service deletion deletes its
	// Hive Metastore), and the remaining catalog is unaffected.
	cats.DropCatalog(ctx, CatalogID("proj-a", "us-central1", "hms"))
	if names, _ := a.ListDatabases(ctx); len(names) != 0 {
		t.Fatalf("dropped catalog still has databases: %v", names)
	}
	if _, err := b.GetDatabase(ctx, "default"); err != nil {
		t.Fatalf("dropping a's catalog affected b: %v", err)
	}
}

// TestCatalogsSnapshotRoundTrip verifies a v2 snapshot restores every catalog.
func TestCatalogsSnapshotRoundTrip(t *testing.T) {
	runCatalogSnapshotRoundTrip(t, func() Catalogs { return NewMemoryCatalogs(nil) })
}

// TestCatalogsRestoresLegacyFlatSnapshot verifies a pre-NF32 (version 1) flat
// snapshot restores into the legacy catalog, so an upgrade loses no catalog.
func TestCatalogsRestoresLegacyFlatSnapshot(t *testing.T) {
	ctx := context.Background()
	legacy := NewMemoryStore()
	if err := legacy.CreateDatabase(ctx, Database{Name: "old"}); err != nil {
		t.Fatal(err)
	}
	var flat bytes.Buffer
	if err := legacy.Snapshot(ctx, &flat); err != nil {
		t.Fatal(err)
	}
	runLegacyFlatRestore(t, NewMemoryCatalogs(nil), flat.Bytes())
}

// runLegacyFlatRestore restores flat (a version 1 single-catalog document) and
// asserts it lands in the legacy catalog, leaving Service catalogs empty.
func runLegacyFlatRestore(t *testing.T, cats Catalogs, flat []byte) {
	t.Helper()
	ctx := context.Background()
	defer cats.Reset(ctx)
	if err := cats.Restore(ctx, bytes.NewReader(flat)); err != nil {
		t.Fatalf("restore flat snapshot: %v", err)
	}
	if _, err := cats.Catalog(LegacyCatalogID).GetDatabase(ctx, "old"); err != nil {
		t.Fatalf("flat snapshot did not restore into the legacy catalog: %v", err)
	}
	if names, _ := cats.Catalog(CatalogID("proj", "loc", "hms")).ListDatabases(ctx); len(names) != 0 {
		t.Fatalf("flat snapshot leaked into a Service catalog: %v", names)
	}
}

func runCatalogSnapshotRoundTrip(t *testing.T, newCatalogs func() Catalogs) {
	t.Helper()
	ctx := context.Background()
	cats := newCatalogs()
	defer cats.Reset(ctx)

	ids := []string{LegacyCatalogID, CatalogID("proj-a", "us-central1", "hms"), CatalogID("proj-b", "europe-west1", "hms")}
	for i, id := range ids {
		s := cats.Catalog(id)
		if err := s.CreateDatabase(ctx, Database{Name: "default", LocationURI: "gs://wh" + string(rune('a'+i))}); err != nil {
			t.Fatalf("%s create default: %v", id, err)
		}
		if err := s.CreateTable(ctx, "default", "t", testCatalogTable("default", "t", "gs://wh/t"+string(rune('a'+i)))); err != nil {
			t.Fatalf("%s create table: %v", id, err)
		}
	}

	var buf bytes.Buffer
	if err := cats.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// The envelope is versioned and names every catalog.
	var env struct {
		Version  int                        `json:"version"`
		Catalogs map[string]json.RawMessage `json:"catalogs"`
	}
	if err := json.Unmarshal(buf.Bytes(), &env); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if env.Version != catalogsSnapshotVersion {
		t.Fatalf("snapshot version = %d, want %d", env.Version, catalogsSnapshotVersion)
	}
	for _, id := range ids {
		if _, ok := env.Catalogs[id]; !ok {
			t.Fatalf("snapshot missing catalog %q", id)
		}
	}

	cats.Reset(ctx)
	if empty, _ := cats.IsEmpty(ctx); !empty {
		t.Fatal("reset left catalogs non-empty")
	}
	if err := cats.Restore(ctx, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("restore: %v", err)
	}
	for i, id := range ids {
		db, err := cats.Catalog(id).GetDatabase(ctx, "default")
		if err != nil {
			t.Fatalf("%s default after restore: %v", id, err)
		}
		if db.LocationURI != "gs://wh"+string(rune('a'+i)) {
			t.Fatalf("%s default location after restore = %q", id, db.LocationURI)
		}
		if _, err := cats.Catalog(id).GetTable(ctx, "default", "t"); err != nil {
			t.Fatalf("%s table after restore: %v", id, err)
		}
	}
}

// testCatalogTable builds a minimal stored table record for the catalog tests.
func testCatalogTable(db, name, location string) Table {
	return Table{
		DBName:    db,
		TableName: name,
		TableJSON: json.RawMessage(`{"dbName":"` + db + `","tableName":"` + name + `","sd":{"location":"` + location + `"}}`),
	}
}
