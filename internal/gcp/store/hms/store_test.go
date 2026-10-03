package hms

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
)

// jsonEqual reports whether two JSON documents are semantically equal. The
// Postgres backend stores Table/Partition JSON in JSONB columns, which normalize
// key order and whitespace, so a byte-for-byte comparison only holds for the
// memory store. The HMS server parses the struct JSON, so semantic equality is
// the correct contract (mirrors internal/gcp/store/dataproc/postgres_test.go).
func jsonEqual(a, b string) bool {
	var av, bv any
	if json.Unmarshal([]byte(a), &av) != nil || json.Unmarshal([]byte(b), &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	// --- Databases ---
	if _, err := s.GetDatabase(ctx, "nope"); err != ErrDatabaseNotFound {
		t.Fatalf("expected ErrDatabaseNotFound, got %v", err)
	}
	db := Database{Name: "default", LocationURI: "gs://w/default", Parameters: map[string]string{"a": "b"}, Description: "d", Owner: "me"}
	if err := s.CreateDatabase(ctx, db); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := s.CreateDatabase(ctx, db); err != ErrDatabaseExists {
		t.Fatalf("expected ErrDatabaseExists, got %v", err)
	}
	got, err := s.GetDatabase(ctx, "default")
	if err != nil || got.LocationURI != "gs://w/default" || got.Parameters["a"] != "b" || got.Owner != "me" {
		t.Fatalf("get database: %v %+v", err, got)
	}
	names, err := s.ListDatabases(ctx)
	if err != nil || len(names) != 1 || names[0] != "default" {
		t.Fatalf("list databases: %v %v", err, names)
	}

	// AlterDatabase.
	if err := s.AlterDatabase(ctx, "default", Database{Name: "default", LocationURI: "gs://w/default2", Parameters: map[string]string{"x": "y"}, Owner: "me"}); err != nil {
		t.Fatalf("alter database: %v", err)
	}
	got, _ = s.GetDatabase(ctx, "default")
	if got.LocationURI != "gs://w/default2" || got.Parameters["x"] != "y" {
		t.Fatalf("alter database lost fields: %+v", got)
	}
	if err := s.AlterDatabase(ctx, "nope", db); err != ErrDatabaseNotFound {
		t.Fatalf("expected ErrDatabaseNotFound on alter, got %v", err)
	}

	// --- Tables ---
	if _, err := s.GetTable(ctx, "default", "t1"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	tblJSON := json.RawMessage(`["o",[[1,["s","t1"]],[2,["s","default"]],[9,["m",11,11,[[["s","metadata_location"],["s","loc1"]]]]]]]`)
	t1 := Table{DBName: "default", TableName: "t1", TableJSON: tblJSON}
	if err := s.CreateTable(ctx, "default", "t1", t1); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.CreateTable(ctx, "default", "t1", t1); err != ErrTableExists {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}
	if err := s.CreateTable(ctx, "missing_db", "t1", t1); err != ErrDatabaseNotFound {
		t.Fatalf("expected ErrDatabaseNotFound on create table, got %v", err)
	}
	gotTbl, err := s.GetTable(ctx, "default", "t1")
	if err != nil || gotTbl.DBName != "default" || gotTbl.TableName != "t1" || !jsonEqual(string(gotTbl.TableJSON), string(tblJSON)) {
		t.Fatalf("get table: %v %+v", err, gotTbl)
	}
	tn, err := s.ListTables(ctx, "default")
	if err != nil || len(tn) != 1 || tn[0] != "t1" {
		t.Fatalf("list tables: %v %v", err, tn)
	}

	// AlterTable — plain overwrite (D5).
	newJSON := json.RawMessage(`["o",[[1,["s","t1"]],[2,["s","default"]],[9,["m",11,11,[[["s","metadata_location"],["s","loc2"]]]]]]]`)
	updated, err := s.AlterTable(ctx, "default", "t1", func(current Table) (Table, error) {
		// Ignore current entirely — plain overwrite.
		return Table{DBName: "default", TableName: "t1", TableJSON: newJSON}, nil
	})
	if err != nil || !jsonEqual(string(updated.TableJSON), string(newJSON)) {
		t.Fatalf("alter table: %v %+v", err, updated)
	}
	gotTbl, _ = s.GetTable(ctx, "default", "t1")
	if !jsonEqual(string(gotTbl.TableJSON), string(newJSON)) {
		t.Fatalf("alter table did not overwrite: %s", gotTbl.TableJSON)
	}
	if _, err := s.AlterTable(ctx, "default", "nope", func(Table) (Table, error) { return Table{}, nil }); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound on alter, got %v", err)
	}

	// AlterTable mutate error aborts without writing.
	if _, err := s.AlterTable(ctx, "default", "t1", func(Table) (Table, error) { return Table{}, errTestMutate }); err != errTestMutate {
		t.Fatalf("expected mutate error to propagate, got %v", err)
	}
	gotTbl, _ = s.GetTable(ctx, "default", "t1")
	if !jsonEqual(string(gotTbl.TableJSON), string(newJSON)) {
		t.Fatal("mutate error should not have written")
	}

	// DropDatabase without cascade fails on a non-empty database.
	if err := s.DropDatabase(ctx, "default", false); err != ErrDatabaseNotEmpty {
		t.Fatalf("expected ErrDatabaseNotEmpty, got %v", err)
	}
	// With cascade it drops the table too.
	if err := s.DropDatabase(ctx, "default", true); err != nil {
		t.Fatalf("drop database cascade: %v", err)
	}
	if _, err := s.GetTable(ctx, "default", "t1"); err != ErrTableNotFound {
		t.Fatalf("expected table gone after cascade drop, got %v", err)
	}
	if _, err := s.GetDatabase(ctx, "default"); err != ErrDatabaseNotFound {
		t.Fatalf("expected database gone, got %v", err)
	}

	// DropTable + rename.
	if err := s.CreateDatabase(ctx, Database{Name: "db2"}); err != nil {
		t.Fatalf("create db2: %v", err)
	}
	if err := s.CreateTable(ctx, "db2", "a", Table{DBName: "db2", TableName: "a", TableJSON: tblJSON}); err != nil {
		t.Fatalf("create table a: %v", err)
	}
	if err := s.CreateTable(ctx, "db2", "b", Table{DBName: "db2", TableName: "b", TableJSON: tblJSON}); err != nil {
		t.Fatalf("create table b: %v", err)
	}
	if err := s.DropTable(ctx, "db2", "a"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if err := s.DropTable(ctx, "db2", "a"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound on re-drop, got %v", err)
	}
	if _, err := s.RenameTable(ctx, "db2", "b", "db2", "c", Table{DBName: "db2", TableName: "c", TableJSON: tblJSON}); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	if _, err := s.GetTable(ctx, "db2", "b"); err != ErrTableNotFound {
		t.Fatalf("expected old name gone, got %v", err)
	}
	if _, err := s.GetTable(ctx, "db2", "c"); err != nil {
		t.Fatalf("expected new name present, got %v", err)
	}
	if _, err := s.RenameTable(ctx, "db2", "nope", "db2", "d", Table{}); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound on rename, got %v", err)
	}

	// --- Locks ---
	id, err := s.Lock(ctx, Lock{DBName: "db2", TableName: "c", User: "u", Hostname: "h"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if id == 0 {
		t.Fatal("lock returned 0")
	}
	if st, _ := s.CheckLock(ctx, id); st != LockStateAcquired {
		t.Fatalf("check lock state = %d, want ACQUIRED", st)
	}
	if err := s.Unlock(ctx, id); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if st, _ := s.CheckLock(ctx, id); st != LockStateNotAcquired {
		t.Fatalf("check lock after unlock = %d, want NOT_ACQUIRED", st)
	}
	if err := s.Unlock(ctx, id); err != nil {
		t.Fatalf("unlock should be idempotent: %v", err)
	}

	// --- Partitions ---
	// Missing table -> ErrTableNotFound (distinct from a missing partition).
	if _, err := s.GetPartition(ctx, "db2", "nope", []string{"2024"}); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	pA := Partition{DBName: "db2", TableName: "c", Values: []string{"2024"}, PartJSON: partJSON("2024")}
	if err := s.CreatePartition(ctx, "db2", "c", pA); err != nil {
		t.Fatalf("create partition: %v", err)
	}
	if err := s.CreatePartition(ctx, "db2", "c", pA); err != ErrPartitionExists {
		t.Fatalf("expected ErrPartitionExists, got %v", err)
	}
	if err := s.CreatePartition(ctx, "missing_db", "c", pA); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound on create, got %v", err)
	}
	gotP, err := s.GetPartition(ctx, "db2", "c", []string{"2024"})
	if err != nil || len(gotP.Values) != 1 || gotP.Values[0] != "2024" || !jsonEqual(string(gotP.PartJSON), string(pA.PartJSON)) {
		t.Fatalf("get partition: %v %+v", err, gotP)
	}
	if _, err := s.GetPartition(ctx, "db2", "c", []string{"2025"}); err != ErrPartitionNotFound {
		t.Fatalf("expected ErrPartitionNotFound, got %v", err)
	}

	// List: ordered by value tuple.
	pB := Partition{DBName: "db2", TableName: "c", Values: []string{"2025"}, PartJSON: partJSON("2025")}
	pC := Partition{DBName: "db2", TableName: "c", Values: []string{"2023"}, PartJSON: partJSON("2023")}
	if err := s.CreatePartition(ctx, "db2", "c", pB); err != nil {
		t.Fatal(err)
	}
	if err := s.CreatePartition(ctx, "db2", "c", pC); err != nil {
		t.Fatal(err)
	}
	listed, err := s.ListPartitions(ctx, "db2", "c")
	if err != nil || len(listed) != 3 {
		t.Fatalf("list partitions: %v %+v", err, listed)
	}
	if listed[0].Values[0] != "2023" || listed[1].Values[0] != "2024" || listed[2].Values[0] != "2025" {
		t.Fatalf("list partitions not ordered: %+v", listed)
	}

	// AlterPartition is a full overwrite keyed by the value tuple.
	newPJ := partJSON("2024-updated")
	if _, err := s.AlterPartition(ctx, "db2", "c", []string{"2024"}, func(Partition) (Partition, error) {
		return Partition{DBName: "db2", TableName: "c", Values: []string{"2024"}, PartJSON: newPJ}, nil
	}); err != nil {
		t.Fatalf("alter partition: %v", err)
	}
	gotP, _ = s.GetPartition(ctx, "db2", "c", []string{"2024"})
	if !jsonEqual(string(gotP.PartJSON), string(newPJ)) {
		t.Fatalf("alter partition did not overwrite: %s", gotP.PartJSON)
	}
	if _, err := s.AlterPartition(ctx, "db2", "c", []string{"1999"}, func(Partition) (Partition, error) {
		return Partition{}, nil
	}); err != ErrPartitionNotFound {
		t.Fatalf("expected ErrPartitionNotFound on alter, got %v", err)
	}

	// RenamePartition re-keys; collision -> ErrPartitionExists.
	if _, err := s.RenamePartition(ctx, "db2", "c", []string{"2024"}, []string{"2024b"}, Partition{PartJSON: newPJ}); err != nil {
		t.Fatalf("rename partition: %v", err)
	}
	if _, err := s.GetPartition(ctx, "db2", "c", []string{"2024"}); err != ErrPartitionNotFound {
		t.Fatalf("expected old partition gone, got %v", err)
	}
	if _, err := s.GetPartition(ctx, "db2", "c", []string{"2024b"}); err != nil {
		t.Fatalf("expected renamed partition present, got %v", err)
	}
	if _, err := s.RenamePartition(ctx, "db2", "c", []string{"2025"}, []string{"2024b"}, Partition{}); err != ErrPartitionExists {
		t.Fatalf("expected ErrPartitionExists on colliding rename, got %v", err)
	}
	if _, err := s.RenamePartition(ctx, "db2", "c", []string{"nope"}, []string{"x"}, Partition{}); err != ErrPartitionNotFound {
		t.Fatalf("expected ErrPartitionNotFound on rename, got %v", err)
	}

	// DropPartition.
	if err := s.DropPartition(ctx, "db2", "c", []string{"2024b"}); err != nil {
		t.Fatalf("drop partition: %v", err)
	}
	if err := s.DropPartition(ctx, "db2", "c", []string{"2024b"}); err != ErrPartitionNotFound {
		t.Fatalf("expected ErrPartitionNotFound on re-drop, got %v", err)
	}

	// Renaming a table preserves its partitions (Hive keeps partition metadata).
	if _, err := s.RenameTable(ctx, "db2", "c", "db2", "c_renamed", Table{DBName: "db2", TableName: "c_renamed", TableJSON: tblJSON}); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	if _, err := s.GetPartition(ctx, "db2", "c_renamed", []string{"2025"}); err != nil {
		t.Fatalf("partitions lost on table rename: %v", err)
	}
	if _, err := s.GetPartition(ctx, "db2", "c", []string{"2025"}); err != ErrTableNotFound {
		t.Fatalf("expected old table gone after rename, got %v", err)
	}
	// Rename back so the cascade check below still targets c.
	if _, err := s.RenameTable(ctx, "db2", "c_renamed", "db2", "c", Table{DBName: "db2", TableName: "c", TableJSON: tblJSON}); err != nil {
		t.Fatalf("rename table back: %v", err)
	}

	// Dropping the table cascades to its partitions.
	if err := s.DropTable(ctx, "db2", "c"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := s.ListPartitions(ctx, "db2", "c"); err != ErrTableNotFound {
		t.Fatalf("expected partitions gone with table, got %v", err)
	}

	// Dropping a database with cascade removes its partitions too.
	if err := s.CreateDatabase(ctx, Database{Name: "db3"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx, "db3", "t", Table{DBName: "db3", TableName: "t", TableJSON: tblJSON}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreatePartition(ctx, "db3", "t", Partition{DBName: "db3", TableName: "t", Values: []string{"p"}, PartJSON: partJSON("p")}); err != nil {
		t.Fatal(err)
	}
	if err := s.DropDatabase(ctx, "db3", true); err != nil {
		t.Fatalf("drop database cascade: %v", err)
	}
	if _, err := s.GetPartition(ctx, "db3", "t", []string{"p"}); err != ErrTableNotFound {
		t.Fatalf("expected partitions gone with database, got %v", err)
	}
}

// partJSON builds a minimal but well-formed canonical Partition JSON (the first
// value tuple is populated) for store-level tests, which persist it verbatim.
func partJSON(value string) json.RawMessage {
	b, _ := json.Marshal([]any{"o", []any{[]any{1, []any{"a", 11, []any{[]any{"s", value}}}}}})
	return b
}

var errTestMutate = context.Canceled // arbitrary sentinel distinct from store errors

// TestMemoryStore runs the shared matrix against the in-memory backend.
func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

// TestAlterTableAtomicity verifies the mutate-closure runs under one lock: two
// concurrent AlterTable calls each observe a serializable read-modify-write and
// neither update is lost.
func TestAlterTableAtomicity(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	defer s.Reset(ctx)

	if err := s.CreateDatabase(ctx, Database{Name: "db"}); err != nil {
		t.Fatal(err)
	}
	init := Table{DBName: "db", TableName: "t", TableJSON: json.RawMessage(`["o",[]]`)}
	if err := s.CreateTable(ctx, "db", "t", init); err != nil {
		t.Fatal(err)
	}

	const writers = 20
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each writer appends a distinct marker field to the table JSON's
			// field list and relies on AtomicUpdate's read-modify-write lock so
			// no two writers interleave.
			_, err := s.AlterTable(ctx, "db", "t", func(cur Table) (Table, error) {
				var fields []any
				if err := json.Unmarshal(cur.TableJSON, &fields); err != nil {
					return Table{}, err
				}
				// fields = ["o", [...]]; append one more marker into the inner list.
				inner := fields[1].([]any)
				marker := float64(len(inner))
				inner = append(inner, []any{marker, []any{"s", "x"}})
				fields[1] = inner
				b, err := json.Marshal(fields)
				if err != nil {
					return Table{}, err
				}
				return Table{DBName: "db", TableName: "t", TableJSON: b}, nil
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("alter table: %v", err)
		}
	}

	got, err := s.GetTable(ctx, "db", "t")
	if err != nil {
		t.Fatal(err)
	}
	var fields []any
	if err := json.Unmarshal(got.TableJSON, &fields); err != nil {
		t.Fatal(err)
	}
	if inner := fields[1].([]any); len(inner) != writers {
		t.Fatalf("lost update: expected %d markers, got %d", writers, len(inner))
	}
}

// TestSnapshotRestore verifies memory snapshot/restore round-trips catalog
// metadata (locks are deliberately excluded).
func TestSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Reset(ctx)
	if err := s.CreateDatabase(ctx, Database{Name: "db", LocationURI: "loc"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx, "db", "t", Table{DBName: "db", TableName: "t", TableJSON: json.RawMessage(`["o",[[1,["s","t"]]]]`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreatePartition(ctx, "db", "t", Partition{DBName: "db", TableName: "t", Values: []string{"2024"}, PartJSON: partJSON("2024")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lock(ctx, Lock{}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	s.Reset(ctx)
	if empty, _ := s.IsEmpty(ctx); !empty {
		t.Fatal("store should be empty after reset")
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDatabase(ctx, "db"); err != nil {
		t.Fatalf("database lost in restore: %v", err)
	}
	if got, err := s.GetTable(ctx, "db", "t"); err != nil || !jsonEqual(string(got.TableJSON), `["o",[[1,["s","t"]]]]`) {
		t.Fatalf("table lost in restore: %v %+v", err, got)
	}
	if got, err := s.GetPartition(ctx, "db", "t", []string{"2024"}); err != nil || !jsonEqual(string(got.PartJSON), string(partJSON("2024"))) {
		t.Fatalf("partition lost in restore: %v %+v", err, got)
	}
}
