package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/snapshottypes"
)

// jsonEqual compares two JSON documents by value. The memory store keeps raw
// bytes while Postgres stores JSONB (which canonicalizes whitespace and key
// order), so byte equality is not part of the store contract — value equality
// is. Both backends must agree semantically. Numbers are decoded as
// json.Number so "1" and "1.0" stay distinct (no float64 coercion).
func jsonEqual(a, b []byte) bool {
	av, aok := decodeJSON(a)
	bv, bok := decodeJSON(b)
	if !aok || !bok {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(av, bv)
}

func decodeJSON(b []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	return v, true
}

// snapshotStore is the Store plus the export/import surface exercised by the
// shared matrices. Both *MemoryStore and *PostgresStore implement it.
type snapshotStore interface {
	Store
	snapshottypes.Snapshotter
}

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s snapshotStore) {
	ctx := context.Background()
	defer s.Reset(ctx)

	if _, err := s.GetDataset(ctx, "proj", "nope"); err != ErrNoSuchDataset {
		t.Fatalf("expected ErrNoSuchDataset, got %v", err)
	}

	d := Dataset{DatasetID: "Sales", Config: []byte(`{"friendlyName":"Sales","description":"d"}`), Labels: map[string]string{"env": "dev"}}
	if err := s.CreateDataset(ctx, "proj", d); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if err := s.CreateDataset(ctx, "proj", d); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
	got, err := s.GetDataset(ctx, "proj", "Sales")
	if err != nil {
		t.Fatalf("get dataset: %v", err)
	}
	if got.Labels["env"] != "dev" {
		t.Fatalf("dataset labels lost: %+v", got)
	}
	if !jsonEqual(got.Config, []byte(`{"friendlyName":"Sales","description":"d"}`)) {
		t.Fatalf("dataset config not verbatim: %s", got.Config)
	}

	// Tables
	if _, err := s.GetTable(ctx, "proj", "Sales", "nope"); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable, got %v", err)
	}
	tb := Table{
		TableID: "Orders",
		Config:  []byte(`{"friendlyName":"Orders Table"}`),
		Schema:  []byte(`{"fields":[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]}`),
		Labels:  map[string]string{"tier": "gold"},
	}
	if err := s.CreateTable(ctx, "proj", "Sales", tb); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.CreateTable(ctx, "proj", "Sales", tb); err != ErrAlreadyExists {
		t.Fatalf("expected table ErrAlreadyExists, got %v", err)
	}
	gotTable, err := s.GetTable(ctx, "proj", "Sales", "Orders")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if gotTable.Labels["tier"] != "gold" || !jsonEqual(gotTable.Schema, tb.Schema) {
		t.Fatalf("table labels/schema lost: %+v", gotTable)
	}

	// Rows round-trip + numRows accounting.
	rows := []Row{
		{Data: []byte(`{"id":1,"name":"alice"}`)},
		{Data: []byte(`{"id":2,"name":"bob"}`)},
	}
	if _, err := s.InsertRows(ctx, "proj", "Sales", "Orders", rows); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
	gotTable, _ = s.GetTable(ctx, "proj", "Sales", "Orders")
	if gotTable.NumRows != 2 {
		t.Fatalf("expected numRows=2, got %d", gotTable.NumRows)
	}
	listRows, err := s.ListRows(ctx, "proj", "Sales", "Orders")
	if err != nil || len(listRows) != 2 {
		t.Fatalf("list rows: %v %d", err, len(listRows))
	}
	if listRows[0].Seq != 1 || listRows[1].Seq != 2 {
		t.Fatalf("row seq not monotonic: %+v", listRows)
	}
	if !jsonEqual(listRows[1].Data, []byte(`{"id":2,"name":"bob"}`)) {
		t.Fatalf("row data lost: %s", listRows[1].Data)
	}
	if _, err := s.InsertRows(ctx, "proj", "Sales", "missing", rows); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable on row insert into missing table, got %v", err)
	}

	// insertId dedup: a repeat id is skipped and reported at its index, and a
	// new id in the same batch is still inserted.
	first, err := s.InsertRows(ctx, "proj", "Sales", "Orders", []Row{
		{InsertID: "id-1", Data: []byte(`{"id":3}`)},
		{InsertID: "id-2", Data: []byte(`{"id":4}`)},
	})
	if err != nil || len(first) != 0 {
		t.Fatalf("first dedup batch: err=%v dups=%v", err, first)
	}
	second, err := s.InsertRows(ctx, "proj", "Sales", "Orders", []Row{{InsertID: "id-1", Data: []byte(`{"id":99}`)}})
	if err != nil || len(second) != 1 || second[0] != 0 {
		t.Fatalf("expected duplicate at index 0, got err=%v dups=%v", err, second)
	}
	gotTable, _ = s.GetTable(ctx, "proj", "Sales", "Orders")
	if gotTable.NumRows != 4 {
		t.Fatalf("expected numRows=4 after dedup, got %d", gotTable.NumRows)
	}

	// ReplaceRows: the whole row set is swapped, Seq reassigned, NumRows updated.
	if err := s.ReplaceRows(ctx, "proj", "Sales", "Orders", []Row{
		{Data: []byte(`{"id":7,"name":"zoe"}`)},
	}); err != nil {
		t.Fatalf("replace rows: %v", err)
	}
	gotTable, _ = s.GetTable(ctx, "proj", "Sales", "Orders")
	if gotTable.NumRows != 1 {
		t.Fatalf("expected numRows=1 after replace, got %d", gotTable.NumRows)
	}
	replaced, err := s.ListRows(ctx, "proj", "Sales", "Orders")
	if err != nil || len(replaced) != 1 || replaced[0].Seq != 1 || !jsonEqual(replaced[0].Data, []byte(`{"id":7,"name":"zoe"}`)) {
		t.Fatalf("replaced rows = %+v, err=%v", replaced, err)
	}
	// An empty replacement clears the table but keeps it.
	if err := s.ReplaceRows(ctx, "proj", "Sales", "Orders", nil); err != nil {
		t.Fatalf("replace with empty: %v", err)
	}
	if gotTable, _ = s.GetTable(ctx, "proj", "Sales", "Orders"); gotTable.NumRows != 0 {
		t.Fatalf("expected numRows=0 after empty replace, got %d", gotTable.NumRows)
	}
	if err := s.ReplaceRows(ctx, "proj", "Sales", "missing", nil); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable on replace into missing table, got %v", err)
	}

	// Jobs
	if _, err := s.GetJob(ctx, "proj", "nope"); err != ErrNoSuchJob {
		t.Fatalf("expected ErrNoSuchJob, got %v", err)
	}
	job := Job{JobID: "j1", Config: []byte(`{"configuration":{"query":{"query":"SELECT 1"}}}`)}
	if err := s.CreateJob(ctx, "proj", job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := s.CreateJob(ctx, "proj", job); err != ErrAlreadyExists {
		t.Fatalf("expected job ErrAlreadyExists, got %v", err)
	}
	gotJob, err := s.GetJob(ctx, "proj", "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !jsonEqual(gotJob.Config, job.Config) {
		t.Fatalf("job config not verbatim: %s", gotJob.Config)
	}
	jobList, err := s.ListJobs(ctx, "proj")
	if err != nil || len(jobList) != 1 {
		t.Fatalf("list jobs: %v %d", err, len(jobList))
	}

	// Lists + deletes cascade.
	dsList, err := s.ListDatasets(ctx, "proj")
	if err != nil || len(dsList) != 1 {
		t.Fatalf("list datasets: %v %d", err, len(dsList))
	}
	tbList, err := s.ListTables(ctx, "proj", "Sales")
	if err != nil || len(tbList) != 1 {
		t.Fatalf("list tables: %v %d", err, len(tbList))
	}
	if err := s.DeleteJob(ctx, "proj", "j1"); err != nil {
		t.Fatalf("delete job: %v", err)
	}
	if err := s.DeleteTable(ctx, "proj", "Sales", "Orders"); err != nil {
		t.Fatalf("delete table: %v", err)
	}
	if _, err := s.GetTable(ctx, "proj", "Sales", "Orders"); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable after delete, got %v", err)
	}
	if err := s.DeleteDataset(ctx, "proj", "Sales"); err != nil {
		t.Fatalf("delete dataset: %v", err)
	}
	if _, err := s.GetDataset(ctx, "proj", "Sales"); err != ErrNoSuchDataset {
		t.Fatalf("expected ErrNoSuchDataset after delete, got %v", err)
	}

	// Timestamp parity (BQ12): zero CreateTime/UpdateTime are defaulted on
	// create, and insertAll/ReplaceRows advance the table's UpdateTime — except
	// an all-duplicate insert, which changes nothing. Both backends must agree.
	runStoreTimestampTests(t, s)

	// Everything seeded above is deleted by the end, so the store is empty.
	if empty, err := s.IsEmpty(ctx); err != nil || !empty {
		t.Fatalf("store not empty after cleanup: empty=%v err=%v", empty, err)
	}
}

// runStoreTimestampTests freezes the global clock so each metadata mutation has
// a deterministic timestamp, then asserts the exact defaulting/bump contract.
// It is shared by the memory and Postgres store matrices.
func runStoreTimestampTests(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	// Zero times are defaulted to the current clock on create.
	if err := s.CreateDataset(ctx, "proj", Dataset{DatasetID: "TsDs"}); err != nil {
		t.Fatalf("create ts dataset: %v", err)
	}
	d, err := s.GetDataset(ctx, "proj", "TsDs")
	if err != nil || !d.CreateTime.Equal(t0) || !d.UpdateTime.Equal(t0) {
		t.Fatalf("dataset timestamps not defaulted to clock: %+v err=%v", d, err)
	}
	if err := s.CreateTable(ctx, "proj", "TsDs", Table{TableID: "Ts"}); err != nil {
		t.Fatalf("create ts table: %v", err)
	}
	tb, err := s.GetTable(ctx, "proj", "TsDs", "Ts")
	if err != nil || !tb.CreateTime.Equal(t0) || !tb.UpdateTime.Equal(t0) {
		t.Fatalf("table timestamps not defaulted to clock: %+v err=%v", tb, err)
	}
	if err := s.CreateJob(ctx, "proj", Job{JobID: "TsJ"}); err != nil {
		t.Fatalf("create ts job: %v", err)
	}
	j, err := s.GetJob(ctx, "proj", "TsJ")
	if err != nil || !j.CreateTime.Equal(t0) {
		t.Fatalf("job CreateTime not defaulted to clock: %+v err=%v", j, err)
	}

	// A real insert advances UpdateTime.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Second)})
	if _, err := s.InsertRows(ctx, "proj", "TsDs", "Ts", []Row{{Data: []byte(`{"id":1}`)}}); err != nil {
		t.Fatalf("insert ts rows: %v", err)
	}
	if tb, _ = s.GetTable(ctx, "proj", "TsDs", "Ts"); !tb.UpdateTime.Equal(t0.Add(time.Second)) {
		t.Fatalf("insert did not bump UpdateTime: %v", tb.UpdateTime)
	}

	// An all-duplicate batch leaves UpdateTime untouched.
	if _, err := s.InsertRows(ctx, "proj", "TsDs", "Ts", []Row{{InsertID: "dup", Data: []byte(`{"id":2}`)}}); err != nil {
		t.Fatalf("insert dedup row: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(2 * time.Second)})
	if dups, err := s.InsertRows(ctx, "proj", "TsDs", "Ts", []Row{{InsertID: "dup", Data: []byte(`{"id":3}`)}}); err != nil || len(dups) != 1 {
		t.Fatalf("duplicate insert: err=%v dups=%v", err, dups)
	}
	if tb, _ = s.GetTable(ctx, "proj", "TsDs", "Ts"); !tb.UpdateTime.Equal(t0.Add(time.Second)) {
		t.Fatalf("all-duplicate insert bumped UpdateTime: %v", tb.UpdateTime)
	}

	// ReplaceRows advances UpdateTime.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(3 * time.Second)})
	if err := s.ReplaceRows(ctx, "proj", "TsDs", "Ts", []Row{{Data: []byte(`{"id":9}`)}}); err != nil {
		t.Fatalf("replace ts rows: %v", err)
	}
	if tb, _ = s.GetTable(ctx, "proj", "TsDs", "Ts"); !tb.UpdateTime.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("ReplaceRows did not bump UpdateTime: %v", tb.UpdateTime)
	}

	_ = s.DeleteJob(ctx, "proj", "TsJ")
	_ = s.DeleteDataset(ctx, "proj", "TsDs")
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	runSnapshotRoundTrip(t, NewMemoryStore(), func() snapshotStore { return NewMemoryStore() })
}

// runSnapshotRoundTrip seeds a dataset/table/row/job, snapshots, restores into a
// fresh store of the same backend, and asserts every resource survived. It is
// shared so the memory and Postgres snapshot formats are both exercised through
// the public Store API (their on-disk JSON layouts differ by design).
func runSnapshotRoundTrip(t *testing.T, s snapshotStore, fresh func() snapshotStore) {
	t.Helper()
	ctx := context.Background()
	defer s.Reset(ctx)

	_ = s.CreateDataset(ctx, "p", Dataset{DatasetID: "d", Labels: map[string]string{"k": "v"}, Config: []byte(`{"friendlyName":"F"}`)})
	_ = s.CreateTable(ctx, "p", "d", Table{TableID: "t", Schema: []byte(`{"fields":[{"name":"a","type":"STRING"}]}`), Labels: map[string]string{"x": "y"}})
	_, _ = s.InsertRows(ctx, "p", "d", "t", []Row{{InsertID: "i1", Data: []byte(`{"a":"hi"}`)}})
	_ = s.CreateJob(ctx, "p", Job{JobID: "j", Config: []byte(`{"configuration":{"query":{"query":"SELECT 1"}}}`)})

	srcTable, err := s.GetTable(ctx, "p", "d", "t")
	if err != nil {
		t.Fatalf("get source table: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := fresh()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	defer s2.Reset(ctx)

	got, err := s2.GetDataset(ctx, "p", "d")
	if err != nil || got.Labels["k"] != "v" {
		t.Fatalf("dataset lost after restore: %v %+v", err, got)
	}
	gotTable, err := s2.GetTable(ctx, "p", "d", "t")
	if err != nil || gotTable.NumRows != 1 || !jsonEqual(gotTable.Schema, []byte(`{"fields":[{"name":"a","type":"STRING"}]}`)) {
		t.Fatalf("table lost after restore: %v %+v", err, gotTable)
	}
	// Timestamps survive the round trip (snapshot/export must not reset them).
	if !gotTable.CreateTime.Equal(srcTable.CreateTime) || !gotTable.UpdateTime.Equal(srcTable.UpdateTime) {
		t.Fatalf("table timestamps changed across snapshot/restore: src=%v/%v got=%v/%v",
			srcTable.CreateTime, srcTable.UpdateTime, gotTable.CreateTime, gotTable.UpdateTime)
	}
	gotRows, err := s2.ListRows(ctx, "p", "d", "t")
	if err != nil || len(gotRows) != 1 || !jsonEqual(gotRows[0].Data, []byte(`{"a":"hi"}`)) {
		t.Fatalf("rows lost after restore: %v %+v", err, gotRows)
	}
	gotJob, err := s2.GetJob(ctx, "p", "j")
	if err != nil || !jsonEqual(gotJob.Config, []byte(`{"configuration":{"query":{"query":"SELECT 1"}}}`)) {
		t.Fatalf("job lost after restore: %v %+v", err, gotJob)
	}
}
