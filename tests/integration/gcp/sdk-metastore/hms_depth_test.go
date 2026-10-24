// Behavioural depth for the Dataproc Metastore Hive Metastore (Thrift) serving
// plane (TD8): `p2` concurrency/contention, `p3` declared error envelopes, `p4`
// partition/listing paging and `p5` scale/boundary. It drives the serving plane
// with an independent, off-the-shelf Go HMS client
// (github.com/slachiewicz/hms-client-go) plus that client's generated Thrift
// package for the typed exceptions and request-struct forms the clean API does
// not surface — the GCP analogue of driving the AWS Glue emulator with the
// official AWS SDK.
//
// Run with the GCP binary running (metastore enabled, Thrift plane on :9083):
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -race ./...
package sdk_metastore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	hms "github.com/slachiewicz/hms-client-go"
	hive_metastore "github.com/slachiewicz/hms-client-go/gen/hive_metastore"
	"github.com/stretchr/testify/require"
)

// icebergTable builds a clean-client Iceberg table with the given partition
// keys. LocationURI/metadata locations are run-scoped so re-runs stay idempotent.
func icebergTable(db, tbl string, cols []*hms.FieldSchema, partKeys []*hms.FieldSchema) *hms.Table {
	loc := "gs://bucket/wh/" + db + "/" + tbl
	t := hms.NewIcebergTable(db, tbl, loc, loc+"/metadata/v1.metadata.json", cols)
	t.PartitionKeys = partKeys
	return t
}

// TestHMSDepthConcurrency covers `p2`: parallel DDL writers, a same-name create
// race (exactly one winner), concurrent partition adds on one table, and lock
// contention, all through the pooled HMS client under `-race`.
func TestHMSDepthConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency depth under -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := dialHMS(t)
	base := hmsName("conc")
	const workers = 8

	// (1) Distinct concurrent DDL: every worker creates its own database and a
	// table inside it; each must be independently visible afterwards.
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := fmt.Sprintf("%s_d%d", base, i)
			if err := c.CreateDatabase(ctx, hmsDB(db)); err != nil {
				errs[i] = fmt.Errorf("create database %s: %w", db, err)
				return
			}
			if err := c.CreateTable(ctx, icebergTable(db, "t", []*hms.FieldSchema{{Name: "id", Type: "bigint"}}, nil)); err != nil {
				errs[i] = fmt.Errorf("create table in %s: %w", db, err)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "distinct-DDL worker %d", i)
	}
	for i := 0; i < workers; i++ {
		db := fmt.Sprintf("%s_d%d", base, i)
		got, err := c.GetDatabase(ctx, db)
		require.NoErrorf(t, err, "get %s", db)
		require.Equal(t, db, got.Name)
		tbl, err := c.GetTable(ctx, db, "t")
		require.NoErrorf(t, err, "get table in %s", db)
		require.Equal(t, "t", tbl.TableName)
	}

	// (2) Same-name create race: exactly one create succeeds, every other caller
	// gets AlreadyExists, and the catalog holds exactly one database.
	same := base + "_same"
	results := make([]error, workers)
	var wg2 sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			results[i] = c.CreateDatabase(ctx, hmsDB(same))
		}(i)
	}
	wg2.Wait()
	winners, duplicates := 0, 0
	for i, err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, hms.ErrAlreadyExists):
			duplicates++
		default:
			require.NoErrorf(t, err, "same-name worker %d", i)
		}
	}
	require.Equal(t, 1, winners, "exactly one same-name create must win")
	require.Equal(t, workers-1, duplicates, "every loser must see AlreadyExists")
	_, err := c.GetDatabase(ctx, same)
	require.NoError(t, err)

	// (3) Concurrent partition adds on one table with disjoint values: all land,
	// none is lost or duplicated.
	pdb := base + "_parts"
	require.NoError(t, c.CreateDatabase(ctx, hmsDB(pdb)))
	require.NoError(t, c.CreateTable(ctx, icebergTable(pdb, "events",
		[]*hms.FieldSchema{{Name: "id", Type: "bigint"}},
		[]*hms.FieldSchema{{Name: "ds", Type: "string"}})))
	perrs := make([]error, workers)
	var wg3 sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg3.Add(1)
		go func(i int) {
			defer wg3.Done()
			perrs[i] = c.AddPartitions(ctx, pdb, "events",
				[]*hms.Partition{{Values: []string{fmt.Sprintf("ds=%02d", i)}}}, false)
		}(i)
	}
	wg3.Wait()
	for i, err := range perrs {
		require.NoErrorf(t, err, "partition writer %d", i)
	}
	names, err := c.GetPartitionNames(ctx, pdb, "events", -1)
	require.NoError(t, err)
	require.Len(t, names, workers, "every concurrent partition must be present exactly once")

	// (4) Lock contention: the emulator's lock manager grants each caller its own
	// lock (Hive LockStateAcquired, never Waiting), and CheckLock reflects the
	// state before and after release. This is the Iceberg commit-lock path.
	lockIDs := make([]int64, workers)
	lerrs := make([]error, workers)
	var wg4 sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg4.Add(1)
		go func(i int) {
			defer wg4.Done()
			resp, err := c.Lock(ctx, hms.LockRequest{
				Components: []hms.LockComponent{{
					Type: hms.LockTypeExclusive, Level: hms.LockLevelTable, Database: base, Table: "t",
				}},
				User: "tester", Host: "localhost",
			})
			if err != nil {
				lerrs[i] = fmt.Errorf("lock: %w", err)
				return
			}
			if resp.State != hms.LockStateAcquired {
				lerrs[i] = fmt.Errorf("lock state = %v, want Acquired", resp.State)
				return
			}
			lockIDs[i] = resp.LockID
			if held, err := c.CheckLock(ctx, resp.LockID); err != nil || held.State != hms.LockStateAcquired {
				lerrs[i] = fmt.Errorf("check_lock held: state=%v err=%v", held.State, err)
				return
			}
			if err := c.Unlock(ctx, resp.LockID); err != nil {
				lerrs[i] = fmt.Errorf("unlock: %w", err)
				return
			}
			if free, err := c.CheckLock(ctx, resp.LockID); err != nil || free.State != hms.LockStateNotAcquired {
				lerrs[i] = fmt.Errorf("check_lock after unlock: state=%v err=%v", free.State, err)
			}
		}(i)
	}
	wg4.Wait()
	for i, err := range lerrs {
		require.NoErrorf(t, err, "lock worker %d", i)
	}
	seen := map[int64]bool{}
	for _, id := range lockIDs {
		require.Falsef(t, seen[id], "lock id %d handed to two callers", id)
		seen[id] = true
	}
	// An unknown lock id is not an error: the serving plane reports NotAcquired.
	unknown, err := c.CheckLock(ctx, int64(1)<<62)
	require.NoError(t, err)
	require.Equal(t, hms.LockStateNotAcquired, unknown.State)
}

// TestHMSDepthDeclaredErrors covers `p3`: the declared Thrift exception types on
// bad input and NOT_FOUND, asserted both through the clean client's sentinels
// and the generated client's typed exceptions.
func TestHMSDepthDeclaredErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := dialHMS(t)
	gen, closeGen := dialGeneratedHMS(t)
	defer closeGen()

	db := hmsName("err")
	require.NoError(t, c.CreateDatabase(ctx, hmsDB(db)))
	require.NoError(t, c.CreateTable(ctx, icebergTable(db, "t", []*hms.FieldSchema{{Name: "id", Type: "bigint"}}, nil)))

	// Clean-client sentinels.
	require.ErrorIs(t, c.CreateDatabase(ctx, hmsDB(db)), hms.ErrAlreadyExists, "duplicate database")
	_, err := c.GetDatabase(ctx, db+"_missing")
	require.ErrorIs(t, err, hms.ErrNotFound, "missing database")
	_, err = c.GetTable(ctx, db, "missing")
	require.ErrorIs(t, err, hms.ErrNotFound, "missing table")

	// Generated-client typed exceptions, each at the field the IDL declares.
	t.Run("create_database duplicate", func(t *testing.T) {
		dup := hive_metastore.NewDatabase()
		dup.Name = db
		var ae *hive_metastore.AlreadyExistsException
		require.ErrorAs(t, gen.CreateDatabase(ctx, dup), &ae)
		require.Contains(t, ae.Message, "already exists")
	})
	t.Run("create_database empty name", func(t *testing.T) {
		empty := hive_metastore.NewDatabase()
		var ioe *hive_metastore.InvalidObjectException
		require.ErrorAs(t, gen.CreateDatabase(ctx, empty), &ioe)
	})
	t.Run("get_database missing", func(t *testing.T) {
		var nso *hive_metastore.NoSuchObjectException
		_, err := gen.GetDatabase(ctx, db+"_missing")
		require.ErrorAs(t, err, &nso)
		require.Contains(t, nso.Message, "not found")
	})
	t.Run("create_table into missing database", func(t *testing.T) {
		tbl := hive_metastore.NewTable()
		tbl.TableName, tbl.DbName = "t", db+"_missing"
		tbl.Sd = &hive_metastore.StorageDescriptor{Location: "gs://bucket/wh/x"}
		var nso *hive_metastore.NoSuchObjectException
		require.ErrorAs(t, gen.CreateTable(ctx, tbl), &nso)
	})
	t.Run("create_table duplicate", func(t *testing.T) {
		tbl := hive_metastore.NewTable()
		tbl.TableName, tbl.DbName = "t", db
		tbl.Sd = &hive_metastore.StorageDescriptor{Location: "gs://bucket/wh/" + db + "/t"}
		var ae *hive_metastore.AlreadyExistsException
		require.ErrorAs(t, gen.CreateTable(ctx, tbl), &ae)
	})
	t.Run("get_table missing", func(t *testing.T) {
		req := hive_metastore.NewGetTableRequest()
		req.DbName, req.TblName = db, "missing"
		var nso *hive_metastore.NoSuchObjectException
		_, err := gen.GetTableReq(ctx, req)
		require.ErrorAs(t, err, &nso)
	})
	t.Run("drop non-empty database", func(t *testing.T) {
		var ioe *hive_metastore.InvalidOperationException
		require.ErrorAs(t, gen.DropDatabase(ctx, db, false, false), &ioe)
		// cascade=true then succeeds.
		require.NoError(t, gen.DropDatabase(ctx, db, false, true))
		_, err := c.GetDatabase(ctx, db)
		require.ErrorIs(t, err, hms.ErrNotFound, "get-after-drop")
	})
}

// TestHMSDepthPartitionPaging covers `p4`: partition listing/paging (max_parts,
// value-prefix forms, filter) and catalogue listing through the official client.
func TestHMSDepthPartitionPaging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := dialHMS(t)
	gen, closeGen := dialGeneratedHMS(t)
	defer closeGen()

	db := hmsName("page")
	require.NoError(t, c.CreateDatabase(ctx, hmsDB(db)))
	require.NoError(t, c.CreateTable(ctx, icebergTable(db, "p",
		[]*hms.FieldSchema{{Name: "id", Type: "bigint"}},
		[]*hms.FieldSchema{{Name: "ds", Type: "string"}, {Name: "region", Type: "string"}})))

	want := []*hms.Partition{
		{Values: []string{"2024", "us"}},
		{Values: []string{"2024", "eu"}},
		{Values: []string{"2025", "us"}},
		{Values: []string{"2025", "eu"}},
		{Values: []string{"2025", "ap"}},
	}
	require.NoError(t, c.AddPartitions(ctx, db, "p", want, false))

	// max_parts caps a full listing; -1 (any non-positive, Hive's "all") returns
	// every partition.
	all, err := c.GetPartitions(ctx, db, "p", -1)
	require.NoError(t, err)
	require.Len(t, all, len(want))
	capped, err := c.GetPartitions(ctx, db, "p", 2)
	require.NoError(t, err)
	require.Len(t, capped, 2, "max_parts must cap the result")

	names, err := c.GetPartitionNames(ctx, db, "p", -1)
	require.NoError(t, err)
	require.Len(t, names, len(want))
	namesCapped, err := c.GetPartitionNames(ctx, db, "p", 3)
	require.NoError(t, err)
	require.Len(t, namesCapped, 3)

	// Value-prefix paging (get_partitions_ps/get_partition_names_ps): the prefix
	// selects the partitions under a parent value, then max_parts is applied.
	ps, err := gen.GetPartitionsPs(ctx, db, "p", []string{"2025"}, -1)
	require.NoError(t, err)
	require.Len(t, ps, 3, "prefix 2025 selects its three regions")
	psCapped, err := gen.GetPartitionsPs(ctx, db, "p", []string{"2025"}, 2)
	require.NoError(t, err)
	require.Len(t, psCapped, 2, "max_parts applies after the prefix filter")
	psDeep, err := gen.GetPartitionsPs(ctx, db, "p", []string{"2025", "us"}, -1)
	require.NoError(t, err)
	require.Len(t, psDeep, 1)
	psNames, err := gen.GetPartitionNamesPs(ctx, db, "p", []string{"2024"}, -1)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"ds=2024/region=eu", "ds=2024/region=us"}, psNames)

	// Filter composed with the cap (get_partitions_by_filter).
	byFilter, err := c.GetPartitionsByFilter(ctx, db, "p", "region='us'", -1)
	require.NoError(t, err)
	require.Len(t, byFilter, 2)
	byFilterCapped, err := gen.GetPartitionsByFilter(ctx, db, "p", "region='us'", 1)
	require.NoError(t, err)
	require.Len(t, byFilterCapped, 1)

	// Catalogue listing includes the new database/table.
	allDBs, err := c.GetAllDatabases(ctx)
	require.NoError(t, err)
	require.Contains(t, allDBs, db)
	tables, err := c.GetAllTables(ctx, db)
	require.NoError(t, err)
	require.Contains(t, tables, "p")

	// An unknown table's partition listing is a declared NOT_FOUND.
	_, err = c.GetPartitions(ctx, db, "nope", -1)
	require.ErrorIs(t, err, hms.ErrNotFound)
}

// TestHMSDepthScale covers `p5`: a wide table schema and a large partition count,
// listed back complete and stable.
func TestHMSDepthScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale depth under -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	c := dialHMS(t)
	db := hmsName("scale")
	require.NoError(t, c.CreateDatabase(ctx, hmsDB(db)))

	const (
		columns = 200
		dsCount = 100
	)
	regions := []string{"us", "eu", "cn"}

	cols := make([]*hms.FieldSchema, columns)
	for i := range cols {
		cols[i] = &hms.FieldSchema{Name: fmt.Sprintf("c%03d", i), Type: "string"}
	}
	require.NoError(t, c.CreateTable(ctx, icebergTable(db, "wide", cols,
		[]*hms.FieldSchema{{Name: "ds", Type: "string"}, {Name: "region", Type: "string"}})))

	got, err := c.GetTable(ctx, db, "wide")
	require.NoError(t, err)
	require.NotNil(t, got.Storage)
	require.Len(t, got.Storage.Columns, columns, "wide schema must round-trip every column")

	parts := make([]*hms.Partition, 0, dsCount*len(regions))
	for d := 0; d < dsCount; d++ {
		for _, r := range regions {
			parts = append(parts, &hms.Partition{Values: []string{fmt.Sprintf("%03d", d), r}})
		}
	}
	require.NoError(t, c.AddPartitions(ctx, db, "wide", parts, false))

	names, err := c.GetPartitionNames(ctx, db, "wide", -1)
	require.NoError(t, err)
	require.Len(t, names, len(parts), "every partition must be listed")
	requireUnique(t, names)

	all, err := c.GetPartitions(ctx, db, "wide", -1)
	require.NoError(t, err)
	require.Len(t, all, len(parts))

	us, err := c.GetPartitionsByFilter(ctx, db, "wide", "region='us'", -1)
	require.NoError(t, err)
	require.Len(t, us, dsCount)

	// Repeated listing is stable (no reordering/duplication between calls).
	names2, err := c.GetPartitionNames(ctx, db, "wide", -1)
	require.NoError(t, err)
	require.Equal(t, names, names2)
}

// TestHMSDepthHive3Paths exercises the Hive-3.x serving paths the clean client's
// API does not surface — get_table_meta and alter_table_with_cascade — through
// the generated client, including a cascade rename's visibility.
func TestHMSDepthHive3Paths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := dialHMS(t)
	gen, closeGen := dialGeneratedHMS(t)
	defer closeGen()

	db := hmsName("hive3")
	require.NoError(t, c.CreateDatabase(ctx, hmsDB(db)))
	require.NoError(t, c.CreateTable(ctx, icebergTable(db, "events",
		[]*hms.FieldSchema{{Name: "id", Type: "bigint"}}, nil)))

	metas, err := gen.GetTableMeta(ctx, db, "events", nil)
	require.NoError(t, err)
	require.Len(t, metas, 1)
	require.Equal(t, "events", metas[0].TableName)
	require.Equal(t, "EXTERNAL_TABLE", metas[0].TableType)

	// Rename through alter_table_with_cascade(true): the old name stops
	// resolving, the new one resolves.
	req := hive_metastore.NewGetTableRequest()
	req.DbName, req.TblName = db, "events"
	res, err := gen.GetTableReq(ctx, req)
	require.NoError(t, err)
	stored := res.Table
	stored.TableName = "events_v2"
	require.NoError(t, gen.AlterTableWithCascade(ctx, db, "events", stored, true))

	_, err = c.GetTable(ctx, db, "events")
	require.ErrorIs(t, err, hms.ErrNotFound, "old name after cascade rename")
	renamed, err := c.GetTable(ctx, db, "events_v2")
	require.NoError(t, err)
	require.Equal(t, "events_v2", renamed.TableName)
}

// requireUnique asserts s has no duplicate entries.
func requireUnique(t *testing.T, s []string) {
	t.Helper()
	seen := make(map[string]bool, len(s))
	for _, v := range s {
		require.Falsef(t, seen[v], "duplicate entry %q", v)
		seen[v] = true
	}
}
