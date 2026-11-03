package hms

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// adminTestTable builds a canonical Hive Table struct with a storage descriptor
// (sd.location), partition keys and a serde-ish field the mutators must leave
// untouched, so a round-trip proves only the addressed field changed.
func adminTestTable(db, name, location string) []byte {
	st := NewBuilder().
		Str(tblTableName, name).
		Str(tblDBName, db).
		Struct(tblSD, NewBuilder().
			ListStr(1, []string{"col1"}). // StorageDescriptor.cols (a field the mutators must preserve)
			Str(sdLocation, location).
			Str(4, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe").
			Build()).
		ListStr(tblPartitionKeys, []string{"dt"}).
		Build()
	raw, err := MarshalStruct(st)
	if err != nil {
		panic(err)
	}
	return raw
}

// adminTestPartition builds a canonical Hive Partition struct for value.
func adminTestPartition(db, name, value, location string) []byte {
	st := NewBuilder().
		ListStr(partValues, []string{value}).
		Str(partDBName, db).
		Str(partTableName, name).
		Struct(partSD, NewBuilder().Str(sdLocation, location).Build()).
		Build()
	raw, err := MarshalStruct(st)
	if err != nil {
		panic(err)
	}
	return raw
}

// tableLocation reads a stored table's storage-descriptor location.
func tableLocation(t *testing.T, store hmsstore.Store, db, name string) string {
	t.Helper()
	tbl, err := store.GetTable(context.Background(), db, name)
	if err != nil {
		t.Fatalf("get table %s.%s: %v", db, name, err)
	}
	st, err := UnmarshalStruct(tbl.TableJSON)
	if err != nil {
		t.Fatalf("decode table %s.%s: %v", db, name, err)
	}
	return st.Struct(tblSD).String(sdLocation)
}

// TestAdminMoveTable covers services.moveTableToDatabase against a real catalog
// store: the table moves to the destination database carrying its Hive struct,
// its partitions follow with their own dbName rewritten, and the source and
// destination conflicts are the store's sentinels.
func TestAdminMoveTable(t *testing.T) {
	ctx := context.Background()
	store := hmsstore.NewMemoryStore()
	admin := NewAdmin(store)

	for _, db := range []string{"src", "dst", "other"} {
		if err := store.CreateDatabase(ctx, hmsstore.Database{Name: db}); err != nil {
			t.Fatalf("create database %s: %v", db, err)
		}
	}
	tblRaw := adminTestTable("src", "t1", "gs://wh/src/t1")
	if err := store.CreateTable(ctx, "src", "t1", hmsstore.Table{DBName: "src", TableName: "t1", TableJSON: tblRaw}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, v := range []string{"2026-01-01", "2026-01-02"} {
		if err := store.CreatePartition(ctx, "src", "t1", hmsstore.Partition{
			DBName: "src", TableName: "t1", Values: []string{v},
			PartJSON: adminTestPartition("src", "t1", v, "gs://wh/src/t1/dt="+v),
		}); err != nil {
			t.Fatalf("create partition %s: %v", v, err)
		}
	}

	// A same-named table in the destination is a conflict, and nothing moves.
	if err := store.CreateTable(ctx, "dst", "t1", hmsstore.Table{DBName: "dst", TableName: "t1", TableJSON: adminTestTable("dst", "t1", "gs://wh/dst/t1")}); err != nil {
		t.Fatalf("create destination table: %v", err)
	}
	if err := admin.MoveTable(ctx, "src", "t1", "dst"); !errors.Is(err, hmsstore.ErrTableExists) {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}
	if err := store.DropTable(ctx, "dst", "t1"); err != nil {
		t.Fatalf("drop destination table: %v", err)
	}

	if err := admin.MoveTable(ctx, "src", "t1", "dst"); err != nil {
		t.Fatalf("MoveTable: %v", err)
	}
	if _, err := store.GetTable(ctx, "src", "t1"); !errors.Is(err, hmsstore.ErrTableNotFound) {
		t.Fatalf("source table still present: %v", err)
	}
	moved, err := store.GetTable(ctx, "dst", "t1")
	if err != nil {
		t.Fatalf("moved table missing: %v", err)
	}
	st, err := UnmarshalStruct(moved.TableJSON)
	if err != nil {
		t.Fatalf("decode moved table: %v", err)
	}
	if got := st.String(tblDBName); got != "dst" {
		t.Fatalf("moved table dbName = %q, want dst", got)
	}
	// Everything but dbName is untouched: the table keeps its location and its
	// storage descriptor's other fields.
	if got := st.Struct(tblSD).String(sdLocation); got != "gs://wh/src/t1" {
		t.Fatalf("move changed the table location to %q (a move copies no data)", got)
	}
	if got := st.Struct(tblSD).String(4); got != "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" {
		t.Fatalf("move dropped the storage descriptor's serde: %q", got)
	}
	if got := st.List(tblPartitionKeys); len(got) != 1 {
		t.Fatalf("move dropped the partition keys: %v", got)
	}

	parts, err := store.ListPartitions(ctx, "dst", "t1")
	if err != nil || len(parts) != 2 {
		t.Fatalf("partitions after move = %d, %v; want 2", len(parts), err)
	}
	for _, p := range parts {
		pst, err := UnmarshalStruct(p.PartJSON)
		if err != nil {
			t.Fatalf("decode partition: %v", err)
		}
		if got := pst.String(partDBName); got != "dst" {
			t.Fatalf("partition %v dbName = %q, want dst", p.Values, got)
		}
		if got := pst.String(partTableName); got != "t1" {
			t.Fatalf("partition %v tableName = %q, want t1", p.Values, got)
		}
		if got := pst.Struct(partSD).String(sdLocation); got != "gs://wh/src/t1/dt="+p.Values[0] {
			t.Fatalf("partition %v location changed to %q", p.Values, got)
		}
	}
	if _, err := store.ListPartitions(ctx, "src", "t1"); !errors.Is(err, hmsstore.ErrTableNotFound) {
		t.Fatalf("partitions still reachable under the source table: %v", err)
	}

	// Missing source / destination database are the store's sentinels.
	if err := admin.MoveTable(ctx, "dst", "missing", "other"); !errors.Is(err, hmsstore.ErrTableNotFound) {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if err := admin.MoveTable(ctx, "dst", "t1", "missing"); !errors.Is(err, hmsstore.ErrDatabaseNotFound) {
		t.Fatalf("expected ErrDatabaseNotFound, got %v", err)
	}

	// Moving into the table's own database is satisfied without touching
	// anything (and still reports a missing table).
	if err := admin.MoveTable(ctx, "dst", "t1", "dst"); err != nil {
		t.Fatalf("same-database move: %v", err)
	}
	if got := tableLocation(t, store, "dst", "t1"); got != "gs://wh/src/t1" {
		t.Fatalf("same-database move changed the table: %q", got)
	}
	if err := admin.MoveTable(ctx, "dst", "missing", "dst"); !errors.Is(err, hmsstore.ErrTableNotFound) {
		t.Fatalf("same-database move of a missing table: %v", err)
	}
}

// TestAdminMutationVisibleOverThrift proves the seam end-to-end in one process:
// a mutation the control plane applies through AdminStore is what the next Hive
// Metastore Thrift client on that catalog's listener observes. The server is the
// real serving plane over the same store, and the client speaks the real
// TBinaryProtocol codec (no shortcuts through the store).
func TestAdminMutationVisibleOverThrift(t *testing.T) {
	ctx := context.Background()
	store := hmsstore.NewMemoryStore()
	srv := NewServer("127.0.0.1:0", store)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	defer srv.Stop()

	addr := srv.Addr()
	if addr == nil {
		t.Fatal("server has no bound address")
	}
	client, err := dialThrift(ctx, addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	for _, db := range []string{"src", "dst"} {
		if err := client.createDatabase(ctx, db, "gs://wh/"+db); err != nil {
			t.Fatalf("create_database %s: %v", db, err)
		}
	}
	if err := client.createTable(ctx, "src", "t1", "gs://wh/src/t1"); err != nil {
		t.Fatalf("create_table: %v", err)
	}
	if err := client.addPartition(ctx, "src", "t1", "2026-01-01", "gs://wh/src/t1/dt=2026-01-01"); err != nil {
		t.Fatalf("add_partition: %v", err)
	}

	admin := NewAdmin(store)
	if err := admin.MoveTable(ctx, "src", "t1", "dst"); err != nil {
		t.Fatalf("MoveTable: %v", err)
	}
	// The Thrift client sees the table under its new database, with the catalog
	// state the move preserved.
	dbName, location, err := client.table(ctx, "dst", "t1")
	if err != nil {
		t.Fatalf("get_table after move: %v", err)
	}
	if dbName != "dst" || location != "gs://wh/src/t1" {
		t.Fatalf("moved table over Thrift: db=%q location=%q", dbName, location)
	}
	if _, _, err := client.table(ctx, "src", "t1"); err == nil {
		t.Fatal("get_table still finds the table under the source database")
	}
	pdb, ploc, err := client.partition(ctx, "dst", "t1", []string{"2026-01-01"})
	if err != nil {
		t.Fatalf("get_partition after move: %v", err)
	}
	if pdb != "dst" || ploc != "gs://wh/src/t1/dt=2026-01-01" {
		t.Fatalf("moved partition over Thrift: db=%q location=%q", pdb, ploc)
	}

	// alterLocation is equally visible: the table's storage descriptor root, and
	// the database's root directory.
	if err := admin.SetTableLocation(ctx, "dst", "t1", "gs://wh/new-root"); err != nil {
		t.Fatalf("SetTableLocation: %v", err)
	}
	if _, location, err := client.table(ctx, "dst", "t1"); err != nil || location != "gs://wh/new-root" {
		t.Fatalf("table location over Thrift = %q, %v", location, err)
	}
	if err := admin.SetDatabaseLocation(ctx, "dst", "gs://wh/dst-moved"); err != nil {
		t.Fatalf("SetDatabaseLocation: %v", err)
	}
	if got, err := client.database(ctx, "dst"); err != nil || got != "gs://wh/dst-moved" {
		t.Fatalf("database location over Thrift = %q, %v", got, err)
	}
}

// thriftClient is a minimal Hive Metastore Thrift client over the package's own
// codec: enough of the wire protocol to seed and read a catalog.
type thriftClient struct {
	transport thrift.TTransport
	proto     thrift.TProtocol
	seq       int32
}

func dialThrift(ctx context.Context, addr string) (*thriftClient, error) {
	transport := thrift.NewTSocketConf(addr, &thrift.TConfiguration{ConnectTimeout: 5 * time.Second})
	if err := transport.Open(); err != nil {
		return nil, err
	}
	return &thriftClient{transport: transport, proto: thrift.NewTBinaryProtocolTransport(transport)}, nil
}

func (c *thriftClient) Close() { _ = c.transport.Close() }

// call issues one Thrift call and returns its result struct (field 0 holds a
// void method's empty value, a struct method's reply).
func (c *thriftClient) call(ctx context.Context, method string, args *Struct) (*Struct, error) {
	c.seq++
	if err := c.proto.WriteMessageBegin(ctx, method, thrift.CALL, c.seq); err != nil {
		return nil, err
	}
	if err := WriteStruct(ctx, c.proto, args); err != nil {
		return nil, err
	}
	if err := c.proto.WriteMessageEnd(ctx); err != nil {
		return nil, err
	}
	if err := c.transport.Flush(ctx); err != nil {
		return nil, err
	}
	_, mtype, _, err := c.proto.ReadMessageBegin(ctx)
	if err != nil {
		return nil, err
	}
	res, err := ReadStruct(ctx, c.proto)
	if err != nil {
		return nil, err
	}
	if err := c.proto.ReadMessageEnd(ctx); err != nil {
		return nil, err
	}
	if mtype == thrift.EXCEPTION {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: "thrift exception"}
	}
	// A declared exception lands in a non-zero result field.
	for _, f := range res.Fields {
		if f.ID != 0 {
			return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: method + " raised a declared exception"}
		}
	}
	return res, nil
}

func (c *thriftClient) createDatabase(ctx context.Context, name, location string) error {
	args := NewBuilder().Struct(1, NewBuilder().Str(dbName, name).Str(dbLocationURI, location).Build()).Build()
	_, err := c.call(ctx, "create_database", args)
	return err
}

func (c *thriftClient) createTable(ctx context.Context, db, name, location string) error {
	tbl := NewBuilder().
		Str(tblTableName, name).
		Str(tblDBName, db).
		Struct(tblSD, NewBuilder().Str(sdLocation, location).Build()).
		Build()
	_, err := c.call(ctx, "create_table", NewBuilder().Struct(1, tbl).Build())
	return err
}

func (c *thriftClient) addPartition(ctx context.Context, db, name, value, location string) error {
	part := NewBuilder().
		ListStr(partValues, []string{value}).
		Str(partDBName, db).
		Str(partTableName, name).
		Struct(partSD, NewBuilder().Str(sdLocation, location).Build()).
		Build()
	_, err := c.call(ctx, "add_partition", NewBuilder().Struct(1, part).Build())
	return err
}

func (c *thriftClient) table(ctx context.Context, db, name string) (dbName, location string, err error) {
	res, err := c.call(ctx, "get_table", NewBuilder().Str(1, db).Str(2, name).Build())
	if err != nil {
		return "", "", err
	}
	tbl := res.Struct(0)
	return tbl.String(tblDBName), tbl.Struct(tblSD).String(sdLocation), nil
}

func (c *thriftClient) partition(ctx context.Context, db, name string, values []string) (dbName, location string, err error) {
	res, err := c.call(ctx, "get_partition", NewBuilder().Str(1, db).Str(2, name).ListStr(3, values).Build())
	if err != nil {
		return "", "", err
	}
	part := res.Struct(0)
	return part.String(partDBName), part.Struct(partSD).String(sdLocation), nil
}

func (c *thriftClient) database(ctx context.Context, name string) (location string, err error) {
	res, err := c.call(ctx, "get_database", NewBuilder().Str(1, name).Build())
	if err != nil {
		return "", err
	}
	return res.Struct(0).String(dbLocationURI), nil
}

// TestAdminAlterLocation covers services.alterLocation at all three levels: the
// database's root, the table's storage descriptor (which leaves its partitions
// alone), and one partition's storage descriptor.
func TestAdminAlterLocation(t *testing.T) {
	ctx := context.Background()
	store := hmsstore.NewMemoryStore()
	admin := NewAdmin(store)

	if err := store.CreateDatabase(ctx, hmsstore.Database{
		Name: "db1", LocationURI: "gs://wh/db1", Description: "kept", Parameters: map[string]string{"owner_note": "kept"}, Owner: "me",
	}); err != nil {
		t.Fatalf("create database: %v", err)
	}
	tblRaw := adminTestTable("db1", "t1", "gs://wh/db1/t1")
	if err := store.CreateTable(ctx, "db1", "t1", hmsstore.Table{DBName: "db1", TableName: "t1", TableJSON: tblRaw}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := store.CreatePartition(ctx, "db1", "t1", hmsstore.Partition{
		DBName: "db1", TableName: "t1", Values: []string{"2026-01-01"},
		PartJSON: adminTestPartition("db1", "t1", "2026-01-01", "gs://wh/db1/t1/dt=2026-01-01"),
	}); err != nil {
		t.Fatalf("create partition: %v", err)
	}

	// Database arm: only locationUri changes.
	if err := admin.SetDatabaseLocation(ctx, "db1", "gs://wh/db1-moved"); err != nil {
		t.Fatalf("SetDatabaseLocation: %v", err)
	}
	db, err := store.GetDatabase(ctx, "db1")
	if err != nil {
		t.Fatalf("get database: %v", err)
	}
	if db.LocationURI != "gs://wh/db1-moved" || db.Description != "kept" || db.Owner != "me" || db.Parameters["owner_note"] != "kept" {
		t.Fatalf("database after alter = %+v", db)
	}
	if got := tableLocation(t, store, "db1", "t1"); got != "gs://wh/db1/t1" {
		t.Fatalf("database alter cascaded into the table location: %q", got)
	}

	// Table arm: the table's sd.location changes, its partitions keep theirs.
	if err := admin.SetTableLocation(ctx, "db1", "t1", "gs://wh/table-moved"); err != nil {
		t.Fatalf("SetTableLocation: %v", err)
	}
	if got := tableLocation(t, store, "db1", "t1"); got != "gs://wh/table-moved" {
		t.Fatalf("table location = %q", got)
	}
	tbl, err := store.GetTable(ctx, "db1", "t1")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	st, err := UnmarshalStruct(tbl.TableJSON)
	if err != nil {
		t.Fatalf("decode table: %v", err)
	}
	if got := st.Struct(tblSD).String(4); got != "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" {
		t.Fatalf("table alter dropped the storage descriptor's serde: %q", got)
	}
	if got := st.List(tblPartitionKeys); len(got) != 1 {
		t.Fatalf("table alter dropped the partition keys: %v", got)
	}
	p, err := store.GetPartition(ctx, "db1", "t1", []string{"2026-01-01"})
	if err != nil {
		t.Fatalf("get partition: %v", err)
	}
	pst, err := UnmarshalStruct(p.PartJSON)
	if err != nil {
		t.Fatalf("decode partition: %v", err)
	}
	if got := pst.Struct(partSD).String(sdLocation); got != "gs://wh/db1/t1/dt=2026-01-01" {
		t.Fatalf("table alter cascaded into the partition location: %q", got)
	}

	// Partition arm.
	if err := admin.SetPartitionLocation(ctx, "db1", "t1", []string{"2026-01-01"}, "gs://wh/part-moved"); err != nil {
		t.Fatalf("SetPartitionLocation: %v", err)
	}
	p, err = store.GetPartition(ctx, "db1", "t1", []string{"2026-01-01"})
	if err != nil {
		t.Fatalf("get partition: %v", err)
	}
	pst, err = UnmarshalStruct(p.PartJSON)
	if err != nil {
		t.Fatalf("decode partition: %v", err)
	}
	if got := pst.Struct(partSD).String(sdLocation); got != "gs://wh/part-moved" {
		t.Fatalf("partition location = %q", got)
	}
	if got := tableLocation(t, store, "db1", "t1"); got != "gs://wh/table-moved" {
		t.Fatalf("partition alter changed the table location to %q", got)
	}

	// Missing resources are the store's sentinels, so the control plane can map
	// them onto NotFound.
	if err := admin.SetDatabaseLocation(ctx, "missing", "gs://wh/x"); !errors.Is(err, hmsstore.ErrDatabaseNotFound) {
		t.Fatalf("expected ErrDatabaseNotFound, got %v", err)
	}
	if err := admin.SetTableLocation(ctx, "db1", "missing", "gs://wh/x"); !errors.Is(err, hmsstore.ErrTableNotFound) {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
	if err := admin.SetPartitionLocation(ctx, "db1", "t1", []string{"nope"}, "gs://wh/x"); !errors.Is(err, hmsstore.ErrPartitionNotFound) {
		t.Fatalf("expected ErrPartitionNotFound, got %v", err)
	}

	// A table stored without a storage descriptor gains one rather than failing.
	if err := store.CreateTable(ctx, "db1", "bare", hmsstore.Table{DBName: "db1", TableName: "bare", TableJSON: []byte(`["o",[[1,["s","bare"]],[2,["s","db1"]]]]`)}); err != nil {
		t.Fatalf("create bare table: %v", err)
	}
	if err := admin.SetTableLocation(ctx, "db1", "bare", "gs://wh/bare"); err != nil {
		t.Fatalf("SetTableLocation on a table with no storage descriptor: %v", err)
	}
	if got := tableLocation(t, store, "db1", "bare"); got != "gs://wh/bare" {
		t.Fatalf("bare table location = %q", got)
	}
}
