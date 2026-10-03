package hms

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// --- test server harness ---

func startServer(t *testing.T, store hmsstore.Store) (addr string, stop func()) {
	t.Helper()
	srv := NewServer("127.0.0.1:0", store)
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	return srv.Addr().String(), func() { srv.Stop(); <-done }
}

// --- raw Thrift client helper ---

type reply struct {
	name    string
	msgType thrift.TMessageType
	result  *Struct
}

// hmsCall dials addr, sends one CALL for method with args, and reads the reply.
func hmsCall(t *testing.T, addr, method string, args *Struct) reply {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	sock := thrift.NewTSocketFromConnTimeout(conn, 2*time.Second)
	proto := thrift.NewTBinaryProtocolTransport(sock)
	ctx := context.Background()

	if err := proto.WriteMessageBegin(ctx, method, thrift.CALL, 1); err != nil {
		t.Fatalf("write msg begin: %v", err)
	}
	if err := WriteStruct(ctx, proto, args); err != nil {
		t.Fatalf("write args: %v", err)
	}
	if err := proto.WriteMessageEnd(ctx); err != nil {
		t.Fatalf("write msg end: %v", err)
	}
	if err := proto.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	name, msgType, _, err := proto.ReadMessageBegin(ctx)
	if err != nil {
		t.Fatalf("read msg begin: %v", err)
	}
	result, err := ReadStruct(ctx, proto)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if err := proto.ReadMessageEnd(ctx); err != nil {
		t.Fatalf("read msg end: %v", err)
	}
	return reply{name: name, msgType: msgType, result: result}
}

func strList(vs []string) []Value {
	out := make([]Value, 0, len(vs))
	for _, v := range vs {
		out = append(out, StringV(v))
	}
	return out
}

// testTable builds a realistic Iceberg Hive Table struct.
func testTable(db, tbl, metadataLocation string) *Struct {
	serde := NewBuilder().
		Str(1, tbl).
		Str(2, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe").
		MapStrStr(3, map[string]string{"serialization.format": "1"}).
		Build()
	sd := NewBuilder().
		ListStruct(1, nil).
		Str(2, "gs://bucket/wh/"+db+"/"+tbl).
		Str(3, "org.apache.hadoop.mapred.FileInputFormat").
		Str(4, "org.apache.hadoop.mapred.FileOutputFormat").
		Bool(5, false).
		I32(6, 0).
		Struct(7, serde).
		ListStr(8, nil).
		ListStruct(9, nil).
		MapStrStr(10, nil).
		Bool(12, false).
		Build()
	return NewBuilder().
		Str(tblTableName, tbl).
		Str(tblDBName, db).
		Str(3, "spark").
		I32(4, 1700000000).
		I32(5, 0).
		I32(6, 0).
		Struct(7, sd).
		ListStruct(8, nil).
		MapStrStr(9, map[string]string{
			"metadata_location": metadataLocation,
			"table_type":        "ICEBERG",
		}).
		Str(10, "").
		Str(11, "").
		Str(12, "EXTERNAL_TABLE").
		Bool(14, false).
		Bool(15, false).
		Build()
}

func TestServerEndToEnd(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	// create_database
	createDB := NewBuilder().Struct(1, NewBuilder().
		Str(dbName, "default").
		Str(dbLocationURI, "gs://bucket/wh/default").
		Build()).Build()
	r := hmsCall(t, addr, "create_database", createDB)
	if r.msgType != thrift.REPLY {
		t.Fatalf("create_database msgType=%d result=%+v", r.msgType, r.result)
	}

	// create_database again -> AlreadyExistsException (field 1)
	r = hmsCall(t, addr, "create_database", createDB)
	if r.msgType != thrift.REPLY {
		t.Fatalf("create_database dup msgType=%d", r.msgType)
	}
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 1 || f[0].V.T != thrift.STRUCT {
		t.Fatalf("expected AlreadyExistsException field 1, got %+v", f)
	}

	// get_database
	getDB := NewBuilder().Str(1, "default").Build()
	r = hmsCall(t, addr, "get_database", getDB)
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_database msgType=%d", r.msgType)
	}
	dbStruct := r.result.Struct(0)
	if dbStruct == nil || dbStruct.String(dbName) != "default" || dbStruct.String(dbLocationURI) != "gs://bucket/wh/default" {
		t.Fatalf("get_database wrong: %+v", r.result)
	}

	// create_table
	ml1 := "gs://bucket/wh/default/t1/metadata/00001.metadata.json"
	createTbl := NewBuilder().Struct(1, testTable("default", "t1", ml1)).Build()
	r = hmsCall(t, addr, "create_table", createTbl)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("create_table failed: type=%d result=%+v", r.msgType, r.result)
	}

	// get_table
	getTbl := NewBuilder().Str(1, "default").Str(2, "t1").Build()
	r = hmsCall(t, addr, "get_table", getTbl)
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_table msgType=%d", r.msgType)
	}
	gotTbl := r.result.Struct(0)
	if gotTbl == nil {
		t.Fatal("get_table returned no table")
	}
	if gotTbl.String(tblTableName) != "t1" || gotTbl.String(tblDBName) != "default" {
		t.Fatalf("get_table identity wrong: %+v", gotTbl)
	}
	if m := gotTbl.MapStrStr(9); m["metadata_location"] != ml1 {
		t.Fatalf("get_table params lost metadata_location: %v", m)
	}
	// the round-tripped table must echo every field, incl. sd.serdeInfo.
	if sd := gotTbl.Struct(7); sd == nil || sd.String(3) != "org.apache.hadoop.mapred.FileInputFormat" {
		t.Fatalf("get_table sd lost: %+v", sd)
	}

	// get_all_tables
	r = hmsCall(t, addr, "get_all_tables", NewBuilder().Str(1, "default").Build())
	if names := r.result.List(0); len(names) != 1 || names[0].Str != "t1" {
		t.Fatalf("get_all_tables wrong: %+v", r.result)
	}

	// get_table_objects_by_name (Iceberg listTables critical path)
	r = hmsCall(t, addr, "get_table_objects_by_name", NewBuilder().Str(1, "default").Add(2, ListV(thrift.STRING, strList([]string{"t1"}))).Build())
	if tables := r.result.List(0); len(tables) != 1 || tables[0].S == nil || tables[0].S.String(tblTableName) != "t1" {
		t.Fatalf("get_table_objects_by_name wrong: %+v", r.result)
	}

	// alter_table — full-Table overwrite with a new metadata_location.
	ml2 := "gs://bucket/wh/default/t1/metadata/00002.metadata.json"
	alter := NewBuilder().Str(1, "default").Str(2, "t1").Struct(3, testTable("default", "t1", ml2)).Build()
	r = hmsCall(t, addr, "alter_table", alter)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("alter_table failed: type=%d result=%+v", r.msgType, r.result)
	}
	r = hmsCall(t, addr, "get_table", getTbl)
	if m := r.result.Struct(0).MapStrStr(9); m["metadata_location"] != ml2 {
		t.Fatalf("alter_table did not overwrite metadata_location: %v", m)
	}

	// rename via alter_table (new table name in the Table struct).
	renamed := testTable("default", "t1_renamed", ml2)
	alter = NewBuilder().Str(1, "default").Str(2, "t1").Struct(3, renamed).Build()
	r = hmsCall(t, addr, "alter_table", alter)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("rename via alter_table failed: type=%d result=%+v", r.msgType, r.result)
	}
	r = hmsCall(t, addr, "get_table", NewBuilder().Str(1, "default").Str(2, "t1_renamed").Build())
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("renamed table not found: %+v", r.result)
	}

	// drop_table
	dropTbl := NewBuilder().Str(1, "default").Str(2, "t1_renamed").Bool(3, false).Build()
	r = hmsCall(t, addr, "drop_table", dropTbl)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("drop_table failed: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_table", NewBuilder().Str(1, "default").Str(2, "t1_renamed").Build())
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 2 { // NoSuchObjectException is field 2
		t.Fatalf("expected NoSuchObjectException after drop, got %+v", f)
	}
}

func TestServerLocks(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	// lock
	comp := NewBuilder().I32(1, 3).I32(2, 2).Str(3, "default").Str(4, "tbl").Build()
	req := NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(comp)})).Str(3, "spark").Str(4, "host").Build()
	r := hmsCall(t, addr, "lock", NewBuilder().Struct(1, req).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("lock msgType=%d", r.msgType)
	}
	resp := r.result.Struct(0)
	if resp == nil {
		t.Fatal("lock returned no LockResponse")
	}
	lockID := resp.I64(lockRespLockID)
	if lockID == 0 {
		t.Fatal("lock returned lockid 0")
	}
	if resp.I32(lockRespState) != int32(hmsstore.LockStateAcquired) {
		t.Fatalf("lock state = %d, want ACQUIRED", resp.I32(lockRespState))
	}

	// check_lock -> ACQUIRED
	check := NewBuilder().I64(1, lockID).Build()
	r = hmsCall(t, addr, "check_lock", NewBuilder().Struct(1, check).Build())
	if resp := r.result.Struct(0); resp == nil || resp.I32(lockRespState) != int32(hmsstore.LockStateAcquired) {
		t.Fatalf("check_lock wrong: %+v", r.result)
	}

	// unlock
	unlock := NewBuilder().I64(1, lockID).Build()
	r = hmsCall(t, addr, "unlock", NewBuilder().Struct(1, unlock).Build())
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("unlock failed: %+v", r.result)
	}

	// check_lock after unlock -> NOT_ACQUIRED (absent, not an error)
	r = hmsCall(t, addr, "check_lock", NewBuilder().Struct(1, check).Build())
	if resp := r.result.Struct(0); resp == nil || resp.I32(lockRespState) != int32(hmsstore.LockStateNotAcquired) {
		t.Fatalf("check_lock after unlock should be NOT_ACQUIRED, got %+v", r.result)
	}
}

func TestServerNotificationsAndFunctions(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	r := hmsCall(t, addr, "get_current_notificationEventId", &Struct{})
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_current_notificationEventId msgType=%d", r.msgType)
	}
	if resp := r.result.Struct(0); resp == nil || resp.I64(1) != 0 {
		t.Fatalf("get_current_notificationEventId wrong: %+v", r.result)
	}

	r = hmsCall(t, addr, "get_next_notification", &Struct{})
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("get_next_notification wrong: %+v", r.result)
	}

	r = hmsCall(t, addr, "get_functions", NewBuilder().Str(1, "default").Str(2, "").Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_functions msgType=%d", r.msgType)
	}
	if l := r.result.List(0); len(l) != 0 {
		t.Fatalf("get_functions should be empty: %+v", l)
	}

	r = hmsCall(t, addr, "get_function", NewBuilder().Str(1, "default").Str(2, "fn").Build())
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 2 { // NoSuchObjectException is field 2
		t.Fatalf("get_function should throw NoSuchObjectException, got %+v", f)
	}
}

func TestServerUnknownMethod(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	r := hmsCall(t, addr, "definitely_not_a_method", &Struct{})
	if r.msgType != thrift.EXCEPTION {
		t.Fatalf("unknown method should be EXCEPTION, got %d", r.msgType)
	}
	if r.result == nil || r.result.String(1) == "" {
		t.Fatalf("unknown method should carry a TApplicationException message, got %+v", r.result)
	}
}

// testPartitionedTable builds an EXTERNAL_TABLE with the given partition keys
// (Table.partitionKeys, field 8) and warehouse root (sd.location). A key may be
// "name" (string) or "name:type".
func testPartitionedTable(db, tbl, root string, keys ...string) *Struct {
	fs := make([]*Struct, 0, len(keys))
	for _, k := range keys {
		name, typ := k, "string"
		if i := strings.Index(k, ":"); i >= 0 {
			name, typ = k[:i], k[i+1:]
		}
		fs = append(fs, NewBuilder().Str(fsName, name).Str(fsType, typ).Str(3, "").Build())
	}
	serde := NewBuilder().
		Str(1, tbl).
		Str(2, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe").
		MapStrStr(3, map[string]string{"serialization.format": "1"}).
		Build()
	sd := NewBuilder().
		ListStruct(1, nil).
		Str(sdLocation, root).
		Str(3, "org.apache.hadoop.mapred.FileInputFormat").
		Str(4, "org.apache.hadoop.mapred.FileOutputFormat").
		Bool(5, false).
		I32(6, 0).
		Struct(7, serde).
		ListStr(8, nil).
		ListStruct(9, nil).
		MapStrStr(10, nil).
		Bool(12, false).
		Build()
	return NewBuilder().
		Str(tblTableName, tbl).
		Str(tblDBName, db).
		Str(3, "spark").
		I32(4, 1700000000).
		I32(5, 0).
		I32(6, 0).
		Struct(tblSD, sd).
		ListStruct(tblPartitionKeys, fs).
		MapStrStr(9, nil).
		Str(10, "").
		Str(11, "").
		Str(12, "EXTERNAL_TABLE").
		Build()
}

// testPartition builds a Partition struct with the given values and location.
func testPartition(db, tbl, location string, values ...string) *Struct {
	return NewBuilder().
		Add(partValues, ListV(thrift.STRING, strList(values))).
		Str(partDBName, db).
		Str(partTableName, tbl).
		I32(partCreateTime, 1700000000).
		I32(partLastAccessTime, 0).
		Struct(partSD, NewBuilder().Str(sdLocation, location).Build()).
		MapStrStr(partParameters, map[string]string{"numFiles": "3"}).
		Build()
}

func TestServerPartitions(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	// Databases + partitioned tables.
	if r := hmsCall(t, addr, "create_database", NewBuilder().Struct(1, NewBuilder().Str(dbName, "default").Str(dbLocationURI, "gs://bucket/wh/default").Build()).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create_database: %+v", r.result)
	}
	root := "gs://bucket/wh/default/tp"
	for _, tbl := range []string{"tp", "tp2"} {
		r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, testPartitionedTable("default", tbl, "gs://bucket/wh/default/"+tbl, "ds", "hr")).Build())
		if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
			t.Fatalf("create_table %s: type=%d result=%+v", tbl, r.msgType, r.result)
		}
	}

	// add_partition returns the added partition.
	p1 := testPartition("default", "tp", root+"/ds=2024/hr=01", "2024", "01")
	r := hmsCall(t, addr, "add_partition", NewBuilder().Struct(1, p1).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("add_partition msgType=%d result=%+v", r.msgType, r.result)
	}
	if got := r.result.Struct(0); got == nil || got.String(partTableName) != "tp" {
		t.Fatalf("add_partition result wrong: %+v", r.result)
	}
	p2 := testPartition("default", "tp", root+"/ds=2024/hr=02", "2024", "02")
	if r := hmsCall(t, addr, "add_partition", NewBuilder().Struct(1, p2).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("add_partition 2: %+v", r.result)
	}
	// Duplicate -> AlreadyExistsException (field 2 for the add clause).
	if r := hmsCall(t, addr, "add_partition", NewBuilder().Struct(1, p1).Build()); len(r.result.Fields) != 1 || r.result.Fields[0].ID != 2 {
		t.Fatalf("add_partition duplicate should throw AlreadyExistsException at field 2, got %+v", r.result.Fields)
	}

	// get_partition / get_partition_by_name.
	getP := NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList([]string{"2024", "01"}))).Build()
	r = hmsCall(t, addr, "get_partition", getP)
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("get_partition: %+v", r.result)
	}
	if m := r.result.Struct(0).MapStrStr(partParameters); m["numFiles"] != "3" {
		t.Fatalf("get_partition params lost: %v", m)
	}
	r = hmsCall(t, addr, "get_partition_by_name", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds=2024/hr=01").Build())
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("get_partition_by_name: %+v", r.result)
	}

	// get_partitions / get_partition_names (sorted, max_parts=-1).
	r = hmsCall(t, addr, "get_partitions", NewBuilder().Str(1, "default").Str(2, "tp").I16(3, -1).Build())
	if l := r.result.List(0); len(l) != 2 {
		t.Fatalf("get_partitions should return 2: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partition_names", NewBuilder().Str(1, "default").Str(2, "tp").I16(3, -1).Build())
	if l := r.result.List(0); len(l) != 2 || l[0].Str != "ds=2024/hr=01" || l[1].Str != "ds=2024/hr=02" {
		t.Fatalf("get_partition_names wrong: %+v", r.result)
	}

	// get_partitions_ps / get_partition_names_ps (prefix).
	r = hmsCall(t, addr, "get_partitions_ps", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList([]string{"2024", "01"}))).I16(4, -1).Build())
	if l := r.result.List(0); len(l) != 1 {
		t.Fatalf("get_partitions_ps prefix should return 1: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partition_names_ps", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList([]string{"2024"}))).I16(4, -1).Build())
	if l := r.result.List(0); len(l) != 2 {
		t.Fatalf("get_partition_names_ps prefix should return 2: %+v", r.result)
	}
	// max_parts is applied AFTER the prefix filter (Hive semantics).
	r = hmsCall(t, addr, "get_partition_names_ps", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList([]string{"2024", "02"}))).I16(4, 1).Build())
	if l := r.result.List(0); len(l) != 1 || l[0].Str != "ds=2024/hr=02" {
		t.Fatalf("get_partition_names_ps max_parts ordering wrong: %+v", r.result)
	}

	// get_num_partitions_by_filter / get_partitions_by_filter.
	r = hmsCall(t, addr, "get_num_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds='2024'").Build())
	if r.result.I32(0) != 2 {
		t.Fatalf("get_num_partitions_by_filter = %d, want 2", r.result.I32(0))
	}
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "hr='02'").I16(4, -1).Build())
	if l := r.result.List(0); len(l) != 1 {
		t.Fatalf("get_partitions_by_filter hr='02' should return 1: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds = '2024' and hr >= '02'").I16(4, -1).Build())
	if l := r.result.List(0); len(l) != 1 {
		t.Fatalf("get_partitions_by_filter compound should return 1: %+v", r.result)
	}
	// Bad filter -> MetaException (field 1 for this clause).
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds ==").I16(4, -1).Build())
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 1 {
		t.Fatalf("bad filter should throw MetaException at field 1, got %+v", f)
	}
	// A trailing operator must not panic the server (regression: index out of range).
	for _, bad := range []string{"ds=", "ds =", "ds = '2024' and hr =", "a = 'x' and b="} {
		r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, bad).I16(4, -1).Build())
		if f := r.result.Fields; len(f) != 1 || f[0].ID != 1 {
			t.Fatalf("malformed filter %q should throw MetaException at field 1, got %+v", bad, f)
		}
	}
	// Server is still healthy after the malformed filters.
	r = hmsCall(t, addr, "get_partitions", NewBuilder().Str(1, "default").Str(2, "tp").I16(3, -1).Build())
	if len(r.result.List(0)) != 2 {
		t.Fatalf("server unhealthy after malformed filters: %+v", r.result)
	}
	// Filter operators: like, or, parentheses, and precedence.
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "hr like '0%'").I16(4, -1).Build())
	if len(r.result.List(0)) != 2 {
		t.Fatalf("like filter should return 2: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "(ds = '2024' and hr = '01') or hr = '02'").I16(4, -1).Build())
	if len(r.result.List(0)) != 2 {
		t.Fatalf("parenthesised or filter should return 2: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "hr = '01' or hr = '02' and ds = 'x'").I16(4, -1).Build())
	if len(r.result.List(0)) != 1 {
		t.Fatalf("precedence filter should return 1: %+v", r.result)
	}

	// partition_name_to_vals / _spec.
	r = hmsCall(t, addr, "partition_name_to_vals", NewBuilder().Str(1, "ds=2024/hr=01").Build())
	if l := r.result.List(0); len(l) != 2 || l[0].Str != "2024" || l[1].Str != "01" {
		t.Fatalf("partition_name_to_vals wrong: %+v", r.result)
	}
	r = hmsCall(t, addr, "partition_name_to_spec", NewBuilder().Str(1, "ds=2024/hr=01").Build())
	if m := r.result.MapStrStr(0); m["ds"] != "2024" || m["hr"] != "01" {
		t.Fatalf("partition_name_to_spec wrong: %+v", r.result)
	}

	// get_partitions_pspec groups both partitions under one shared SD.
	r = hmsCall(t, addr, "get_partitions_pspec", NewBuilder().Str(1, "default").Str(2, "tp").I32(3, -1).Build())
	specs := r.result.List(0)
	if len(specs) != 1 || specs[0].S == nil {
		t.Fatalf("get_partitions_pspec should return 1 spec: %+v", r.result)
	}
	shared := specs[0].S.Struct(4)
	if shared == nil || len(shared.List(1)) != 2 || shared.Struct(2) == nil {
		t.Fatalf("get_partitions_pspec shared SD wrong: %+v", specs[0].S)
	}
	if rel := shared.List(1)[0].S.String(4); rel != "ds=2024/hr=01" {
		t.Fatalf("get_partitions_pspec relativePath = %q", rel)
	}

	// add_partitions_pspec round-trips: drop both, re-add from the spec.
	for _, vals := range [][]string{{"2024", "01"}, {"2024", "02"}} {
		if r := hmsCall(t, addr, "drop_partition", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList(vals))).Bool(4, false).Build()); r.msgType != thrift.REPLY || !r.result.Bool(0) {
			t.Fatalf("drop_partition: %+v", r.result)
		}
	}
	r = hmsCall(t, addr, "add_partitions_pspec", NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(specs[0].S)})).Build())
	if r.msgType != thrift.REPLY || r.result.I32(0) != 2 {
		t.Fatalf("add_partitions_pspec count = %d, want 2", r.result.I32(0))
	}
	r = hmsCall(t, addr, "get_partitions", NewBuilder().Str(1, "default").Str(2, "tp").I16(3, -1).Build())
	if l := r.result.List(0); len(l) != 2 {
		t.Fatalf("partitions not restored by pspec: %+v", r.result)
	}
	// The reconstructed location must be root + relativePath.
	r = hmsCall(t, addr, "get_partition", getP)
	if sd := r.result.Struct(0).Struct(partSD); sd == nil || sd.String(sdLocation) != root+"/ds=2024/hr=01" {
		t.Fatalf("pspec-restored location wrong: %+v", r.result.Struct(0))
	}

	// alter_partition (full overwrite, keyed by new_part.values).
	altered := testPartition("default", "tp", root+"/ds=2024/hr=01", "2024", "01")
	altered.Set(partParameters, MapV(thrift.STRING, thrift.STRING, []MapEntry{{K: StringV("numFiles"), V: StringV("9")}}))
	if r := hmsCall(t, addr, "alter_partition", NewBuilder().Str(1, "default").Str(2, "tp").Struct(3, altered).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("alter_partition: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partition", getP)
	if m := r.result.Struct(0).MapStrStr(partParameters); m["numFiles"] != "9" {
		t.Fatalf("alter_partition did not overwrite: %v", m)
	}

	// rename_partition: (2024,01) -> (2024,03).
	renamed := testPartition("default", "tp", root+"/ds=2024/hr=03", "2024", "03")
	if r := hmsCall(t, addr, "rename_partition", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList([]string{"2024", "01"}))).Struct(4, renamed).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("rename_partition: %+v", r.result)
	}
	if r := hmsCall(t, addr, "get_partition_by_name", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds=2024/hr=03").Build()); r.result.Struct(0) == nil {
		t.Fatalf("renamed partition not found: %+v", r.result)
	}
	if r := hmsCall(t, addr, "get_partition", getP); len(r.result.Fields) != 1 || r.result.Fields[0].ID != 2 {
		t.Fatalf("old partition should be gone (NoSuchObjectException field 2), got %+v", r.result.Fields)
	}

	// add_partitions on tp2 -> count, then drop_partition_by_name.
	b1 := testPartition("default", "tp2", "gs://bucket/wh/default/tp2/ds=2025/hr=01", "2025", "01")
	b2 := testPartition("default", "tp2", "gs://bucket/wh/default/tp2/ds=2025/hr=02", "2025", "02")
	r = hmsCall(t, addr, "add_partitions", NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(b1), StructV(b2)})).Build())
	if r.msgType != thrift.REPLY || r.result.I32(0) != 2 {
		t.Fatalf("add_partitions count = %d, want 2", r.result.I32(0))
	}
	if r := hmsCall(t, addr, "drop_partition_by_name", NewBuilder().Str(1, "default").Str(2, "tp2").Str(3, "ds=2025/hr=01").Bool(4, false).Build()); r.msgType != thrift.REPLY || !r.result.Bool(0) {
		t.Fatalf("drop_partition_by_name: %+v", r.result)
	}
	// Missing drop -> NoSuchObjectException (field 1 for the drop clause).
	if r := hmsCall(t, addr, "drop_partition", NewBuilder().Str(1, "default").Str(2, "tp2").Add(3, ListV(thrift.STRING, strList([]string{"2025", "01"}))).Bool(4, false).Build()); len(r.result.Fields) != 1 || r.result.Fields[0].ID != 1 {
		t.Fatalf("missing drop should throw NoSuchObjectException at field 1, got %+v", r.result.Fields)
	}

	// get_partitions_by_names tolerates missing names (returns found only).
	r = hmsCall(t, addr, "get_partitions_by_names", NewBuilder().Str(1, "default").Str(2, "tp2").Add(3, ListV(thrift.STRING, strList([]string{"ds=2025/hr=02", "ds=2025/hr=zz"}))).Build())
	if l := r.result.List(0); len(l) != 1 {
		t.Fatalf("get_partitions_by_names should return 1 found: %+v", r.result)
	}

	// partition_name_has_valid_characters stays a valid true stub.
	if r := hmsCall(t, addr, "partition_name_has_valid_characters", NewBuilder().Add(1, ListV(thrift.STRING, strList([]string{"2024"}))).Bool(2, false).Build()); !r.result.Bool(0) {
		t.Fatalf("partition_name_has_valid_characters should be true: %+v", r.result)
	}
}

func TestServerPartitionFiltersAndSpecs(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	if r := hmsCall(t, addr, "create_database", NewBuilder().Struct(1, NewBuilder().Str(dbName, "default").Str(dbLocationURI, "gs://bucket/wh/default").Build()).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create_database: %+v", r.result)
	}
	if r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, testPartitionedTable("default", "tp", "gs://bucket/wh/default/tp", "ds", "hr")).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create tp: %+v", r.result)
	}
	if r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, testPartitionedTable("default", "tn", "gs://bucket/wh/default/tn", "n:int")).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create tn: %+v", r.result)
	}
	for _, p := range []*Struct{
		testPartition("default", "tp", "gs://bucket/wh/default/tp/ds=2024/hr=01", "2024", "01"),
		testPartition("default", "tp", "gs://bucket/wh/default/tp/ds=2024/hr=02", "2024", "02"),
		testPartition("default", "tn", "gs://bucket/wh/default/tn/n=9", "9"),
		testPartition("default", "tn", "gs://bucket/wh/default/tn/n=10", "10"),
	} {
		if r := hmsCall(t, addr, "add_partition", NewBuilder().Struct(1, p).Build()); r.msgType != thrift.REPLY {
			t.Fatalf("add_partition: %+v", r.result)
		}
	}

	// int column: numeric comparison ("10" > "9"); a lexicographic compare would
	// return 0 rows instead of 1.
	r := hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tn").Str(3, "n > '9'").I16(4, -1).Build())
	if l := r.result.List(0); len(l) != 1 || l[0].S.List(partValues)[0].Str != "10" {
		t.Fatalf("int partition filter should be numeric: %+v", r.result)
	}
	// string column: lexicographic ("01" < "1").
	r = hmsCall(t, addr, "get_partitions_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "hr < '1'").I16(4, -1).Build())
	if len(r.result.List(0)) != 2 {
		t.Fatalf("string partition filter should be lexicographic: %+v", r.result)
	}

	// get_part_specs_by_filter -> one shared-SD spec holding both matches.
	r = hmsCall(t, addr, "get_part_specs_by_filter", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds = '2024'").I32(4, -1).Build())
	if specs := r.result.List(0); len(specs) != 1 || specs[0].S.Struct(4) == nil || len(specs[0].S.Struct(4).List(1)) != 2 {
		t.Fatalf("get_part_specs_by_filter wrong: %+v", r.result)
	}

	// alter_partitions updates every listed partition.
	withParams := func(p *Struct, v string) *Struct {
		p.Set(partParameters, MapV(thrift.STRING, thrift.STRING, []MapEntry{{K: StringV("numFiles"), V: StringV(v)}}))
		return p
	}
	a1 := withParams(testPartition("default", "tp", "gs://bucket/wh/default/tp/ds=2024/hr=01", "2024", "01"), "7")
	a2 := withParams(testPartition("default", "tp", "gs://bucket/wh/default/tp/ds=2024/hr=02", "2024", "02"), "7")
	if r := hmsCall(t, addr, "alter_partitions", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRUCT, []Value{StructV(a1), StructV(a2)})).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("alter_partitions: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_partition_by_name", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds=2024/hr=01").Build())
	if m := r.result.Struct(0).MapStrStr(partParameters); m["numFiles"] != "7" {
		t.Fatalf("alter_partitions did not apply: %v", m)
	}

	// add_partitions_req: ifNotExists=false on an existing partition fails the
	// whole call before writing (AlreadyExists at field 2).
	reqArgs := func(ifNotExists, needResult bool) *Struct {
		req := NewBuilder().Str(1, "default").Str(2, "tp").
			Add(3, ListV(thrift.STRUCT, []Value{StructV(testPartition("default", "tp", "gs://bucket/wh/default/tp/ds=2024/hr=01", "2024", "01"))})).
			Bool(4, ifNotExists).Bool(5, needResult).Build()
		return NewBuilder().Struct(1, req).Build()
	}
	if r := hmsCall(t, addr, "add_partitions_req", reqArgs(false, true)); len(r.result.Fields) != 1 || r.result.Fields[0].ID != 2 {
		t.Fatalf("add_partitions_req duplicate should throw AlreadyExists at field 2, got %+v", r.result.Fields)
	}
	if r := hmsCall(t, addr, "add_partitions_req", reqArgs(true, false)); r.msgType != thrift.REPLY {
		t.Fatalf("add_partitions_req ifNotExists: %+v", r.result)
	}

	// append_partition derives sd.location from the table root + partition name.
	r = hmsCall(t, addr, "append_partition", NewBuilder().Str(1, "default").Str(2, "tp").Add(3, ListV(thrift.STRING, strList([]string{"2024", "05"}))).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("append_partition: %+v", r.result)
	}
	if sd := r.result.Struct(0).Struct(partSD); sd == nil || sd.String(sdLocation) != "gs://bucket/wh/default/tp/ds=2024/hr=05" {
		t.Fatalf("append_partition location wrong: %+v", r.result.Struct(0))
	}

	// add_partitions_pspec composing form (field 5: PartitionListComposingSpec).
	part := testPartition("default", "tp", "gs://bucket/wh/default/tp/ds=2025/hr=01", "2025", "01")
	comp := NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(part)})).Build()
	spec := NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "gs://bucket/wh/default/tp").Add(5, StructV(comp)).Build()
	r = hmsCall(t, addr, "add_partitions_pspec", NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(spec)})).Build())
	if r.msgType != thrift.REPLY || r.result.I32(0) != 1 {
		t.Fatalf("add_partitions_pspec composing count = %d, want 1", r.result.I32(0))
	}
	if r := hmsCall(t, addr, "get_partition_by_name", NewBuilder().Str(1, "default").Str(2, "tp").Str(3, "ds=2025/hr=01").Build()); r.result.Struct(0) == nil {
		t.Fatalf("composing pspec partition not found: %+v", r.result)
	}
}

func TestServerPartitionDeferredFailLoud(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	for _, m := range []string{"drop_partitions_req", "get_partition_values", "get_partitions_by_expr", "exchange_partition", "exchange_partitions"} {
		r := hmsCall(t, addr, m, &Struct{})
		if r.msgType != thrift.EXCEPTION {
			t.Fatalf("%s should fail loud with a TApplicationException, got msgType=%d", m, r.msgType)
		}
	}
}

// metaTable builds a minimal Hive Table with the fields get_table_meta reads:
// tableType (12), the optional "comment" parameter (9), and catName (16).
func metaTable(db, tbl, tableType, comment, catName string) *Struct {
	b := NewBuilder().
		Str(tblTableName, tbl).
		Str(tblDBName, db).
		Str(3, "spark").
		Struct(tblSD, NewBuilder().Str(sdLocation, "gs://bucket/wh/"+db+"/"+tbl).Build()).
		ListStruct(tblPartitionKeys, nil)
	if comment != "" {
		b.MapStrStr(tblParameters, map[string]string{tableCommentKey: comment})
	}
	b.Str(tblTableType, tableType)
	if catName != "" {
		b.Str(tblCatName, catName)
	}
	return b.Build()
}

func TestServerGetTableMeta(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	for _, db := range []string{"default", "other"} {
		r := hmsCall(t, addr, "create_database", NewBuilder().Struct(1, NewBuilder().
			Str(dbName, db).Str(dbLocationURI, "gs://bucket/wh/"+db).Build()).Build())
		if r.msgType != thrift.REPLY {
			t.Fatalf("create_database %s: %+v", db, r.result)
		}
	}
	create := func(tbl *Struct) {
		r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, tbl).Build())
		if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
			t.Fatalf("create_table failed: type=%d result=%+v", r.msgType, r.result)
		}
	}
	create(metaTable("default", "t1", "MANAGED_TABLE", "first", ""))
	create(metaTable("default", "t2", "EXTERNAL_TABLE", "", ""))
	create(metaTable("other", "t1", "MANAGED_TABLE", "", "hive"))

	meta := func(dbPattern, tblPattern string, types []string) []*Struct {
		args := NewBuilder().Str(1, dbPattern).Str(2, tblPattern)
		if types != nil {
			args.Add(3, ListV(thrift.STRING, strList(types)))
		}
		r := hmsCall(t, addr, "get_table_meta", args.Build())
		if r.msgType != thrift.REPLY {
			t.Fatalf("get_table_meta(%q,%q,%v): msgType=%d result=%+v", dbPattern, tblPattern, types, r.msgType, r.result)
		}
		vals := r.result.List(0)
		out := make([]*Struct, 0, len(vals))
		for _, v := range vals {
			if v.S == nil {
				t.Fatalf("TableMeta is not a struct: %+v", v)
			}
			out = append(out, v.S)
		}
		return out
	}

	// Empty patterns match every db/table; order is deterministic (db, then table).
	all := meta("", "", nil)
	if len(all) != 3 {
		t.Fatalf("want 3 TableMeta, got %d: %+v", len(all), all)
	}
	if all[0].String(tmDBName) != "default" || all[0].String(tmTableName) != "t1" ||
		all[0].String(tmTableType) != "MANAGED_TABLE" || all[0].String(tmComments) != "first" {
		t.Fatalf("meta[0] wrong: %+v", all[0])
	}
	if _, ok := all[0].Get(tmCatName); ok {
		t.Fatalf("meta[0] should omit an empty catName: %+v", all[0])
	}
	if all[1].String(tmTableName) != "t2" || all[1].String(tmTableType) != "EXTERNAL_TABLE" {
		t.Fatalf("meta[1] wrong: %+v", all[1])
	}
	if _, ok := all[1].Get(tmComments); ok {
		t.Fatalf("meta[1] should omit an empty comment: %+v", all[1])
	}
	if all[2].String(tmDBName) != "other" || all[2].String(tmTableName) != "t1" || all[2].String(tmCatName) != "hive" {
		t.Fatalf("meta[2] wrong: %+v", all[2])
	}

	// db pattern narrows to one database.
	if got := meta("default", "", nil); len(got) != 2 {
		t.Fatalf("db filter: want 2, got %d", len(got))
	}
	// table pattern spans databases (regex semantics).
	if got := meta("", "^t1$", nil); len(got) != 2 || got[0].String(tmDBName) != "default" || got[1].String(tmDBName) != "other" {
		t.Fatalf("table filter wrong: %+v", got)
	}
	// tbl_types filters on the exact table type.
	if got := meta(".*", "t.*", []string{"EXTERNAL_TABLE"}); len(got) != 1 || got[0].String(tmTableName) != "t2" {
		t.Fatalf("type filter wrong: %+v", got)
	}
	if got := meta("", "", []string{"MANAGED_TABLE"}); len(got) != 2 {
		t.Fatalf("managed filter: want 2, got %d", len(got))
	}
	// no match -> empty list, not an error.
	if got := meta("nope", "", nil); len(got) != 0 {
		t.Fatalf("no-match should be empty, got %+v", got)
	}
}

func TestServerAlterTableWithCascade(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	createDB := NewBuilder().Struct(1, NewBuilder().
		Str(dbName, "default").Str(dbLocationURI, "gs://bucket/wh/default").Build()).Build()
	if r := hmsCall(t, addr, "create_database", createDB); r.msgType != thrift.REPLY {
		t.Fatalf("create_database: %+v", r.result)
	}
	ml1 := "gs://bucket/wh/default/t1/metadata/00001.metadata.json"
	if r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, testTable("default", "t1", ml1)).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create_table: %+v", r.result)
	}

	// alter_table_with_cascade carries the extra bool at field 4; it behaves as
	// alter_table (cascade does not touch stored partitions — MP9).
	ml2 := "gs://bucket/wh/default/t1/metadata/00002.metadata.json"
	alter := NewBuilder().Str(1, "default").Str(2, "t1").Struct(3, testTable("default", "t1", ml2)).Bool(4, true).Build()
	if r := hmsCall(t, addr, "alter_table_with_cascade", alter); r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("alter_table_with_cascade: type=%d result=%+v", r.msgType, r.result)
	}
	r := hmsCall(t, addr, "get_table", NewBuilder().Str(1, "default").Str(2, "t1").Build())
	if m := r.result.Struct(0).MapStrStr(tblParameters); m["metadata_location"] != ml2 {
		t.Fatalf("cascade alter did not overwrite table: %v", m)
	}

	// Rename through the cascade path.
	renamed := testTable("default", "t1_renamed", ml2)
	alter = NewBuilder().Str(1, "default").Str(2, "t1").Struct(3, renamed).Bool(4, true).Build()
	if r := hmsCall(t, addr, "alter_table_with_cascade", alter); r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("cascade rename: type=%d result=%+v", r.msgType, r.result)
	}
	r = hmsCall(t, addr, "get_table", NewBuilder().Str(1, "default").Str(2, "t1_renamed").Build())
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("renamed table not found: %+v", r.result)
	}
}

// TestServerSetUgi covers set_ugi(user, group_names) -> list<string>: Hive
// clients issue it on connect, and strict clients (hms-client-go) refuse to
// connect if it is missing. The emulator echoes the requested groups.
func TestServerSetUgi(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	args := NewBuilder().Str(1, "alice").Add(2, ListV(thrift.STRING, strList([]string{"g1", "g2"}))).Build()
	r := hmsCall(t, addr, "set_ugi", args)
	if r.msgType != thrift.REPLY {
		t.Fatalf("set_ugi: type=%d result=%+v", r.msgType, r.result)
	}
	l := r.result.List(0)
	if len(l) != 2 || l[0].Str != "g1" || l[1].Str != "g2" {
		t.Fatalf("set_ugi groups not echoed: %+v", r.result)
	}
}

// TestServerGetTableReq covers get_table_req(GetTableRequest) -> GetTableResult,
// the Hive 2.3+/Hive 4 request-struct form of get_table (get_table_req carries no
// legacy fallback in Hive 4 IDL-generated clients).
func TestServerGetTableReq(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	if r := hmsCall(t, addr, "create_database", NewBuilder().Struct(1, NewBuilder().
		Str(dbName, "default").Str(dbLocationURI, "gs://bucket/wh/default").Build()).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create_database: %+v", r.result)
	}
	ml := "gs://bucket/wh/default/t1/metadata/00001.metadata.json"
	if r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, testTable("default", "t1", ml)).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create_table: %+v", r.result)
	}

	// engine(10)/id(11) are required Hive-4 additions the schema-free codec
	// ignores; catName(4) is omitted (default catalog).
	req := NewBuilder().Str(reqDBName, "default").Str(reqTblName, "t1").Str(10, "hive").I64(11, -1).Build()
	r := hmsCall(t, addr, "get_table_req", NewBuilder().Struct(1, req).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_table_req: type=%d result=%+v", r.msgType, r.result)
	}
	inner := r.result.Struct(0)
	if inner == nil {
		t.Fatalf("get_table_req missing result struct: %+v", r.result)
	}
	tbl := inner.Struct(reqResultTable)
	if tbl == nil || tbl.String(tblTableName) != "t1" {
		t.Fatalf("get_table_req table wrong: %+v", inner)
	}
	if m := tbl.MapStrStr(tblParameters); m["metadata_location"] != ml {
		t.Fatalf("get_table_req params wrong: %v", m)
	}

	// Missing table -> NoSuchObjectException at result field 2.
	req = NewBuilder().Str(reqDBName, "default").Str(reqTblName, "nope").Build()
	r = hmsCall(t, addr, "get_table_req", NewBuilder().Struct(1, req).Build())
	if len(r.result.Fields) != 1 || r.result.Fields[0].ID != 2 {
		t.Fatalf("get_table_req missing should throw NoSuchObjectException at field 2: %+v", r.result.Fields)
	}
}

// TestServerGetTableObjectsByNameReq covers
// get_table_objects_by_name_req(GetTablesRequest) -> GetTablesResult, the
// request-struct form of get_table_objects_by_name. Missing names are skipped.
func TestServerGetTableObjectsByNameReq(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	if r := hmsCall(t, addr, "create_database", NewBuilder().Struct(1, NewBuilder().
		Str(dbName, "default").Str(dbLocationURI, "gs://bucket/wh/default").Build()).Build()); r.msgType != thrift.REPLY {
		t.Fatalf("create_database: %+v", r.result)
	}
	for _, tbl := range []string{"t1", "t2"} {
		md := "gs://bucket/wh/default/" + tbl + "/metadata/00001.metadata.json"
		if r := hmsCall(t, addr, "create_table", NewBuilder().Struct(1, testTable("default", tbl, md)).Build()); r.msgType != thrift.REPLY {
			t.Fatalf("create_table %s: %+v", tbl, r.result)
		}
	}

	req := NewBuilder().Str(reqDBName, "default").
		Add(reqTblNames, ListV(thrift.STRING, strList([]string{"t1", "missing", "t2"}))).Build()
	r := hmsCall(t, addr, "get_table_objects_by_name_req", NewBuilder().Struct(1, req).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_table_objects_by_name_req: type=%d result=%+v", r.msgType, r.result)
	}
	inner := r.result.Struct(0)
	if inner == nil {
		t.Fatalf("missing result struct: %+v", r.result)
	}
	tables := inner.List(reqResultTables)
	if len(tables) != 2 {
		t.Fatalf("want 2 tables (missing skipped), got %d: %+v", len(tables), inner)
	}
	if tables[0].S == nil || tables[0].S.String(tblTableName) != "t1" ||
		tables[1].S == nil || tables[1].S.String(tblTableName) != "t2" {
		t.Fatalf("table order/names wrong: %+v", tables)
	}
}
