package hms

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
)

// A reference TBinaryProtocol encoder (apache/thrift primitives, independent of
// this package's codec) is used to "capture" authoritative byte fixtures for
// the round-trip tests. writeRefStruct writes fields in the given order and
// returns the wire bytes, exactly as a generated Java client would.

type refField struct {
	typ thrift.TType
	id  int16
	val refValue
}

// refValue is one of: bool, int8, int16, int32, int64, float64, string,
// refStruct, refList, refMap.
type refValue struct {
	kind string
	b    bool
	i8   int8
	i16  int16
	i32  int32
	i64  int64
	f64  float64
	str  string
	st   refStruct
	lst  refList
	m    refMap
}

type refStruct struct{ fields []refField }

type refList struct {
	elem  thrift.TType
	items []refValue
}

type refMap struct {
	k, v  thrift.TType
	items []refMapEntry
}

type refMapEntry struct{ k, v refValue }

func rfBool(v bool) refValue      { return refValue{kind: "bool", b: v} }
func rfByte(v int8) refValue      { return refValue{kind: "i8", i8: v} }
func rfI16(v int16) refValue      { return refValue{kind: "i16", i16: v} }
func rfI32(v int32) refValue      { return refValue{kind: "i32", i32: v} }
func rfI64(v int64) refValue      { return refValue{kind: "i64", i64: v} }
func rfDouble(v float64) refValue { return refValue{kind: "f64", f64: v} }
func rfStr(v string) refValue     { return refValue{kind: "str", str: v} }
func rfStruct(s refStruct) refValue {
	return refValue{kind: "struct", st: s}
}
func rfList(e thrift.TType, items ...refValue) refValue {
	return refValue{kind: "list", lst: refList{elem: e, items: items}}
}
func rfMap(k, v thrift.TType, items ...refMapEntry) refValue {
	return refValue{kind: "map", m: refMap{k: k, v: v, items: items}}
}
func rfEntry(k, v refValue) refMapEntry { return refMapEntry{k: k, v: v} }

func refWriteValue(p thrift.TProtocol, v refValue) error {
	ctx := context.Background()
	switch v.kind {
	case "bool":
		return p.WriteBool(ctx, v.b)
	case "i8":
		return p.WriteByte(ctx, v.i8)
	case "i16":
		return p.WriteI16(ctx, v.i16)
	case "i32":
		return p.WriteI32(ctx, v.i32)
	case "i64":
		return p.WriteI64(ctx, v.i64)
	case "f64":
		return p.WriteDouble(ctx, v.f64)
	case "str":
		return p.WriteString(ctx, v.str)
	case "struct":
		return refWriteStruct(p, v.st)
	case "list":
		if err := p.WriteListBegin(ctx, v.lst.elem, len(v.lst.items)); err != nil {
			return err
		}
		for _, it := range v.lst.items {
			if err := refWriteValue(p, it); err != nil {
				return err
			}
		}
		return p.WriteListEnd(ctx)
	case "map":
		if err := p.WriteMapBegin(ctx, v.m.k, v.m.v, len(v.m.items)); err != nil {
			return err
		}
		for _, it := range v.m.items {
			if err := refWriteValue(p, it.k); err != nil {
				return err
			}
			if err := refWriteValue(p, it.v); err != nil {
				return err
			}
		}
		return p.WriteMapEnd(ctx)
	}
	return nil
}

func refWriteStruct(p thrift.TProtocol, s refStruct) error {
	ctx := context.Background()
	if err := p.WriteStructBegin(ctx, ""); err != nil {
		return err
	}
	for _, f := range s.fields {
		if err := p.WriteFieldBegin(ctx, "", f.typ, f.id); err != nil {
			return err
		}
		if err := refWriteValue(p, f.val); err != nil {
			return err
		}
		if err := p.WriteFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	return p.WriteStructEnd(ctx)
}

// refEncode renders a struct's wire bytes with the reference encoder.
func refEncode(s refStruct) []byte {
	buf := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(buf)
	if err := refWriteStruct(p, s); err != nil {
		panic(err)
	}
	p.Flush(context.Background())
	return append([]byte(nil), buf.Bytes()...)
}

// refDecode reads a struct with this package's codec.
func refDecode(t *testing.T, b []byte) *Struct {
	t.Helper()
	buf := thrift.NewTMemoryBuffer()
	buf.Write(b)
	p := thrift.NewTBinaryProtocolTransport(buf)
	s, err := ReadStruct(context.Background(), p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return s
}

// refReencode writes a struct with this package's codec.
func refReencode(t *testing.T, s *Struct) []byte {
	t.Helper()
	buf := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(buf)
	if err := WriteStruct(context.Background(), p, s); err != nil {
		t.Fatalf("encode: %v", err)
	}
	p.Flush(context.Background())
	return append([]byte(nil), buf.Bytes()...)
}

func assertRoundTrip(t *testing.T, name string, src refStruct, want []byte, check func(*Struct)) {
	t.Helper()
	decoded := refDecode(t, want)
	if check != nil {
		check(decoded)
	}
	got := refReencode(t, decoded)
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: encode(decode(bytes)) != bytes\n got  %x\n want %x", name, got, want)
	}
	// Also round-trip through the reference encoder's own struct (sanity).
	if src.fields != nil && len(decoded.Fields) != len(src.fields) {
		t.Fatalf("%s: decoded %d fields, want %d", name, len(decoded.Fields), len(src.fields))
	}
}

// TestGoldenFieldSchema is a fully hand-computed fixture: FieldSchema
// {1:"id", 2:"string", 3:"id column"} in strict TBinaryProtocol.
func TestGoldenFieldSchema(t *testing.T) {
	golden, err := hex.DecodeString("0b00010000000269640b000200000006737472696e670b000300000009696420636f6c756d6e00")
	if err != nil {
		t.Fatal(err)
	}
	assertRoundTrip(t, "FieldSchema", refStruct{}, golden, func(s *Struct) {
		if s.String(1) != "id" || s.String(2) != "string" || s.String(3) != "id column" {
			t.Fatalf("field values wrong: name=%q type=%q comment=%q", s.String(1), s.String(2), s.String(3))
		}
	})
}

// TestRoundTripScalars covers every scalar type the codec handles.
func TestRoundTripScalars(t *testing.T) {
	src := refStruct{fields: []refField{
		{thrift.BOOL, 1, rfBool(true)},
		{thrift.BYTE, 2, rfByte(-3)},
		{thrift.I16, 3, rfI16(-1234)},
		{thrift.I32, 4, rfI32(123456)},
		{thrift.I64, 5, rfI64(1234567890123)},
		{thrift.DOUBLE, 6, rfDouble(3.141592653589793)},
		{thrift.STRING, 7, rfStr("hello world")},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "Scalars", src, got, func(s *Struct) {
		if v, _ := s.Get(1); !v.B {
			t.Fatal("bool wrong")
		}
		if v, _ := s.Get(2); v.I8 != -3 {
			t.Fatal("i8 wrong")
		}
		if v, _ := s.Get(3); v.I16 != -1234 {
			t.Fatal("i16 wrong")
		}
		if v, _ := s.Get(4); v.I32 != 123456 {
			t.Fatal("i32 wrong")
		}
		if v, _ := s.Get(5); v.I64 != 1234567890123 {
			t.Fatal("i64 wrong")
		}
		if v, _ := s.Get(6); v.F64 != 3.141592653589793 {
			t.Fatal("double wrong")
		}
		if s.String(7) != "hello world" {
			t.Fatal("string wrong")
		}
	})
}

// TestRoundTripContainers covers list/set/map, including nested containers and
// struct-typed elements.
func TestRoundTripContainers(t *testing.T) {
	src := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRING, rfStr("a"), rfStr("b"), rfStr("c"))},
		{thrift.SET, 2, rfList(thrift.I32, rfI32(1), rfI32(2))},
		{thrift.MAP, 3, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("k1"), rfStr("v1")), rfEntry(rfStr("k2"), rfStr("v2")))},
		{thrift.LIST, 4, rfList(thrift.LIST, rfList(thrift.STRING, rfStr("x"), rfStr("y")), rfList(thrift.STRING, rfStr("z")))},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "Containers", src, got, func(s *Struct) {
		if l := s.List(1); len(l) != 3 || l[0].Str != "a" {
			t.Fatalf("list wrong: %+v", l)
		}
		if l := s.List(2); len(l) != 2 {
			t.Fatalf("set wrong: %+v", l)
		}
		if m := s.MapStrStr(3); m["k1"] != "v1" || m["k2"] != "v2" {
			t.Fatalf("map wrong: %+v", m)
		}
		if l := s.List(4); len(l) != 2 {
			t.Fatalf("nested list wrong: %+v", l)
		}
	})
}

// TestRoundTripSerDeInfo exercises the SerDeInfo struct + a map field.
func TestRoundTripSerDeInfo(t *testing.T) {
	src := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("iceberg")},
		{thrift.STRING, 2, rfStr("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")},
		{thrift.MAP, 3, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("serialization.format"), rfStr("1")))},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "SerDeInfo", src, got, func(s *Struct) {
		if s.String(1) != "iceberg" || s.String(2) != "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" {
			t.Fatal("serde fields wrong")
		}
		if m := s.MapStrStr(3); m["serialization.format"] != "1" {
			t.Fatal("serde params wrong")
		}
	})
}

// TestRoundTripStorageDescriptor exercises the full StorageDescriptor struct.
func TestRoundTripStorageDescriptor(t *testing.T) {
	fs := refStruct{fields: []refField{{thrift.STRING, 1, rfStr("id")}, {thrift.STRING, 2, rfStr("int")}}}
	serde := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("t")},
		{thrift.STRING, 2, rfStr("lib")},
		{thrift.MAP, 3, rfMap(thrift.STRING, thrift.STRING)},
	}}
	src := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRUCT, rfStruct(fs))},
		{thrift.STRING, 2, rfStr("gs://bucket/wh/db/tbl")},
		{thrift.STRING, 3, rfStr("InputFormat")},
		{thrift.STRING, 4, rfStr("OutputFormat")},
		{thrift.BOOL, 5, rfBool(false)},
		{thrift.I32, 6, rfI32(0)},
		{thrift.STRUCT, 7, rfStruct(serde)},
		{thrift.LIST, 8, rfList(thrift.STRING)},
		{thrift.LIST, 9, rfList(thrift.STRUCT)},
		{thrift.MAP, 10, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("k"), rfStr("v")))},
		{thrift.BOOL, 12, rfBool(false)},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "StorageDescriptor", src, got, func(s *Struct) {
		if s.String(2) != "gs://bucket/wh/db/tbl" {
			t.Fatal("sd location wrong")
		}
		if s.String(3) != "InputFormat" || s.String(4) != "OutputFormat" {
			t.Fatal("sd formats wrong")
		}
		if sd := s.Struct(7); sd == nil || sd.String(2) != "lib" {
			t.Fatal("sd serdeInfo wrong")
		}
		if m := s.MapStrStr(10); m["k"] != "v" {
			t.Fatal("sd parameters wrong")
		}
	})
}

// TestRoundTripDatabase exercises the Database struct (the decomposed store
// fields plus ownerName/ownerType).
func TestRoundTripDatabase(t *testing.T) {
	src := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("default")},
		{thrift.STRING, 2, rfStr("desc")},
		{thrift.STRING, 3, rfStr("file:/tmp/default")},
		{thrift.MAP, 4, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("a"), rfStr("b")))},
		{thrift.STRING, 6, rfStr("owner")},
		{thrift.I32, 7, rfI32(1)},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "Database", src, got, func(s *Struct) {
		if s.String(1) != "default" || s.String(2) != "desc" || s.String(3) != "file:/tmp/default" {
			t.Fatal("db fields wrong")
		}
		if m := s.MapStrStr(4); m["a"] != "b" {
			t.Fatal("db params wrong")
		}
		if s.String(6) != "owner" || s.I32(7) != 1 {
			t.Fatal("db owner wrong")
		}
	})
}

// TestRoundTripTable exercises the FULL Table struct: every field populated,
// including nested StorageDescriptor -> SerDeInfo/FieldSchema/Order and
// partitionKeys. This is the lossless-round-trip guard F5 demands.
func TestRoundTripTable(t *testing.T) {
	fs := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("id")},
		{thrift.STRING, 2, rfStr("bigint")},
		{thrift.STRING, 3, rfStr("the id")},
	}}
	serde := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("tbl")},
		{thrift.STRING, 2, rfStr("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")},
		{thrift.MAP, 3, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("serialization.format"), rfStr("1")))},
	}}
	order := refStruct{fields: []refField{{thrift.STRING, 1, rfStr("id")}, {thrift.I32, 2, rfI32(1)}}}
	sd := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRUCT, rfStruct(fs))},
		{thrift.STRING, 2, rfStr("gs://bucket/wh/db/tbl")},
		{thrift.STRING, 3, rfStr("org.apache.hadoop.mapred.FileInputFormat")},
		{thrift.STRING, 4, rfStr("org.apache.hadoop.mapred.FileOutputFormat")},
		{thrift.BOOL, 5, rfBool(false)},
		{thrift.I32, 6, rfI32(0)},
		{thrift.STRUCT, 7, rfStruct(serde)},
		{thrift.LIST, 8, rfList(thrift.STRING)},
		{thrift.LIST, 9, rfList(thrift.STRUCT, rfStruct(order))},
		{thrift.MAP, 10, rfMap(thrift.STRING, thrift.STRING)},
		{thrift.BOOL, 12, rfBool(false)},
	}}
	tbl := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("iceberg_table")},
		{thrift.STRING, 2, rfStr("default")},
		{thrift.STRING, 3, rfStr("spark")},
		{thrift.I32, 4, rfI32(1700000000)},
		{thrift.I32, 5, rfI32(0)},
		{thrift.I32, 6, rfI32(0)},
		{thrift.STRUCT, 7, rfStruct(sd)},
		{thrift.LIST, 8, rfList(thrift.STRUCT)},
		{thrift.MAP, 9, rfMap(thrift.STRING, thrift.STRING,
			rfEntry(rfStr("metadata_location"), rfStr("gs://bucket/wh/db/tbl/metadata/00001.metadata.json")),
			rfEntry(rfStr("previous_metadata_location"), rfStr("gs://bucket/wh/db/tbl/metadata/00000.metadata.json")),
			rfEntry(rfStr("table_type"), rfStr("ICEBERG")),
		)},
		{thrift.STRING, 10, rfStr("")},
		{thrift.STRING, 11, rfStr("")},
		{thrift.STRING, 12, rfStr("EXTERNAL_TABLE")},
		{thrift.BOOL, 14, rfBool(false)},
		{thrift.BOOL, 15, rfBool(false)},
		{thrift.STRING, 16, rfStr("hive")},
	}}
	got := refEncode(tbl)
	assertRoundTrip(t, "Table", tbl, got, func(s *Struct) {
		if s.String(tblTableName) != "iceberg_table" || s.String(tblDBName) != "default" {
			t.Fatal("table identity wrong")
		}
		if m := s.MapStrStr(9); m["metadata_location"] == "" || m["table_type"] != "ICEBERG" {
			t.Fatalf("table parameters wrong: %v", m)
		}
		sdDecoded := s.Struct(7)
		if sdDecoded == nil || sdDecoded.String(2) != "gs://bucket/wh/db/tbl" {
			t.Fatal("table sd wrong")
		}
		if sdDecoded.String(3) != "org.apache.hadoop.mapred.FileInputFormat" {
			t.Fatal("table sd inputFormat wrong")
		}
		if serdeDecoded := sdDecoded.Struct(7); serdeDecoded == nil || serdeDecoded.String(2) == "" {
			t.Fatal("table sd serde wrong")
		}
		if s.String(12) != "EXTERNAL_TABLE" || s.String(16) != "hive" {
			t.Fatal("table type/catName wrong")
		}
	})
}

// TestRoundTripPartition covers the FULL Partition struct: every field
// populated (values, identity, times, sd, parameters, privileges, catName).
// The store persists this struct verbatim, so it must round-trip losslessly.
func TestRoundTripPartition(t *testing.T) {
	priv := refStruct{fields: []refField{
		{thrift.MAP, 1, rfMap(thrift.STRING, thrift.STRUCT,
			rfEntry(rfStr("user"), rfStruct(refStruct{fields: []refField{{thrift.LIST, 1, rfList(thrift.STRING, rfStr("SELECT"))}}})))},
	}}
	src := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRING, rfStr("2024"), rfStr("01"))},
		{thrift.STRING, 2, rfStr("default")},
		{thrift.STRING, 3, rfStr("tbl")},
		{thrift.I32, 4, rfI32(1700000000)},
		{thrift.I32, 5, rfI32(1700000100)},
		{thrift.STRUCT, 6, rfStruct(refStruct{fields: []refField{
			{thrift.STRING, 2, rfStr("gs://bucket/wh/default/tbl/2024=01")},
			{thrift.STRING, 3, rfStr("org.apache.hadoop.mapred.FileInputFormat")},
		}})},
		{thrift.MAP, 7, rfMap(thrift.STRING, thrift.STRING,
			rfEntry(rfStr("numFiles"), rfStr("3")),
			rfEntry(rfStr("totalSize"), rfStr("1024")),
		)},
		{thrift.STRUCT, 8, rfStruct(priv)},
		{thrift.STRING, 9, rfStr("hive")},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "Partition", src, got, func(s *Struct) {
		if l := s.List(partValues); len(l) != 2 || l[0].Str != "2024" || l[1].Str != "01" {
			t.Fatal("partition values wrong")
		}
		if s.String(partDBName) != "default" || s.String(partTableName) != "tbl" {
			t.Fatal("partition identity wrong")
		}
		if s.I32(partCreateTime) != 1700000000 || s.I32(partLastAccessTime) != 1700000100 {
			t.Fatal("partition times wrong")
		}
		if sd := s.Struct(partSD); sd == nil || sd.String(sdLocation) == "" {
			t.Fatal("partition sd wrong")
		}
		if m := s.MapStrStr(partParameters); m["numFiles"] != "3" || m["totalSize"] != "1024" {
			t.Fatalf("partition parameters wrong: %v", m)
		}
		if s.Struct(partPrivileges) == nil {
			t.Fatal("partition privileges wrong")
		}
		if s.String(9) != "hive" {
			t.Fatal("partition catName wrong")
		}
	})
}

// TestRoundTripLocks covers LockRequest/LockComponent/LockResponse.
func TestRoundTripLocks(t *testing.T) {
	comp := refStruct{fields: []refField{
		{thrift.I32, 1, rfI32(3)}, // EXCLUSIVE
		{thrift.I32, 2, rfI32(2)}, // TABLE
		{thrift.STRING, 3, rfStr("default")},
		{thrift.STRING, 4, rfStr("tbl")},
	}}
	req := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRUCT, rfStruct(comp))},
		{thrift.I64, 2, rfI64(0)},
		{thrift.STRING, 3, rfStr("spark")},
		{thrift.STRING, 4, rfStr("host")},
	}}
	got := refEncode(req)
	assertRoundTrip(t, "LockRequest", req, got, func(s *Struct) {
		comps := s.List(lrComponent)
		if len(comps) != 1 {
			t.Fatal("lock components wrong")
		}
		c := comps[0].S
		if c.String(lcDBName) != "default" || c.String(lcTableName) != "tbl" {
			t.Fatal("lock component wrong")
		}
		if s.String(lrUser) != "spark" || s.String(lrHostname) != "host" {
			t.Fatal("lock request user/hostname wrong")
		}
	})

	resp := refStruct{fields: []refField{
		{thrift.I64, 1, rfI64(42)},
		{thrift.I32, 2, rfI32(1)}, // ACQUIRED
	}}
	got = refEncode(resp)
	assertRoundTrip(t, "LockResponse", resp, got, func(s *Struct) {
		if s.I64(lockRespLockID) != 42 || s.I32(lockRespState) != 1 {
			t.Fatal("lock response wrong")
		}
	})
}

// TestRoundTripTableMeta pins the wire shape get_table_meta returns
// (hive_metastore.thrift TableMeta): required dbName/tableName/tableType plus
// the optional comments (4) and catName (5).
func TestRoundTripTableMeta(t *testing.T) {
	src := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("default")},
		{thrift.STRING, 2, rfStr("t1")},
		{thrift.STRING, 3, rfStr("MANAGED_TABLE")},
		{thrift.STRING, 4, rfStr("the comment")},
		{thrift.STRING, 5, rfStr("hive")},
	}}
	got := refEncode(src)
	assertRoundTrip(t, "TableMeta", src, got, func(s *Struct) {
		if s.String(tmDBName) != "default" || s.String(tmTableName) != "t1" || s.String(tmTableType) != "MANAGED_TABLE" {
			t.Fatal("TableMeta identity wrong")
		}
		if s.String(tmComments) != "the comment" || s.String(tmCatName) != "hive" {
			t.Fatal("TableMeta comments/catName wrong")
		}
	})
}

// TestBuildTableMeta pins the field-omission rules: comments and catName are
// emitted only when the stored Table supplies them.
func TestBuildTableMeta(t *testing.T) {
	full := buildTableMeta("default", "t1", "MANAGED_TABLE", NewBuilder().
		MapStrStr(tblParameters, map[string]string{tableCommentKey: "c"}).
		Str(tblCatName, "hive").
		Build())
	if full.String(tmDBName) != "default" || full.String(tmTableName) != "t1" || full.String(tmTableType) != "MANAGED_TABLE" {
		t.Fatalf("identity wrong: %+v", full)
	}
	if full.String(tmComments) != "c" || full.String(tmCatName) != "hive" {
		t.Fatalf("optional fields wrong: %+v", full)
	}
	bare := buildTableMeta("default", "t2", "EXTERNAL_TABLE", &Struct{})
	if _, ok := bare.Get(tmComments); ok {
		t.Fatalf("empty comment should be omitted: %+v", bare)
	}
	if _, ok := bare.Get(tmCatName); ok {
		t.Fatalf("empty catName should be omitted: %+v", bare)
	}
}
