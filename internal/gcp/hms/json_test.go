package hms

import (
	"bytes"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
)

// TestJSONRoundTripTable verifies MarshalStruct/UnmarshalStruct round-trips a
// full Table losslessly: the canonical JSON preserves every field (including
// i64 and nested containers), so re-encoding produces the identical Thrift
// bytes.
func TestJSONRoundTripTable(t *testing.T) {
	fs := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("id")},
		{thrift.STRING, 2, rfStr("bigint")},
	}}
	serde := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("t")},
		{thrift.STRING, 2, rfStr("lib")},
		{thrift.MAP, 3, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("k"), rfStr("v")))},
	}}
	sd := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRUCT, rfStruct(fs))},
		{thrift.STRING, 2, rfStr("gs://b/wh/db/t")},
		{thrift.STRING, 3, rfStr("in")},
		{thrift.STRING, 4, rfStr("out")},
		{thrift.BOOL, 5, rfBool(false)},
		{thrift.I32, 6, rfI32(0)},
		{thrift.STRUCT, 7, rfStruct(serde)},
		{thrift.MAP, 10, rfMap(thrift.STRING, thrift.STRING)},
	}}
	tbl := refStruct{fields: []refField{
		{thrift.STRING, 1, rfStr("tbl")},
		{thrift.STRING, 2, rfStr("db")},
		{thrift.I32, 4, rfI32(1700000000)},
		{thrift.I64, 5, rfI64(0)}, // i64 must survive the JSON string encoding
		{thrift.STRUCT, 7, rfStruct(sd)},
		{thrift.MAP, 9, rfMap(thrift.STRING, thrift.STRING, rfEntry(rfStr("metadata_location"), rfStr("loc")))},
		{thrift.STRING, 12, rfStr("EXTERNAL_TABLE")},
	}}

	want := refEncode(tbl)

	// Thrift bytes -> Struct -> JSON -> Struct -> Thrift bytes.
	decoded := refDecode(t, want)
	js, err := MarshalStruct(decoded)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	restored, err := UnmarshalStruct(js)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := refReencode(t, restored)
	if !bytes.Equal(got, want) {
		t.Fatalf("JSON round-trip lost bytes:\n got  %x\n want %x", got, want)
	}

	// Field values survive the JSON hop.
	if restored.String(tblTableName) != "tbl" || restored.String(tblDBName) != "db" {
		t.Fatal("identity lost")
	}
	if restored.MapStrStr(9)["metadata_location"] != "loc" {
		t.Fatal("parameters lost")
	}
}

// TestJSONEmptyAndContainers exercises empty containers and nested lists/maps
// through the JSON layer.
func TestJSONEmptyAndContainers(t *testing.T) {
	src := refStruct{fields: []refField{
		{thrift.LIST, 1, rfList(thrift.STRING)},
		{thrift.SET, 2, rfList(thrift.STRUCT)},
		{thrift.MAP, 3, rfMap(thrift.STRING, thrift.I64)},
		{thrift.LIST, 4, rfList(thrift.LIST, rfList(thrift.STRING, rfStr("x")))},
	}}
	want := refEncode(src)
	decoded := refDecode(t, want)
	js, err := MarshalStruct(decoded)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := UnmarshalStruct(js)
	if err != nil {
		t.Fatal(err)
	}
	if got := refReencode(t, restored); !bytes.Equal(got, want) {
		t.Fatalf("empty container round-trip lost bytes:\n got  %x\n want %x", got, want)
	}
	// Round-trip through my codec again for sanity.
	if got := refReencode(t, restored); !bytes.Equal(got, refReencode(t, decoded)) {
		t.Fatal("restored struct does not match decoded struct")
	}
}
