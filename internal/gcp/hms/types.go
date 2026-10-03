// Package hms implements the DPMS Hive Metastore serving plane over Thrift: a
// TBinaryProtocol codec + struct layer + method dispatch, backed by
// internal/gcp/store/hms. It is the GCP analogue of the AWS Glue table-metadata
// serving path, but over the real Apache Hive Metastore protocol
// (hive_metastore.thrift) rather than Glue's REST surface.
//
// The codec is generic and lossless: every value is decoded into a type-tagged
// Value (carrying its wire TType), and a struct is an ordered field list. This
// lets the full Table struct round-trip byte-for-byte through get_table ->
// alter_table without a schema, which is the load-bearing requirement for
// Iceberg-on-Hive (F5). Field IDs and the method subset are derived from
// hive_metastore.thrift (Apache Hive branch-2.3).
package hms

import (
	"sort"

	"github.com/apache/thrift/lib/go/thrift"
)

// Value is a decoded Thrift value, tagged with its wire type so a generic
// struct can round-trip byte-for-byte without a schema. Only the fields
// relevant to the value's type are populated.
type Value struct {
	T       thrift.TType
	B       bool
	I8      int8
	I16     int16
	I32     int32
	I64     int64
	F64     float64
	Str     string
	S       *Struct
	Elem    thrift.TType
	List    []Value
	K       thrift.TType
	V       thrift.TType
	Entries []MapEntry
}

// MapEntry is a single map key/value pair (order preserved for round-trip).
type MapEntry struct {
	K Value
	V Value
}

// Field is one struct field: its ID and value.
type Field struct {
	ID int16
	V  Value
}

// Struct is a decoded Thrift struct: an ordered field list (order preserved so
// encode(decode(bytes)) == bytes).
type Struct struct {
	Fields []Field
}

// --- Value constructors ---

func BoolV(v bool) Value      { return Value{T: thrift.BOOL, B: v} }
func ByteV(v int8) Value      { return Value{T: thrift.BYTE, I8: v} }
func I16V(v int16) Value      { return Value{T: thrift.I16, I16: v} }
func I32V(v int32) Value      { return Value{T: thrift.I32, I32: v} }
func I64V(v int64) Value      { return Value{T: thrift.I64, I64: v} }
func DoubleV(v float64) Value { return Value{T: thrift.DOUBLE, F64: v} }
func StringV(v string) Value  { return Value{T: thrift.STRING, Str: v} }
func StructV(s *Struct) Value { return Value{T: thrift.STRUCT, S: s} }
func ListV(e thrift.TType, l []Value) Value {
	return Value{T: thrift.LIST, Elem: e, List: l}
}
func SetV(e thrift.TType, l []Value) Value {
	return Value{T: thrift.SET, Elem: e, List: l}
}
func MapV(k, v thrift.TType, entries []MapEntry) Value {
	return Value{T: thrift.MAP, K: k, V: v, Entries: entries}
}

// --- Struct accessors ---

// Get returns the field with the given ID and whether it is present. When a
// field ID appears more than once (not produced by well-formed clients), the
// first occurrence wins.
func (s *Struct) Get(id int16) (Value, bool) {
	if s == nil {
		return Value{}, false
	}
	for i := range s.Fields {
		if s.Fields[i].ID == id {
			return s.Fields[i].V, true
		}
	}
	return Value{}, false
}

func (s *Struct) String(id int16) string {
	if v, ok := s.Get(id); ok && v.T == thrift.STRING {
		return v.Str
	}
	return ""
}

func (s *Struct) I16(id int16) int16 {
	if v, ok := s.Get(id); ok && v.T == thrift.I16 {
		return v.I16
	}
	return 0
}

func (s *Struct) I32(id int16) int32 {
	if v, ok := s.Get(id); ok && v.T == thrift.I32 {
		return v.I32
	}
	return 0
}

func (s *Struct) I64(id int16) int64 {
	if v, ok := s.Get(id); ok && v.T == thrift.I64 {
		return v.I64
	}
	return 0
}

func (s *Struct) Bool(id int16) bool {
	if v, ok := s.Get(id); ok && v.T == thrift.BOOL {
		return v.B
	}
	return false
}

func (s *Struct) Struct(id int16) *Struct {
	if v, ok := s.Get(id); ok && v.T == thrift.STRUCT {
		return v.S
	}
	return nil
}

func (s *Struct) List(id int16) []Value {
	if v, ok := s.Get(id); ok && (v.T == thrift.LIST || v.T == thrift.SET) {
		return v.List
	}
	return nil
}

func (s *Struct) MapStrStr(id int16) map[string]string {
	v, ok := s.Get(id)
	if !ok || v.T != thrift.MAP {
		return nil
	}
	m := make(map[string]string, len(v.Entries))
	for _, e := range v.Entries {
		if e.K.T == thrift.STRING && e.V.T == thrift.STRING {
			m[e.K.Str] = e.V.Str
		}
	}
	return m
}

// Set replaces the field with the given ID, appending it if absent.
func (s *Struct) Set(id int16, v Value) {
	for i := range s.Fields {
		if s.Fields[i].ID == id {
			s.Fields[i].V = v
			return
		}
	}
	s.Fields = append(s.Fields, Field{ID: id, V: v})
}

// --- Ordered builder ---

// Builder constructs a Struct with fields in a fixed order. Callers must add
// fields in ascending field-ID order (matching the Java generated code) so
// hand-encoded golden fixtures round-trip byte-for-byte.
type Builder struct{ s *Struct }

// NewBuilder returns an empty struct builder.
func NewBuilder() *Builder { return &Builder{s: &Struct{}} }

func (b *Builder) Add(id int16, v Value) *Builder {
	b.s.Fields = append(b.s.Fields, Field{ID: id, V: v})
	return b
}

func (b *Builder) Str(id int16, v string) *Builder { return b.Add(id, StringV(v)) }
func (b *Builder) I16(id int16, v int16) *Builder  { return b.Add(id, I16V(v)) }
func (b *Builder) I32(id int16, v int32) *Builder  { return b.Add(id, I32V(v)) }
func (b *Builder) I64(id int16, v int64) *Builder  { return b.Add(id, I64V(v)) }
func (b *Builder) Bool(id int16, v bool) *Builder  { return b.Add(id, BoolV(v)) }
func (b *Builder) Struct(id int16, s *Struct) *Builder {
	return b.Add(id, StructV(s))
}

func (b *Builder) ListStr(id int16, elems []string) *Builder {
	vals := make([]Value, 0, len(elems))
	for _, e := range elems {
		vals = append(vals, StringV(e))
	}
	return b.Add(id, ListV(thrift.STRING, vals))
}

func (b *Builder) ListStruct(id int16, elems []*Struct) *Builder {
	vals := make([]Value, 0, len(elems))
	for _, e := range elems {
		vals = append(vals, StructV(e))
	}
	return b.Add(id, ListV(thrift.STRUCT, vals))
}

// MapStrStr adds a map<string,string> field with keys sorted for deterministic
// output.
func (b *Builder) MapStrStr(id int16, m map[string]string) *Builder {
	if len(m) == 0 {
		return b
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]MapEntry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, MapEntry{K: StringV(k), V: StringV(m[k])})
	}
	return b.Add(id, MapV(thrift.STRING, thrift.STRING, entries))
}

func (b *Builder) Build() *Struct { return b.s }
