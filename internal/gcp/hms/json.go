package hms

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/thrift/lib/go/thrift"
)

// Canonical (type-tagged, lossless) JSON encoding of Thrift values. The store
// persists a full Hive Table as this JSON (jc_hms_tables.table_json); the codec
// owns both directions so get_table -> alter_table round-trips every field the
// client sent, including fields the emulator does not interpret.
//
// A value is a JSON array [tag, payload...]:
//
//	["b", true]                    bool
//	["y", 5]                       byte (i8)
//	["h", 5]                       i16
//	["i", 5]                       i32
//	["l", "5"]                     i64 (string to avoid float64 precision loss)
//	["d", 1.5]                     double
//	["s", "abc"]                   string
//	["o", [[id, v], ...]]          struct (ordered fields)
//	["a", elemType, [v, ...]]      list
//	["g", elemType, [v, ...]]      set
//	["m", kType, vType, [[k, v], ...]]  map (ordered entries)
var errBadJSON = errors.New("hms: malformed thrift JSON")

// MarshalStruct encodes a Struct as canonical JSON.
func MarshalStruct(s *Struct) ([]byte, error) {
	if s == nil {
		s = &Struct{}
	}
	return json.Marshal(valueToAny(StructV(s)))
}

// UnmarshalStruct decodes canonical JSON into a Struct.
func UnmarshalStruct(b []byte) (*Struct, error) {
	var a any
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	v, err := anyToValue(a)
	if err != nil {
		return nil, err
	}
	if v.T != thrift.STRUCT {
		return nil, fmt.Errorf("hms: top-level JSON value is not a struct")
	}
	return v.S, nil
}

func valueToAny(v Value) any {
	switch v.T {
	case thrift.BOOL:
		return []any{"b", v.B}
	case thrift.BYTE:
		return []any{"y", int64(v.I8)}
	case thrift.I16:
		return []any{"h", int64(v.I16)}
	case thrift.I32:
		return []any{"i", int64(v.I32)}
	case thrift.I64:
		return []any{"l", strconv.FormatInt(v.I64, 10)}
	case thrift.DOUBLE:
		return []any{"d", v.F64}
	case thrift.STRING:
		return []any{"s", v.Str}
	case thrift.STRUCT:
		fields := make([]any, 0, len(v.S.Fields))
		for _, f := range v.S.Fields {
			fields = append(fields, []any{int64(f.ID), valueToAny(f.V)})
		}
		return []any{"o", fields}
	case thrift.LIST:
		elems := make([]any, 0, len(v.List))
		for _, e := range v.List {
			elems = append(elems, valueToAny(e))
		}
		return []any{"a", int64(v.Elem), elems}
	case thrift.SET:
		elems := make([]any, 0, len(v.List))
		for _, e := range v.List {
			elems = append(elems, valueToAny(e))
		}
		return []any{"g", int64(v.Elem), elems}
	case thrift.MAP:
		entries := make([]any, 0, len(v.Entries))
		for _, e := range v.Entries {
			entries = append(entries, []any{valueToAny(e.K), valueToAny(e.V)})
		}
		return []any{"m", int64(v.K), int64(v.V), entries}
	}
	return []any{"z"}
}

func anyToValue(a any) (Value, error) {
	arr, ok := a.([]any)
	if !ok || len(arr) == 0 {
		return Value{}, errBadJSON
	}
	tag, _ := arr[0].(string)
	switch tag {
	case "b":
		b, ok := arr[1].(bool)
		if !ok {
			return Value{}, errBadJSON
		}
		return BoolV(b), nil
	case "y":
		n, err := jsonNum(arr[1])
		return ByteV(int8(n)), err
	case "h":
		n, err := jsonNum(arr[1])
		return I16V(int16(n)), err
	case "i":
		n, err := jsonNum(arr[1])
		return I32V(int32(n)), err
	case "l":
		str, ok := arr[1].(string)
		if !ok {
			return Value{}, errBadJSON
		}
		n, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return Value{}, errBadJSON
		}
		return I64V(n), nil
	case "d":
		f, ok := arr[1].(float64)
		if !ok {
			return Value{}, errBadJSON
		}
		return DoubleV(f), nil
	case "s":
		str, ok := arr[1].(string)
		if !ok {
			return Value{}, errBadJSON
		}
		return StringV(str), nil
	case "o":
		fieldsArr, ok := arr[1].([]any)
		if !ok {
			return Value{}, errBadJSON
		}
		fields := make([]Field, 0, len(fieldsArr))
		for _, fa := range fieldsArr {
			pair, ok := fa.([]any)
			if !ok || len(pair) != 2 {
				return Value{}, errBadJSON
			}
			id, err := jsonNum(pair[0])
			if err != nil {
				return Value{}, err
			}
			fv, err := anyToValue(pair[1])
			if err != nil {
				return Value{}, err
			}
			fields = append(fields, Field{ID: int16(id), V: fv})
		}
		return StructV(&Struct{Fields: fields}), nil
	case "a", "g":
		et, err := jsonNum(arr[1])
		if err != nil {
			return Value{}, err
		}
		elemsArr, ok := arr[2].([]any)
		if !ok {
			return Value{}, errBadJSON
		}
		elems := make([]Value, 0, len(elemsArr))
		for _, ea := range elemsArr {
			e, err := anyToValue(ea)
			if err != nil {
				return Value{}, err
			}
			elems = append(elems, e)
		}
		if tag == "a" {
			return ListV(thrift.TType(et), elems), nil
		}
		return SetV(thrift.TType(et), elems), nil
	case "m":
		kt, err := jsonNum(arr[1])
		if err != nil {
			return Value{}, err
		}
		vt, err := jsonNum(arr[2])
		if err != nil {
			return Value{}, err
		}
		entriesArr, ok := arr[3].([]any)
		if !ok {
			return Value{}, errBadJSON
		}
		entries := make([]MapEntry, 0, len(entriesArr))
		for _, ea := range entriesArr {
			pair, ok := ea.([]any)
			if !ok || len(pair) != 2 {
				return Value{}, errBadJSON
			}
			k, err := anyToValue(pair[0])
			if err != nil {
				return Value{}, err
			}
			v, err := anyToValue(pair[1])
			if err != nil {
				return Value{}, err
			}
			entries = append(entries, MapEntry{K: k, V: v})
		}
		return MapV(thrift.TType(kt), thrift.TType(vt), entries), nil
	}
	return Value{}, errBadJSON
}

// jsonNum reads an encoding/json number (float64) as an int64.
func jsonNum(a any) (int64, error) {
	f, ok := a.(float64)
	if !ok {
		return 0, errBadJSON
	}
	return int64(f), nil
}
