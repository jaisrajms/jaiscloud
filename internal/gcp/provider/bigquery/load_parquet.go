package bigquery

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"jaiscloud/internal/model"
)

// ─── Parquet ─────────────────────────────────────────────────────────────────

// The Parquet contract implemented here is the type table on
// https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
// (page rev 2026-10-27):
//
//	BOOLEAN                                -> BOOLEAN
//	INT32 (none / INTEGER)                  -> INT64
//	INT32 DECIMAL                           -> NUMERIC / BIGNUMERIC / STRING
//	INT32 DATE                              -> DATE
//	INT32 TIME (millis)                     -> TIME
//	INT64 (none / INTEGER)                  -> INT64
//	INT64 DECIMAL                           -> NUMERIC / BIGNUMERIC / STRING
//	INT64 TIME (micros)                     -> TIME
//	INT64 TIMESTAMP (millis / micros)       -> TIMESTAMP
//	INT96 (none)                            -> TIMESTAMP
//	FLOAT / DOUBLE                          -> FLOAT64
//	BYTE_ARRAY                              -> BYTES
//	BYTE_ARRAY STRING (UTF8)                -> STRING
//	FIXED_LEN_BYTE_ARRAY DECIMAL            -> NUMERIC / BIGNUMERIC / STRING
//	FIXED_LEN_BYTE_ARRAY (none)             -> BYTES
//
// "Nested groups are converted into STRUCT types. Other combinations of Parquet
// types and converted types are not supported." MAP has no entry in that table;
// its documented BigQuery shape comes from the vendored Discovery
// ParquetOptions.mapTargetType enum. GEOMETRY/GEOGRAPHY are the native Parquet
// logical types: the geospatial page
// (https://cloud.google.com/bigquery/docs/geospatial-data) documents WKB — and
// hex-encoded WKB — as an accepted GEOGRAPHY input form, so the WKB payload is
// stored as a hex string. Parquet VARIANT has no documented BigQuery mapping and
// is rejected; see unsupportedParquetVariant.

// decodeParquet decodes a Parquet file into schema-coerced rows. The Parquet
// schema is mapped to BigQuery types before any row is read, and each row is
// assembled from the raw parquet.Row values so that logical types keep their
// exact representation (a DECIMAL stays an unscaled integer plus its scale and
// is rendered as an exact decimal string; the generic map decode used to return
// a lossy float64/*big.Float).
func decodeParquet(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, invalidArgument("failed to read Parquet source: " + err.Error())
	}
	nodes, mapArrayOfStruct, err := parquetNodes(f.Schema(), load)
	if err != nil {
		return nil, err
	}
	ignore := boolValue(load, "ignoreUnknownValues")
	dec := &pqDecoder{mapArrayOfStruct: mapArrayOfStruct}
	rdr := parquet.NewReader(f)
	defer rdr.Close()

	res := &loadRecords{}
	columns := make([][]parquet.Value, len(f.Schema().Columns()))
	rowbuf := make([]parquet.Row, 1)
	row := 0
	for {
		n, rerr := rdr.ReadRows(rowbuf[:])
		if n == 0 {
			if rerr == nil || errors.Is(rerr, io.EOF) {
				break
			}
			return nil, invalidArgument("failed to read Parquet source: " + rerr.Error())
		}
		row++
		for i := range columns {
			columns[i] = columns[i][:0]
		}
		rowbuf[0].Range(func(columnIndex int, values []parquet.Value) bool {
			if columnIndex >= 0 && columnIndex < len(columns) {
				columns[columnIndex] = append(columns[columnIndex], values...)
			}
			return true
		})
		obj, derr := dec.decodeRow(nodes, columns)
		if derr != nil {
			var rangeErr *decimalRangeError
			if errors.As(derr, &rangeErr) {
				// A value the selected decimal target cannot hold is a
				// request-level failure, per JobConfigurationLoad.
				// decimalTargetTypes: "if a value exceeds the supported range
				// when reading the data, an error will be thrown".
				return nil, invalidArgument(fmt.Sprintf("Parquet row %d: %v", row, rangeErr))
			}
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, derr))
			continue
		}
		cv, cerr := coerceRow(fields, obj, ignore)
		if cerr != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, cerr))
			continue
		}
		res.rows = append(res.rows, cv)
	}
	return res, nil
}

// pqShape is the structural role of a normalized Parquet node.
type pqShape int

const (
	pqLeafShape pqShape = iota
	pqGroupShape
	pqListShape
	pqMapShape
)

// pqNode is a normalized view of one Parquet schema node: its BigQuery field
// shape plus what is needed to assemble the node from raw parquet.Row values.
type pqNode struct {
	bq       schemaField
	shape    pqShape
	optional bool
	// fields are a group's children, in schema order.
	fields []*pqNode
	// elem is a repeated node's element (the LIST element or repeated leaf).
	elem *pqNode
	// mapKey/mapValue are a MAP's key and value nodes.
	mapKey   *pqNode
	mapValue *pqNode
	// convert renders a leaf's raw parquet.Value as a BigQuery value.
	convert func(parquet.Value) (any, error)
	// columns are the leaf column indexes in this subtree, in schema order.
	columns []int
}

// pqBuilder maps a Parquet schema to pqNodes. nextColumn is the next leaf
// column index in the schema's depth-first column order.
type pqBuilder struct {
	targets          []decimalTarget
	mapArrayOfStruct bool
	nextColumn       int
	leafPaths        [][]string
}

// parquetNodes maps a Parquet schema to normalized nodes, failing loud on
// shapes BigQuery has no documented mapping for (VARIANT). It also returns
// whether ParquetOptions.mapTargetType asks for the ARRAY_OF_STRUCT map shape.
func parquetNodes(schema *parquet.Schema, load map[string]any) ([]*pqNode, bool, error) {
	targets, err := parseDecimalTargetTypes(load)
	if err != nil {
		return nil, false, err
	}
	mapArrayOfStruct, err := parquetMapTargetType(load)
	if err != nil {
		return nil, false, err
	}
	b := &pqBuilder{targets: targets, mapArrayOfStruct: mapArrayOfStruct}
	fields := schema.Fields()
	out := make([]*pqNode, 0, len(fields))
	for _, f := range fields {
		n, err := b.node(f, []string{f.Name()})
		if err != nil {
			return nil, false, err
		}
		out = append(out, n)
	}
	if err := b.checkColumnOrder(schema); err != nil {
		return nil, false, err
	}
	return out, mapArrayOfStruct, nil
}

// parquetMapTargetType reads configuration.load.parquetOptions.mapTargetType.
// The vendored Discovery enum defines the two documented shapes:
//
//	MAP_TARGET_TYPE_UNSPECIFIED: struct map_field_name { repeated struct key_value { key value } }
//	ARRAY_OF_STRUCT:             repeated struct map_field_name { key value }
func parquetMapTargetType(load map[string]any) (bool, error) {
	opts := mapValue(load, "parquetOptions")
	if opts == nil {
		return false, nil
	}
	switch v := strings.ToUpper(strValue(opts, "mapTargetType")); v {
	case "", "MAP_TARGET_TYPE_UNSPECIFIED":
		return false, nil
	case "ARRAY_OF_STRUCT":
		return true, nil
	default:
		return false, invalidArgument("configuration.load.parquetOptions.mapTargetType " + strconv.Quote(v) +
			" is not supported (expected MAP_TARGET_TYPE_UNSPECIFIED or ARRAY_OF_STRUCT)")
	}
}

// checkColumnOrder verifies that the depth-first leaf walk matches the schema's
// column order, so a node's column indexes address the right parquet.Row slots.
func (b *pqBuilder) checkColumnOrder(schema *parquet.Schema) error {
	columns := schema.Columns()
	if len(columns) != len(b.leafPaths) {
		return model.NewProviderError("Internal", fmt.Sprintf(
			"Parquet schema has %d leaf columns but the decoder mapped %d", len(columns), len(b.leafPaths)), 500)
	}
	for i, path := range columns {
		if len(path) != len(b.leafPaths[i]) {
			return model.NewProviderError("Internal", "Parquet column order mismatch at column "+strconv.Itoa(i), 500)
		}
		for j := range path {
			if path[j] != b.leafPaths[i][j] {
				return model.NewProviderError("Internal", "Parquet column order mismatch at column "+strconv.Itoa(i), 500)
			}
		}
	}
	return nil
}

func (b *pqBuilder) node(f parquet.Field, path []string) (*pqNode, error) {
	name := f.Name()
	if !f.Leaf() {
		lt := f.Type().LogicalType()
		switch {
		case lt != nil && lt.Map != nil:
			return b.mapNode(f, path)
		case lt != nil && lt.Variant != nil:
			return nil, unsupportedParquetVariant(name)
		case lt != nil && lt.List != nil:
			return b.listNode(f, path)
		}
		kids := f.Fields()
		if len(kids) == 0 {
			return nil, unsupportedParquetShape("empty group", name)
		}
		children := make([]*pqNode, 0, len(kids))
		for _, kid := range kids {
			child, err := b.node(kid, append(append([]string{}, path...), kid.Name()))
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		childFields := make([]schemaField, 0, len(children))
		for _, c := range children {
			childFields = append(childFields, c.bq)
		}
		columns := pqColumnsOf(children)
		if f.Repeated() {
			// A legacy repeated group without the LIST annotation: a REPEATED
			// RECORD rather than a silently flattened one.
			elem := &pqNode{
				bq:      schemaField{Name: name, Type: "RECORD", Mode: "NULLABLE", Fields: childFields},
				shape:   pqGroupShape,
				fields:  children,
				columns: columns,
			}
			return &pqNode{
				bq:      schemaField{Name: name, Type: "RECORD", Mode: "REPEATED", Fields: childFields},
				shape:   pqListShape,
				elem:    elem,
				columns: columns,
			}, nil
		}
		return &pqNode{
			bq:       schemaField{Name: name, Type: "RECORD", Mode: parquetMode(f), Fields: childFields},
			shape:    pqGroupShape,
			optional: f.Optional(),
			fields:   children,
			columns:  columns,
		}, nil
	}
	return b.leafNode(f, path)
}

func (b *pqBuilder) listNode(f parquet.Field, path []string) (*pqNode, error) {
	kids := f.Fields()
	// The standard three-level form: <group> (LIST) { repeated group list {
	// <element> element } }. The Parquet load page documents list inference for
	// exactly this form (and its backward-compatible spellings of the middle and
	// element names).
	if len(kids) != 1 || kids[0].Leaf() || !kids[0].Repeated() {
		return nil, unsupportedParquetShape("LIST", f.Name())
	}
	elems := kids[0].Fields()
	if len(elems) != 1 {
		return nil, unsupportedParquetShape("LIST", f.Name())
	}
	elem, err := b.node(elems[0], append(append([]string{}, path...), kids[0].Name(), elems[0].Name()))
	if err != nil {
		return nil, err
	}
	if elem.shape == pqListShape {
		// BigQuery's type system has no ARRAY<ARRAY<...>>; the Avro load page
		// states the same rule ("Arrays of arrays are not supported").
		return nil, unsupportedParquetShape("nested LIST", f.Name())
	}
	bq := elem.bq
	bq.Name = f.Name()
	bq.Mode = "REPEATED"
	return &pqNode{bq: bq, shape: pqListShape, optional: f.Optional(), elem: elem, columns: elem.columns}, nil
}

func (b *pqBuilder) mapNode(f parquet.Field, path []string) (*pqNode, error) {
	kv := parquetMapKeyValue(f)
	if kv == nil {
		return nil, unsupportedParquetShape("MAP", f.Name())
	}
	keyField := parquetChildField(kv, "key")
	valueField := parquetChildField(kv, "value")
	if keyField == nil || valueField == nil || !keyField.Required() {
		return nil, unsupportedParquetShape("MAP", f.Name())
	}
	key, err := b.node(keyField, append(append([]string{}, path...), kv.Name(), "key"))
	if err != nil {
		return nil, err
	}
	value, err := b.node(valueField, append(append([]string{}, path...), kv.Name(), "value"))
	if err != nil {
		return nil, err
	}
	if key.shape != pqLeafShape {
		return nil, unsupportedParquetShape("MAP with a non-scalar key", f.Name())
	}
	// BigQuery stores a map key as a STRING (the Avro load page states this
	// explicitly for map<T>, and the Discovery map shape has a plain `key`
	// field), so a non-string Parquet key is rendered through its scalar form.
	key.bq.Type = "STRING"
	key.bq.Mode = "NULLABLE"
	key.convert = pqMapKeyString(key.convert)

	keyBQ := key.bq
	keyBQ.Name = "key"
	valueBQ := value.bq
	valueBQ.Name = "value"
	mapFields := []schemaField{keyBQ, valueBQ}
	columns := append(append([]int{}, key.columns...), value.columns...)
	if b.mapArrayOfStruct {
		return &pqNode{
			bq:       schemaField{Name: f.Name(), Type: "RECORD", Mode: "REPEATED", Fields: mapFields},
			shape:    pqMapShape,
			optional: f.Optional(),
			mapKey:   key,
			mapValue: value,
			columns:  columns,
		}, nil
	}
	return &pqNode{
		bq: schemaField{Name: f.Name(), Type: "RECORD", Mode: parquetMode(f), Fields: []schemaField{{
			Name:   "key_value",
			Type:   "RECORD",
			Mode:   "REPEATED",
			Fields: mapFields,
		}}},
		shape:    pqMapShape,
		optional: f.Optional(),
		mapKey:   key,
		mapValue: value,
		columns:  columns,
	}, nil
}

// parquetMapKeyValue returns the repeated key/value group of a MAP node, either
// the standard "key_value" name or the legacy "map" name.
func parquetMapKeyValue(f parquet.Field) parquet.Field {
	for _, name := range []string{"key_value", "map"} {
		child := parquetChildField(f, name)
		if child != nil && !child.Leaf() && child.Repeated() {
			return child
		}
	}
	return nil
}

func parquetChildField(f parquet.Field, name string) parquet.Field {
	for _, child := range f.Fields() {
		if child.Name() == name {
			return child
		}
	}
	return nil
}

func (b *pqBuilder) leafNode(f parquet.Field, path []string) (*pqNode, error) {
	bqType, convert, err := b.leafType(f)
	if err != nil {
		return nil, fmt.Errorf("Parquet field %q: %w", f.Name(), err)
	}
	b.leafPaths = append(b.leafPaths, path)
	leaf := &pqNode{
		bq:       schemaField{Name: f.Name(), Type: bqType, Mode: parquetMode(f)},
		shape:    pqLeafShape,
		optional: f.Optional(),
		convert:  convert,
		columns:  []int{b.nextColumn},
	}
	b.nextColumn++
	if !f.Repeated() {
		return leaf, nil
	}
	// A repeated leaf without a LIST wrapper: one REPEATED field.
	elem := &pqNode{bq: schemaField{Name: f.Name(), Type: bqType, Mode: "NULLABLE"}, shape: pqLeafShape, convert: convert, columns: leaf.columns}
	return &pqNode{bq: schemaField{Name: f.Name(), Type: bqType, Mode: "REPEATED"}, shape: pqListShape, elem: elem, columns: leaf.columns}, nil
}

func parquetMode(f parquet.Field) string {
	if f.Required() {
		return "REQUIRED"
	}
	return "NULLABLE"
}

func pqColumnsOf(nodes []*pqNode) []int {
	var out []int
	for _, n := range nodes {
		out = append(out, n.columns...)
	}
	return out
}

func unsupportedParquetShape(shape, name string) error {
	return model.NewProviderError("Unimplemented",
		"Parquet "+shape+" field "+strconv.Quote(name)+" is not supported: the Parquet load type table documents no "+
			"BigQuery mapping for it (https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet: "+
			"\"Other combinations of Parquet types and converted types are not supported\")", 501)
}

// unsupportedParquetVariant rejects the Parquet VARIANT logical type. It has no
// entry in the Parquet load type table, and a VARIANT column would only be
// representable in BigQuery as JSON, whose batch-load page states: "If you use a
// batch load job to ingest JSON data into a table, the source data must be in
// CSV, Avro, or JSON format. Other batch load formats are not supported."
// (https://cloud.google.com/bigquery/docs/json-data, Limitations).
func unsupportedParquetVariant(name string) error {
	return model.NewProviderError("Unimplemented",
		"Parquet VARIANT field "+strconv.Quote(name)+" is not supported: the Parquet load type table has no VARIANT "+
			"mapping, and BigQuery's JSON type only accepts batch loads from CSV, Avro or JSON — \"Other batch load "+
			"formats are not supported\" (https://cloud.google.com/bigquery/docs/json-data)", 501)
}

// leafType maps a Parquet leaf (physical + logical type) to its BigQuery type
// and the exact conversion of a raw parquet.Value.
func (b *pqBuilder) leafType(f parquet.Field) (string, func(parquet.Value) (any, error), error) {
	t := f.Type()
	if lt := t.LogicalType(); lt != nil {
		switch {
		case lt.Date != nil:
			return "DATE", pqDate, nil
		case lt.Time != nil:
			factor, err := parquetTimeFactor(lt.Time.Unit)
			if err != nil {
				return "", nil, err
			}
			return "TIME", func(v parquet.Value) (any, error) { return pqTime(v, factor) }, nil
		case lt.Timestamp != nil:
			factor, err := parquetTimeFactor(lt.Timestamp.Unit)
			if err != nil {
				return "", nil, err
			}
			if !lt.Timestamp.IsAdjustedToUTC {
				// A local (non-UTC-adjusted) timestamp is a BigQuery DATETIME.
				return "DATETIME", func(v parquet.Value) (any, error) { return pqDatetime(v, factor) }, nil
			}
			return "TIMESTAMP", func(v parquet.Value) (any, error) { return pqTimestamp(v, factor) }, nil
		case lt.Decimal != nil:
			precision, scale := int(lt.Decimal.Precision), int(lt.Decimal.Scale)
			target := selectDecimalTarget(b.targets, precision, scale)
			return target.name, func(v parquet.Value) (any, error) {
				unscaled, err := pqDecimalUnscaled(v)
				if err != nil {
					return nil, err
				}
				return renderDecimal(unscaled, scale, target, precision)
			}, nil
		case lt.Geometry != nil, lt.Geography != nil:
			// Native Parquet GEOMETRY/GEOGRAPHY carry WKB; BigQuery's documented
			// GEOGRAPHY input forms include WKB and hex-encoded WKB
			// (https://cloud.google.com/bigquery/docs/geospatial-data), so the
			// value is stored as the hex string.
			return "GEOGRAPHY", pqWKBHex, nil
		case lt.UTF8 != nil, lt.Enum != nil, lt.Json != nil:
			return "STRING", pqString, nil
		case lt.UUID != nil:
			return "STRING", pqUUID, nil
		case lt.Bson != nil:
			return "BYTES", nil, nil
		case lt.Integer != nil:
			convert := pqSignedInt
			if lt.Integer.IsSigned == false {
				if lt.Integer.BitWidth == 64 {
					convert = pqUnsignedInt64
				} else {
					convert = pqUnsignedInt32
				}
			}
			return "INTEGER", convert, nil
		case lt.Float16 != nil:
			return "FLOAT", pqFloat16, nil
		case lt.Unknown != nil:
			return "", nil, fmt.Errorf("NULL logical type is not supported")
		}
	}
	switch t.Kind() {
	case parquet.Boolean:
		return "BOOLEAN", pqBoolean, nil
	case parquet.Int32, parquet.Int64:
		return "INTEGER", pqSignedInt, nil
	case parquet.Int96:
		return "TIMESTAMP", pqInt96, nil
	case parquet.Float, parquet.Double:
		return "FLOAT", pqFloat, nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return "BYTES", pqBytes, nil
	}
	return "", nil, fmt.Errorf("type %s is not supported", t)
}

// pqMapKeyString adapts a leaf conversion so a non-string map key renders as
// the STRING BigQuery stores.
func pqMapKeyString(convert func(parquet.Value) (any, error)) func(parquet.Value) (any, error) {
	if convert == nil {
		return func(v parquet.Value) (any, error) { return pqString(v) }
	}
	return func(v parquet.Value) (any, error) {
		decoded, err := convert(v)
		if err != nil {
			return nil, err
		}
		return scalarString(decoded), nil
	}
}

// ─── Leaf conversions ────────────────────────────────────────────────────────

func pqBoolean(v parquet.Value) (any, error) { return v.Boolean(), nil }

func pqSignedInt(v parquet.Value) (any, error) {
	switch v.Kind() {
	case parquet.Int32:
		return int64(v.Int32()), nil
	case parquet.Int64:
		return v.Int64(), nil
	}
	return nil, fmt.Errorf("INTEGER value has physical type %s", v.Kind())
}

func pqUnsignedInt32(v parquet.Value) (any, error) { return int64(v.Uint32()), nil }

// pqUnsignedInt64 rejects a UINT_64 beyond the signed INTEGER range, as the
// Parquet load page documents ("an error will be returned if the unsigned value
// exceeds the maximum INTEGER value of 9,223,372,036,854,775,807").
func pqUnsignedInt64(v parquet.Value) (any, error) {
	u := v.Uint64()
	if u > math.MaxInt64 {
		return nil, fmt.Errorf("unsigned value %d exceeds the maximum INTEGER value %d", u, int64(math.MaxInt64))
	}
	return int64(u), nil
}

func pqFloat(v parquet.Value) (any, error) {
	switch v.Kind() {
	case parquet.Float:
		return float64(v.Float()), nil
	case parquet.Double:
		return v.Double(), nil
	}
	return nil, fmt.Errorf("FLOAT value has physical type %s", v.Kind())
}

func pqFloat16(v parquet.Value) (any, error) {
	b := v.Bytes()
	if len(b) != 2 {
		return nil, fmt.Errorf("FLOAT16 value is %d bytes, want 2", len(b))
	}
	return float64(float16FromBits(uint16(b[0]) | uint16(b[1])<<8)), nil
}

// float16FromBits converts an IEEE 754 half-precision bit pattern to float32.
func float16FromBits(bits uint16) float32 {
	sign := uint32(bits>>15) & 1
	exp := uint32(bits>>10) & 0x1f
	frac := uint32(bits) & 0x3ff
	var f float32
	switch {
	case exp == 0 && frac == 0:
		f = 0 // ±0
	case exp == 0:
		f = float32(math.Ldexp(float64(frac), -24)) // subnormal
	case exp == 0x1f:
		if frac == 0 {
			f = float32(math.Inf(1))
		} else {
			f = float32(math.NaN())
		}
	default:
		f = float32(math.Ldexp(float64(frac|0x400), int(exp)-25))
	}
	if sign == 1 {
		f = -f
	}
	return f
}

func pqString(v parquet.Value) (any, error) { return string(v.Bytes()), nil }

// pqUUID renders the 16-byte UUID logical type in its canonical text form.
func pqUUID(v parquet.Value) (any, error) {
	b := v.Bytes()
	if len(b) != 16 {
		return nil, fmt.Errorf("UUID value is %d bytes, want 16", len(b))
	}
	s := hex.EncodeToString(b)
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:], nil
}

// pqBytes copies the payload: a parquet.Value's bytes may be a view into a page
// buffer the reader reuses for the next batch.
func pqBytes(v parquet.Value) (any, error) { return bytes.Clone(v.Bytes()), nil }

// pqWKBHex renders WKB as the hex-encoded string BigQuery accepts as a
// GEOGRAPHY input form.
func pqWKBHex(v parquet.Value) (any, error) {
	b := v.Bytes()
	if len(b) == 0 {
		return nil, fmt.Errorf("GEOGRAPHY value is empty (want WKB)")
	}
	return hex.EncodeToString(b), nil
}

// pqDecimalUnscaled reads a DECIMAL's unscaled integer from the physical value:
// INT32/INT64 store it directly and BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY store it as
// a two's-complement big-endian integer.
func pqDecimalUnscaled(v parquet.Value) (*big.Int, error) {
	switch v.Kind() {
	case parquet.Int32:
		return big.NewInt(int64(v.Int32())), nil
	case parquet.Int64:
		return big.NewInt(v.Int64()), nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return decimalFromBytes(v.Bytes()), nil
	}
	return nil, fmt.Errorf("DECIMAL value has unsupported physical type %s", v.Kind())
}

// parquetTimeFactor returns the nanoseconds-per-unit factor for a Parquet time
// or timestamp unit.
func parquetTimeFactor(u format.TimeUnit) (int64, error) {
	switch {
	case u.Millis != nil:
		return int64(time.Millisecond), nil
	case u.Micros != nil:
		return int64(time.Microsecond), nil
	case u.Nanos != nil:
		return int64(time.Nanosecond), nil
	}
	return 0, fmt.Errorf("unknown time unit")
}

// pqDate converts a Parquet DATE (days since the Unix epoch) to a BigQuery DATE
// string.
func pqDate(v parquet.Value) (any, error) {
	if v.Kind() != parquet.Int32 {
		return nil, fmt.Errorf("DATE value is not an INT32")
	}
	return time.Unix(int64(v.Int32())*86400, 0).UTC().Format("2006-01-02"), nil
}

// pqTime converts a Parquet TIME (a count of units since midnight) to a BigQuery
// TIME string.
func pqTime(v parquet.Value, factor int64) (any, error) {
	n, ok := pqInt64(v)
	if !ok {
		return nil, fmt.Errorf("TIME value is not an integer")
	}
	return formatDuration(time.Duration(n) * time.Duration(factor)), nil
}

// pqTimestamp converts a Parquet TIMESTAMP (a count of units since the Unix
// epoch) to a BigQuery TIMESTAMP string.
func pqTimestamp(v parquet.Value, factor int64) (any, error) {
	n, ok := pqInt64(v)
	if !ok {
		return nil, fmt.Errorf("TIMESTAMP value is not an integer")
	}
	ts := time.Unix(0, n*factor).UTC()
	return trimFraction(ts.Format("2006-01-02 15:04:05.000000000")) + " UTC", nil
}

// pqDatetime converts a Parquet local (non-UTC-adjusted) TIMESTAMP to a BigQuery
// DATETIME string.
func pqDatetime(v parquet.Value, factor int64) (any, error) {
	n, ok := pqInt64(v)
	if !ok {
		return nil, fmt.Errorf("TIMESTAMP value is not an integer")
	}
	ts := time.Unix(0, n*factor).UTC()
	return trimFraction(ts.Format("2006-01-02T15:04:05.000000000")), nil
}

// pqInt64 reads the integer payload of an INT32/INT64 value.
func pqInt64(v parquet.Value) (int64, bool) {
	switch v.Kind() {
	case parquet.Int32:
		return int64(v.Int32()), true
	case parquet.Int64:
		return v.Int64(), true
	}
	return 0, false
}

// pqInt96 converts a legacy INT96 timestamp (nanoseconds-of-day + Julian day,
// both little-endian) to a BigQuery TIMESTAMP string, per the Parquet load
// type table's "INT96 -> TIMESTAMP".
func pqInt96(v parquet.Value) (any, error) {
	// The deprecated Int96 type is three little-endian 32-bit words: the first
	// two hold nanoseconds since midnight, the third the Julian day number.
	raw := v.Int96()
	nanos := uint64(raw[0]) | uint64(raw[1])<<32
	const julianEpochDays = 2440588 // Julian day number of 1970-01-01
	days := int64(raw[2]) - julianEpochDays
	seconds := int64(nanos / uint64(time.Second))
	fraction := int64(nanos % uint64(time.Second))
	ts := time.Unix(days*86400+seconds, fraction).UTC()
	return trimFraction(ts.Format("2006-01-02 15:04:05.000000000")) + " UTC", nil
}

// ─── Row assembly ────────────────────────────────────────────────────────────

// pqLevels tracks the repetition/definition depth while walking a schema, the
// same way parquet-go's own row reconstruction does.
type pqLevels struct {
	repetitionDepth int
	definitionLevel int
}

// pqDecoder assembles a Parquet row into BigQuery-shaped Go values.
type pqDecoder struct {
	mapArrayOfStruct bool
}

func (d *pqDecoder) decodeRow(nodes []*pqNode, columns [][]parquet.Value) (map[string]any, error) {
	out := make(map[string]any, len(nodes))
	offset := 0
	for _, n := range nodes {
		end := offset + len(n.columns)
		if end > len(columns) {
			return nil, model.NewProviderError("Internal", "Parquet row has fewer columns than the schema", 500)
		}
		v, err := d.decode(n, pqLevels{}, columns[offset:end])
		if err != nil {
			return nil, err
		}
		offset = end
		if v != nil {
			out[n.bq.Name] = v
		}
	}
	return out, nil
}

// decode returns the BigQuery value of one node, or nil when the node is absent
// (a NULL optional field, or an absent struct).
func (d *pqDecoder) decode(n *pqNode, levels pqLevels, columns [][]parquet.Value) (any, error) {
	if n.optional {
		levels.definitionLevel++
		if !pqDefined(columns, levels.definitionLevel) {
			return nil, nil
		}
	}
	switch n.shape {
	case pqLeafShape:
		return pqLeafValue(n, columns)
	case pqGroupShape:
		return d.decodeGroup(n, levels, columns)
	case pqListShape:
		return d.decodeList(n, levels, columns)
	case pqMapShape:
		return d.decodeMap(n, levels, columns)
	}
	return nil, model.NewProviderError("Internal", "unknown Parquet node shape", 500)
}

func (d *pqDecoder) decodeGroup(n *pqNode, levels pqLevels, columns [][]parquet.Value) (any, error) {
	out := make(map[string]any, len(n.fields))
	offset := 0
	for _, f := range n.fields {
		end := offset + len(f.columns)
		if end > len(columns) {
			return nil, model.NewProviderError("Internal", "Parquet group is missing columns", 500)
		}
		v, err := d.decode(f, levels, columns[offset:end])
		if err != nil {
			return nil, err
		}
		offset = end
		if v != nil {
			out[f.bq.Name] = v
		}
	}
	return out, nil
}

func (d *pqDecoder) decodeList(n *pqNode, levels pqLevels, columns [][]parquet.Value) (any, error) {
	levels.repetitionDepth++
	levels.definitionLevel++
	if !pqDefined(columns, levels.definitionLevel) {
		return []any{}, nil
	}
	out := make([]any, 0, pqElementCount(columns[0], levels.repetitionDepth))
	err := pqEachElement(columns, levels.repetitionDepth, func(chunk [][]parquet.Value) error {
		v, err := d.decode(n.elem, levels, chunk)
		if err != nil {
			return err
		}
		out = append(out, v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (d *pqDecoder) decodeMap(n *pqNode, levels pqLevels, columns [][]parquet.Value) (any, error) {
	keyColumns := len(n.mapKey.columns)
	if keyColumns > len(columns) {
		return nil, model.NewProviderError("Internal", "Parquet MAP is missing its key column", 500)
	}
	levels.repetitionDepth++
	levels.definitionLevel++
	entries := []any{}
	if pqDefined(columns, levels.definitionLevel) {
		err := pqEachElement(columns, levels.repetitionDepth, func(chunk [][]parquet.Value) error {
			key, err := d.decode(n.mapKey, levels, chunk[:keyColumns])
			if err != nil {
				return err
			}
			value, err := d.decode(n.mapValue, levels, chunk[keyColumns:])
			if err != nil {
				return err
			}
			entries = append(entries, map[string]any{"key": key, "value": value})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if d.mapArrayOfStruct {
		return entries, nil
	}
	return map[string]any{"key_value": entries}, nil
}

// pqDefined reports whether the first leaf of a node's subtree carries a value
// at the given definition level.
func pqDefined(columns [][]parquet.Value, definitionLevel int) bool {
	if len(columns) == 0 || len(columns[0]) == 0 {
		return false
	}
	return columns[0][0].DefinitionLevel() >= definitionLevel
}

// pqLeafValue reads the single leaf value of a leaf node.
func pqLeafValue(n *pqNode, columns [][]parquet.Value) (any, error) {
	if len(columns) == 0 || len(columns[0]) == 0 {
		return nil, nil
	}
	v := columns[0][0]
	if v.IsNull() {
		return nil, nil
	}
	if n.convert == nil {
		return pqScalarValue(v)
	}
	return n.convert(v)
}

// pqScalarValue is the fallback conversion for leaves without an explicit
// converter (BYTES, ENUM/JSON/UUID-as-STRING and their repeated forms).
func pqScalarValue(v parquet.Value) (any, error) {
	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean(), nil
	case parquet.Int32:
		return int64(v.Int32()), nil
	case parquet.Int64:
		return v.Int64(), nil
	case parquet.Float:
		return float64(v.Float()), nil
	case parquet.Double:
		return v.Double(), nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return bytes.Clone(v.Bytes()), nil
	}
	return nil, fmt.Errorf("unsupported Parquet value kind %s", v.Kind())
}

// pqElementCount counts the elements of one repeated node from its first leaf
// column: each element starts at a value whose repetition level is at most the
// node's repetition depth.
func pqElementCount(first []parquet.Value, depth int) int {
	n := 0
	for i := 0; i < len(first); {
		i++
		n++
		for i < len(first) && first[i].RepetitionLevel() > depth {
			i++
		}
	}
	return n
}

// pqEachElement splits every leaf column of a repeated node into per-element
// chunks and calls emit for each element.
func pqEachElement(columns [][]parquet.Value, depth int, emit func(chunk [][]parquet.Value) error) error {
	remaining := make([][]parquet.Value, len(columns))
	copy(remaining, columns)
	if len(columns) == 0 || len(columns[0]) == 0 {
		return nil
	}
	count := pqElementCount(columns[0], depth)
	chunk := make([][]parquet.Value, len(columns))
	for i := 0; i < count; i++ {
		for j := range remaining {
			col := remaining[j]
			k := 0
			if len(col) > 0 {
				k = 1
				for k < len(col) && col[k].RepetitionLevel() > depth {
					k++
				}
			}
			chunk[j] = col[:k]
			remaining[j] = col[k:]
		}
		if err := emit(chunk); err != nil {
			return err
		}
	}
	return nil
}
