package bigquery

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
)

// BigQuery's JSON encoding for a non-finite FLOAT64 value is a string:
// "NaN", "Infinity" or "-Infinity" (tabledata.list, jobs.query, and the
// CSV/NEWLINE_DELIMITED_JSON load formats all use these). Rows are stored as
// JSON in jc_bq_rows, and JSON has no representation for an IEEE non-finite
// number, so a non-finite FLOAT64 value is written as its sentinel string and
// decoded back to the float on read.
const (
	nonFiniteNaN    = "NaN"
	nonFinitePosInf = "Infinity"
	nonFiniteNegInf = "-Infinity"
)

// nonFiniteSentinel renders a non-finite float64 as the string BigQuery uses
// on the wire, and reports whether f was non-finite.
func nonFiniteSentinel(f float64) (string, bool) {
	switch {
	case math.IsNaN(f):
		return nonFiniteNaN, true
	case math.IsInf(f, 1):
		return nonFinitePosInf, true
	case math.IsInf(f, -1):
		return nonFiniteNegInf, true
	}
	return "", false
}

// parseNonFiniteSentinel parses BigQuery's non-finite FLOAT64 strings. The
// forms BigQuery accepts on load (and Go's strconv.ParseFloat accepts) are
// matched case-insensitively.
func parseNonFiniteSentinel(s string) (float64, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "nan":
		return math.NaN(), true
	case "infinity", "+infinity", "inf", "+inf":
		return math.Inf(1), true
	case "-infinity", "-inf":
		return math.Inf(-1), true
	}
	return 0, false
}

// isFloatFieldType reports whether a BigQuery field type is a floating-point
// type (the only type whose sentinel strings decode back to a number).
func isFloatFieldType(t string) bool {
	switch strings.ToUpper(t) {
	case "FLOAT", "FLOAT64", "DOUBLE", "REAL":
		return true
	}
	return false
}

// encodeRowForStore renders a row map as the JSON stored in jc_bq_rows,
// replacing a non-finite FLOAT64 value with BigQuery's sentinel string so the
// row survives JSON encoding.
func encodeRowForStore(m map[string]any) ([]byte, error) {
	return json.Marshal(sanitizeRowValue(m))
}

// sanitizeRowValue copies v, replacing every non-finite float with its
// BigQuery sentinel string. The input is not modified.
func sanitizeRowValue(v any) any {
	switch x := v.(type) {
	case float64:
		if s, ok := nonFiniteSentinel(x); ok {
			return s
		}
		return x
	case float32:
		if s, ok := nonFiniteSentinel(float64(x)); ok {
			return s
		}
		return x
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = sanitizeRowValue(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = sanitizeRowValue(val)
		}
		return out
	}
	return v
}

// decodeStoredRow parses stored row JSON, restoring a non-finite FLOAT64
// sentinel string to its float64 value. The schema is required to tell a
// sentinel from a literal STRING value such as "NaN". Numbers are decoded with
// json.Number so INT64 values keep full precision.
func decodeStoredRow(data []byte, fields []schemaField) (map[string]any, error) {
	m := map[string]any{}
	if len(data) > 0 {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}
	}
	decodeRowFields(m, fields)
	return m, nil
}

// decodeRowFields converts non-finite FLOAT64 sentinel strings in m in place.
func decodeRowFields(m map[string]any, fields []schemaField) {
	for _, f := range fields {
		v, ok := m[f.Name]
		if !ok {
			continue
		}
		m[f.Name] = decodeStoredValue(f, v)
	}
}

func decodeStoredValue(f schemaField, v any) any {
	if v == nil {
		return nil
	}
	if strings.EqualFold(f.Mode, "REPEATED") {
		arr, ok := v.([]any)
		if !ok {
			return v
		}
		elem := f
		elem.Mode = "NULLABLE"
		for i := range arr {
			arr[i] = decodeStoredValue(elem, arr[i])
		}
		return arr
	}
	if isRecordType(f.Type) {
		if obj, ok := v.(map[string]any); ok && len(f.Fields) > 0 {
			decodeRowFields(obj, f.Fields)
		}
		return v
	}
	if s, ok := v.(string); ok && isFloatFieldType(f.Type) {
		if fl, ok := parseNonFiniteSentinel(s); ok {
			return fl
		}
	}
	return v
}
