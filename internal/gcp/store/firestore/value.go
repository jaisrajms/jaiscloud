package firestore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// NullEnumValue is the wire value of the Firestore nullValue enum.
const NullEnumValue = "NULL_VALUE"

// GeoPoint is a Firestore geoPointValue ({latitude, longitude}).
type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// ArrayValue is the Firestore arrayValue container.
type ArrayValue struct {
	Values []*Value `json:"values,omitempty"`
}

// MapValue is the Firestore mapValue container.
type MapValue struct {
	Fields map[string]*Value `json:"fields,omitempty"`
}

// Value is a Firestore value: exactly one variant field is set. It mirrors the
// Firestore REST wire encoding, where integerValue is a decimal string,
// bytesValue is base64, timestampValue is an RFC 3339 string, and nullValue is
// the enum string "NULL_VALUE". Integers and doubles are split (unlike
// DynamoDB's arbitrary-precision {"N": "42"}), and timestamp is first-class.
type Value struct {
	NullValue      *string     `json:"nullValue,omitempty"`
	BooleanValue   *bool       `json:"booleanValue,omitempty"`
	IntegerValue   *int64      `json:"integerValue,omitempty"`
	DoubleValue    *float64    `json:"doubleValue,omitempty"`
	TimestampValue *time.Time  `json:"timestampValue,omitempty"`
	StringValue    *string     `json:"stringValue,omitempty"`
	BytesValue     []byte      `json:"bytesValue,omitempty"`
	ReferenceValue *string     `json:"referenceValue,omitempty"`
	GeoPointValue  *GeoPoint   `json:"geoPointValue,omitempty"`
	ArrayValue     *ArrayValue `json:"arrayValue,omitempty"`
	MapValue       *MapValue   `json:"mapValue,omitempty"`
}

// MarshalJSON encodes the value with exactly one variant field, using the
// Firestore wire shapes (integerValue as a decimal string, bytesValue as
// base64, timestampValue as RFC 3339).
func (v *Value) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, 1)
	switch {
	case v.NullValue != nil:
		m["nullValue"] = *v.NullValue
	case v.BooleanValue != nil:
		m["booleanValue"] = *v.BooleanValue
	case v.IntegerValue != nil:
		m["integerValue"] = strconv.FormatInt(*v.IntegerValue, 10)
	case v.DoubleValue != nil:
		m["doubleValue"] = encodeDouble(*v.DoubleValue)
	case v.TimestampValue != nil:
		m["timestampValue"] = v.TimestampValue.Format(time.RFC3339Nano)
	case v.StringValue != nil:
		m["stringValue"] = *v.StringValue
	case v.BytesValue != nil:
		m["bytesValue"] = base64.StdEncoding.EncodeToString(v.BytesValue)
	case v.ReferenceValue != nil:
		m["referenceValue"] = *v.ReferenceValue
	case v.GeoPointValue != nil:
		m["geoPointValue"] = v.GeoPointValue
	case v.ArrayValue != nil:
		m["arrayValue"] = v.ArrayValue
	case v.MapValue != nil:
		m["mapValue"] = v.MapValue
	}
	return json.Marshal(m)
}

// encodeDouble renders a doubleValue for the wire: NaN/±Infinity are encoded as
// the strings "NaN"/"Infinity"/"-Infinity" (protobuf JSON mapping) since
// encoding/json cannot emit them as JSON numbers.
func encodeDouble(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	default:
		return f
	}
}

// UnmarshalJSON decodes a Firestore value from its REST wire form. A value with
// more than one variant field is rejected; an empty value object yields the
// zero Value (which is only valid transiently, never persisted).
func (v *Value) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) > 1 {
		return fmt.Errorf("Firestore value must have exactly one variant, found %d", len(raw))
	}
	for key, rv := range raw {
		switch key {
		case "nullValue":
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				return err
			}
			v.NullValue = &s
		case "booleanValue":
			var b bool
			if err := json.Unmarshal(rv, &b); err != nil {
				return err
			}
			v.BooleanValue = &b
		case "integerValue":
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				return err
			}
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			v.IntegerValue = &n
		case "doubleValue":
			var f float64
			if err := json.Unmarshal(rv, &f); err != nil {
				// Not a plain JSON number: the special double strings
				// ("NaN"/"Infinity"/"-Infinity") per the protobuf JSON mapping.
				var s string
				if serr := json.Unmarshal(rv, &s); serr != nil {
					return err
				}
				switch s {
				case "NaN":
					f = math.NaN()
				case "Infinity":
					f = math.Inf(1)
				case "-Infinity":
					f = math.Inf(-1)
				default:
					return fmt.Errorf("invalid doubleValue %q", s)
				}
			}
			v.DoubleValue = &f
		case "timestampValue":
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				return err
			}
			t, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return err
			}
			v.TimestampValue = &t
		case "stringValue":
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				return err
			}
			v.StringValue = &s
		case "bytesValue":
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				return err
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return err
			}
			v.BytesValue = b
		case "referenceValue":
			var s string
			if err := json.Unmarshal(rv, &s); err != nil {
				return err
			}
			v.ReferenceValue = &s
		case "geoPointValue":
			var g GeoPoint
			if err := json.Unmarshal(rv, &g); err != nil {
				return err
			}
			v.GeoPointValue = &g
		case "arrayValue":
			var a ArrayValue
			if err := json.Unmarshal(rv, &a); err != nil {
				return err
			}
			v.ArrayValue = &a
		case "mapValue":
			var m MapValue
			if err := json.Unmarshal(rv, &m); err != nil {
				return err
			}
			v.MapValue = &m
		default:
			return fmt.Errorf("unknown Firestore value variant %q", key)
		}
	}
	return nil
}

// ─── constructors ─────────────────────────────────────────────────────────────

// StringVal returns a stringValue.
func StringVal(s string) *Value { return &Value{StringValue: &s} }

// BoolVal returns a booleanValue.
func BoolVal(b bool) *Value { return &Value{BooleanValue: &b} }

// IntVal returns an integerValue.
func IntVal(n int64) *Value { return &Value{IntegerValue: &n} }

// DoubleVal returns a doubleValue.
func DoubleVal(f float64) *Value { return &Value{DoubleValue: &f} }

// TimestampVal returns a timestampValue.
func TimestampVal(t time.Time) *Value { return &Value{TimestampValue: &t} }

// NullVal returns a nullValue.
func NullVal() *Value { return &Value{NullValue: strPtr(NullEnumValue)} }

// BytesVal returns a bytesValue.
func BytesVal(b []byte) *Value { return &Value{BytesValue: b} }

// ReferenceVal returns a referenceValue (a document resource name).
func ReferenceVal(r string) *Value { return &Value{ReferenceValue: &r} }

// GeoPointVal returns a geoPointValue.
func GeoPointVal(lat, lon float64) *Value {
	return &Value{GeoPointValue: &GeoPoint{Latitude: lat, Longitude: lon}}
}

// ArrayVal returns an arrayValue.
func ArrayVal(vals ...*Value) *Value { return &Value{ArrayValue: &ArrayValue{Values: vals}} }

// MapVal returns a mapValue.
func MapVal(fields map[string]*Value) *Value { return &Value{MapValue: &MapValue{Fields: fields}} }

func strPtr(s string) *string { return &s }

// ─── accessors ────────────────────────────────────────────────────────────────

// Type returns the variant kind ("stringValue", "integerValue", ...), or ""
// when no variant is set.
func (v *Value) Type() string {
	switch {
	case v == nil:
		return ""
	case v.NullValue != nil:
		return "nullValue"
	case v.BooleanValue != nil:
		return "booleanValue"
	case v.IntegerValue != nil:
		return "integerValue"
	case v.DoubleValue != nil:
		return "doubleValue"
	case v.TimestampValue != nil:
		return "timestampValue"
	case v.StringValue != nil:
		return "stringValue"
	case v.BytesValue != nil:
		return "bytesValue"
	case v.ReferenceValue != nil:
		return "referenceValue"
	case v.GeoPointValue != nil:
		return "geoPointValue"
	case v.ArrayValue != nil:
		return "arrayValue"
	case v.MapValue != nil:
		return "mapValue"
	}
	return ""
}

// AsString returns the stringValue variant.
func (v *Value) AsString() (string, bool) {
	if v == nil || v.StringValue == nil {
		return "", false
	}
	return *v.StringValue, true
}

// AsBool returns the booleanValue variant.
func (v *Value) AsBool() (bool, bool) {
	if v == nil || v.BooleanValue == nil {
		return false, false
	}
	return *v.BooleanValue, true
}

// AsInt64 returns the integerValue variant.
func (v *Value) AsInt64() (int64, bool) {
	if v == nil || v.IntegerValue == nil {
		return 0, false
	}
	return *v.IntegerValue, true
}

// AsFloat64 returns the doubleValue variant.
func (v *Value) AsFloat64() (float64, bool) {
	if v == nil || v.DoubleValue == nil {
		return 0, false
	}
	return *v.DoubleValue, true
}

// AsTimestamp returns the timestampValue variant.
func (v *Value) AsTimestamp() (time.Time, bool) {
	if v == nil || v.TimestampValue == nil {
		return time.Time{}, false
	}
	return *v.TimestampValue, true
}

// AsBytes returns the bytesValue variant.
func (v *Value) AsBytes() ([]byte, bool) {
	if v == nil || v.BytesValue == nil {
		return nil, false
	}
	return v.BytesValue, true
}

// AsReference returns the referenceValue variant.
func (v *Value) AsReference() (string, bool) {
	if v == nil || v.ReferenceValue == nil {
		return "", false
	}
	return *v.ReferenceValue, true
}

// AsGeoPoint returns the geoPointValue variant.
func (v *Value) AsGeoPoint() (GeoPoint, bool) {
	if v == nil || v.GeoPointValue == nil {
		return GeoPoint{}, false
	}
	return *v.GeoPointValue, true
}

// AsArray returns the arrayValue variant.
func (v *Value) AsArray() ([]*Value, bool) {
	if v == nil || v.ArrayValue == nil {
		return nil, false
	}
	return v.ArrayValue.Values, true
}

// AsMap returns the mapValue variant.
func (v *Value) AsMap() (map[string]*Value, bool) {
	if v == nil || v.MapValue == nil {
		return nil, false
	}
	return v.MapValue.Fields, true
}
