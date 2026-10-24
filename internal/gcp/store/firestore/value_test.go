package firestore

import (
	"encoding/json"
	"math"
	"testing"
)

// TestValueSpecialDoubleJSONRoundTrip asserts NaN/±Infinity doubles round-trip
// through the Firestore wire encoding: they are emitted as the strings
// "NaN"/"Infinity"/"-Infinity" (protobuf JSON mapping), then decoded back to
// the matching special float values.
func TestValueSpecialDoubleJSONRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		val   float64
		wire  string
		check func(float64) bool
	}{
		{"nan", math.NaN(), "NaN", math.IsNaN},
		{"pos_inf", math.Inf(1), "Infinity", func(f float64) bool { return math.IsInf(f, 1) }},
		{"neg_inf", math.Inf(-1), "-Infinity", func(f float64) bool { return math.IsInf(f, -1) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(DoubleVal(tc.val))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var raw map[string]any
			if err := json.Unmarshal(b, &raw); err != nil {
				t.Fatalf("unmarshal raw: %v", err)
			}
			if got, ok := raw["doubleValue"].(string); !ok || got != tc.wire {
				t.Fatalf("expected doubleValue %q, got %v", tc.wire, raw["doubleValue"])
			}

			var got Value
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			f, ok := got.AsFloat64()
			if !ok {
				t.Fatalf("expected doubleValue, got %+v", got)
			}
			if !tc.check(f) {
				t.Fatalf("round-trip mismatch: got %v", f)
			}
		})
	}
}

// TestValueNullDecodesToCanonicalEnum asserts both wire encodings of a null
// value — protojson's JSON null (what real GCP's REST transcoder emits, and what
// the Firestore REST reference sends) and the Discovery enum name "NULL_VALUE"
// (a tolerant input) — decode to the canonical enum name, so a document written
// over REST reads back the same null variant as one written over gRPC. The
// encode side renders JSON null, matching real GCP (AUD3-13).
func TestValueNullDecodesToCanonicalEnum(t *testing.T) {
	for _, wire := range []string{`{"nullValue":null}`, `{"nullValue":"NULL_VALUE"}`} {
		var v Value
		if err := json.Unmarshal([]byte(wire), &v); err != nil {
			t.Fatalf("unmarshal %s: %v", wire, err)
		}
		if v.Type() != "nullValue" {
			t.Fatalf("%s: expected a nullValue, got %q (%+v)", wire, v.Type(), v)
		}
		if v.NullValue == nil || *v.NullValue != NullEnumValue {
			t.Fatalf("%s: expected NullValue %q, got %v", wire, NullEnumValue, v.NullValue)
		}
		out, err := json.Marshal(&v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(out) != `{"nullValue":null}` {
			t.Fatalf("%s: wire round-trip = %s", wire, out)
		}
	}
}
