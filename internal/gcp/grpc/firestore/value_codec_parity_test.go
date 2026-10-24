package firestore

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protojson"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestValueCodecsAgree pins Firestore's two Value codecs to one logical
// mapping. The transport-neutral provider Service operates on
// firestorestore.Value, but the two transport boundaries encode it
// independently:
//
//   - the REST boundary uses the store Value's bespoke Discovery-JSON
//     MarshalJSON/UnmarshalJSON, and
//   - the gRPC boundary uses encodeValue/decodeValue (protobuf ⇄ store Value).
//
// They are separate implementations, so a change to one can silently diverge
// from the other — exactly what produced the AUD3-3 null-loss bug, where the
// REST decoder turned a JSON-null nullValue into the empty string while the gRPC
// path stored NULL_VALUE. For every variant (including nested array/map and the
// zero scalar forms) this test asserts that:
//
//  1. the two encodings are the same logical JSON — both render
//     google.protobuf.NullValue as JSON null (AUD3-13), so a one-sided
//     regression fails here,
//  2. each codec round-trips its own wire form back to the same store value,
//     and
//  3. each decoder accepts the other boundary's wire form, so the two wire
//     encodings are mutually intelligible.
//
// The cross-transport parity suite (tests/gcpparity) catches this class
// end-to-end against a live emulator for the fields its scenarios send; this
// test guards the whole Value union, including variants the scenario does not.
func TestValueCodecsAgree(t *testing.T) {
	cases := map[string]*firestorestore.Value{
		"null":         firestorestore.NullVal(),
		"bool_true":    firestorestore.BoolVal(true),
		"bool_false":   firestorestore.BoolVal(false),
		"int":          firestorestore.IntVal(-42),
		"int_zero":     firestorestore.IntVal(0),
		"double":       firestorestore.DoubleVal(3.14),
		"double_zero":  firestorestore.DoubleVal(0),
		"timestamp":    firestorestore.TimestampVal(time.Date(2026, 9, 5, 12, 0, 0, 123456789, time.UTC)),
		"string":       firestorestore.StringVal("hi"),
		"string_empty": firestorestore.StringVal(""),
		"bytes":        firestorestore.BytesVal([]byte{0x01, 0x02}),
		"reference":    firestorestore.ReferenceVal("projects/p/databases/(default)/documents/c/d"),
		"geopoint":     firestorestore.GeoPointVal(37.7749, -122.4194),
		"array": firestorestore.ArrayVal(
			firestorestore.IntVal(1),
			firestorestore.StringVal("x"),
			firestorestore.NullVal(),
		),
		"map": firestorestore.MapVal(map[string]*firestorestore.Value{
			"k": firestorestore.IntVal(7),
			"n": firestorestore.NullVal(),
		}),
	}

	grpcJSON := func(t *testing.T, v *firestorestore.Value) json.RawMessage {
		t.Helper()
		b, err := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(encodeValue(v))
		if err != nil {
			t.Fatalf("protojson marshal %s: %v", v.Type(), err)
		}
		return b
	}
	restJSON := func(t *testing.T, v *firestorestore.Value) json.RawMessage {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("rest marshal %s: %v", v.Type(), err)
		}
		return b
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			gj := grpcJSON(t, in)
			rj := restJSON(t, in)

			// 1. The two boundaries must encode the same logical value —
			// including google.protobuf.NullValue, which both render as JSON
			// null since AUD3-13. A one-sided regression (the REST codec back
			// to the Discovery enum name, or the gRPC codec off JSON null)
			// fails here.
			if got, want := asAny(gj), asAny(rj); !reflect.DeepEqual(got, want) {
				t.Fatalf("codecs disagree:\n grpc=%s\n rest=%s", gj, rj)
			}

			// 2a. The gRPC codec must round-trip its wire form.
			back, err := decodeValue(encodeValue(in))
			if err != nil {
				t.Fatalf("gRPC decode: %v", err)
			}
			if got, want := mustJSON(t, back), mustJSON(t, in); got != want {
				t.Fatalf("gRPC round-trip changed the value:\n got=%s\nwant=%s", got, want)
			}

			// 2b. The REST codec must round-trip its wire form.
			var restBack firestorestore.Value
			if err := json.Unmarshal(rj, &restBack); err != nil {
				t.Fatalf("rest unmarshal: %v", err)
			}
			if got, want := mustJSON(t, &restBack), mustJSON(t, in); got != want {
				t.Fatalf("REST round-trip changed the value:\n got=%s\nwant=%s", got, want)
			}

			// 3a. The REST decoder must accept the wire form the gRPC boundary
			// produces. A REST client sends the same logical JSON a typed client
			// does, so this is the direction the AUD3-3 null-loss bug broke:
			// protojson renders google.protobuf.NullValue as JSON null, and the
			// REST decoder once collapsed that to the empty string.
			var crossREST firestorestore.Value
			if err := json.Unmarshal(gj, &crossREST); err != nil {
				t.Fatalf("rest decode of the gRPC wire form %s: %v", gj, err)
			}
			if got, want := mustJSON(t, &crossREST), mustJSON(t, in); got != want {
				t.Fatalf("REST decoder rejected the gRPC wire form:\n grpc=%s\n got=%s\nwant=%s", gj, got, want)
			}

			// 3b. The gRPC decoder must accept the REST wire form, so the two
			// wire encodings are mutually intelligible.
			var pbVal firestorepb.Value
			if err := protojson.Unmarshal(rj, &pbVal); err != nil {
				t.Fatalf("protojson decode of the REST wire form %s: %v", rj, err)
			}
			crossGRPC, err := decodeValue(&pbVal)
			if err != nil {
				t.Fatalf("gRPC decode of the REST wire form: %v", err)
			}
			if got, want := mustJSON(t, crossGRPC), mustJSON(t, in); got != want {
				t.Fatalf("gRPC decoder rejected the REST wire form:\n rest=%s\n got=%s\nwant=%s", rj, got, want)
			}
		})
	}
}

// mustJSON renders a store Value through the REST (Discovery-JSON) encoding,
// which is the canonical string form two values are compared by.
func mustJSON(t *testing.T, v *firestorestore.Value) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %s: %v", v.Type(), err)
	}
	return string(b)
}

// asAny decodes a Value JSON body into a generic form so two encodings can be
// compared semantically (map key order is irrelevant to reflect.DeepEqual).
func asAny(raw json.RawMessage) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		panic(err)
	}
	return v
}

// TestValueCodecsAgreeNullIsNotDropped is a focused regression guard for the
// AUD3-3 bug specifically: a null value must survive both codecs as a null
// variant, never collapse to an empty string or a missing field.
func TestValueCodecsAgreeNullIsNotDropped(t *testing.T) {
	// The gRPC boundary: encodeValue/decodeValue keep the null variant.
	back, err := decodeValue(encodeValue(firestorestore.NullVal()))
	if err != nil {
		t.Fatal(err)
	}
	if back.Type() != "nullValue" || back.NullValue == nil || *back.NullValue != firestorestore.NullEnumValue {
		t.Fatalf("gRPC null round-trip lost the variant: %+v", back)
	}

	// The REST boundary: a JSON-null nullValue (what protojson sends) must
	// decode to the canonical enum name, not the empty string.
	for _, wire := range []string{`{"nullValue":null}`, `{"nullValue":"NULL_VALUE"}`} {
		var v firestorestore.Value
		if err := json.Unmarshal([]byte(wire), &v); err != nil {
			t.Fatalf("rest unmarshal %s: %v", wire, err)
		}
		if v.NullValue == nil || *v.NullValue != firestorestore.NullEnumValue {
			t.Fatalf("rest decode of %s lost the variant: %+v", wire, v)
		}
	}
}
