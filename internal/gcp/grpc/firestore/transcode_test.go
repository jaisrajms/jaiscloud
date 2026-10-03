package firestore

import (
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

func TestValueRoundTrip(t *testing.T) {
	vals := []*firestorestore.Value{
		firestorestore.NullVal(),
		firestorestore.BoolVal(true),
		firestorestore.IntVal(-42),
		firestorestore.DoubleVal(3.14),
		firestorestore.TimestampVal(time.Date(2026, 9, 5, 12, 0, 0, 123456789, time.UTC)),
		firestorestore.StringVal("hi"),
		firestorestore.BytesVal([]byte{0x01, 0x02}),
		firestorestore.ReferenceVal("projects/p/databases/(default)/documents/c/d"),
		firestorestore.GeoPointVal(37.7749, -122.4194),
		firestorestore.ArrayVal(firestorestore.IntVal(1), firestorestore.StringVal("x")),
		firestorestore.MapVal(map[string]*firestorestore.Value{"k": firestorestore.IntVal(7)}),
	}
	for _, in := range vals {
		pb := encodeValue(in)
		out, err := decodeValue(pb)
		if err != nil {
			t.Fatalf("decode(%v): %v", in.Type(), err)
		}
		if out.Type() != in.Type() {
			t.Fatalf("type mismatch: got %q want %q", out.Type(), in.Type())
		}
	}
}

func TestEnumMappings(t *testing.T) {
	for s, op := range map[string]firestorepb.StructuredQuery_FieldFilter_Operator{
		"LESS_THAN":             firestorepb.StructuredQuery_FieldFilter_LESS_THAN,
		"LESS_THAN_OR_EQUAL":    firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
		"GREATER_THAN":          firestorepb.StructuredQuery_FieldFilter_GREATER_THAN,
		"GREATER_THAN_OR_EQUAL": firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
		"EQUAL":                 firestorepb.StructuredQuery_FieldFilter_EQUAL,
		"NOT_EQUAL":             firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL,
		"ARRAY_CONTAINS":        firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS,
		"IN":                    firestorepb.StructuredQuery_FieldFilter_IN,
		"ARRAY_CONTAINS_ANY":    firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY,
		"NOT_IN":                firestorepb.StructuredQuery_FieldFilter_NOT_IN,
	} {
		if got := fieldFilterOpToString(op); got != s {
			t.Fatalf("fieldFilterOpToString(%v) = %q, want %q", op, got, s)
		}
		if got := fieldFilterOpFromString(s); got != op {
			t.Fatalf("fieldFilterOpFromString(%q) = %v, want %v", s, got, op)
		}
	}
	if directionToString(firestorepb.StructuredQuery_DESCENDING) != "DESCENDING" {
		t.Fatal("direction DESCENDING mismatch")
	}
	if directionToString(firestorepb.StructuredQuery_ASCENDING) != "ASCENDING" {
		t.Fatal("direction ASCENDING mismatch")
	}
	if directionFromString("ASCENDING") != firestorepb.StructuredQuery_ASCENDING {
		t.Fatal("directionFromString ASCENDING mismatch")
	}
	if compositeOpToString(firestorepb.StructuredQuery_CompositeFilter_OR) != "OR" {
		t.Fatal("compositeOpToString OR mismatch")
	}
	if unaryOpToString(firestorepb.StructuredQuery_UnaryFilter_IS_NULL) != "IS_NULL" {
		t.Fatal("unaryOpToString IS_NULL mismatch")
	}
	if serverValueToString(firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME) != "REQUEST_TIME" {
		t.Fatal("serverValueToString REQUEST_TIME mismatch")
	}
}

func TestDecodeStructuredQuery(t *testing.T) {
	pb := &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{
			{CollectionId: "cities"},
		},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
				FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
					Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "pop"},
					Op:    firestorepb.StructuredQuery_FieldFilter_GREATER_THAN,
					Value: &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 100}},
				},
			},
		},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			{
				Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "pop"},
				Direction: firestorepb.StructuredQuery_DESCENDING,
			},
		},
	}
	q, err := decodeStructuredQuery(pb)
	if err != nil {
		t.Fatalf("decodeStructuredQuery: %v", err)
	}
	if len(q.From) != 1 || q.From[0].CollectionID != "cities" {
		t.Fatalf("from mismatch: %+v", q.From)
	}
	if q.Where == nil || q.Where.FieldFilter == nil || q.Where.FieldFilter.Op != "GREATER_THAN" {
		t.Fatalf("where mismatch: %+v", q.Where)
	}
	if len(q.OrderBy) != 1 || q.OrderBy[0].Direction != "DESCENDING" {
		t.Fatalf("orderBy mismatch: %+v", q.OrderBy)
	}
}

func TestDecodeWriteTransform(t *testing.T) {
	pb := &firestorepb.Write{
		Operation: &firestorepb.Write_Transform{
			Transform: &firestorepb.DocumentTransform{
				Document: "projects/p/databases/(default)/documents/c/d",
				FieldTransforms: []*firestorepb.DocumentTransform_FieldTransform{
					{
						FieldPath: "ts",
						TransformType: &firestorepb.DocumentTransform_FieldTransform_SetToServerValue{
							SetToServerValue: firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME,
						},
					},
				},
			},
		},
	}
	w, err := decodeWrite(pb)
	if err != nil {
		t.Fatalf("decodeWrite: %v", err)
	}
	if w.Transform == nil || w.Transform.Document != "projects/p/databases/(default)/documents/c/d" {
		t.Fatalf("transform mismatch: %+v", w.Transform)
	}
	if len(w.Transform.FieldTransforms) != 1 || w.Transform.FieldTransforms[0].SetToServerValue != "REQUEST_TIME" {
		t.Fatalf("fieldTransforms mismatch: %+v", w.Transform.FieldTransforms)
	}
}
