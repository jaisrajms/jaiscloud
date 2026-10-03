package firestore

import (
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

func TestApplyFieldTransforms(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	base := map[string]*firestorestore.Value{
		"count":  firestorestore.IntVal(5),
		"score":  firestorestore.DoubleVal(1.5),
		"arr":    firestorestore.ArrayVal(firestorestore.IntVal(1), firestorestore.IntVal(2)),
		"exists": firestorestore.StringVal("keep"),
	}

	fields, results, err := applyFieldTransforms(base, []fieldTransformWire{
		{FieldPath: "count", Increment: firestorestore.IntVal(3)},
		{FieldPath: "score", Maximum: firestorestore.DoubleVal(2.0)},
		{FieldPath: "min", Minimum: firestorestore.IntVal(10)},
		{FieldPath: "created", SetToServerValue: "REQUEST_TIME"},
		{FieldPath: "arr", AppendMissingElements: &firestorestore.ArrayValue{Values: []*firestorestore.Value{firestorestore.IntVal(2), firestorestore.IntVal(3)}}},
		{FieldPath: "arr", RemoveAllFromArray: &firestorestore.ArrayValue{Values: []*firestorestore.Value{firestorestore.IntVal(1)}}},
	}, now)
	if err != nil {
		t.Fatalf("applyFieldTransforms: %v", err)
	}
	if len(results) != 6 {
		t.Fatalf("expected 6 transform results, got %d", len(results))
	}
	if v, _ := fields["count"].AsInt64(); v != 8 {
		t.Errorf("increment: got %v, want 8", v)
	}
	if v, _ := fields["score"].AsFloat64(); v != 2.0 {
		t.Errorf("maximum: got %v, want 2.0", v)
	}
	if v, _ := fields["min"].AsInt64(); v != 10 {
		t.Errorf("minimum on missing: got %v, want 10", v)
	}
	if v, _ := fields["created"].AsTimestamp(); !v.Equal(now) {
		t.Errorf("setToServerValue: got %v, want %v", v, now)
	}
	// setToServerValue transform result is the written Timestamp, not null.
	if v, _ := results[3].AsTimestamp(); !v.Equal(now) {
		t.Errorf("setToServerValue transform result: got %+v, want timestamp %v", results[3], now)
	}
	// appendMissingElements and removeAllFromArray still produce null results.
	if results[4].NullValue == nil || results[5].NullValue == nil {
		t.Errorf("appendMissing/removeAll transform results should be null, got %+v, %+v", results[4], results[5])
	}
	arr, _ := fields["arr"].AsArray()
	if len(arr) != 2 || !valuesEqual(arr[0], firestorestore.IntVal(2)) || !valuesEqual(arr[1], firestorestore.IntVal(3)) {
		t.Errorf("append+remove array: got %+v", arr)
	}
}

func TestApplyIncrementOverflow(t *testing.T) {
	cur := firestorestore.IntVal(1<<63 - 1)
	got := applyIncrement(cur, firestorestore.IntVal(1))
	if v, _ := got.AsInt64(); v != 1<<63-1 {
		t.Errorf("overflow should clamp, got %v", v)
	}
}
