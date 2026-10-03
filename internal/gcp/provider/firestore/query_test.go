package firestore

import (
	"math"
	"strings"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

func doc(name string, fields map[string]*firestorestore.Value) *firestorestore.Document {
	d := &firestorestore.Document{Name: name, Fields: fields}
	project, db, path, ok := firestorestore.ParseDocumentName(name)
	if ok {
		segs := strings.Split(path, "/")
		if len(segs) >= 2 {
			d.CollectionID = segs[len(segs)-2]
			d.ParentPath = "projects/" + project + "/databases/" + db + "/documents/" + strings.Join(segs[:len(segs)-1], "/")
		}
	}
	return d
}

func intField(v int64) *firestorestore.Value  { return firestorestore.IntVal(v) }
func strField(s string) *firestorestore.Value { return firestorestore.StringVal(s) }

func TestEvalFieldFilters(t *testing.T) {
	d := doc("projects/p/databases/(default)/documents/cities/SF", map[string]*firestorestore.Value{
		"state": strField("CA"),
		"pop":   intField(800000),
		"tags":  firestorestore.ArrayVal(strField("a"), strField("b"), strField("c")),
		"meta":  firestorestore.MapVal(map[string]*firestorestore.Value{"n": intField(5)}),
	})

	ff := func(op string, v *firestorestore.Value) *filter {
		return &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "state"}, Op: op, Value: v}}
	}

	cases := []struct {
		name string
		f    *filter
		want bool
	}{
		{"equal", ff("EQUAL", strField("CA")), true},
		{"equal miss", ff("EQUAL", strField("NY")), false},
		{"not_equal", ff("NOT_EQUAL", strField("NY")), true},
		{"gt", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "pop"}, Op: "GREATER_THAN", Value: intField(1)}}, true},
		{"lt miss", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "pop"}, Op: "LESS_THAN", Value: intField(1)}}, false},
		{"array_contains", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "tags"}, Op: "ARRAY_CONTAINS", Value: strField("b")}}, true},
		{"array_contains miss", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "tags"}, Op: "ARRAY_CONTAINS", Value: strField("z")}}, false},
		{"in", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "state"}, Op: "IN", Value: firestorestore.ArrayVal(strField("CA"), strField("TX"))}}, true},
		{"not_in", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "state"}, Op: "NOT_IN", Value: firestorestore.ArrayVal(strField("NY"))}}, true},
		{"array_contains_any", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "tags"}, Op: "ARRAY_CONTAINS_ANY", Value: firestorestore.ArrayVal(strField("x"), strField("b"))}}, true},
		{"nested field", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "meta.n"}, Op: "EQUAL", Value: intField(5)}}, true},
		{"nested field miss", &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "meta.n"}, Op: "EQUAL", Value: intField(6)}}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := eval(d, tc.f)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tc.want {
				t.Fatalf("eval = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvalCompositeFilters(t *testing.T) {
	d := doc("n", map[string]*firestorestore.Value{"a": intField(1), "b": strField("x")})

	andF := &filter{CompositeFilter: &compositeFilter{Op: "AND", Filters: []*filter{
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(1)}},
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "b"}, Op: "EQUAL", Value: strField("x")}},
	}}}
	if ok, _ := eval(d, andF); !ok {
		t.Errorf("AND should match")
	}

	orF := &filter{CompositeFilter: &compositeFilter{Op: "OR", Filters: []*filter{
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(99)}},
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "b"}, Op: "EQUAL", Value: strField("x")}},
	}}}
	if ok, _ := eval(d, orF); !ok {
		t.Errorf("OR should match")
	}

	orMiss := &filter{CompositeFilter: &compositeFilter{Op: "OR", Filters: []*filter{
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(99)}},
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "b"}, Op: "EQUAL", Value: strField("nope")}},
	}}}
	if ok, _ := eval(d, orMiss); ok {
		t.Errorf("OR should not match")
	}
}

func TestCompareValuesOrdering(t *testing.T) {
	vals := []*firestorestore.Value{
		firestorestore.NullVal(),
		firestorestore.BoolVal(false),
		firestorestore.BoolVal(true),
		firestorestore.IntVal(1),
		firestorestore.DoubleVal(1.5),
		firestorestore.TimestampVal(mustTime("2026-01-01T00:00:00Z")),
		firestorestore.StringVal("a"),
		firestorestore.StringVal("b"),
		firestorestore.BytesVal([]byte{1}),
		firestorestore.ReferenceVal("projects/p/databases/(default)/documents/c/d"),
		firestorestore.GeoPointVal(0, 0),
		firestorestore.ArrayVal(intField(1)),
		firestorestore.MapVal(map[string]*firestorestore.Value{"k": intField(1)}),
	}
	for i := 1; i < len(vals); i++ {
		if compareValues(vals[i-1], vals[i]) >= 0 {
			t.Errorf("expected vals[%d] < vals[%d]", i-1, i)
		}
	}
	// int/double equality across representation.
	if !valuesEqual(intField(3), firestorestore.DoubleVal(3.0)) {
		t.Errorf("3 and 3.0 should be equal")
	}
}

func TestExecuteQueryOrderingLimitCursor(t *testing.T) {
	docs := []*firestorestore.Document{
		doc("projects/p/databases/(default)/documents/cities/a", map[string]*firestorestore.Value{"pop": intField(10)}),
		doc("projects/p/databases/(default)/documents/cities/b", map[string]*firestorestore.Value{"pop": intField(30)}),
		doc("projects/p/databases/(default)/documents/cities/c", map[string]*firestorestore.Value{"pop": intField(20)}),
	}
	q := &structuredQuery{
		From: []collectionSelector{{CollectionID: "cities"}},
		OrderBy: []order{
			{Field: fieldReference{FieldPath: "pop"}, Direction: "DESCENDING"},
		},
		Limit: 2,
	}
	res, err := executeQuery(docs, q, "projects/p/databases/(default)/documents")
	if err != nil {
		t.Fatalf("executeQuery: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 results, got %d", len(res))
	}
	if res[0].Name != "projects/p/databases/(default)/documents/cities/b" || res[1].Name != "projects/p/databases/(default)/documents/cities/c" {
		t.Errorf("unexpected descending order: %s, %s", res[0].Name, res[1].Name)
	}

	// Offset.
	q2 := &structuredQuery{
		From:    []collectionSelector{{CollectionID: "cities"}},
		OrderBy: []order{{Field: fieldReference{FieldPath: "pop"}, Direction: "ASCENDING"}},
		Offset:  1,
	}
	res2, err := executeQuery(docs, q2, "projects/p/databases/(default)/documents")
	if err != nil {
		t.Fatalf("executeQuery offset: %v", err)
	}
	if len(res2) != 2 || res2[0].Name != "projects/p/databases/(default)/documents/cities/c" {
		t.Errorf("unexpected offset result: %+v", res2)
	}
}

func TestExecuteQueryCursorInclusive(t *testing.T) {
	docs := []*firestorestore.Document{
		doc("projects/p/databases/(default)/documents/cities/a", map[string]*firestorestore.Value{"pop": intField(10)}),
		doc("projects/p/databases/(default)/documents/cities/b", map[string]*firestorestore.Value{"pop": intField(20)}),
		doc("projects/p/databases/(default)/documents/cities/c", map[string]*firestorestore.Value{"pop": intField(30)}),
	}
	base := &structuredQuery{
		From:    []collectionSelector{{CollectionID: "cities"}},
		OrderBy: []order{{Field: fieldReference{FieldPath: "pop"}, Direction: "ASCENDING"}},
	}
	names := func(res []*firestorestore.Document) []string {
		out := make([]string, len(res))
		for i, d := range res {
			out[i] = d.Name
		}
		return out
	}

	// Cursor.before is a *position*, not an inclusivity flag: before=true sits
	// just before the given values (so the cursor document itself is kept for
	// start_at, giving startAt), before=false just after it (exclusive, giving
	// startAfter). The Java SDK maps startAfter → before=false.

	// startAt (inclusive) at 20 → b, c.
	q := *base
	q.StartAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: true}
	res, _ := executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 2 || got[0] != "projects/p/databases/(default)/documents/cities/b" {
		t.Errorf("startAt before=true (inclusive): got %v", got)
	}

	// startAfter (exclusive) at 20 → c only.
	q = *base
	q.StartAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: false}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 1 || got[0] != "projects/p/databases/(default)/documents/cities/c" {
		t.Errorf("startAfter before=false (exclusive): got %v", got)
	}

	// endAt (inclusive) at 20 → a, b.
	q = *base
	q.EndAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: false}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 2 || got[1] != "projects/p/databases/(default)/documents/cities/b" {
		t.Errorf("endAt before=false (inclusive): got %v", got)
	}

	// endBefore (exclusive) at 20 → a only.
	q = *base
	q.EndAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: true}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 1 || got[0] != "projects/p/databases/(default)/documents/cities/a" {
		t.Errorf("endBefore before=true (exclusive): got %v", got)
	}

	// startAt + endAt window [20, 30] → b, c.
	q = *base
	q.StartAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: true}
	q.EndAt = &cursor{Values: []*firestorestore.Value{intField(30)}, Before: false}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 2 || got[0] != "projects/p/databases/(default)/documents/cities/b" || got[1] != "projects/p/databases/(default)/documents/cities/c" {
		t.Errorf("startAt/endAt window: got %v", got)
	}

	// Descending order: startAfter 20 → a only (the next value below 20).
	// Sort is c(30), b(20), a(10); startAfter excludes 20.
	descBase := &structuredQuery{
		From:    []collectionSelector{{CollectionID: "cities"}},
		OrderBy: []order{{Field: fieldReference{FieldPath: "pop"}, Direction: "DESCENDING"}},
	}
	q = *descBase
	q.StartAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: false}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 1 || got[0] != "projects/p/databases/(default)/documents/cities/a" {
		t.Errorf("descending startAfter: got %v", got)
	}

	// Descending endBefore 20 → c only (values strictly above 20, since the
	// sort is descending).
	q = *descBase
	q.EndAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: true}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 1 || got[0] != "projects/p/databases/(default)/documents/cities/c" {
		t.Errorf("descending endBefore: got %v", got)
	}

	// Composite (prefix) cursor on the first order-by field: startAfter pop=20
	// with a second order-by field still excludes the 20 document.
	composite := &structuredQuery{
		From: []collectionSelector{{CollectionID: "cities"}},
		OrderBy: []order{
			{Field: fieldReference{FieldPath: "pop"}, Direction: "ASCENDING"},
			{Field: fieldReference{FieldPath: "name"}, Direction: "ASCENDING"},
		},
	}
	for _, d := range docs {
		d.Fields["name"] = strField(d.Name)
	}
	q = *composite
	q.StartAt = &cursor{Values: []*firestorestore.Value{intField(20)}, Before: false}
	res, _ = executeQuery(docs, &q, "projects/p/databases/(default)/documents")
	if got := names(res); len(got) != 1 || got[0] != "projects/p/databases/(default)/documents/cities/c" {
		t.Errorf("composite startAfter prefix: got %v", got)
	}
}

func TestExecuteQueryProjection(t *testing.T) {
	docs := []*firestorestore.Document{
		doc("projects/p/databases/(default)/documents/cities/a", map[string]*firestorestore.Value{
			"name": strField("A"), "pop": intField(10),
		}),
	}
	q := &structuredQuery{
		From:   []collectionSelector{{CollectionID: "cities"}},
		Select: &projection{Fields: []fieldReference{{FieldPath: "name"}}},
	}
	res, err := executeQuery(docs, q, "projects/p/databases/(default)/documents")
	if err != nil {
		t.Fatalf("executeQuery: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 result, got %d", len(res))
	}
	if _, ok := res[0].Fields["name"]; !ok {
		t.Errorf("expected projected field name, got %+v", res[0].Fields)
	}
	if _, ok := res[0].Fields["pop"]; ok {
		t.Errorf("pop should not be projected")
	}
}

func TestValidateFilterLimits(t *testing.T) {
	// NOT_IN with 11 values → rejected (max 10).
	big := make([]*firestorestore.Value, 11)
	for i := range big {
		big[i] = intField(int64(i))
	}
	f := &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "x"}, Op: "NOT_IN", Value: firestorestore.ArrayVal(big...)}}
	if err := validateFilter(f); err == nil {
		t.Errorf("expected NOT_IN with 11 values to be rejected")
	}

	// IN with 11-30 values → accepted.
	for _, n := range []int{11, 30} {
		vals := make([]*firestorestore.Value, n)
		for i := range vals {
			vals[i] = intField(int64(i))
		}
		f := &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "x"}, Op: "IN", Value: firestorestore.ArrayVal(vals...)}}
		if err := validateFilter(f); err != nil {
			t.Errorf("expected IN with %d values to be accepted, got %v", n, err)
		}
	}

	// IN with 31 values → rejected (max 30).
	vals31 := make([]*firestorestore.Value, 31)
	for i := range vals31 {
		vals31[i] = intField(int64(i))
	}
	f = &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "x"}, Op: "IN", Value: firestorestore.ArrayVal(vals31...)}}
	if err := validateFilter(f); err == nil {
		t.Errorf("expected IN with 31 values to be rejected")
	}

	// 31 disjunctions via 3 IN filters of sizes 3,4,3 = 36.
	f = &filter{CompositeFilter: &compositeFilter{Op: "AND", Filters: []*filter{
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "IN", Value: firestorestore.ArrayVal(intField(1), intField(2), intField(3))}},
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "b"}, Op: "IN", Value: firestorestore.ArrayVal(intField(1), intField(2), intField(3), intField(4))}},
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "c"}, Op: "IN", Value: firestorestore.ArrayVal(intField(1), intField(2), intField(3))}},
	}}}
	if err := validateFilter(f); err == nil {
		t.Errorf("expected 36 disjunctions to be rejected")
	}
}

func TestAnalyzeQuery(t *testing.T) {
	// Single field: no index required.
	q := &structuredQuery{OrderBy: []order{{Field: fieldReference{FieldPath: "pop"}, Direction: "ASCENDING"}}}
	req, err := analyzeQuery(q)
	if err != nil || len(req) != 0 {
		t.Errorf("single orderBy should not require index: req=%v err=%v", req, err)
	}

	// Multiple orderBy fields: composite required.
	q = &structuredQuery{OrderBy: []order{
		{Field: fieldReference{FieldPath: "a"}, Direction: "ASCENDING"},
		{Field: fieldReference{FieldPath: "b"}, Direction: "ASCENDING"},
	}}
	req, err = analyzeQuery(q)
	if err != nil || len(req) != 2 {
		t.Errorf("multi orderBy should require index: req=%v err=%v", req, err)
	}

	// Inequality field not first orderBy → error.
	q = &structuredQuery{
		Where:   &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "GREATER_THAN", Value: intField(1)}},
		OrderBy: []order{{Field: fieldReference{FieldPath: "b"}, Direction: "ASCENDING"}},
	}
	if _, err := analyzeQuery(q); err == nil {
		t.Errorf("inequality not first orderBy should error")
	}

	// Inequality first + extra orderBy → composite required.
	q = &structuredQuery{
		Where: &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "GREATER_THAN", Value: intField(1)}},
		OrderBy: []order{
			{Field: fieldReference{FieldPath: "a"}, Direction: "ASCENDING"},
			{Field: fieldReference{FieldPath: "b"}, Direction: "ASCENDING"},
		},
	}
	req, err = analyzeQuery(q)
	if err != nil || len(req) != 2 {
		t.Errorf("inequality + extra orderBy should require index: req=%v err=%v", req, err)
	}

	// Composite filter over two fields → index required.
	q = &structuredQuery{Where: &filter{CompositeFilter: &compositeFilter{Op: "AND", Filters: []*filter{
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(1)}},
		{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "b"}, Op: "EQUAL", Value: intField(2)}},
	}}}}
	req, err = analyzeQuery(q)
	if err != nil || len(req) != 2 {
		t.Errorf("composite filter should require index: req=%v err=%v", req, err)
	}

	// Equality filter + orderBy on another field → (a,b) required.
	q = &structuredQuery{
		Where:   &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(1)}},
		OrderBy: []order{{Field: fieldReference{FieldPath: "b"}, Direction: "ASCENDING"}},
	}
	req, err = analyzeQuery(q)
	if err != nil || len(req) != 2 || req[0] != "a" || req[1] != "b" {
		t.Errorf("equality + orderBy should require (a,b): req=%v err=%v", req, err)
	}

	// Equality + multi orderBy → (a,b,c) required.
	q = &structuredQuery{
		Where: &filter{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(1)}},
		OrderBy: []order{
			{Field: fieldReference{FieldPath: "b"}, Direction: "ASCENDING"},
			{Field: fieldReference{FieldPath: "c"}, Direction: "ASCENDING"},
		},
	}
	req, err = analyzeQuery(q)
	if err != nil || len(req) != 3 || req[0] != "a" || req[1] != "b" || req[2] != "c" {
		t.Errorf("equality + multi orderBy should require (a,b,c): req=%v err=%v", req, err)
	}

	// Two equality filters + orderBy on another field → (a,b,c) required.
	q = &structuredQuery{
		Where: &filter{CompositeFilter: &compositeFilter{Op: "AND", Filters: []*filter{
			{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "a"}, Op: "EQUAL", Value: intField(1)}},
			{FieldFilter: &fieldFilter{Field: fieldReference{FieldPath: "b"}, Op: "EQUAL", Value: intField(2)}},
		}}},
		OrderBy: []order{{Field: fieldReference{FieldPath: "c"}, Direction: "ASCENDING"}},
	}
	req, err = analyzeQuery(q)
	if err != nil || len(req) != 3 || req[0] != "a" || req[1] != "b" || req[2] != "c" {
		t.Errorf("two equality + orderBy should require (a,b,c): req=%v err=%v", req, err)
	}
}

func TestEvalUnaryFilters(t *testing.T) {
	nan := math.NaN()
	d := doc("n", map[string]*firestorestore.Value{
		"a":   strField("x"),
		"n":   firestorestore.NullVal(),
		"d":   firestorestore.DoubleVal(1.5),
		"nan": firestorestore.DoubleVal(nan),
	})

	uf := func(fp, op string) *filter {
		return &filter{UnaryFilter: &unaryFilter{Field: fieldReference{FieldPath: fp}, Op: op}}
	}

	cases := []struct {
		name string
		f    *filter
		want bool
	}{
		{"is_null present null", uf("n", "IS_NULL"), true},
		{"is_null absent", uf("missing", "IS_NULL"), true},
		{"is_null non-null", uf("a", "IS_NULL"), false},
		{"is_not_null present", uf("a", "IS_NOT_NULL"), true},
		{"is_not_null null", uf("n", "IS_NOT_NULL"), false},
		{"is_not_null absent", uf("missing", "IS_NOT_NULL"), false},
		{"is_nan nan", uf("nan", "IS_NAN"), true},
		{"is_nan non-nan double", uf("d", "IS_NAN"), false},
		{"is_nan absent", uf("missing", "IS_NAN"), false},
		{"is_not_nan non-nan double", uf("d", "IS_NOT_NAN"), true},
		{"is_not_nan nan", uf("nan", "IS_NOT_NAN"), false},
		{"is_not_nan absent", uf("missing", "IS_NOT_NAN"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := eval(d, tc.f)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tc.want {
				t.Fatalf("eval = %v, want %v", got, tc.want)
			}
		})
	}

	// A unary filter on __name__ is invalid.
	if err := validateFilter(uf("__name__", "IS_NULL")); err == nil {
		t.Errorf("expected unary filter on __name__ to be rejected")
	}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
