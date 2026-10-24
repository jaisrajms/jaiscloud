package datastore

import (
	"math"
	"strconv"
	"strings"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// ─── value constructors (one variant each) ────────────────────────────────────

func valStr(s string) dsstore.Value  { return dsstore.Value{StringValue: &s} }
func valInt(n int64) dsstore.Value   { return dsstore.Value{IntegerValue: ptrInt64(n)} }
func valDbl(f float64) dsstore.Value { return dsstore.Value{DoubleValue: ptrFloat64(f)} }
func valNaN() dsstore.Value          { return dsstore.Value{DoubleValue: ptrFloat64(math.NaN())} }
func valBool(b bool) dsstore.Value   { return dsstore.Value{BooleanValue: ptrBool(b)} }
func valTS(s string) dsstore.Value   { return dsstore.Value{TimestampValue: &s} }
func valKey(s string) dsstore.Value  { return dsstore.Value{KeyValue: &s} }
func valBlob(b []byte) dsstore.Value { return dsstore.Value{BlobValue: b} }

func valNull() dsstore.Value {
	s := "NULL_VALUE"
	return dsstore.Value{NullValue: &s}
}

func valGeo(lat, lon float64) dsstore.Value {
	return dsstore.Value{GeoPointValue: &dsstore.GeoPoint{Latitude: lat, Longitude: lon}}
}

func valEntity(e dsstore.Entity) dsstore.Value { return dsstore.Value{EntityValue: &e} }

func valArray(vs ...dsstore.Value) dsstore.Value {
	return dsstore.Value{ArrayValue: &dsstore.ArrayValue{Values: vs}}
}

// inValues returns n distinct integer operands, for sizing an IN filter's array.
func inValues(n int) []dsstore.Value {
	vs := make([]dsstore.Value, n)
	for i := range vs {
		vs[i] = valInt(int64(i))
	}
	return vs
}

func entityWith(kind, key string, props map[string]dsstore.Value) dsstore.Entity {
	return dsstore.Entity{Kind: kind, Key: key, Properties: props}
}

// TestValueEqual covers every value variant plus the cross-variant negatives.
// Integers and doubles are separate types in Datastore mode (the documented
// value-type ordering), so an integer and a double are NOT equal even when they
// hold the same number; a NaN equals another NaN. (TD21)
func TestValueEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b dsstore.Value
		want bool
	}{
		{"int!=double same numeric", valInt(3), valDbl(3.0), false},
		{"double!=int same numeric", valDbl(3.0), valInt(3), false},
		{"int==int", valInt(3), valInt(3), true},
		{"int!=int", valInt(3), valInt(5), false},
		{"int!=double", valInt(3), valDbl(3.5), false},
		{"double==double", valDbl(3.0), valDbl(3.0), true},
		{"double!=double", valDbl(3.0), valDbl(3.5), false},
		{"nan==nan", valNaN(), valNaN(), true},
		{"nan!=double", valNaN(), valDbl(3.0), false},
		{"nan!=int", valNaN(), valInt(3), false},
		{"string==string", valStr("a"), valStr("a"), true},
		{"string!=string", valStr("a"), valStr("b"), false},
		{"string!=int", valStr("3"), valInt(3), false},
		{"null==null", valNull(), valNull(), true},
		{"null!=string", valNull(), valStr("x"), false},
		{"bool==bool", valBool(true), valBool(true), true},
		{"bool!=bool", valBool(true), valBool(false), false},
		{"bool!=int", valBool(true), valInt(1), false},
		{"ts==ts", valTS("2020-01-01T00:00:00Z"), valTS("2020-01-01T00:00:00Z"), true},
		{"ts!=ts", valTS("2020-01-01T00:00:00Z"), valTS("2021-01-01T00:00:00Z"), false},
		// A timestamp is an instant: the same instant with a different UTC
		// offset, or an equivalent fractional spelling, is the same value. (TD30)
		{"ts offset-equivalent", valTS("2019-12-31T19:00:00-05:00"), valTS("2020-01-01T00:00:00Z"), true},
		{"ts fraction-equivalent", valTS("2020-01-01T00:00:00.500Z"), valTS("2020-01-01T00:00:00.5Z"), true},
		// Datastore stores a timestamp only to microseconds, so two values that
		// differ below the microsecond are the same stored value. (TD42)
		{"ts sub-microsecond-equivalent", valTS("2020-01-01T00:00:00.123456Z"), valTS("2020-01-01T00:00:00.123456789Z"), true},
		{"ts sub-microsecond differs by a microsecond", valTS("2020-01-01T00:00:00.123456Z"), valTS("2020-01-01T00:00:00.123457Z"), false},
		{"ts offset different instant", valTS("2019-12-31T19:00:00-05:00"), valTS("2020-01-01T00:00:01Z"), false},
		// An unparseable timestamp falls back to the literal string, so it
		// equals itself and never a parseable instant.
		{"ts unparseable equal", valTS("not-a-time"), valTS("not-a-time"), true},
		{"ts unparseable vs parseable", valTS("not-a-time"), valTS("2020-01-01T00:00:00Z"), false},
		{"key==key", valKey("k1"), valKey("k1"), true},
		{"key!=key", valKey("k1"), valKey("k2"), false},
		{"key!=string", valKey("k1"), valStr("k1"), false},
		{"blob==blob", valBlob([]byte{1, 2}), valBlob([]byte{1, 2}), true},
		{"blob!=blob", valBlob([]byte{1, 2}), valBlob([]byte{1, 3}), false},
		{"blob!=string", valBlob([]byte("x")), valStr("x"), false},
		{"double!=string", valDbl(1), valStr("x"), false},
		{"string!=double", valStr("x"), valDbl(1), false},
		{"int!=string", valInt(1), valStr("x"), false},
		{"geo==geo", valGeo(1, 2), valGeo(1, 2), true},
		{"geo!=geo", valGeo(1, 2), valGeo(1, 3), false},
		{"geo!=string", valGeo(1, 2), valStr("geo"), false},
		{"entity==entity", valEntity(entityWith("K", "k1", nil)), valEntity(entityWith("K", "k1", nil)), true},
		{"entity!=entity", valEntity(entityWith("K", "k1", nil)), valEntity(entityWith("K", "k2", nil)), false},
		{"entity!=string", valEntity(entityWith("K", "k1", nil)), valStr("k1"), false},
		{"array==array", valArray(valInt(1), valStr("a")), valArray(valInt(1), valStr("a")), true},
		{"array len differs", valArray(valInt(1)), valArray(valInt(1), valInt(2)), false},
		{"array elem differs", valArray(valInt(1)), valArray(valInt(2)), false},
		{"array!=string", valArray(valInt(1)), valStr("x"), false},
		{"empty!=empty", dsstore.Value{}, dsstore.Value{}, false},
		// Cross-type against an absent value: these reach the later variant
		// arms (entity/array/geo/blob) and their nil guards, which the
		// same-type cases above never touch.
		{"string vs empty", valStr("x"), dsstore.Value{}, false},
		{"empty vs string", dsstore.Value{}, valStr("a"), false},
		{"empty vs bool", dsstore.Value{}, valBool(true), false},
		{"empty vs ts", dsstore.Value{}, valTS("2020-01-01T00:00:00Z"), false},
		{"empty vs key", dsstore.Value{}, valKey("a"), false},
		{"empty vs blob", dsstore.Value{}, valBlob([]byte{1}), false},
		{"empty vs double", dsstore.Value{}, valDbl(1), false},
		{"empty vs int", dsstore.Value{}, valInt(1), false},
		{"empty vs geo", dsstore.Value{}, valGeo(1, 2), false},
		{"empty vs entity", dsstore.Value{}, valEntity(entityWith("K", "k1", nil)), false},
		{"empty vs array", dsstore.Value{}, valArray(valInt(1)), false},
		// A non-numeric value never equals a numeric 0 (the numeric branch is
		// entered but only compares when both are numeric).
		{"string vs int zero", valStr("x"), valInt(0), false},
		{"int zero vs string", valInt(0), valStr("x"), false},
	}
	for _, tc := range cases {
		if got := valueEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: valueEqual = %v, want %v", tc.name, got, tc.want)
		}
		if got := valueEqual(tc.b, tc.a); got != tc.want {
			t.Errorf("%s (swapped): valueEqual = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestTimestampEqualityMatchesOrdering pins the TD30 invariant: equality and
// ordering agree on timestamps. Both compare parsed instants, so an entity
// written over REST with a non-UTC offset (`2019-12-31T19:00:00-05:00`, stored
// verbatim by transport/rest/datastore/wire.go) is EQUAL to the same instant
// spelled in UTC and is also `<=`/`>=` it — previously EQUAL compared the raw
// strings and missed it.
func TestTimestampEqualityMatchesOrdering(t *testing.T) {
	const stored = "2019-12-31T19:00:00-05:00" // == 2020-01-01T00:00:00Z
	sameInstant := valTS("2020-01-01T00:00:00Z")
	later := valTS("2020-01-01T00:00:01Z")

	if !valueEqual(valTS(stored), sameInstant) {
		t.Fatal("offset-equivalent timestamps must be equal")
	}
	if c, ok := valueCompare(valTS(stored), sameInstant); !ok || c != 0 {
		t.Fatalf("valueCompare = %d,%v; want 0,true (equality and ordering must agree)", c, ok)
	}
	if valueEqual(valTS(stored), later) {
		t.Fatal("a different instant must not be equal")
	}

	// End to end: the equality/range operators agree on the stored instant.
	e := entityWith("Kind", "k", map[string]dsstore.Value{"t": valTS(stored)})
	cases := []struct {
		name string
		op   PropertyOp
		v    dsstore.Value
		want bool
	}{
		{"equal across offsets", PropertyEqual, sameInstant, true},
		{"not equal across offsets", PropertyNotEqual, sameInstant, false},
		{"less or equal across offsets", PropertyLessThanOrEqual, sameInstant, true},
		{"greater or equal across offsets", PropertyGreaterThanOrEqual, sameInstant, true},
		{"greater than across offsets", PropertyGreaterThan, sameInstant, false},
		{"equal to a later instant", PropertyEqual, later, false},
	}
	for _, tc := range cases {
		pf := &PropertyFilter{Property: "t", Op: tc.op, Value: tc.v}
		if got := matchPropertyFilter(e, pf); got != tc.want {
			t.Errorf("%s: matchPropertyFilter = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestValueCompare covers the ordering result (-1/0/1) and the ok flag for
// orderable vs unorderable pairs. Ordering follows Datastore's documented
// value-type ordering (null < integer < timestamp < boolean < blob < string <
// double < NaN < geo point < key) and is total across those types; only the
// types with no sort order — arrays, embedded entities and absent values —
// return ok=false. Integers sort before doubles as separate types. (TD21)
func TestValueCompare(t *testing.T) {
	cases := []struct {
		name   string
		a, b   dsstore.Value
		wantC  int
		wantOK bool
	}{
		{"int<int", valInt(3), valInt(5), -1, true},
		{"int>int", valInt(5), valInt(3), 1, true},
		{"int==int", valInt(3), valInt(3), 0, true},
		// Integers are a separate type ordered before floats: 7 < 3.2.
		{"int<double same numeric", valInt(3), valDbl(3.0), -1, true},
		{"int<double", valInt(3), valDbl(3.5), -1, true},
		{"double<int", valDbl(3.5), valInt(3), 1, true},
		{"double<double", valDbl(2.5), valDbl(3.5), -1, true},
		{"double==double", valDbl(3.5), valDbl(3.5), 0, true},
		{"double<nan", valDbl(3.5), valNaN(), -1, true},
		{"nan>double", valNaN(), valDbl(3.5), 1, true},
		{"nan==nan", valNaN(), valNaN(), 0, true},
		{"str<str", valStr("a"), valStr("b"), -1, true},
		{"str>str", valStr("b"), valStr("a"), 1, true},
		{"str==str", valStr("a"), valStr("a"), 0, true},
		{"bool<", valBool(false), valBool(true), -1, true},
		{"bool>", valBool(true), valBool(false), 1, true},
		{"bool==", valBool(true), valBool(true), 0, true},
		{"ts<", valTS("2020-01-01T00:00:00Z"), valTS("2021-01-01T00:00:00Z"), -1, true},
		{"ts>", valTS("2021-01-01T00:00:00Z"), valTS("2020-01-01T00:00:00Z"), 1, true},
		{"ts==", valTS("2020-01-01T00:00:00Z"), valTS("2020-01-01T00:00:00Z"), 0, true},
		{"ts invalid", valTS("not-a-time"), valTS("2020-01-01T00:00:00Z"), 0, false},
		// Ordering is at Datastore's microsecond resolution: values differing
		// only below the microsecond order equal. (TD42)
		{"ts sub-microsecond equal", valTS("2020-01-01T00:00:00.123456Z"), valTS("2020-01-01T00:00:00.123456789Z"), 0, true},
		{"key<", valKey("a"), valKey("b"), -1, true},
		{"key>", valKey("b"), valKey("a"), 1, true},
		{"key==", valKey("a"), valKey("a"), 0, true},
		{"blob<", valBlob([]byte{1}), valBlob([]byte{2}), -1, true},
		{"blob>", valBlob([]byte{2}), valBlob([]byte{1}), 1, true},
		{"blob==", valBlob([]byte{1}), valBlob([]byte{1}), 0, true},
		{"geo<lat", valGeo(1, 2), valGeo(2, 0), -1, true},
		{"geo>lat", valGeo(2, 0), valGeo(1, 2), 1, true},
		{"geo<lon", valGeo(1, 2), valGeo(1, 3), -1, true},
		{"geo>lon", valGeo(1, 3), valGeo(1, 2), 1, true},
		{"geo==geo", valGeo(1, 2), valGeo(1, 2), 0, true},
		// The full cross-type ordering, in rank order: every type sorts before
		// the next. These pin the rank table itself.
		{"null<int", valNull(), valInt(-100), -1, true},
		{"int<ts", valInt(999999), valTS("1970-01-01T00:00:00Z"), -1, true},
		{"ts<bool", valTS("2100-01-01T00:00:00Z"), valBool(false), -1, true},
		{"bool<blob", valBool(true), valBlob([]byte{0}), -1, true},
		{"blob<string", valBlob([]byte{0xff}), valStr(""), -1, true},
		{"string<double", valStr("zzz"), valDbl(-1e300), -1, true},
		{"double<nan", valDbl(1e300), valNaN(), -1, true},
		{"nan<geo", valNaN(), valGeo(-90, -180), -1, true},
		{"geo<key", valGeo(90, 180), valKey(""), -1, true},
		// Only the types with no sort order are incomparable.
		{"array incomparable", valArray(valInt(1)), valArray(valInt(1)), 0, false},
		{"entity incomparable", valEntity(entityWith("K", "k", nil)), valEntity(entityWith("K", "k", nil)), 0, false},
		{"empty incomparable", dsstore.Value{}, dsstore.Value{}, 0, false},
		{"empty vs string", dsstore.Value{}, valStr("a"), 0, false},
		{"string vs empty", valStr("a"), dsstore.Value{}, 0, false},
		{"empty vs bool", dsstore.Value{}, valBool(true), 0, false},
		{"empty vs ts", dsstore.Value{}, valTS("2020-01-01T00:00:00Z"), 0, false},
		{"empty vs key", dsstore.Value{}, valKey("a"), 0, false},
		{"empty vs blob", dsstore.Value{}, valBlob([]byte{1}), 0, false},
		{"empty vs double", dsstore.Value{}, valDbl(1), 0, false},
	}
	for _, tc := range cases {
		c, ok := valueCompare(tc.a, tc.b)
		if ok != tc.wantOK {
			t.Errorf("%s: valueCompare ok = %v, want %v", tc.name, ok, tc.wantOK)
			continue
		}
		if ok && c != tc.wantC {
			t.Errorf("%s: valueCompare = %d, want %d", tc.name, c, tc.wantC)
		}
		// Ordering is antisymmetric: swapping swaps the sign.
		if c2, ok2 := valueCompare(tc.b, tc.a); ok2 != tc.wantOK || (ok2 && c2 != -tc.wantC) {
			t.Errorf("%s (swapped): valueCompare = %d, %v; want %d, %v", tc.name, c2, ok2, -tc.wantC, tc.wantOK)
		}
	}
}

// TestEntityEqual covers the key and property matching entityEqual performs for
// the entity_value variant of valueEqual (it compares the canonical Key and the
// property map; Kind is not part of the comparison).
func TestEntityEqual(t *testing.T) {
	base := entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(1)})
	if !entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(1)})) {
		t.Fatal("identical entities should be equal")
	}
	if entityEqual(base, entityWith("Kind", "k2", map[string]dsstore.Value{"a": valInt(1)})) {
		t.Fatal("entities with different keys must not be equal")
	}
	if entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(1), "b": valInt(2)})) {
		t.Fatal("entities with different property counts must not be equal")
	}
	if entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(2)})) {
		t.Fatal("entities with different property values must not be equal")
	}
	if entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"b": valInt(1)})) {
		t.Fatal("entities with different property names must not be equal")
	}
}

// TestOperatorNames covers the error-message renderers for every operator plus
// their unspecified/default arms.
func TestOperatorNames(t *testing.T) {
	propOps := map[PropertyOp]string{
		PropertyEqual:              "EQUAL",
		PropertyNotEqual:           "NOT_EQUAL",
		PropertyIn:                 "IN",
		PropertyLessThan:           "LESS_THAN",
		PropertyLessThanOrEqual:    "LESS_THAN_OR_EQUAL",
		PropertyGreaterThan:        "GREATER_THAN",
		PropertyGreaterThanOrEqual: "GREATER_THAN_OR_EQUAL",
		PropertyHasAncestor:        "HAS_ANCESTOR",
		PropertyUnspecified:        "PROPERTY_FILTER_OP_UNSPECIFIED",
		PropertyOp(999):            "PROPERTY_FILTER_OP_UNSPECIFIED",
	}
	for op, want := range propOps {
		if got := propertyOpName(op); got != want {
			t.Errorf("propertyOpName(%d) = %q, want %q", op, got, want)
		}
	}
	compOps := map[CompositeOp]string{
		CompositeAnd:         "AND",
		CompositeOr:          "OR",
		CompositeUnspecified: "COMPOSITE_FILTER_OP_UNSPECIFIED",
		CompositeOp(999):     "COMPOSITE_FILTER_OP_UNSPECIFIED",
	}
	for op, want := range compOps {
		if got := compositeOpName(op); got != want {
			t.Errorf("compositeOpName(%d) = %q, want %q", op, got, want)
		}
	}
}

// TestUnsupportedQueryOperatorMessage locks the fail-closed error shape: an
// unsupported operator must name the operator, never match-all.
func TestUnsupportedQueryOperatorMessage(t *testing.T) {
	err := unsupportedQueryOperator("OR")
	pe := asProviderError(t, err)
	if pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("unsupportedQueryOperator = {%q,%d}, want InvalidArgument/400", pe.Code, pe.HTTPStatus)
	}
	if !strings.Contains(pe.Message, "OR") {
		t.Fatalf("message %q should name the operator", pe.Message)
	}
}

// TestValidateFilter covers the up-front static filter validation RunQuery and
// RunAggregationQuery perform before scanning any candidate, so a request real
// Datastore rejects fails loud on an empty collection too. (TD21)
func TestValidateFilter(t *testing.T) {
	key := "projects/p/…/Child,c"
	ok := []struct {
		name string
		f    *Filter
	}{
		{"nil", nil},
		{"equal", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(1)}}},
		{"not equal", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyNotEqual, Value: valInt(1)}}},
		{"less than", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valInt(1)}}},
		{"less or equal", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyLessThanOrEqual, Value: valInt(1)}}},
		{"greater than", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyGreaterThan, Value: valInt(1)}}},
		{"greater or equal", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyGreaterThanOrEqual, Value: valInt(1)}}},
		{"in array", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valInt(1))}}},
		{"in at disjunction limit", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(inValues(maxInValues)...)}}},
		{"has ancestor", &Filter{Property: &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor, Value: dsstore.Value{KeyValue: &key}}}},
		{"and of valid", &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{
			{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(1)}},
			{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valInt(1))}},
		}}}},
	}
	for _, tc := range ok {
		if err := validateFilter(tc.f); err != nil {
			t.Errorf("%s: validateFilter = %v, want nil", tc.name, err)
		}
	}

	bad := []struct {
		name string
		f    *Filter
	}{
		{"in non-array", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valInt(1)}}},
		{"in empty array", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray()}}},
		{"in absent value", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn}}},
		{"in over disjunction limit", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(inValues(maxInValues + 1)...)}}},
		{"has ancestor wrong prop", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyHasAncestor, Value: dsstore.Value{KeyValue: &key}}}},
		{"has ancestor non-key", &Filter{Property: &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor, Value: valStr("x")}}},
		{"unspecified op", &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyUnspecified}}},
		{"or rejected", &Filter{Composite: &CompositeFilter{Op: CompositeOr, Filters: []*Filter{
			{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(1)}},
		}}}},
		{"composite unspecified", &Filter{Composite: &CompositeFilter{}}},
		{"and with no filters", &Filter{Composite: &CompositeFilter{Op: CompositeAnd}}},
		{"and with a bad clause", &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{
			{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(1)}},
			{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valInt(2)}},
		}}}},
		{"empty filter", &Filter{}},
	}
	for _, tc := range bad {
		if err := validateFilter(tc.f); err == nil {
			t.Errorf("%s: validateFilter = nil, want an error", tc.name)
		}
	}
}

// TestValidateINDisjunctionLimit pins the documented disjunction cap on an IN
// filter's operand array: exactly maxInValues (30) values is accepted, one more
// fails loud with InvalidArgument/400 and a message naming the limit. Real
// Datastore: "Use the IN operator to combine up to 30 equality (==) clauses on
// the same property with a logical OR" (TD31).
func TestValidateINDisjunctionLimit(t *testing.T) {
	atLimit := &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(inValues(maxInValues)...)}}
	if err := validateFilter(atLimit); err != nil {
		t.Fatalf("IN with %d values: validateFilter = %v, want nil", maxInValues, err)
	}

	over := &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(inValues(maxInValues + 1)...)}}
	err := validateFilter(over)
	if err == nil {
		t.Fatalf("IN with %d values: validateFilter = nil, want an error", maxInValues+1)
	}
	pe := asProviderError(t, err)
	if pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("IN over limit = {%q,%d}, want InvalidArgument/400", pe.Code, pe.HTTPStatus)
	}
	if !strings.Contains(pe.Message, strconv.Itoa(maxInValues)) {
		t.Fatalf("message %q should name the %d-value limit", pe.Message, maxInValues)
	}
}

// TestValidateQueryDisjunctionBudget pins the per-query disjunction cap (TD43):
// real GCP limits a query to 30 disjunctions based on its disjunctive normal
// form, so several IN filters in one AND multiply and an oversized combined
// query fails loud with InvalidArgument/400 naming the limit — even though each
// IN operand is individually within maxInValues. The emulator is AND-only, so
// the running product of the IN operand counts is the DNF size.
func TestValidateQueryDisjunctionBudget(t *testing.T) {
	in := func(prop string, n int) *Filter {
		return &Filter{Property: &PropertyFilter{Property: prop, Op: PropertyIn, Value: valArray(inValues(n)...)}}
	}
	and := func(fs ...*Filter) *Filter {
		return &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: fs}}
	}

	ok := []struct {
		name string
		f    *Filter
	}{
		{"single IN at limit", in("a", maxInValues)},
		{"two INs at product limit", and(in("a", 6), in("b", 5))},
		{"non-disjunctive clause does not multiply", and(in("a", maxInValues), &Filter{
			Property: &PropertyFilter{Property: "b", Op: PropertyEqual, Value: valInt(1)}})},
		{"nested AND at product limit", and(in("a", 5), and(in("b", 6), in("c", 1)))},
	}
	for _, tc := range ok {
		if err := validateFilter(tc.f); err != nil {
			t.Errorf("%s: validateFilter = %v, want nil", tc.name, err)
		}
	}

	bad := []struct {
		name string
		f    *Filter
	}{
		// 20*20 = 400 disjunctions, each operand individually legal: the exact
		// shape the TD31 review identified as accepted-but-rejected-by-real-GCP.
		{"two INs over product limit", and(in("a", 20), in("b", 20))},
		{"product just over limit", and(in("a", 10), in("b", 4))},
		{"nested AND over product limit", and(in("a", 5), and(in("b", 7), in("c", 1)))},
	}
	for _, tc := range bad {
		err := validateFilter(tc.f)
		if err == nil {
			t.Errorf("%s: validateFilter = nil, want an error", tc.name)
			continue
		}
		pe := asProviderError(t, err)
		if pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("%s: = {%q,%d}, want InvalidArgument/400", tc.name, pe.Code, pe.HTTPStatus)
		}
		if !strings.Contains(pe.Message, strconv.Itoa(maxDisjunctions)) {
			t.Errorf("%s: message %q should name the %d-disjunction limit", tc.name, pe.Message, maxDisjunctions)
		}
	}
}

// TestMatchesFilterDispatch covers the filter dispatcher: nil matches, AND
// composites evaluate every clause, and every unsupported/incomplete shape
// fails closed with an InvalidArgument error rather than matching everything.
func TestMatchesFilterDispatch(t *testing.T) {
	e := entityWith("Kind", "k", map[string]dsstore.Value{
		"n":   valInt(5),
		"tag": valStr("x"),
	})

	if ok, err := matchesFilter(e, nil); !ok || err != nil {
		t.Fatalf("nil filter = %v,%v; want true,nil", ok, err)
	}

	eq := &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(5)}}
	neq := &Filter{Property: &PropertyFilter{Property: "tag", Op: PropertyEqual, Value: valStr("y")}}
	if ok, err := matchesFilter(e, &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{eq, eq}}}); !ok || err != nil {
		t.Fatalf("AND(all true) = %v,%v; want true,nil", ok, err)
	}
	if ok, err := matchesFilter(e, &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{eq, neq}}}); ok || err != nil {
		t.Fatalf("AND(one false) = %v,%v; want false,nil", ok, err)
	}

	errCases := []struct {
		name string
		f    *Filter
	}{
		{"composite OR", &Filter{Composite: &CompositeFilter{Op: CompositeOr, Filters: []*Filter{eq}}}},
		{"composite AND with no filters", &Filter{Composite: &CompositeFilter{Op: CompositeAnd}}},
		{"composite unspecified", &Filter{Composite: &CompositeFilter{}}},
		{"empty filter", &Filter{}},
	}
	for _, tc := range errCases {
		if _, err := matchesFilter(e, tc.f); err == nil {
			t.Errorf("%s: expected an InvalidArgument error", tc.name)
		}
	}

	// An error in a nested AND clause must propagate, not be swallowed.
	bad := &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyUnspecified}}
	if _, err := matchesFilter(e, &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{bad}}}); err == nil {
		t.Fatal("AND with an unsupported clause must propagate the error")
	}
}

// TestMatchPropertyFilterOperators covers every property operator against a
// present, absent and incomparable property. The filter shapes here are all
// valid; the invalid-shape contract is TestValidateFilter's.
func TestMatchPropertyFilterOperators(t *testing.T) {
	e := entityWith("Kind", "k", map[string]dsstore.Value{
		"n":    valInt(5),
		"tags": valArray(valStr("a"), valStr("b")),
	})

	cases := []struct {
		name string
		pf   *PropertyFilter
		want bool
	}{
		{"equal match", &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(5)}, true},
		{"equal miss", &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(6)}, false},
		{"equal absent prop", &PropertyFilter{Property: "missing", Op: PropertyEqual, Value: valInt(5)}, false},
		{"not equal match", &PropertyFilter{Property: "n", Op: PropertyNotEqual, Value: valInt(6)}, true},
		{"not equal same", &PropertyFilter{Property: "n", Op: PropertyNotEqual, Value: valInt(5)}, false},
		{"not equal absent prop", &PropertyFilter{Property: "missing", Op: PropertyNotEqual, Value: valInt(5)}, false},
		{"in match", &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valInt(1), valInt(5))}, true},
		{"in miss", &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valInt(1), valInt(2))}, false},
		{"in absent prop", &PropertyFilter{Property: "missing", Op: PropertyIn, Value: valArray(valInt(1))}, false},
		{"in cross-type no match", &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valDbl(5))}, false},
		{"lt", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valInt(6)}, true},
		{"lt equal", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valInt(5)}, false},
		{"lte equal", &PropertyFilter{Property: "n", Op: PropertyLessThanOrEqual, Value: valInt(5)}, true},
		{"gt", &PropertyFilter{Property: "n", Op: PropertyGreaterThan, Value: valInt(4)}, true},
		{"gt equal is not greater", &PropertyFilter{Property: "n", Op: PropertyGreaterThan, Value: valInt(5)}, false},
		{"gte equal", &PropertyFilter{Property: "n", Op: PropertyGreaterThanOrEqual, Value: valInt(5)}, true},
		{"gte miss", &PropertyFilter{Property: "n", Op: PropertyGreaterThanOrEqual, Value: valInt(6)}, false},
		{"lte miss", &PropertyFilter{Property: "n", Op: PropertyLessThanOrEqual, Value: valInt(4)}, false},
		{"comparison absent prop", &PropertyFilter{Property: "missing", Op: PropertyLessThan, Value: valInt(9)}, false},
		// An array-valued property is compared elementwise (TD28): the entity
		// satisfies the filter when any element compares to the operand. The
		// `tags` property is ["a","b"].
		{"range over array value", &PropertyFilter{Property: "tags", Op: PropertyGreaterThanOrEqual, Value: valStr("a")}, true},
		{"range over array value lt", &PropertyFilter{Property: "tags", Op: PropertyLessThan, Value: valStr("a")}, false},
		{"array equal element", &PropertyFilter{Property: "tags", Op: PropertyEqual, Value: valStr("b")}, true},
		{"array equal miss", &PropertyFilter{Property: "tags", Op: PropertyEqual, Value: valStr("c")}, false},
		{"array equal array operand", &PropertyFilter{Property: "tags", Op: PropertyEqual, Value: valArray(valStr("a"), valStr("b"))}, false},
		{"array not equal element", &PropertyFilter{Property: "tags", Op: PropertyNotEqual, Value: valStr("a")}, true},
		{"array not equal miss", &PropertyFilter{Property: "tags", Op: PropertyNotEqual, Value: valStr("c")}, true},
		{"array in element", &PropertyFilter{Property: "tags", Op: PropertyIn, Value: valArray(valStr("x"), valStr("b"))}, true},
		{"array in miss", &PropertyFilter{Property: "tags", Op: PropertyIn, Value: valArray(valStr("x"), valStr("y"))}, false},
		// A cross-type comparison order by the documented type ordering: the
		// integer 5 sorts before ANY string, so `n < "z"` matches. (TD21)
		{"comparison cross-type", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valStr("z")}, true},
		{"comparison cross-type reverse", &PropertyFilter{Property: "n", Op: PropertyGreaterThan, Value: valStr("z")}, false},
		// A float filter does not compare equal to an integer property, but the
		// integer still sorts before the float.
		{"equal cross-type misses", &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valDbl(5)}, false},
		{"lt cross-type int<float", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valDbl(5)}, true},
	}
	for _, tc := range cases {
		if got := matchPropertyFilter(e, tc.pf); got != tc.want {
			t.Errorf("%s: = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestArrayPropertyFilterSemantics pins the documented array-valued property
// semantics (TD28): "If the property of the given name is array-valued, the
// entity satisfies the filter if any of the values compares to the value
// specified in the filter in the manner described by the comparison operator"
// (https://cloud.google.com/datastore/docs/concepts/queries). The stored array
// is expanded elementwise for EQUAL, NOT_EQUAL, the four ordering comparisons
// and IN; an array operand compared to a stored array still matches nothing
// because no element is an array.
func TestArrayPropertyFilterSemantics(t *testing.T) {
	// propertyElements: a scalar is a singleton set, an array its elements.
	if got := propertyElements(valInt(1)); len(got) != 1 || got[0].IntegerValue == nil {
		t.Fatalf("propertyElements(scalar) = %+v, want the scalar", got)
	}
	if got := propertyElements(valArray(valStr("a"), valStr("b"))); len(got) != 2 {
		t.Fatalf("propertyElements(array) = %d elements, want 2", len(got))
	}
	if got := propertyElements(valArray()); len(got) != 0 {
		t.Fatalf("propertyElements(empty array) = %d elements, want 0", len(got))
	}

	// propertyEqual: any element equals the operand; array operands never match.
	if !propertyEqual(valArray(valStr("a"), valStr("b")), valStr("b")) {
		t.Fatal("array property should match an element equal to the operand")
	}
	if propertyEqual(valArray(valStr("a"), valStr("b")), valStr("c")) {
		t.Fatal("array property should not match a value no element equals")
	}
	if propertyEqual(valArray(valStr("a"), valStr("b")), valArray(valStr("a"), valStr("b"))) {
		t.Fatal("an array operand should not match an array-valued property elementwise")
	}
	if !propertyEqual(valStr("a"), valStr("a")) || propertyEqual(valStr("a"), valStr("b")) {
		t.Fatal("scalar propertyEqual is ordinary value equality")
	}

	// propertyNotEqual: any element differs; an empty array has no element and a
	// single-element array equal to the operand has none differing.
	if !propertyNotEqual(valArray(valStr("a"), valStr("b")), valStr("a")) {
		t.Fatal("array property should match not-equal when another element differs")
	}
	if propertyNotEqual(valArray(valStr("a")), valStr("a")) {
		t.Fatal("a single-element array equal to the operand should not match not-equal")
	}
	if propertyNotEqual(valArray(), valStr("a")) {
		t.Fatal("an empty array should not match not-equal")
	}
	if propertyNotEqual(valStr("a"), valStr("a")) || !propertyNotEqual(valStr("a"), valStr("b")) {
		t.Fatal("scalar propertyNotEqual is ordinary value inequality")
	}

	// End to end through matchPropertyFilter: an integer array and a string
	// array, so the range comparisons cross both the element and type axes.
	e := entityWith("Kind", "k", map[string]dsstore.Value{
		"tags": valArray(valStr("a"), valStr("b")),
		"nums": valArray(valInt(1), valInt(3)),
		"one":  valArray(valStr("a")),
		"none": valArray(),
	})
	cases := []struct {
		name string
		pf   *PropertyFilter
		want bool
	}{
		{"eq any element", &PropertyFilter{Property: "tags", Op: PropertyEqual, Value: valStr("a")}, true},
		{"eq no element", &PropertyFilter{Property: "tags", Op: PropertyEqual, Value: valStr("z")}, false},
		// != is elementwise: only one element need differ, so an array that
		// contains the operand still matches when it has another element.
		{"ne matches when another element differs", &PropertyFilter{Property: "tags", Op: PropertyNotEqual, Value: valStr("a")}, true},
		{"ne matches an absent value", &PropertyFilter{Property: "tags", Op: PropertyNotEqual, Value: valStr("z")}, true},
		{"ne excludes a single-element array equal to it", &PropertyFilter{Property: "one", Op: PropertyNotEqual, Value: valStr("a")}, false},
		{"ne excludes an empty array", &PropertyFilter{Property: "none", Op: PropertyNotEqual, Value: valStr("a")}, false},
		{"eq excludes an empty array", &PropertyFilter{Property: "none", Op: PropertyEqual, Value: valStr("a")}, false},
		{"in any element", &PropertyFilter{Property: "tags", Op: PropertyIn, Value: valArray(valStr("z"), valStr("b"))}, true},
		{"in no element", &PropertyFilter{Property: "tags", Op: PropertyIn, Value: valArray(valStr("z"), valStr("y"))}, false},
		{"in excludes an empty array", &PropertyFilter{Property: "none", Op: PropertyIn, Value: valArray(valStr("a"))}, false},
		{"lt any element", &PropertyFilter{Property: "nums", Op: PropertyLessThan, Value: valInt(2)}, true},
		{"lte any element", &PropertyFilter{Property: "nums", Op: PropertyLessThanOrEqual, Value: valInt(1)}, true},
		{"gt any element", &PropertyFilter{Property: "nums", Op: PropertyGreaterThan, Value: valInt(2)}, true},
		{"gte any element", &PropertyFilter{Property: "nums", Op: PropertyGreaterThanOrEqual, Value: valInt(3)}, true},
		{"gt no element", &PropertyFilter{Property: "nums", Op: PropertyGreaterThan, Value: valInt(3)}, false},
		{"lte no element", &PropertyFilter{Property: "nums", Op: PropertyLessThanOrEqual, Value: valInt(0)}, false},
		{"range excludes an empty array", &PropertyFilter{Property: "none", Op: PropertyGreaterThanOrEqual, Value: valStr("a")}, false},
	}
	for _, tc := range cases {
		if got := matchPropertyFilter(e, tc.pf); got != tc.want {
			t.Errorf("%s: = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestHasAncestorGuards verifies HAS_ANCESTOR only accepts the reserved
// __key__ property and a key value (failing closed through matchesFilter), and
// that a matching ancestor key is satisfied end to end.
func TestHasAncestorGuards(t *testing.T) {
	root := idKey("Parent", 1)
	grand := dsstore.PathElement{Kind: "Parent", ID: 2, HasID: true}
	child := Key{Kind: "Child", Name: "c", HasName: true,
		Ancestors: []dsstore.PathElement{{Kind: "Parent", ID: 1, HasID: true}, grand}}
	e := entityWith("Child", CanonicalKey(child), nil)

	ancestor := CanonicalKey(root)
	pf := &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor, Value: dsstore.Value{KeyValue: &ancestor}}
	if ok, err := matchesFilter(e, &Filter{Property: pf}); err != nil || !ok {
		t.Fatalf("HAS_ANCESTOR match = %v,%v; want true,nil", ok, err)
	}

	wrongProp := &PropertyFilter{Property: "n", Op: PropertyHasAncestor, Value: dsstore.Value{KeyValue: &ancestor}}
	if _, err := matchesFilter(e, &Filter{Property: wrongProp}); err == nil {
		t.Fatal("HAS_ANCESTOR on a non-__key__ property must fail closed")
	}
	noKey := &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor, Value: valStr("not-a-key")}
	if _, err := matchesFilter(e, &Filter{Property: noKey}); err == nil {
		t.Fatal("HAS_ANCESTOR with a non-key value must fail closed")
	}

	// A key is not its own ancestor, and an ancestor value that is not a
	// parseable key matches nothing.
	self := &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor,
		Value: valKey(CanonicalKey(child))}
	if ok, err := matchesFilter(e, &Filter{Property: self}); err != nil || ok {
		t.Fatalf("HAS_ANCESTOR with the entity's own key = %v,%v; want false,nil", ok, err)
	}
	garbage := &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor,
		Value: valKey("not-a-canonical-key")}
	if ok, err := matchesFilter(e, &Filter{Property: garbage}); err != nil || ok {
		t.Fatalf("HAS_ANCESTOR with an unparseable key = %v,%v; want false,nil", ok, err)
	}
}
