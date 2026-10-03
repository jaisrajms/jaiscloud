package datastore

import (
	"context"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func gqlIV(n int64) dsstore.Value   { return dsstore.Value{IntegerValue: ptrInt64(n)} }
func gqlDV(f float64) dsstore.Value { return dsstore.Value{DoubleValue: ptrFloat64(f)} }
func gqlSV(s string) dsstore.Value {
	return dsstore.Value{StringValue: &s}
}
func gqlBV(b bool) dsstore.Value {
	return dsstore.Value{BooleanValue: &b}
}
func gqlAV(vals ...dsstore.Value) dsstore.Value {
	return dsstore.Value{ArrayValue: &dsstore.ArrayValue{Values: vals}}
}
func gqlBindingVal(v dsstore.Value) GQLBinding { return GQLBinding{Value: &v} }

// propFilter extracts and checks the sole property filter, returning it.
func propFilter(t *testing.T, f *Filter, prop string, op PropertyOp) *PropertyFilter {
	t.Helper()
	if f == nil || f.Property == nil {
		t.Fatalf("filter = %+v, want a property filter on %q", f, prop)
	}
	if f.Property.Property != prop || f.Property.Op != op {
		t.Fatalf("filter = {%s %s}, want {%s %s}", f.Property.Property, propertyOpName(f.Property.Op), prop, propertyOpName(op))
	}
	return f.Property
}

func TestParseGQLBasic(t *testing.T) {
	q, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE n >= 4 ORDER BY n DESC LIMIT 10 OFFSET 2", AllowLiterals: true})
	if err != nil {
		t.Fatalf("ParseGQL: %v", err)
	}
	if q.Kind != "Task" {
		t.Fatalf("Kind = %q, want Task", q.Kind)
	}
	pf := propFilter(t, q.Filter, "n", PropertyGreaterThanOrEqual)
	if pf.Value.IntegerValue == nil || *pf.Value.IntegerValue != 4 {
		t.Fatalf("filter value = %+v, want integer 4", pf.Value)
	}
	if q.Limit == nil || *q.Limit != 10 {
		t.Fatalf("Limit = %v, want 10", q.Limit)
	}
	if q.Offset != 2 {
		t.Fatalf("Offset = %d, want 2", q.Offset)
	}
}

func TestParseGQLProjections(t *testing.T) {
	for _, qs := range []string{
		"SELECT * FROM Task",
		"SELECT __key__ FROM Task",
		"SELECT a, b, c FROM Task",
	} {
		if _, err := ParseGQL(GQLQuery{QueryString: qs}); err != nil {
			t.Errorf("ParseGQL(%q): %v", qs, err)
		}
	}
}

func TestParseGQLFilterClauses(t *testing.T) {
	cases := []struct {
		clause string
		prop   string
		op     PropertyOp
		check  func(t *testing.T, v dsstore.Value)
	}{
		{"n = 3", "n", PropertyEqual, func(t *testing.T, v dsstore.Value) {
			if v.IntegerValue == nil || *v.IntegerValue != 3 {
				t.Fatalf("value = %+v", v)
			}
		}},
		{"n != 3", "n", PropertyNotEqual, nil},
		{"n < 3", "n", PropertyLessThan, nil},
		{"n <= 3", "n", PropertyLessThanOrEqual, nil},
		{"n > 3", "n", PropertyGreaterThan, nil},
		{"n >= 3", "n", PropertyGreaterThanOrEqual, nil},
		{"name = 'bob'", "name", PropertyEqual, func(t *testing.T, v dsstore.Value) {
			if v.StringValue == nil || *v.StringValue != "bob" {
				t.Fatalf("value = %+v", v)
			}
		}},
		{"d = 1.5", "d", PropertyEqual, func(t *testing.T, v dsstore.Value) {
			if v.DoubleValue == nil || *v.DoubleValue != 1.5 {
				t.Fatalf("value = %+v", v)
			}
		}},
		{"flag = TRUE", "flag", PropertyEqual, func(t *testing.T, v dsstore.Value) {
			if v.BooleanValue == nil || !*v.BooleanValue {
				t.Fatalf("value = %+v", v)
			}
		}},
		{"flag = false", "flag", PropertyEqual, nil},
		{"x = NULL", "x", PropertyEqual, func(t *testing.T, v dsstore.Value) {
			if v.NullValue == nil {
				t.Fatalf("value = %+v, want null", v)
			}
		}},
		{"x IS NULL", "x", PropertyEqual, nil},
		{"x IS NOT NULL", "x", PropertyNotEqual, nil},
		{"n IN (1, 2, 3)", "n", PropertyIn, func(t *testing.T, v dsstore.Value) {
			if v.ArrayValue == nil || len(v.ArrayValue.Values) != 3 {
				t.Fatalf("value = %+v, want a 3-element array", v)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.clause, func(t *testing.T) {
			q, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE " + tc.clause, AllowLiterals: true})
			if err != nil {
				t.Fatalf("ParseGQL: %v", err)
			}
			pf := propFilter(t, q.Filter, tc.prop, tc.op)
			if tc.check != nil {
				tc.check(t, pf.Value)
			}
		})
	}
}

func TestParseGQLComposite(t *testing.T) {
	q, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE a = 1 AND b = 2", AllowLiterals: true})
	if err != nil {
		t.Fatalf("ParseGQL AND: %v", err)
	}
	if q.Filter.Composite == nil || q.Filter.Composite.Op != CompositeAnd || len(q.Filter.Composite.Filters) != 2 {
		t.Fatalf("AND filter = %+v", q.Filter)
	}
	q, err = ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE a = 1 OR b = 2", AllowLiterals: true})
	if err != nil {
		t.Fatalf("ParseGQL OR: %v", err)
	}
	if q.Filter.Composite == nil || q.Filter.Composite.Op != CompositeOr {
		t.Fatalf("OR filter = %+v", q.Filter)
	}
	// Parenthesized grouping.
	if _, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE (a = 1 OR b = 2) AND c = 3", AllowLiterals: true}); err != nil {
		t.Fatalf("ParseGQL grouped: %v", err)
	}
}

func TestParseGQLHasAncestor(t *testing.T) {
	q, err := ParseGQL(GQLQuery{
		QueryString:   "SELECT * FROM Task WHERE __key__ HAS ANCESTOR KEY('Parent', 1)",
		AllowLiterals: true,
	})
	if err != nil {
		t.Fatalf("ParseGQL HAS ANCESTOR: %v", err)
	}
	pf := propFilter(t, q.Filter, "__key__", PropertyHasAncestor)
	if pf.Value.KeyValue == nil {
		t.Fatalf("HAS ANCESTOR value = %+v, want a key", pf.Value)
	}
}

func TestParseGQLBindings(t *testing.T) {
	// Named binding.
	q, err := ParseGQL(GQLQuery{
		QueryString:   "SELECT * FROM Task WHERE n = @limit",
		NamedBindings: map[string]GQLBinding{"limit": gqlBindingVal(gqlIV(7))},
	})
	if err != nil {
		t.Fatalf("named binding: %v", err)
	}
	if v := propFilter(t, q.Filter, "n", PropertyEqual).Value; v.IntegerValue == nil || *v.IntegerValue != 7 {
		t.Fatalf("named binding value = %+v", v)
	}

	// Positional binding (1-based).
	q, err = ParseGQL(GQLQuery{
		QueryString:        "SELECT * FROM Task WHERE n = @1",
		PositionalBindings: []GQLBinding{gqlBindingVal(gqlIV(9))},
	})
	if err != nil {
		t.Fatalf("positional binding: %v", err)
	}
	if v := propFilter(t, q.Filter, "n", PropertyEqual).Value; v.IntegerValue == nil || *v.IntegerValue != 9 {
		t.Fatalf("positional binding value = %+v", v)
	}

	// IN over an array binding.
	if _, err := ParseGQL(GQLQuery{
		QueryString:   "SELECT * FROM Task WHERE n IN @ids",
		NamedBindings: map[string]GQLBinding{"ids": gqlBindingVal(gqlAV(gqlIV(1), gqlIV(2)))},
	}); err != nil {
		t.Fatalf("IN array binding: %v", err)
	}

	// Errors.
	for _, tc := range []struct {
		name string
		q    GQLQuery
	}{
		{"missing named", GQLQuery{QueryString: "SELECT * FROM Task WHERE n = @nope"}},
		{"positional out of range", GQLQuery{QueryString: "SELECT * FROM Task WHERE n = @2", PositionalBindings: []GQLBinding{gqlBindingVal(gqlIV(1))}}},
		{"cursor binding", GQLQuery{QueryString: "SELECT * FROM Task WHERE n = @c", NamedBindings: map[string]GQLBinding{"c": {Cursor: []byte("x")}}}},
		{"empty binding value", GQLQuery{QueryString: "SELECT * FROM Task WHERE n = @e", NamedBindings: map[string]GQLBinding{"e": {}}}},
		{"IN non-array", GQLQuery{QueryString: "SELECT * FROM Task WHERE n IN @one", NamedBindings: map[string]GQLBinding{"one": gqlBindingVal(gqlIV(1))}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseGQL(tc.q); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestParseGQLLiteralsGated(t *testing.T) {
	// Literals are rejected when allow_literals is false...
	if _, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE n = 5"}); err == nil {
		t.Fatal("literal with allow_literals=false should be rejected")
	}
	// ...but bindings still work.
	if _, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE n = @1", PositionalBindings: []GQLBinding{gqlBindingVal(gqlIV(5))}}); err != nil {
		t.Fatalf("binding with allow_literals=false: %v", err)
	}
}

func TestParseGQLLiteralTypes(t *testing.T) {
	cases := []struct {
		clause string
		check  func(t *testing.T, v dsstore.Value)
	}{
		{"t > DATETIME('2020-01-01T00:00:00Z')", func(t *testing.T, v dsstore.Value) {
			if v.TimestampValue == nil || *v.TimestampValue != "2020-01-01T00:00:00Z" {
				t.Fatalf("value = %+v", v)
			}
		}},
		{"__key__ = KEY('Task', 7)", func(t *testing.T, v dsstore.Value) {
			want := dsstore.KeyOfID("Task", 7)
			if v.KeyValue == nil || *v.KeyValue != want {
				t.Fatalf("value = %+v, want key %q", v, want)
			}
		}},
		{"__key__ = KEY('Task', 'abc')", func(t *testing.T, v dsstore.Value) {
			want := dsstore.KeyOfName("Task", "abc")
			if v.KeyValue == nil || *v.KeyValue != want {
				t.Fatalf("value = %+v, want key %q", v, want)
			}
		}},
		{"b = BLOB('aGk=')", func(t *testing.T, v dsstore.Value) {
			if string(v.BlobValue) != "hi" {
				t.Fatalf("value = %+v, want blob hi", v)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.clause, func(t *testing.T) {
			q, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE " + tc.clause, AllowLiterals: true})
			if err != nil {
				t.Fatalf("ParseGQL: %v", err)
			}
			tc.check(t, q.Filter.Property.Value)
		})
	}

	if _, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE t = DATETIME('not-a-time')", AllowLiterals: true}); err == nil {
		t.Fatal("invalid DATETIME should be rejected")
	}
	if _, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task WHERE b = BLOB('!!!')", AllowLiterals: true}); err == nil {
		t.Fatal("invalid BLOB should be rejected")
	}
}

func TestParseGQLErrors(t *testing.T) {
	queries := []string{
		"",                                 // empty
		"SELECT * FROM",                    // missing kind
		"SELECT FROM Task",                 // missing projection
		"SELECT * Task",                    // missing FROM
		"SELECT * FROM Task WHERE",         // missing condition
		"SELECT * FROM Task WHERE n",       // missing operator
		"SELECT * FROM Task WHERE n === 1", // bad operator
		"SELECT * FROM Task WHERE n = 'unterminated",                     // unterminated string
		"SELECT * FROM Task WHERE __key__ HAS DESCENDANT KEY('Task', 1)", // unsupported HAS
		"SELECT * FROM Task WHERE tags CONTAINS 'x'",                     // unsupported CONTAINS
		"SELECT * FROM Task WHERE NOT n = 1",                             // unsupported NOT
		"SELECT * FROM Task WHERE n = 1 + 2",                             // trailing tokens
		"SELECT * FROM Task # comment",                                   // unexpected character
		"SELECT * FROM Task WHERE n = @",                                 // empty binding
	}
	for _, qs := range queries {
		t.Run(qs, func(t *testing.T) {
			if _, err := ParseGQL(GQLQuery{QueryString: qs, AllowLiterals: true}); err == nil {
				t.Fatalf("ParseGQL(%q) should fail", qs)
			}
		})
	}
}

func TestParseGQLAggregation(t *testing.T) {
	aq, err := ParseGQLAggregation(GQLQuery{
		QueryString:   "AGGREGATE COUNT(*), COUNT_UP_TO(10), SUM(n), AVG(n) OVER (SELECT * FROM Task WHERE n >= 1)",
		AllowLiterals: true,
	})
	if err != nil {
		t.Fatalf("ParseGQLAggregation: %v", err)
	}
	if aq.Nested.Kind != "Task" || len(aq.Aggregations) != 4 {
		t.Fatalf("aggregation = %+v", aq)
	}
	if aq.Aggregations[0].Op != AggCount || aq.Aggregations[1].UpTo == nil || *aq.Aggregations[1].UpTo != 10 {
		t.Fatalf("count aggregations = %+v", aq.Aggregations[:2])
	}
	if aq.Aggregations[2].Op != AggSum || aq.Aggregations[2].Property != "n" {
		t.Fatalf("sum aggregation = %+v", aq.Aggregations[2])
	}
	if aq.Aggregations[3].Op != AggAvg || aq.Aggregations[3].Property != "n" {
		t.Fatalf("avg aggregation = %+v", aq.Aggregations[3])
	}

	// Alias.
	aq, err = ParseGQLAggregation(GQLQuery{QueryString: "AGGREGATE COUNT(*) AS total OVER (SELECT * FROM Task)"})
	if err != nil {
		t.Fatalf("aliased aggregation: %v", err)
	}
	if aq.Aggregations[0].Alias != "total" {
		t.Fatalf("alias = %q, want total", aq.Aggregations[0].Alias)
	}

	for _, qs := range []string{
		"SELECT * FROM Task",                                // not an aggregation
		"AGGREGATE COUNT(*) SELECT * FROM Task",             // missing OVER
		"AGGREGATE COUNT(n) OVER (SELECT * FROM Task)",      // COUNT requires *
		"AGGREGATE MEDIAN(n) OVER (SELECT * FROM Task)",     // unsupported function
		"AGGREGATE SUM(*) OVER (SELECT * FROM Task)",        // sum requires property
		"AGGREGATE COUNT(*) OVER (SELECT * FROM Task) junk", // trailing tokens
	} {
		t.Run(qs, func(t *testing.T) {
			if _, err := ParseGQLAggregation(GQLQuery{QueryString: qs}); err == nil {
				t.Fatalf("ParseGQLAggregation(%q) should fail", qs)
			}
		})
	}
}

func TestRunQueryGQLEndToEnd(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	seed := func(name string, n int64) {
		e := dsstore.Entity{Kind: "Task", Key: dsstore.KeyOfName("Task", name), Properties: map[string]dsstore.Value{"n": gqlIV(n)}}
		if err := s.store.Upsert(ctx, "p", e); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	seed("a", 1)
	seed("b", 5)

	res, err := s.RunQueryGQL(ctx, "p", GQLQuery{QueryString: "SELECT * FROM Task WHERE n >= 4", AllowLiterals: true}, nil, "", "")
	if err != nil {
		t.Fatalf("RunQueryGQL: %v", err)
	}
	if len(res.Entities) != 1 || res.Entities[0].Entity.Key != dsstore.KeyOfName("Task", "b") {
		t.Fatalf("RunQueryGQL results = %+v", res.Entities)
	}

	agg, err := s.RunAggregationQueryGQL(ctx, "p", GQLQuery{QueryString: "AGGREGATE COUNT(*) OVER (SELECT * FROM Task)", AllowLiterals: true}, nil, "", "")
	if err != nil {
		t.Fatalf("RunAggregationQueryGQL: %v", err)
	}
	if v := agg.Aggregates["property_1"]; v.IntegerValue == nil || *v.IntegerValue != 2 {
		t.Fatalf("count = %+v, want 2", v)
	}

	// A malformed GQL query surfaces a parse error rather than running.
	if _, err := s.RunQueryGQL(ctx, "p", GQLQuery{QueryString: "not gql"}, nil, "", ""); err == nil {
		t.Fatal("malformed GQL should error")
	}
}
