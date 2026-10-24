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

// ─── error-arm tables (TD19) ──────────────────────────────────────────────────
//
// TD13's mutation spike showed the parser's fail-closed arms were asserted
// against only a handful of blanket strings, so an emptied error branch survived
// as a mutant (the parse would silently succeed). These tables name each arm.

// TestParseGQLErrorArms covers every fail-closed arm of the parser: the lexer's
// character classes, the SELECT/projection/FROM grammar, WHERE conditions, the
// value and DATETIME/KEY/BLOB literal parsers, IN/HAS/ORDER, LIMIT/OFFSET, and
// the binding resolver.
func TestParseGQLErrorArms(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		// lexer character-class arms.
		{"bang without equals", "SELECT * FROM Task WHERE n = 1!"},
		{"lt at eof", "SELECT * FROM Task WHERE n <"},
		{"gt at eof", "SELECT * FROM Task WHERE n >"},
		{"unterminated single quote", "SELECT * FROM Task WHERE n = 'a"},
		{"unterminated double quote", `SELECT * FROM Task WHERE n = "a`},
		{"trailing escape", `SELECT * FROM Task WHERE n = 'a\`},
		{"bare dot", "SELECT * FROM Task WHERE n = ."},
		{"comment char", "SELECT * FROM Task # x"},

		// SELECT / projection / FROM arms.
		{"missing select", "FROM Task"},
		{"projection not ident", "SELECT 1 FROM Task"},
		{"from not ident", "SELECT * FROM 1"},
		{"trailing token", "SELECT * FROM Task junk"},

		// WHERE arms.
		{"where nothing", "SELECT * FROM Task WHERE"},
		{"condition not ident", "SELECT * FROM Task WHERE 1 = 1"},
		{"operator missing", "SELECT * FROM Task WHERE n"},
		{"unknown value keyword", "SELECT * FROM Task WHERE n = BOGUS"},
		{"value is rparen", "SELECT * FROM Task WHERE n = )"},

		// value / literal arms.
		{"integer overflow", "SELECT * FROM Task WHERE n = 99999999999999999999"},
		{"float overflow", "SELECT * FROM Task WHERE n = 1e999"},
		{"unterminated literal", "SELECT * FROM Task WHERE n = DATETIME('x'"},

		// DATETIME arms.
		{"datetime not an ident", "SELECT * FROM Task WHERE t = DATETIME(1)"},
		{"datetime unclosed", "SELECT * FROM Task WHERE t = DATETIME('2020-01-01T00:00:00Z'"},
		{"datetime missing lparen", "SELECT * FROM Task WHERE t = DATETIME"},
		{"datetime unparseable", "SELECT * FROM Task WHERE t = DATETIME('not-a-time')"},

		// KEY arms.
		{"key non string kind", "SELECT * FROM Task WHERE __key__ = KEY(1, 2)"},
		{"key missing comma", "SELECT * FROM Task WHERE __key__ = KEY('Task')"},
		{"key float id", "SELECT * FROM Task WHERE __key__ = KEY('Task', 1.5)"},
		{"key id overflow", "SELECT * FROM Task WHERE __key__ = KEY('Task', 99999999999999999999)"},
		{"key unclosed", "SELECT * FROM Task WHERE __key__ = KEY('Task', 7"},
		{"key missing lparen", "SELECT * FROM Task WHERE __key__ = KEY"},

		// BLOB arms.
		{"blob non string", "SELECT * FROM Task WHERE b = BLOB(1)"},
		{"blob unclosed", "SELECT * FROM Task WHERE b = BLOB('aGk='"},
		{"blob bad base64", "SELECT * FROM Task WHERE b = BLOB('!!!')"},
		{"blob missing lparen", "SELECT * FROM Task WHERE b = BLOB"},

		// IN arms.
		{"in not paren or binding", "SELECT * FROM Task WHERE n IN 5"},
		{"in empty paren", "SELECT * FROM Task WHERE n IN ()"},
		{"in unclosed", "SELECT * FROM Task WHERE n IN (1, 2"},

		// HAS arms.
		{"has nothing", "SELECT * FROM Task WHERE __key__ HAS"},
		{"has descendant", "SELECT * FROM Task WHERE __key__ HAS DESCENDANT KEY('Task', 1)"},
		{"has ancestor nothing", "SELECT * FROM Task WHERE __key__ HAS ANCESTOR"},

		// ORDER BY arms.
		{"order without by", "SELECT * FROM Task ORDER n"},
		{"order by nothing", "SELECT * FROM Task ORDER BY"},

		// LIMIT / OFFSET arms.
		{"limit negative", "SELECT * FROM Task LIMIT -1"},
		{"limit not int", "SELECT * FROM Task LIMIT x"},
		{"limit missing", "SELECT * FROM Task LIMIT"},
		{"limit overflow", "SELECT * FROM Task LIMIT 99999999999999999999"},
		{"offset negative", "SELECT * FROM Task OFFSET -1"},
		{"offset overflow", "SELECT * FROM Task OFFSET 99999999999999999999"},

		// binding resolver arms.
		{"positional zero", "SELECT * FROM Task WHERE n = @0"},
		{"positional out of range", "SELECT * FROM Task WHERE n = @3"},
		{"named missing", "SELECT * FROM Task WHERE n = @nope"},
	}
	bindings := []GQLBinding{gqlBindingVal(gqlIV(1)), gqlBindingVal(gqlIV(2))}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseGQL(GQLQuery{
				QueryString:        tc.query,
				AllowLiterals:      true,
				PositionalBindings: bindings,
			}); err == nil {
				t.Fatalf("ParseGQL(%q) should fail", tc.query)
			}
		})
	}
}

// TestParseGQLAcceptsEdgeArms is the positive complement: the boundary inputs
// the error table above sits next to must still parse (so a mutant that widens a
// guard into rejecting a valid query is caught too).
func TestParseGQLAcceptsEdgeArms(t *testing.T) {
	for _, qs := range []string{
		"SELECT * FROM Task LIMIT 0",
		"SELECT * FROM Task OFFSET 0",
		"SELECT * FROM Task LIMIT 1 OFFSET 2",
		"SELECT * FROM Task OFFSET 2 LIMIT 1",
		"SELECT * FROM Task ORDER BY a ASC, b DESC",
		"SELECT a, b FROM Task",
		"SELECT * FROM Task WHERE __key__ = KEY('Task', -3)",
		"SELECT * FROM Task WHERE t = DATETIME('2020-01-01T00:00:00Z')",
		"SELECT * FROM Task WHERE x IS NOT NULL",
		"SELECT * FROM Task WHERE n IN (1)",
	} {
		if _, err := ParseGQL(GQLQuery{QueryString: qs, AllowLiterals: true}); err != nil {
			t.Errorf("ParseGQL(%q) = %v, want success", qs, err)
		}
	}
}

// TestParseGQLDatetimeNormalization pins every accepted DATETIME layout and the
// RFC 3339 normalization, so removing a layout or dropping the UTC conversion is
// detected.
func TestParseGQLDatetimeNormalization(t *testing.T) {
	cases := []struct{ in, want string }{
		{"2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"},
		{"2020-01-01T05:00:00+05:00", "2020-01-01T00:00:00Z"},
		{"2020-01-01T00:00:00.5Z", "2020-01-01T00:00:00.500Z"},
		{"2020-01-01T00:00:00.123456789", "2020-01-01T00:00:00.123456Z"},
		{"2020-01-01T00:00:00", "2020-01-01T00:00:00Z"},
		{"2020-01-01", "2020-01-01T00:00:00Z"},
	}
	for _, tc := range cases {
		got, err := parseGQLDatetime(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseGQLDatetime(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseGQLDatetime("not-a-time"); err == nil {
		t.Fatal("parseGQLDatetime(garbage) should fail")
	}
}

// TestParseGQLAggregationErrorArms covers the aggregation grammar's fail-closed
// arms (missing OVER/parens, the COUNT-is-* rule, an unknown function, the
// alias, and a negative COUNT_UP_TO).
func TestParseGQLAggregationErrorArms(t *testing.T) {
	cases := []string{
		"AGGREGATE COUNT(*)",                                  // missing OVER
		"AGGREGATE COUNT(*) OVER SELECT * FROM Task",          // missing '('
		"AGGREGATE COUNT(*) OVER (SELECT * FROM Task",         // missing ')'
		"AGGREGATE COUNT(1) OVER (SELECT * FROM Task)",        // COUNT requires *
		"AGGREGATE COUNT(* OVER (SELECT * FROM Task)",         // missing ')' after *
		"AGGREGATE MEDIAN(n) OVER (SELECT * FROM Task)",       // unsupported function
		"AGGREGATE SUM(*) OVER (SELECT * FROM Task)",          // SUM requires a property
		"AGGREGATE SUM(n, m) OVER (SELECT * FROM Task)",       // missing ')' after the aggregation
		"AGGREGATE COUNT_UP_TO(-1) OVER (SELECT * FROM Task)", // negative up-to
		"AGGREGATE COUNT(*) AS 1 OVER (SELECT * FROM Task)",   // alias is not an ident
		"AGGREGATE 1(*) OVER (SELECT * FROM Task)",            // not a function name
		"AGGREGATE COUNT(*) OVER (SELECT * FROM Task) junk",   // trailing tokens
	}
	for _, qs := range cases {
		t.Run(qs, func(t *testing.T) {
			if _, err := ParseGQLAggregation(GQLQuery{QueryString: qs, AllowLiterals: true}); err == nil {
				t.Fatalf("ParseGQLAggregation(%q) should fail", qs)
			}
		})
	}
}

// TestExpectNonNegInt covers the integer guard's three arms: a non-integer
// token, a negative value, and a value strconv.Atoi cannot represent.
func TestExpectNonNegInt(t *testing.T) {
	ok, err := newGQLParser(GQLQuery{QueryString: "5"})
	if err != nil {
		t.Fatalf("newGQLParser: %v", err)
	}
	if n, err := ok.expectNonNegInt(); err != nil || n != 5 {
		t.Fatalf("expectNonNegInt(5) = %d, %v; want 5,nil", n, err)
	}
	for _, s := range []string{"-1", "x", "99999999999999999999", ""} {
		p, err := newGQLParser(GQLQuery{QueryString: s})
		if err != nil {
			t.Fatalf("newGQLParser(%q): %v", s, err)
		}
		if _, err := p.expectNonNegInt(); err == nil {
			t.Errorf("expectNonNegInt(%q) should fail", s)
		}
	}
}

// TestIsAllDigits covers the positional-binding discriminator's empty, digit and
// non-digit inputs.
func TestIsAllDigits(t *testing.T) {
	for _, s := range []string{"0", "12", "007"} {
		if !isAllDigits(s) {
			t.Errorf("isAllDigits(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "a", "1a", "a1", "1.2", "-1"} {
		if isAllDigits(s) {
			t.Errorf("isAllDigits(%q) = true, want false", s)
		}
	}
}

// TestGQLParserPeekPastEnd exercises peek() beyond the token stream (a token
// stream always ends in EOF; next() past it must not index out of range and
// peek() must still report EOF).
func TestGQLParserPeekPastEnd(t *testing.T) {
	p, err := newGQLParser(GQLQuery{QueryString: "SELECT"})
	if err != nil {
		t.Fatalf("newGQLParser: %v", err)
	}
	if got := p.next(); got.kind != tokIdent {
		t.Fatalf("next() = %+v, want the SELECT ident", got)
	}
	if got := p.next(); got.kind != tokEOF {
		t.Fatalf("next() at end = %+v, want EOF", got)
	}
	if got := p.peek(); got.kind != tokEOF {
		t.Fatalf("peek() past end = %+v, want EOF", got)
	}
}
