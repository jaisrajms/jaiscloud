package firestore

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// ─── pipeline protobuf builders ───────────────────────────────────────────────

func pbStr(v string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: v}}
}
func pbInt(v int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
}
func pbRef(v string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: v}}
}
func pbField(p string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_FieldReferenceValue{FieldReferenceValue: p}}
}
func pbFn(name string, args ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_FunctionValue{FunctionValue: &firestorepb.Function{Name: name, Args: args}}}
}
func pbMap(fields map[string]*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{MapValue: &firestorepb.MapValue{Fields: fields}}}
}
func pbArray(vals ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: vals}}}
}
func pbStage(name string, args ...*firestorepb.Value) *firestorepb.Pipeline_Stage {
	return &firestorepb.Pipeline_Stage{Name: name, Args: args}
}
func pbOrder(field, dir string) *firestorepb.Value {
	return pbMap(map[string]*firestorepb.Value{"direction": pbStr(dir), "expression": pbField(field)})
}

// ─── engine fixtures ──────────────────────────────────────────────────────────

// bookProvider seeds a small "books" collection (and one nested collection for
// the collection_group source) and returns the provider.
func bookProvider(t *testing.T) *Provider {
	t.Helper()
	p := newTestProvider()
	ctx := context.Background()
	seed := func(rel string, fields map[string]*firestorestore.Value) {
		name := "projects/proj/databases/(default)/documents/" + rel
		if err := p.store.CreateDocument(ctx, firestorestore.Document{Name: name, Fields: fields}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	seed("books/b1", map[string]*firestorestore.Value{
		"title": strField("A"), "price": intField(5), "genre": strField("x"),
		"tags": firestorestore.ArrayVal(firestorestore.StringVal("sale"), firestorestore.StringVal("new")),
	})
	seed("books/b2", map[string]*firestorestore.Value{
		"title": strField("B"), "price": intField(20), "genre": strField("x"),
		"tags": firestorestore.ArrayVal(firestorestore.StringVal("used")),
	})
	seed("books/b3", map[string]*firestorestore.Value{
		"title": strField("B"), "price": intField(30), "genre": strField("y"),
	})
	// A nested collection named "books" under books/b1, for collection_group.
	seed("books/b1/shelf/books/nested", map[string]*firestorestore.Value{
		"title": strField("N"), "price": intField(1),
	})
	return p
}

func runPipelineStages(t *testing.T, p *Provider, stages ...*firestorepb.Pipeline_Stage) ([]firestorestore.Document, error) {
	t.Helper()
	res, err := p.Service.RunPipeline(context.Background(), "proj", "(default)", stages, nil, false)
	return res.Documents, err
}

func titlesOf(docs []firestorestore.Document) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		if v := d.Fields["title"]; v != nil && v.StringValue != nil {
			out = append(out, *v.StringValue)
		}
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ─── sources ──────────────────────────────────────────────────────────────────

func TestPipelineSources(t *testing.T) {
	p := bookProvider(t)
	coll := pbStage("collection", pbRef("/books"))

	// collection: only the direct children of the top-level "books" collection.
	docs, err := runPipelineStages(t, p, coll)
	if err != nil {
		t.Fatalf("collection: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("collection returned %d docs, want 3 (%v)", len(docs), titlesOf(docs))
	}

	// collection_group: every collection named "books" at any depth, including
	// the nested one under books/b1.
	group, err := runPipelineStages(t, p,
		pbStage("collection_group", pbStr(""), pbStr("books")))
	if err != nil {
		t.Fatalf("collection_group: %v", err)
	}
	if len(group) != 4 {
		t.Fatalf("collection_group returned %d docs, want 4 (%v)", len(group), titlesOf(group))
	}

	// database: every document in the database (both collections).
	db, err := runPipelineStages(t, p, pbStage("database"))
	if err != nil {
		t.Fatalf("database: %v", err)
	}
	if len(db) != 4 {
		t.Fatalf("database returned %d docs, want 4", len(db))
	}

	// documents: an explicit document set.
	set, err := runPipelineStages(t, p,
		pbStage("documents", pbRef("/books/b1"), pbRef("/books/b2")))
	if err != nil {
		t.Fatalf("documents: %v", err)
	}
	if !equalStrings(titlesOf(set), []string{"A", "B"}) {
		t.Fatalf("documents returned %v, want [A B]", titlesOf(set))
	}

	// literals: inline constant documents.
	lit, err := runPipelineStages(t, p,
		pbStage("literals", pbMap(map[string]*firestorepb.Value{"title": pbStr("L")})))
	if err != nil {
		t.Fatalf("literals: %v", err)
	}
	if len(lit) != 1 || titlesOf(lit)[0] != "L" {
		t.Fatalf("literals returned %v, want one doc titled L", lit)
	}

	// A documents stage with no arguments yields no documents.
	none, err := runPipelineStages(t, p, pbStage("documents"))
	if err != nil {
		t.Fatalf("empty documents: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("empty documents returned %d docs, want 0", len(none))
	}
}

func TestPipelineSourceErrors(t *testing.T) {
	p := bookProvider(t)
	cases := []struct {
		name   string
		stages []*firestorepb.Pipeline_Stage
	}{
		{"no stages", nil},
		{"no source", []*firestorepb.Pipeline_Stage{pbStage("limit", pbInt(1))}},
		{"second source", []*firestorepb.Pipeline_Stage{
			pbStage("collection", pbRef("/books")), pbStage("database"),
		}},
		{"collection without ref", []*firestorepb.Pipeline_Stage{pbStage("collection")}},
		{"collection_group without id", []*firestorepb.Pipeline_Stage{
			pbStage("collection_group", pbStr("")),
		}},
		{"documents without ref", []*firestorepb.Pipeline_Stage{
			pbStage("documents", pbInt(1)),
		}},
		{"documents missing", []*firestorepb.Pipeline_Stage{
			pbStage("documents", pbRef("/books/does-not-exist")),
		}},
		{"unsupported source", []*firestorepb.Pipeline_Stage{pbStage("union")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runPipelineStages(t, p, tc.stages...); err == nil {
				t.Fatalf("expected an error")
			}
		})
	}
}

// ─── where ────────────────────────────────────────────────────────────────────

func TestPipelineWhereOperators(t *testing.T) {
	p := bookProvider(t)
	source := pbStage("collection", pbRef("/books"))
	cases := []struct {
		name  string
		where *firestorepb.Value
		want  []string
	}{
		{"equal", pbFn("equal", pbField("genre"), pbStr("y")), []string{"B"}},
		{"not_equal", pbFn("not_equal", pbField("genre"), pbStr("y")), []string{"A", "B"}},
		{"less_than", pbFn("less_than", pbField("price"), pbInt(20)), []string{"A"}},
		{"less_than_or_equal", pbFn("less_than_or_equal", pbField("price"), pbInt(20)), []string{"A", "B"}},
		{"greater_than", pbFn("greater_than", pbField("price"), pbInt(5)), []string{"B", "B"}},
		{"greater_than_or_equal", pbFn("greater_than_or_equal", pbField("price"), pbInt(30)), []string{"B"}},
		{"array_contains", pbFn("array_contains", pbField("tags"), pbStr("sale")), []string{"A"}},
		{"array_contains_any", pbFn("array_contains_any", pbField("tags"), pbArray(pbStr("new"), pbStr("used"))), []string{"A", "B"}},
		{"equal_any", pbFn("equal_any", pbField("title"), pbFn("array", pbStr("A"), pbStr("B"))), []string{"A", "B", "B"}},
		{"not_equal_any", pbFn("not_equal_any", pbField("title"), pbArray(pbStr("A"))), []string{"B", "B"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docs, err := runPipelineStages(t, p, source, pbStage("where", tc.where), pbStage("sort", pbOrder("title", "ascending")))
			if err != nil {
				t.Fatalf("where: %v", err)
			}
			if got := titlesOf(docs); !equalStrings(got, tc.want) {
				t.Fatalf("where %s returned %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestPipelineWhereComposite(t *testing.T) {
	p := bookProvider(t)
	source := pbStage("collection", pbRef("/books"))

	and, err := runPipelineStages(t, p, source, pbStage("where", pbFn("and",
		pbFn("equal", pbField("genre"), pbStr("x")),
		pbFn("greater_than", pbField("price"), pbInt(10)),
	)), pbStage("sort", pbOrder("title", "ascending")))
	if err != nil {
		t.Fatalf("and: %v", err)
	}
	if got := titlesOf(and); !equalStrings(got, []string{"B"}) {
		t.Fatalf("and returned %v, want [B]", got)
	}

	or, err := runPipelineStages(t, p, source, pbStage("where", pbFn("or",
		pbFn("equal", pbField("genre"), pbStr("y")),
		pbFn("less_than", pbField("price"), pbInt(10)),
	)), pbStage("sort", pbOrder("title", "ascending")))
	if err != nil {
		t.Fatalf("or: %v", err)
	}
	if got := titlesOf(or); !equalStrings(got, []string{"A", "B"}) {
		t.Fatalf("or returned %v, want [A B]", got)
	}
}

func TestPipelineWhereErrors(t *testing.T) {
	p := bookProvider(t)
	source := pbStage("collection", pbRef("/books"))
	cases := []struct {
		name string
		arg  *firestorepb.Value
	}{
		{"no argument", nil},
		{"argument is not a function", pbRef("title")},
		{"empty and", pbFn("and")},
		{"and argument not boolean", pbFn("and", pbStr("x"))},
		{"comparison without field", pbFn("equal", pbStr("x"), pbStr("y"))},
		{"comparison without value", pbFn("equal", pbField("title"))},
		{"comparison value is computed", pbFn("equal", pbField("title"), pbFn("length", pbField("title")))},
		{"array op without array", pbFn("equal_any", pbField("title"), pbStr("A"))},
		{"array_contains without value", pbFn("array_contains", pbField("tags"))},
		{"unsupported function", pbFn("like", pbField("title"), pbStr("A"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := []*firestorepb.Value{}
			if tc.arg != nil {
				args = append(args, tc.arg)
			}
			if _, err := runPipelineStages(t, p, source, pbStage("where", args...)); err == nil {
				t.Fatalf("expected an error")
			}
		})
	}
}

// ─── sort / select / distinct / limit / offset ────────────────────────────────

func TestPipelineSortSelectDistinctLimitOffset(t *testing.T) {
	p := bookProvider(t)
	source := pbStage("collection", pbRef("/books"))

	// sort descending by price.
	sorted, err := runPipelineStages(t, p, source, pbStage("sort", pbOrder("price", "descending")))
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if got := titlesOf(sorted); !equalStrings(got, []string{"B", "B", "A"}) {
		t.Fatalf("sort desc returned %v, want [B B A]", got)
	}

	// offset 1 + limit 1 over the price-descending order.
	window, err := runPipelineStages(t, p, source,
		pbStage("sort", pbOrder("price", "descending")),
		pbStage("offset", pbInt(1)), pbStage("limit", pbInt(1)))
	if err != nil {
		t.Fatalf("offset/limit: %v", err)
	}
	if got := titlesOf(window); !equalStrings(got, []string{"B"}) {
		t.Fatalf("offset 1 limit 1 returned %v, want [B]", got)
	}

	// offset past the end yields nothing.
	empty, err := runPipelineStages(t, p, source, pbStage("offset", pbInt(99)))
	if err != nil {
		t.Fatalf("offset past end: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("offset past end returned %d docs, want 0", len(empty))
	}

	// select projects (and can rename via the alias).
	sel, err := runPipelineStages(t, p, source,
		pbStage("select", pbMap(map[string]*firestorepb.Value{"renamed": pbField("title")})))
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(sel) != 3 {
		t.Fatalf("select returned %d docs, want 3", len(sel))
	}
	for _, d := range sel {
		if _, ok := d.Fields["renamed"]; !ok {
			t.Fatalf("select did not project under the alias: %+v", d.Fields)
		}
		if _, ok := d.Fields["price"]; ok {
			t.Fatalf("select left an unselected field: %+v", d.Fields)
		}
	}

	// distinct by title collapses the two "B" documents.
	dist, err := runPipelineStages(t, p, source,
		pbStage("distinct", pbMap(map[string]*firestorepb.Value{"title": pbField("title")})))
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(dist) != 2 {
		t.Fatalf("distinct by title returned %d docs, want 2", len(dist))
	}

	// distinct with no field list is whole-document distinctness.
	whole, err := runPipelineStages(t, p, source, pbStage("distinct"))
	if err != nil {
		t.Fatalf("distinct whole: %v", err)
	}
	if len(whole) != 3 {
		t.Fatalf("whole-document distinct returned %d docs, want 3", len(whole))
	}

	// select with no arguments is a no-op (all fields).
	all, err := runPipelineStages(t, p, source, pbStage("select"))
	if err != nil {
		t.Fatalf("empty select: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("empty select returned %d docs, want 3", len(all))
	}
}

func TestPipelineSortSelectErrors(t *testing.T) {
	p := bookProvider(t)
	source := pbStage("collection", pbRef("/books"))
	cases := []struct {
		name  string
		stage *firestorepb.Pipeline_Stage
	}{
		{"sort without orderings", pbStage("sort")},
		{"sort non-map ordering", pbStage("sort", pbStr("price"))},
		{"sort computed key", pbStage("sort", pbMap(map[string]*firestorepb.Value{"expression": pbFn("length", pbField("title"))}))},
		{"sort bad direction", pbStage("sort", pbOrder("price", "sideways"))},
		{"select non-map", pbStage("select", pbStr("title"))},
		{"select computed", pbStage("select", pbMap(map[string]*firestorepb.Value{"n": pbFn("length", pbField("title"))}))},
		{"distinct computed", pbStage("distinct", pbMap(map[string]*firestorepb.Value{"n": pbFn("length", pbField("title"))}))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runPipelineStages(t, p, source, tc.stage); err == nil {
				t.Fatalf("expected an error")
			}
		})
	}
}

// TestRunPipelineNewTransaction asserts a new transaction is begun, used for the
// reads, and returned.
func TestRunPipelineNewTransaction(t *testing.T) {
	p := bookProvider(t)
	res, err := p.Service.RunPipeline(context.Background(), "proj", "(default)",
		[]*firestorepb.Pipeline_Stage{pbStage("collection", pbRef("/books"))}, nil, true)
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	if len(res.Transaction) == 0 {
		t.Fatal("expected a begun transaction id")
	}
	if res.ExecutionTime.IsZero() {
		t.Fatal("expected an execution time")
	}
}

// ─── proto value decoders ─────────────────────────────────────────────────────

func TestValueFromProtoVariants(t *testing.T) {
	ok := []*firestorepb.Value{
		pbStr("s"), pbInt(7), pbRef("r"),
		{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: true}},
		{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: 1.5}},
		{ValueType: &firestorepb.Value_NullValue{}},
		{ValueType: &firestorepb.Value_BytesValue{BytesValue: []byte("b")}},
		pbArray(pbStr("a"), pbInt(1)),
		pbMap(map[string]*firestorepb.Value{"k": pbStr("v")}),
	}
	for i, v := range ok {
		got, err := ValueFromProto(v)
		if err != nil {
			t.Fatalf("variant %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("variant %d: nil value", i)
		}
	}
	if got, err := ValueFromProto(nil); err != nil || got != nil {
		t.Fatalf("nil value: got (%v, %v), want (nil, nil)", got, err)
	}
	// A function or a field reference is not a constant value.
	for _, v := range []*firestorepb.Value{pbFn("x"), pbField("y")} {
		if _, err := ValueFromProto(v); err == nil {
			t.Fatalf("expected an error for %T", v.GetValueType())
		}
	}
}

func TestArrayAndFieldsFromProto(t *testing.T) {
	av, err := ArrayValueFromProto(nil)
	if err != nil || av == nil || len(av.Values) != 0 {
		t.Fatalf("nil array: got (%v, %v)", av, err)
	}
	av, err = ArrayValueFromProto(&firestorepb.ArrayValue{Values: []*firestorepb.Value{pbStr("a"), pbInt(2)}})
	if err != nil || len(av.Values) != 2 {
		t.Fatalf("array: got (%v, %v)", av, err)
	}

	fields, err := FieldsFromProto(nil)
	if err != nil || fields == nil || len(fields) != 0 {
		t.Fatalf("nil fields: got (%v, %v)", fields, err)
	}
	fields, err = FieldsFromProto(&firestorepb.Document{Fields: map[string]*firestorepb.Value{"a": pbInt(1)}})
	if err != nil || len(fields) != 1 {
		t.Fatalf("fields: got (%v, %v)", fields, err)
	}
}
