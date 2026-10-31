package datastore

import (
	"encoding/base64"
	"reflect"
	"testing"
	"time"

	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func str(s string) dsstore.Value { return dsstore.Value{StringValue: &s} }

func testKey(kind, name string) core.Key {
	return core.Key{Kind: kind, Name: name, HasName: true}
}

// The exported encoders are the inverse of the decoders the handlers use: a request encoded
// for a plugin must decode to the request the console meant.
func TestExportedQueryEncodingRoundTripsThroughTheHandlerDecoder(t *testing.T) {
	limit := 5
	q := &core.Query{
		Kind: "Task", Offset: 2, Limit: &limit,
		StartCursor: []byte("s"), EndCursor: []byte("e"),
		Projection: &core.Projection{Properties: []string{"a", "b"}},
		Filter: &core.Filter{Composite: &core.CompositeFilter{Op: core.CompositeAnd, Filters: []*core.Filter{
			{Property: &core.PropertyFilter{Property: "done", Op: core.PropertyEqual, Value: str("no")}},
			{Property: &core.PropertyFilter{Property: "n", Op: core.PropertyGreaterThanOrEqual, Value: str("1")}},
		}}},
	}
	got, err := queryFromWire(QueryToWire(q, "proj"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "Task" || got.Offset != 2 || got.Limit == nil || *got.Limit != 5 || string(got.StartCursor) != "s" || string(got.EndCursor) != "e" {
		t.Fatalf("query = %+v", got)
	}
	if got.Projection == nil || !reflect.DeepEqual(got.Projection.Properties, []string{"a", "b"}) {
		t.Fatalf("projection = %+v", got.Projection)
	}
	if got.Filter == nil || got.Filter.Composite == nil || got.Filter.Composite.Op != core.CompositeAnd || len(got.Filter.Composite.Filters) != 2 ||
		got.Filter.Composite.Filters[1].Property.Op != core.PropertyGreaterThanOrEqual {
		t.Fatalf("filter = %+v", got.Filter)
	}
	if empty := QueryToWire(nil, "proj"); len(empty) != 0 {
		t.Fatalf("a nil query encodes as %v", empty)
	}
}

func TestExportedOperatorNamesDecodeBack(t *testing.T) {
	for _, op := range []core.PropertyOp{core.PropertyEqual, core.PropertyNotEqual, core.PropertyIn, core.PropertyLessThan,
		core.PropertyLessThanOrEqual, core.PropertyGreaterThan, core.PropertyGreaterThanOrEqual, core.PropertyHasAncestor} {
		if got := propertyOpFromWire(propertyOpToWire(op)); got != op {
			t.Errorf("property op %v came back as %v", op, got)
		}
	}
	for _, op := range []core.CompositeOp{core.CompositeAnd, core.CompositeOr} {
		if got := compositeOpFromWire(compositeOpToWire(op)); got != op {
			t.Errorf("composite op %v came back as %v", op, got)
		}
	}
	if propertyOpToWire(core.PropertyOp(99)) != "OPERATOR_UNSPECIFIED" || compositeOpToWire(core.CompositeOp(99)) != "OPERATOR_UNSPECIFIED" {
		t.Error("an unknown operator must encode as OPERATOR_UNSPECIFIED")
	}
	if filterToWire(nil, "p") != nil || filterToWire(&core.Filter{}, "p") != nil {
		t.Error("an empty filter must encode as nothing")
	}
}

func TestExportedGQLEncodingRoundTrips(t *testing.T) {
	v := str("x")
	in := core.GQLQuery{
		QueryString: "SELECT * FROM T WHERE a = @a AND b = @1", AllowLiterals: true,
		NamedBindings:      map[string]core.GQLBinding{"a": {Value: &v}, "c": {Cursor: []byte("cur")}},
		PositionalBindings: []core.GQLBinding{{Value: &v}},
	}
	got, err := gqlQueryFromWire(GQLQueryToWire(in, "proj"))
	if err != nil {
		t.Fatal(err)
	}
	if got.QueryString != in.QueryString || !got.AllowLiterals || len(got.NamedBindings) != 2 || len(got.PositionalBindings) != 1 ||
		got.NamedBindings["a"].Value == nil || string(got.NamedBindings["c"].Cursor) != "cur" {
		t.Fatalf("gql = %+v", got)
	}
}

func TestExportedMutationEncodingRoundTrips(t *testing.T) {
	base := int64(7)
	upd := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	k := testKey("Task", "t1")
	ent := dsstore.Entity{Kind: "Task", Key: core.CanonicalKey(k), Properties: map[string]dsstore.Value{"a": str("x")}}
	for name, m := range map[string]core.Mutation{
		"insert": {Op: core.MutationInsert, Key: k, Entity: ent},
		"upsert": {Op: core.MutationUpsert, Key: k, Entity: ent, Precondition: &dsstore.Precondition{BaseVersion: &base}},
		"update": {Op: core.MutationUpdate, Key: k, Entity: ent, Precondition: &dsstore.Precondition{UpdateTime: &upd}},
		"delete": {Op: core.MutationDelete, DeleteKey: k},
	} {
		got, err := mutationFromWire(MutationToWire(m, "proj"))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Op != m.Op {
			t.Errorf("%s: op %v, want %v", name, got.Op, m.Op)
		}
		if (m.Precondition == nil) != (got.Precondition == nil) {
			t.Errorf("%s: precondition %v, want %v", name, got.Precondition, m.Precondition)
		}
	}
	if CommitModeToWire(core.CommitModeTransactional) != "TRANSACTIONAL" || CommitModeToWire(core.CommitModeNonTransactional) != "NON_TRANSACTIONAL" || CommitModeToWire(core.CommitModeUnspecified) != "" {
		t.Error("commit mode names")
	}
	if got := KeyToWire(k, "proj"); got == nil {
		t.Error("KeyToWire returned nothing")
	}
}

func TestExportedAnswerDecoders(t *testing.T) {
	k := testKey("Task", "t1")
	entity := func() map[string]any {
		return map[string]any{"entity": map[string]any{"key": keyToWire(k, "proj"), "properties": map[string]any{"a": map[string]any{"stringValue": "x"}}},
			"version": "4", "createTime": "2026-01-02T03:04:05Z", "cursor": base64.StdEncoding.EncodeToString([]byte("c"))}
	}

	qr, err := QueryResultFromWire(map[string]any{"batch": map[string]any{
		"entityResultType": "PROJECTION", "moreResults": "MORE_RESULTS_AFTER_LIMIT", "snapshotVersion": "9", "skippedResults": float64(3),
		"endCursor": base64.StdEncoding.EncodeToString([]byte("end")), "skippedCursor": base64.StdEncoding.EncodeToString([]byte("skip")),
		"readTime": "2026-01-02T03:04:05Z", "entityResults": []any{entity()}}})
	if err != nil || qr.ResultType != core.ResultProjection || qr.MoreResults != core.MoreResultsAfterLimit || qr.SnapshotVersion != 9 || qr.Skipped != 3 ||
		string(qr.EndCursor) != "end" || string(qr.SkippedCursor) != "skip" || qr.ReadTime.IsZero() || len(qr.Entities) != 1 ||
		qr.Entities[0].Version != 4 || string(qr.Entities[0].Cursor) != "c" || qr.Entities[0].CreateTime.IsZero() {
		t.Fatalf("QueryResultFromWire = %+v, %v", qr, err)
	}
	for wire, want := range map[string]core.ResultType{"KEY_ONLY": core.ResultKeysOnly, "": core.ResultFull} {
		got, err := QueryResultFromWire(map[string]any{"batch": map[string]any{"entityResultType": wire, "moreResults": "MORE_RESULTS_AFTER_CURSOR"}})
		if err != nil || got.ResultType != want || got.MoreResults != core.MoreResultsAfterCursor {
			t.Fatalf("QueryResultFromWire(%q) = %+v, %v", wire, got, err)
		}
	}

	lr, err := LookupResultFromWire(map[string]any{"readTime": "2026-01-02T03:04:05Z", "found": []any{entity()},
		"missing": []any{map[string]any{"entity": map[string]any{"key": keyToWire(testKey("Task", "gone"), "proj")}}}})
	if err != nil || len(lr.Found) != 1 || len(lr.Missing) != 1 || lr.Missing[0].Name != "gone" || lr.ReadTime.IsZero() {
		t.Fatalf("LookupResultFromWire = %+v, %v", lr, err)
	}

	cr, err := CommitResponseFromWire(map[string]any{"commitTime": "2026-01-02T03:04:05Z", "indexUpdates": float64(2), "mutationResults": []any{
		map[string]any{"version": "5", "conflictDetected": true, "createTime": "2026-01-02T03:04:05Z", "updateTime": "2026-01-02T03:04:06Z", "key": keyToWire(k, "proj")},
		map[string]any{"version": "6"}}})
	if err != nil || cr.IndexUpdates != 2 || cr.CommitTime.IsZero() || len(cr.Results) != 2 || cr.Results[0].Key == nil || !cr.Results[0].ConflictDetected ||
		cr.Results[0].Version != 5 || cr.Results[1].Key != nil {
		t.Fatalf("CommitResponseFromWire = %+v, %v", cr, err)
	}
}

func TestExportedAnswerDecodersRefuseABadKey(t *testing.T) {
	bad := map[string]any{"key": "not a key"}
	if _, err := LookupResultFromWire(map[string]any{"missing": []any{map[string]any{"entity": bad}}}); err == nil {
		t.Error("a missing entry with a malformed key must be refused")
	}
	if _, err := CommitResponseFromWire(map[string]any{"mutationResults": []any{bad}}); err == nil {
		t.Error("a mutation result with a malformed key must be refused")
	}
	if _, err := QueryResultFromWire(map[string]any{"batch": map[string]any{"entityResults": []any{map[string]any{"entity": "nope"}}}}); err == nil {
		t.Error("a malformed entity in a batch must be refused")
	}
}
