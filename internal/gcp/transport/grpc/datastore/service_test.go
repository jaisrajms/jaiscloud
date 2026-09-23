package datastore

import (
	"context"
	"net"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	core "jaiscloud/internal/gcp/service/datastore"
	datastorestore "jaiscloud/internal/gcp/store/datastore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func testServer(t *testing.T) (datastorepb.DatastoreClient, func()) {
	t.Helper()
	svc := NewService(core.NewService(datastorestore.NewMemoryStore(), "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	datastorepb.RegisterDatastoreServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return datastorepb.NewDatastoreClient(conn), func() { conn.Close(); srv.Stop() }
}

func nameKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "test"},
		Path: []*datastorepb.Key_PathElement{{
			Kind:   kind,
			IdType: &datastorepb.Key_PathElement_Name{Name: name},
		}},
	}
}

func incompleteKey(kind string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "test"},
		Path:        []*datastorepb.Key_PathElement{{Kind: kind}},
	}
}

func strVal(s string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: s}}
}

func boolVal(b bool) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: b}}
}

func intVal(n int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
}

func doubleVal(f float64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: f}}
}

func idKey(kind string, id int64) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "test"},
		Path: []*datastorepb.Key_PathElement{{
			Kind:   kind,
			IdType: &datastorepb.Key_PathElement_Id{Id: id},
		}},
	}
}

func entity(key *datastorepb.Key, props map[string]*datastorepb.Value) *datastorepb.Entity {
	return &datastorepb.Entity{Key: key, Properties: props}
}

func TestCommitInsertUpsertUpdateDelete(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	key := nameKey("Task", "a")

	// insert.
	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(key, map[string]*datastorepb.Value{"Done": boolVal(false)})},
		}},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(resp.GetMutationResults()) != 1 {
		t.Fatalf("insert results = %v", resp.GetMutationResults())
	}

	// insert on existing fails with AlreadyExists.
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(key, map[string]*datastorepb.Value{})},
		}},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate insert err = %v, want AlreadyExists", err)
	}

	// update on existing.
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Update{Update: entity(key, map[string]*datastorepb.Value{"Done": boolVal(true)})},
		}},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// update on missing fails with FailedPrecondition.
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "missing"), map[string]*datastorepb.Value{})},
		}},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("update missing err = %v, want FailedPrecondition", err)
	}

	// upsert.
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Done": boolVal(false), "Desc": strVal("x")})},
		}},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// delete.
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Delete{Delete: key},
		}},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// get after delete → missing.
	look, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{key}})
	if err != nil {
		t.Fatalf("lookup after delete: %v", err)
	}
	if len(look.GetMissing()) != 1 {
		t.Fatalf("missing = %v, want 1", look.GetMissing())
	}
}

func TestCommitInsertAutoAllocatesKey(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(incompleteKey("Task"), map[string]*datastorepb.Value{"Done": boolVal(false)})},
		}},
	})
	if err != nil {
		t.Fatalf("insert incomplete: %v", err)
	}
	mr := resp.GetMutationResults()[0]
	if mr.GetKey() == nil {
		t.Fatal("auto-allocated key is nil")
	}
	el := mr.GetKey().GetPath()[len(mr.GetKey().GetPath())-1]
	if el.GetId() <= 0 {
		t.Fatalf("allocated id = %d, want > 0", el.GetId())
	}

	// The allocated key is retrievable.
	look, err := client.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{mr.GetKey()}})
	if err != nil {
		t.Fatalf("lookup allocated: %v", err)
	}
	if len(look.GetFound()) != 1 {
		t.Fatalf("found = %v, want 1", look.GetFound())
	}
}

func TestLookupFoundAndMissing(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	key := nameKey("Task", "a")
	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, map[string]*datastorepb.Value{"Desc": strVal("hi")})},
		}},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	look, err := client.Lookup(ctx, &datastorepb.LookupRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{key, nameKey("Task", "missing")},
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(look.GetFound()) != 1 {
		t.Fatalf("found = %v, want 1", look.GetFound())
	}
	got := look.GetFound()[0].GetEntity()
	if got.GetProperties()["Desc"].GetStringValue() != "hi" {
		t.Fatalf("found properties = %v", got.GetProperties())
	}
	if len(look.GetMissing()) != 1 {
		t.Fatalf("missing = %v, want 1", look.GetMissing())
	}
}

func TestRunQueryEqualityFilter(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	put := func(name string, done bool) {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{
					"Done": boolVal(done),
					"Desc": strVal(name),
				})},
			}},
		}); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	put("a", false)
	put("b", true)

	q := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
			Property: &datastorepb.PropertyReference{Name: "Done"},
			Op:       datastorepb.PropertyFilter_EQUAL,
			Value:    boolVal(false),
		}}},
	}
	resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
	})
	if err != nil {
		t.Fatalf("runquery: %v", err)
	}
	results := resp.GetBatch().GetEntityResults()
	if len(results) != 1 {
		t.Fatalf("query results = %v, want 1 (Done=false)", results)
	}
	if results[0].GetEntity().GetProperties()["Desc"].GetStringValue() != "a" {
		t.Fatalf("query matched = %v, want 'a'", results[0].GetEntity().GetProperties())
	}
}

func TestRunQueryCompositeAnd(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	put := func(name, desc string, done bool) {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{
					"Done": boolVal(done),
					"Desc": strVal(desc),
				})},
			}},
		}); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	put("a", "x", false)
	put("b", "y", true)

	propFilter := func(name string, v *datastorepb.Value) *datastorepb.Filter {
		return &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
			Property: &datastorepb.PropertyReference{Name: name},
			Op:       datastorepb.PropertyFilter_EQUAL,
			Value:    v,
		}}}
	}
	q := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{
			Op:      datastorepb.CompositeFilter_AND,
			Filters: []*datastorepb.Filter{propFilter("Desc", strVal("y")), propFilter("Done", boolVal(true))},
		}}},
	}
	resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
	})
	if err != nil {
		t.Fatalf("runquery: %v", err)
	}
	results := resp.GetBatch().GetEntityResults()
	if len(results) != 1 || results[0].GetEntity().GetProperties()["Desc"].GetStringValue() != "y" {
		t.Fatalf("composite AND results = %v, want exactly ['y']", results)
	}
}

func TestAllocateIds(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{incompleteKey("Task"), incompleteKey("Task")},
	})
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	keys := resp.GetKeys()
	if len(keys) != 2 {
		t.Fatalf("allocated = %d keys, want 2", len(keys))
	}
	first := keys[0].GetPath()[0].GetId()
	second := keys[1].GetPath()[0].GetId()
	if first <= 0 || second <= first {
		t.Fatalf("allocated ids = %d, %d; want positive and monotonic", first, second)
	}
}

func TestRunQueryLessThanFilter(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	put := func(name string, priority int64) {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{
					"Priority": intVal(priority),
				})},
			}},
		}); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	put("low", 1)
	put("mid", 5)
	put("high", 10)

	q := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
			Property: &datastorepb.PropertyReference{Name: "Priority"},
			Op:       datastorepb.PropertyFilter_LESS_THAN,
			Value:    intVal(5),
		}}},
	}
	resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
	})
	if err != nil {
		t.Fatalf("runquery: %v", err)
	}
	results := resp.GetBatch().GetEntityResults()
	if len(results) != 1 {
		t.Fatalf("less-than results = %v, want exactly 1", results)
	}
	got := results[0].GetEntity().GetKey().GetPath()[0].GetName()
	if got != "low" {
		t.Fatalf("less-than matched = %q, want 'low'", got)
	}
}

func TestRunQueryUnsupportedOperatorsFailClosed(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"Done": boolVal(false)})},
		}},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	// HAS_ANCESTOR is an unsupported operator → InvalidArgument, not match-all.
	hasAncestor := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
			Property: &datastorepb.PropertyReference{Name: "Done"},
			Op:       datastorepb.PropertyFilter_HAS_ANCESTOR,
			Value:    boolVal(false),
		}}},
	}
	if _, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: hasAncestor},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("HAS_ANCESTOR err = %v, want InvalidArgument", err)
	}

	// CompositeFilter OR is unsupported → InvalidArgument.
	or := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{
			Op: datastorepb.CompositeFilter_OR,
			Filters: []*datastorepb.Filter{
				{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{Property: &datastorepb.PropertyReference{Name: "Done"}, Op: datastorepb.PropertyFilter_EQUAL, Value: boolVal(false)}}},
				{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{Property: &datastorepb.PropertyReference{Name: "Done"}, Op: datastorepb.PropertyFilter_EQUAL, Value: boolVal(true)}}},
			},
		}}},
	}
	if _, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: or},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("OR err = %v, want InvalidArgument", err)
	}
}

func TestRunQueryGqlNotSupported(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_GqlQuery{GqlQuery: &datastorepb.GqlQuery{QueryString: "SELECT * FROM Task"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GQL err = %v, want InvalidArgument", err)
	}
}

func TestCommitUpdateIncompleteKey(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Update{Update: entity(incompleteKey("Task"), map[string]*datastorepb.Value{})},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("update incomplete err = %v, want InvalidArgument", err)
	}
}

func TestAllocateIdsCompleteKey(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{idKey("Task", 1)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("allocate complete err = %v, want InvalidArgument", err)
	}
}

func TestMultiElementKeyRejected(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	multi := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "test"},
		Path: []*datastorepb.Key_PathElement{
			{Kind: "Parent", IdType: &datastorepb.Key_PathElement_Id{Id: 1}},
			{Kind: "Child", IdType: &datastorepb.Key_PathElement_Id{Id: 2}},
		},
	}
	if _, err := client.Lookup(ctx, &datastorepb.LookupRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{multi},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("multi-element lookup err = %v, want InvalidArgument", err)
	}
}

func TestExplicitIDAdvancesAllocator(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(idKey("Task", 100), map[string]*datastorepb.Value{})},
		}},
	}); err != nil {
		t.Fatalf("upsert explicit id: %v", err)
	}

	resp, err := client.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{incompleteKey("Task")},
	})
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	got := resp.GetKeys()[0].GetPath()[0].GetId()
	if got <= 100 {
		t.Fatalf("allocated id = %d, want > 100", got)
	}
}

// TestRunQueryLimitAndOffset verifies RunQuery honors a query's limit/offset
// (the official client rejects a server that returns more than the requested
// limit) and reports the skipped count and MoreResults.
func TestRunQueryLimitAndOffset(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{
					"Group": strVal("g"),
				})},
			}},
		}); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	filter := &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
		Property: &datastorepb.PropertyReference{Name: "Group"},
		Op:       datastorepb.PropertyFilter_EQUAL,
		Value:    strVal("g"),
	}}}
	run := func(q *datastorepb.Query) *datastorepb.RunQueryResponse {
		t.Helper()
		resp, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
			ProjectId: "test",
			QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
		})
		if err != nil {
			t.Fatalf("runquery: %v", err)
		}
		return resp
	}
	names := func(resp *datastorepb.RunQueryResponse) []string {
		var out []string
		for _, e := range resp.GetBatch().GetEntityResults() {
			out = append(out, e.GetEntity().GetKey().GetPath()[0].GetName())
		}
		return out
	}

	// Limit(1) caps the batch at one result and flags that a limit ended it.
	limited := run(&datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: filter,
		Limit:  wrapperspb.Int32(1),
	})
	if got := names(limited); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Limit(1) results = %v, want [a]", got)
	}
	if limited.GetBatch().GetMoreResults() != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
		t.Fatalf("MoreResults = %v, want MORE_RESULTS_AFTER_LIMIT", limited.GetBatch().GetMoreResults())
	}

	// Offset(1) skips one match and reports it in SkippedResults.
	offset := run(&datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: filter,
		Offset: 1,
		Limit:  wrapperspb.Int32(1),
	})
	if got := names(offset); len(got) != 1 || got[0] != "b" {
		t.Fatalf("Offset(1) results = %v, want [b]", got)
	}
	if offset.GetBatch().GetSkippedResults() != 1 {
		t.Fatalf("SkippedResults = %d, want 1", offset.GetBatch().GetSkippedResults())
	}

	// A negative limit is rejected rather than treated as unbounded.
	if _, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind:   []*datastorepb.KindExpression{{Name: "Task"}},
			Filter: filter,
			Limit:  wrapperspb.Int32(-1),
		}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative limit = %v, want InvalidArgument", err)
	}
}
