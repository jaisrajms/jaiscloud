package datastore

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─── ReserveIds ───────────────────────────────────────────────────────────────

func TestReserveIdsAdvancesAllocator(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	// Reserve numeric IDs 1..3 that AllocateIds would otherwise issue first.
	if _, err := client.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{idKey("Task", 1), idKey("Task", 2), idKey("Task", 3)},
	}); err != nil {
		t.Fatalf("reserve ids: %v", err)
	}

	resp, err := client.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{incompleteKey("Task"), incompleteKey("Task")},
	})
	if err != nil {
		t.Fatalf("allocate ids: %v", err)
	}
	if len(resp.GetKeys()) != 2 {
		t.Fatalf("allocated %d keys, want 2", len(resp.GetKeys()))
	}
	for i, k := range resp.GetKeys() {
		id := k.GetPath()[0].GetId()
		if id <= 3 {
			t.Fatalf("allocated id[%d] = %d, want > 3 (reserved IDs must not be reissued)", i, id)
		}
	}
}

// TestReserveIdsThenAllocateSkipsReserved checks the exact-next-ID case: after
// reserving the ID that would have been allocated next, the allocator moves
// past it.
func TestReserveIdsThenAllocateSkipsReserved(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{idKey("Task", 1)},
	}); err != nil {
		t.Fatalf("reserve ids: %v", err)
	}

	resp, err := client.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{incompleteKey("Task")},
	})
	if err != nil {
		t.Fatalf("allocate ids: %v", err)
	}
	if got := resp.GetKeys()[0].GetPath()[0].GetId(); got == 1 {
		t.Fatalf("allocated reserved id 1")
	}
}

func TestReserveIdsNameKeyIsNoOp(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{nameKey("Task", "foo")},
	}); err != nil {
		t.Fatalf("reserve name key: %v", err)
	}

	// A name cannot collide with numeric allocation, so the allocator is
	// untouched and the first allocated ID is still 1.
	resp, err := client.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{incompleteKey("Task")},
	})
	if err != nil {
		t.Fatalf("allocate ids: %v", err)
	}
	if got := resp.GetKeys()[0].GetPath()[0].GetId(); got != 1 {
		t.Fatalf("allocated id = %d, want 1 (name reservation must not advance the allocator)", got)
	}
}

func TestReserveIdsIncompleteKeyInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{
		ProjectId: "test",
		Keys:      []*datastorepb.Key{incompleteKey("Task")},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reserve incomplete key err = %v, want InvalidArgument", err)
	}
}

func TestReserveIdsDatabaseScopedInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{
		ProjectId:  "test",
		DatabaseId: "other-db",
		Keys:       []*datastorepb.Key{idKey("Task", 1)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reserve with database id err = %v, want InvalidArgument", err)
	}
}

// ─── RunAggregationQuery ──────────────────────────────────────────────────────

func eqFilter(prop string, v *datastorepb.Value) *datastorepb.Filter {
	return &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
		Property: &datastorepb.PropertyReference{Name: prop},
		Op:       datastorepb.PropertyFilter_EQUAL,
		Value:    v,
	}}}
}

func countAgg(alias string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias:    alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Count_{Count: &datastorepb.AggregationQuery_Aggregation_Count{}},
	}
}

func sumAgg(prop, alias string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Sum_{Sum: &datastorepb.AggregationQuery_Aggregation_Sum{
			Property: &datastorepb.PropertyReference{Name: prop},
		}},
	}
}

func avgAgg(prop, alias string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Avg_{Avg: &datastorepb.AggregationQuery_Aggregation_Avg{
			Property: &datastorepb.PropertyReference{Name: prop},
		}},
	}
}

func aggRequest(kind string, filter *datastorepb.Filter, aggs ...*datastorepb.AggregationQuery_Aggregation) *datastorepb.RunAggregationQueryRequest {
	return &datastorepb.RunAggregationQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{
					NestedQuery: &datastorepb.Query{
						Kind:   []*datastorepb.KindExpression{{Name: kind}},
						Filter: filter,
					},
				},
				Aggregations: aggs,
			},
		},
	}
}

// aggProps runs the request and returns the single result's alias → value map.
func aggProps(t *testing.T, client datastorepb.DatastoreClient, req *datastorepb.RunAggregationQueryRequest) map[string]*datastorepb.Value {
	t.Helper()
	resp, err := client.RunAggregationQuery(context.Background(), req)
	if err != nil {
		t.Fatalf("run aggregation query: %v", err)
	}
	batch := resp.GetBatch()
	if batch == nil {
		t.Fatal("aggregation response has no batch")
	}
	if batch.GetMoreResults() != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
		t.Fatalf("more_results = %v, want NO_MORE_RESULTS", batch.GetMoreResults())
	}
	if batch.GetReadTime() == nil {
		t.Fatal("aggregation batch must set read_time")
	}
	if len(batch.GetAggregationResults()) != 1 {
		t.Fatalf("aggregation_results = %d, want 1", len(batch.GetAggregationResults()))
	}
	return batch.GetAggregationResults()[0].GetAggregateProperties()
}

// seedMetrics writes the fixture shared by the count/sum/avg tests.
func seedMetrics(t *testing.T, client datastorepb.DatastoreClient) {
	t.Helper()
	ctx := context.Background()
	props := []map[string]*datastorepb.Value{
		{"group": strVal("alpha"), "score": intVal(10)},
		{"group": strVal("alpha"), "score": intVal(20)},
		{"group": strVal("beta"), "score": intVal(30)},
		{"group": strVal("alpha"), "ratio": doubleVal(1.5)},
		{"group": strVal("alpha"), "score": strVal("not-a-number")},
	}
	for i, p := range props {
		key := nameKey("Metric", "m"+string(rune('a'+i)))
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(key, p)}}},
		}); err != nil {
			t.Fatalf("seed metric %d: %v", i, err)
		}
	}
}

func TestRunAggregationQueryCount(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	seedMetrics(t, client)

	props := aggProps(t, client, aggRequest("Metric", nil, countAgg("total")))
	total, ok := props["total"]
	if !ok {
		t.Fatalf("alias total missing from %v", props)
	}
	if got := total.GetIntegerValue(); got != 5 {
		t.Fatalf("count = %d, want 5", got)
	}
}

func TestRunAggregationQueryCountWithFilter(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	seedMetrics(t, client)

	props := aggProps(t, client, aggRequest("Metric", eqFilter("group", strVal("alpha")), countAgg("total")))
	if got := props["total"].GetIntegerValue(); got != 4 {
		t.Fatalf("filtered count = %d, want 4", got)
	}
}

func TestRunAggregationQuerySumInteger(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	seedMetrics(t, client)

	// d has no score and e's score is a string: both are skipped.
	props := aggProps(t, client, aggRequest("Metric", nil, sumAgg("score", "sum_score")))
	if _, isDouble := props["sum_score"].GetValueType().(*datastorepb.Value_DoubleValue); isDouble {
		t.Fatalf("integer-property sum returned a double: %v", props["sum_score"])
	}
	if got := props["sum_score"].GetIntegerValue(); got != 60 {
		t.Fatalf("sum(score) = %d, want 60", got)
	}
}

func TestRunAggregationQuerySumDouble(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	seedMetrics(t, client)

	props := aggProps(t, client, aggRequest("Metric", nil, sumAgg("ratio", "sum_ratio")))
	if got := props["sum_ratio"].GetDoubleValue(); got != 1.5 {
		t.Fatalf("sum(ratio) = %v, want 1.5", got)
	}
}

// TestRunAggregationQuerySumMixedIsDouble covers the proto rule that a sum is a
// double as soon as any contributing value is a double.
func TestRunAggregationQuerySumMixedIsDouble(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	for _, e := range []struct {
		name string
		val  *datastorepb.Value
	}{
		{"a", intVal(1)},
		{"b", doubleVal(2.5)},
	} {
		if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: "test",
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Mix", e.name), map[string]*datastorepb.Value{"v": e.val})}}},
		}); err != nil {
			t.Fatalf("seed %s: %v", e.name, err)
		}
	}

	props := aggProps(t, client, aggRequest("Mix", nil, sumAgg("v", "s")))
	if _, isInt := props["s"].GetValueType().(*datastorepb.Value_IntegerValue); isInt {
		t.Fatalf("mixed integer/double sum returned an integer: %v", props["s"])
	}
	if got := props["s"].GetDoubleValue(); got != 3.5 {
		t.Fatalf("mixed sum = %v, want 3.5", got)
	}
}

func TestRunAggregationQueryAvg(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	seedMetrics(t, client)

	// 10 + 20 + 30 over the three numeric scores; d/e skipped.
	props := aggProps(t, client, aggRequest("Metric", nil, avgAgg("score", "avg_score")))
	if _, isDouble := props["avg_score"].GetValueType().(*datastorepb.Value_DoubleValue); !isDouble {
		t.Fatalf("avg must return a double: %v", props["avg_score"])
	}
	if got := props["avg_score"].GetDoubleValue(); got != 20 {
		t.Fatalf("avg(score) = %v, want 20", got)
	}
}

// TestRunAggregationQueryAvgEmptyIsNull covers the proto rule that avg of an
// empty contributing set is NULL.
func TestRunAggregationQueryAvgEmptyIsNull(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Empty", "a"), map[string]*datastorepb.Value{})}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// No entity carries "n": sum is 0, avg is NULL.
	props := aggProps(t, client, aggRequest("Empty", nil, sumAgg("n", "s"), avgAgg("n", "a")))
	if got := props["s"].GetIntegerValue(); got != 0 {
		t.Fatalf("empty sum = %d, want 0", got)
	}
	if props["a"].GetNullValue() != 0 { // structpb.NullValue_NULL_VALUE == 0
		t.Fatalf("empty avg = %v, want NULL", props["a"])
	}
}

// TestRunAggregationQueryAliasesAndDefaults covers multiple aliases in one
// result and the server-generated "property_<n>" alias for an unnamed
// aggregation.
func TestRunAggregationQueryAliasesAndDefaults(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	seedMetrics(t, client)

	props := aggProps(t, client, aggRequest("Metric", nil, countAgg("total"), sumAgg("score", "")))
	if _, ok := props["total"]; !ok {
		t.Fatalf("explicit alias total missing from %v", props)
	}
	if _, ok := props["property_1"]; !ok {
		t.Fatalf("default alias property_1 missing from %v", props)
	}
	if got := props["property_1"].GetIntegerValue(); got != 60 {
		t.Fatalf("property_1 = %d, want 60", got)
	}
}

// TestRunAggregationQueryRecordsReads proves RunAggregationQuery is
// transaction-aware: a conflicting write after the aggregation aborts a
// transactional commit that carries no mutations (read-set validation only).
func TestRunAggregationQueryRecordsReads(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	upsertTask(t, client, "a", 1)
	upsertTask(t, client, "b", 1)

	txn := beginTxn(t, client)
	req := aggRequest("Task", nil, countAgg("total"))
	req.ReadOptions = readTxn(txn)
	if _, err := client.RunAggregationQuery(ctx, req); err != nil {
		t.Fatalf("transactional aggregation: %v", err)
	}

	// Modify one of the entities the nested query returned.
	upsertTask(t, client, "b", 2)

	if _, err := txnCommit(t, client, txn); status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}
}

func TestRunAggregationQueryUnknownTransactionInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	req := aggRequest("Task", nil, countAgg("total"))
	req.ReadOptions = readTxn([]byte("not-a-real-transaction"))
	_, err := client.RunAggregationQuery(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestRunAggregationQueryGQLInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	_, err := client.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunAggregationQueryRequest_GqlQuery{GqlQuery: &datastorepb.GqlQuery{}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GQL aggregation err = %v, want InvalidArgument", err)
	}
}

func TestRunAggregationQueryNoAggregationsInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	_, err := client.RunAggregationQuery(context.Background(), aggRequest("Task", nil))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no-aggregation err = %v, want InvalidArgument", err)
	}
}

func TestRunAggregationQueryNoNestedQueryInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	_, err := client.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{
		ProjectId: "test",
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("total")},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing nested query err = %v, want InvalidArgument", err)
	}
}
