package datastore

import (
	"context"
	"math"
	"strconv"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// props builds an entity carrying the given properties (Kind/Key are irrelevant
// to the aggregate reducer, which only reads Properties).
func props(m map[string]dsstore.Value) dsstore.Entity {
	return dsstore.Entity{Properties: m}
}

func countAgg(upto *int64) Aggregation { return Aggregation{Op: AggCount, UpTo: upto} }

func int64ptr(n int64) *int64 { return &n }

// TestAggregateCount covers the count operator: plain count, the up_to clamp,
// the negative up_to rejection, and the unknown-operator rejection.
func TestAggregateCount(t *testing.T) {
	entities := []dsstore.Entity{props(nil), props(nil), props(nil)}

	v, err := aggregate(countAgg(nil), entities)
	if err != nil || v.IntegerValue == nil || *v.IntegerValue != 3 {
		t.Fatalf("count = %+v, %v; want 3", v, err)
	}

	if v, err = aggregate(countAgg(int64ptr(2)), entities); err != nil || v.IntegerValue == nil || *v.IntegerValue != 2 {
		t.Fatalf("count up_to=2 = %+v, %v; want 2", v, err)
	}
	// up_to above the result size leaves the count unchanged.
	if v, err = aggregate(countAgg(int64ptr(10)), entities); err != nil || v.IntegerValue == nil || *v.IntegerValue != 3 {
		t.Fatalf("count up_to=10 = %+v, %v; want 3", v, err)
	}
	// up_to=0 is a legal cap of zero.
	if v, err = aggregate(countAgg(int64ptr(0)), entities); err != nil || v.IntegerValue == nil || *v.IntegerValue != 0 {
		t.Fatalf("count up_to=0 = %+v, %v; want 0", v, err)
	}
	if _, err = aggregate(countAgg(int64ptr(-1)), entities); err == nil {
		t.Fatal("count up_to=-1 must be rejected")
	}
	if _, err = aggregate(Aggregation{Op: AggUnspecified}, entities); err == nil {
		t.Fatal("an unspecified aggregation must be rejected")
	}
}

// TestAggregateSum covers the documented Sum semantics: numeric-only
// contribution, integer result when all contributors are integers and no
// overflow, double promotion otherwise, and integer 0 for an empty set.
func TestAggregateSum(t *testing.T) {
	if _, err := aggregate(Aggregation{Op: AggSum}, nil); err == nil {
		t.Fatal("sum without a property must be rejected")
	}

	v, err := aggregate(Aggregation{Op: AggSum, Property: "n"}, nil)
	if err != nil || v.IntegerValue == nil || *v.IntegerValue != 0 {
		t.Fatalf("empty sum = %+v, %v; want integer 0", v, err)
	}

	ints := []dsstore.Entity{props(map[string]dsstore.Value{"n": valInt(2)}), props(map[string]dsstore.Value{"n": valInt(3)})}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, ints); err != nil || v.IntegerValue == nil || *v.IntegerValue != 5 {
		t.Fatalf("int sum = %+v, %v; want 5", v, err)
	}

	mixed := []dsstore.Entity{props(map[string]dsstore.Value{"n": valInt(2)}), props(map[string]dsstore.Value{"n": valDbl(0.5)})}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, mixed); err != nil || v.DoubleValue == nil || *v.DoubleValue != 2.5 {
		t.Fatalf("mixed sum = %+v, %v; want double 2.5", v, err)
	}

	// A missing property and a non-numeric value are skipped, not fatal.
	skipped := []dsstore.Entity{
		props(map[string]dsstore.Value{"n": valInt(4)}),
		props(map[string]dsstore.Value{"other": valInt(100)}),
		props(map[string]dsstore.Value{"n": valStr("x")}),
	}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, skipped); err != nil || v.IntegerValue == nil || *v.IntegerValue != 4 {
		t.Fatalf("sum with skipped = %+v, %v; want 4", v, err)
	}

	// A hole in the middle must be skipped, not end the reduction: the last
	// entity still contributes.
	holed := []dsstore.Entity{
		props(map[string]dsstore.Value{"n": valInt(1)}),
		props(map[string]dsstore.Value{"other": valInt(100)}),
		props(map[string]dsstore.Value{"n": valInt(2)}),
	}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, holed); err != nil || v.IntegerValue == nil || *v.IntegerValue != 3 {
		t.Fatalf("sum with a hole = %+v, %v; want 3", v, err)
	}

	// A negative contribution is not an overflow: the result stays an integer.
	neg := []dsstore.Entity{
		props(map[string]dsstore.Value{"n": valInt(-5)}),
		props(map[string]dsstore.Value{"n": valInt(2)}),
	}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, neg); err != nil || v.IntegerValue == nil || *v.IntegerValue != -3 {
		t.Fatalf("negative sum = %+v, %v; want integer -3", v, err)
	}

	overflow := []dsstore.Entity{
		props(map[string]dsstore.Value{"n": valInt(math.MaxInt64)}),
		props(map[string]dsstore.Value{"n": valInt(1)}),
	}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, overflow); err != nil || v.DoubleValue == nil {
		t.Fatalf("overflow sum = %+v, %v; want a double", v, err)
	} else if want := float64(math.MaxInt64) + 1; *v.DoubleValue != want {
		t.Fatalf("overflow sum = %v, want %v", *v.DoubleValue, want)
	}

	// Underflow is detected the same way, on the negative side.
	underflow := []dsstore.Entity{
		props(map[string]dsstore.Value{"n": valInt(math.MinInt64)}),
		props(map[string]dsstore.Value{"n": valInt(-1)}),
	}
	if v, err = aggregate(Aggregation{Op: AggSum, Property: "n"}, underflow); err != nil || v.DoubleValue == nil {
		t.Fatalf("underflow sum = %+v, %v; want a double", v, err)
	}
}

// TestAggregateAvg covers the documented Avg semantics: numeric-only
// contribution, NULL for an empty set, and always a double result.
func TestAggregateAvg(t *testing.T) {
	if _, err := aggregate(Aggregation{Op: AggAvg}, nil); err == nil {
		t.Fatal("avg without a property must be rejected")
	}

	v, err := aggregate(Aggregation{Op: AggAvg, Property: "n"}, nil)
	if err != nil || v.NullValue == nil {
		t.Fatalf("empty avg = %+v, %v; want NULL", v, err)
	}

	ints := []dsstore.Entity{props(map[string]dsstore.Value{"n": valInt(2)}), props(map[string]dsstore.Value{"n": valInt(4)})}
	if v, err = aggregate(Aggregation{Op: AggAvg, Property: "n"}, ints); err != nil || v.DoubleValue == nil || *v.DoubleValue != 3 {
		t.Fatalf("int avg = %+v, %v; want double 3", v, err)
	}

	mixed := []dsstore.Entity{
		props(map[string]dsstore.Value{"n": valInt(1)}),
		props(map[string]dsstore.Value{"other": valInt(9)}),
		props(map[string]dsstore.Value{"n": valDbl(2.5)}),
		props(map[string]dsstore.Value{"n": valStr("skip")}),
	}
	if v, err = aggregate(Aggregation{Op: AggAvg, Property: "n"}, mixed); err != nil || v.DoubleValue == nil || *v.DoubleValue != 1.75 {
		t.Fatalf("mixed avg = %+v, %v; want double 1.75", v, err)
	}
}

// TestRunAggregationQuery covers the aggregation-query request validation and
// the alias naming/duplication rules end to end.
func TestRunAggregationQuery(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()
	upsert(t, s, "p", nameKey("Task", "a"), intProp(1))
	upsert(t, s, "p", nameKey("Task", "b"), intProp(2))

	nested := Query{Kind: "Task"}

	// Request validation.
	if _, err := s.RunAggregationQuery(ctx, "p", nil, nil); err == nil {
		t.Fatal("nil aggregation query must be rejected")
	}
	if _, err := s.RunAggregationQuery(ctx, "p", &AggregationQuery{Nested: nested}, nil); err == nil {
		t.Fatal("an aggregation query with no aggregations must be rejected")
	}
	six := make([]Aggregation, 6)
	for i := range six {
		six[i] = countAgg(nil)
	}
	if _, err := s.RunAggregationQuery(ctx, "p", &AggregationQuery{Nested: nested, Aggregations: six}, nil); err == nil {
		t.Fatal("more than five aggregations must be rejected")
	}
	// A malformed nested filter is rejected up front, before the reduction
	// runs (the RunAggregationQuery half of the up-front validation). (TD21)
	if _, err := s.RunAggregationQuery(ctx, "p", &AggregationQuery{
		Nested: Query{Kind: "Task", Filter: &Filter{Property: &PropertyFilter{
			Property: "n", Op: PropertyIn, Value: valInt(1),
		}}},
		Aggregations: []Aggregation{countAgg(nil)},
	}, nil); err == nil {
		t.Fatal("an IN filter without an array value must be rejected")
	}

	// Exactly five is allowed, and unnamed aggregations get property_1..N.
	five := make([]Aggregation, 5)
	for i := range five {
		five[i] = countAgg(nil)
	}
	res, err := s.RunAggregationQuery(ctx, "p", &AggregationQuery{Nested: nested, Aggregations: five}, nil)
	if err != nil {
		t.Fatalf("five aggregations: %v", err)
	}
	for i := 1; i <= 5; i++ {
		v, ok := res.Aggregates["property_"+strconv.Itoa(i)]
		if !ok || v.IntegerValue == nil || *v.IntegerValue != 2 {
			t.Fatalf("aggregate property_%d = %+v (present=%v), want count 2", i, v, ok)
		}
	}

	// A named aggregation is keyed by its alias; a duplicate alias is rejected.
	res, err = s.RunAggregationQuery(ctx, "p", &AggregationQuery{
		Nested:       nested,
		Aggregations: []Aggregation{{Op: AggCount, Alias: "total"}},
	}, nil)
	if err != nil || res.Aggregates["total"].IntegerValue == nil || *res.Aggregates["total"].IntegerValue != 2 {
		t.Fatalf("named count = %+v, %v; want total=2", res, err)
	}
	if _, err := s.RunAggregationQuery(ctx, "p", &AggregationQuery{
		Nested:       nested,
		Aggregations: []Aggregation{{Op: AggCount, Alias: "x"}, {Op: AggCount, Alias: "x"}},
	}, nil); err == nil {
		t.Fatal("a duplicate alias must be rejected")
	}

	// The nested filter scopes the reduction.
	filtered := Query{Kind: "Task", Filter: &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(2)}}}
	res, err = s.RunAggregationQuery(ctx, "p", &AggregationQuery{Nested: filtered, Aggregations: []Aggregation{{Op: AggCount, Alias: "c"}}}, nil)
	if err != nil || *res.Aggregates["c"].IntegerValue != 1 {
		t.Fatalf("filtered count = %+v, %v; want 1", res, err)
	}
}
