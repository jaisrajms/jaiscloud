package firestore

import (
	"context"
	"errors"
	"io"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

func pipelineDB() string { return "projects/test/databases/(default)" }

func refVal(v string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: v}}
}

func intValue(v int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
}

// fnVal builds a pipeline Function value (the DSL's expression node).
func fnVal(name string, args ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_FunctionValue{FunctionValue: &firestorepb.Function{Name: name, Args: args}}}
}

// fieldRef builds a field-reference expression value (FieldOf in the SDK).
func fieldRef(path string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_FieldReferenceValue{FieldReferenceValue: path}}
}

// mapValue builds a MapValue stage argument (projections, sort orderings).
func mapValue(fields map[string]*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{MapValue: &firestorepb.MapValue{Fields: fields}}}
}

// sortOrdering builds one {direction, expression} sort argument.
func sortOrdering(field, direction string) *firestorepb.Value {
	return mapValue(map[string]*firestorepb.Value{
		"direction":  strVal(direction),
		"expression": fieldRef(field),
	})
}

// pipelineRequest wraps stages into an ExecutePipelineRequest.
func pipelineRequest(stages ...*firestorepb.Pipeline_Stage) *firestorepb.ExecutePipelineRequest {
	return &firestorepb.ExecutePipelineRequest{
		Database: pipelineDB(),
		PipelineType: &firestorepb.ExecutePipelineRequest_StructuredPipeline{
			StructuredPipeline: &firestorepb.StructuredPipeline{
				Pipeline: &firestorepb.Pipeline{Stages: stages},
			},
		},
	}
}

// collectPipeline drains a single-response ExecutePipeline stream.
func collectPipeline(t *testing.T, stream firestorepb.Firestore_ExecutePipelineClient) *firestorepb.ExecutePipelineResponse {
	t.Helper()
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after results, got %v", err)
	}
	return resp
}

func seedBook(t *testing.T, svc *firestoreprovider.Service, id string, fields map[string]*firestorestore.Value) {
	t.Helper()
	if _, err := svc.CreateDocument(context.Background(), "test", "(default)", "books", id, fields); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

func TestExecutePipelineCollectionLimit(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c"} {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "books", id, map[string]*firestorestore.Value{
			"title": firestorestore.StringVal(id),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	stream, err := client.ExecutePipeline(ctx, pipelineRequest(
		&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
		&firestorepb.Pipeline_Stage{Name: "limit", Args: []*firestorepb.Value{intValue(2)}},
	))
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	resp := collectPipeline(t, stream)
	if len(resp.GetResults()) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.GetResults()))
	}
	if resp.GetExecutionTime() == nil {
		t.Fatal("expected execution_time")
	}
	if got := resp.GetResults()[0].GetFields()["title"].GetStringValue(); got != "a" {
		t.Fatalf("expected first result title a, got %q", got)
	}
}

// TestExecutePipelineUnsupportedStage asserts a stage outside the relational
// subset fails loud with Unimplemented rather than fabricating a result.
func TestExecutePipelineUnsupportedStage(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()

	stream, err := client.ExecutePipeline(context.Background(), pipelineRequest(
		&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
		&firestorepb.Pipeline_Stage{Name: "aggregate", Args: []*firestorepb.Value{refVal("title")}},
	))
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented for unsupported stage, got %v", err)
	}
}

func TestExecutePipelineRequiresPipeline(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()

	stream, err := client.ExecutePipeline(context.Background(), &firestorepb.ExecutePipelineRequest{
		Database: pipelineDB(),
	})
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a missing pipeline, got %v", err)
	}
}

// TestExecutePipelineWhereSortSelectDistinct exercises the relational stages
// end to end, in canonical order, using the real SDK encoding (function values,
// map-valued projections and orderings).
func TestExecutePipelineWhereSortSelectDistinct(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	seedBook(t, svc, "b1", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("A"), "price": firestorestore.IntVal(5), "genre": firestorestore.StringVal("x"),
	})
	seedBook(t, svc, "b2", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("B"), "price": firestorestore.IntVal(20), "genre": firestorestore.StringVal("x"),
	})
	seedBook(t, svc, "b3", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("B"), "price": firestorestore.IntVal(30), "genre": firestorestore.StringVal("y"),
	})
	seedBook(t, svc, "b4", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("C"), "price": firestorestore.IntVal(15), "genre": firestorestore.StringVal("y"),
	})

	stream, err := client.ExecutePipeline(ctx, pipelineRequest(
		&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
		&firestorepb.Pipeline_Stage{Name: "where", Args: []*firestorepb.Value{
			fnVal("greater_than", fieldRef("price"), intValue(10)),
		}},
		&firestorepb.Pipeline_Stage{Name: "sort", Args: []*firestorepb.Value{sortOrdering("price", "descending")}},
		&firestorepb.Pipeline_Stage{Name: "select", Args: []*firestorepb.Value{
			mapValue(map[string]*firestorepb.Value{
				"title": fieldRef("title"),
				"price": fieldRef("price"),
			}),
		}},
		&firestorepb.Pipeline_Stage{Name: "distinct", Args: []*firestorepb.Value{
			mapValue(map[string]*firestorepb.Value{"title": fieldRef("title")}),
		}},
	))
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	resp := collectPipeline(t, stream)
	results := resp.GetResults()
	if len(results) != 2 {
		t.Fatalf("expected 2 results after where/sort/select/distinct, got %d: %+v", len(results), results)
	}
	if got := results[0].GetFields()["title"].GetStringValue(); got != "B" {
		t.Fatalf("first result title = %q, want B (highest price)", got)
	}
	if got := results[0].GetFields()["price"].GetIntegerValue(); got != 30 {
		t.Fatalf("first result price = %d, want 30", got)
	}
	if got := results[1].GetFields()["title"].GetStringValue(); got != "C" {
		t.Fatalf("second result title = %q, want C", got)
	}
	if _, ok := results[0].GetFields()["genre"]; ok {
		t.Fatalf("select must project genre away, got %+v", results[0].GetFields())
	}
}

// TestExecutePipelineWhereComposite covers the and/or composite filter subset.
func TestExecutePipelineWhereComposite(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()

	seedBook(t, svc, "b1", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("A"), "price": firestorestore.IntVal(5), "genre": firestorestore.StringVal("x"),
	})
	seedBook(t, svc, "b2", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("B"), "price": firestorestore.IntVal(20), "genre": firestorestore.StringVal("x"),
	})
	seedBook(t, svc, "b3", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("C"), "price": firestorestore.IntVal(15), "genre": firestorestore.StringVal("y"),
	})

	for _, tc := range []struct {
		name  string
		where *firestorepb.Value
		want  []string
	}{
		{
			name: "and",
			where: fnVal("and",
				fnVal("equal", fieldRef("genre"), strVal("x")),
				fnVal("greater_than", fieldRef("price"), intValue(10)),
			),
			want: []string{"B"},
		},
		{
			name: "or",
			where: fnVal("or",
				fnVal("equal", fieldRef("genre"), strVal("y")),
				fnVal("less_than", fieldRef("price"), intValue(10)),
			),
			want: []string{"A", "C"},
		},
		{
			name:  "in",
			where: fnVal("equal_any", fieldRef("title"), fnVal("array", strVal("A"), strVal("C"))),
			want:  []string{"A", "C"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := client.ExecutePipeline(context.Background(), pipelineRequest(
				&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
				&firestorepb.Pipeline_Stage{Name: "where", Args: []*firestorepb.Value{tc.where}},
				&firestorepb.Pipeline_Stage{Name: "sort", Args: []*firestorepb.Value{sortOrdering("title", "ascending")}},
			))
			if err != nil {
				t.Fatalf("ExecutePipeline: %v", err)
			}
			resp := collectPipeline(t, stream)
			got := make([]string, 0, len(resp.GetResults()))
			for _, d := range resp.GetResults() {
				got = append(got, d.GetFields()["title"].GetStringValue())
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestExecutePipelineSelectAlias asserts a renamed projection is written under
// the alias.
func TestExecutePipelineSelectAlias(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()

	seedBook(t, svc, "b1", map[string]*firestorestore.Value{"title": firestorestore.StringVal("A")})

	stream, err := client.ExecutePipeline(context.Background(), pipelineRequest(
		&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
		&firestorepb.Pipeline_Stage{Name: "select", Args: []*firestorepb.Value{
			mapValue(map[string]*firestorepb.Value{"renamed": fieldRef("title")}),
		}},
	))
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	resp := collectPipeline(t, stream)
	if len(resp.GetResults()) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.GetResults()))
	}
	if got := resp.GetResults()[0].GetFields()["renamed"].GetStringValue(); got != "A" {
		t.Fatalf("renamed field = %q, want A (fields %+v)", got, resp.GetResults()[0].GetFields())
	}
}

// TestExecutePipelineDistinctWholeDocument covers distinct with no field list:
// whole-document distinctness includes the resource name, so two different
// documents that happen to share a field map are both kept, while the same
// document emitted twice collapses.
func TestExecutePipelineDistinctWholeDocument(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()

	seedBook(t, svc, "b1", map[string]*firestorestore.Value{"title": firestorestore.StringVal("A")})
	seedBook(t, svc, "b2", map[string]*firestorestore.Value{"title": firestorestore.StringVal("A")})
	seedBook(t, svc, "b3", map[string]*firestorestore.Value{"title": firestorestore.StringVal("B")})

	stream, err := client.ExecutePipeline(context.Background(), pipelineRequest(
		&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
		&firestorepb.Pipeline_Stage{Name: "distinct"},
		&firestorepb.Pipeline_Stage{Name: "sort", Args: []*firestorepb.Value{sortOrdering("title", "ascending")}},
	))
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	resp := collectPipeline(t, stream)
	if len(resp.GetResults()) != 3 {
		t.Fatalf("distinct by whole document must keep distinct documents, got %d results", len(resp.GetResults()))
	}

	// The same document reference twice collapses to one.
	stream, err = client.ExecutePipeline(context.Background(), pipelineRequest(
		&firestorepb.Pipeline_Stage{Name: "documents", Args: []*firestorepb.Value{refVal("/books/b1"), refVal("/books/b1")}},
		&firestorepb.Pipeline_Stage{Name: "distinct"},
	))
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	resp = collectPipeline(t, stream)
	if len(resp.GetResults()) != 1 {
		t.Fatalf("expected the repeated document to collapse to 1 result, got %d", len(resp.GetResults()))
	}
}

// TestExecutePipelineUnsupportedExpressions asserts unsupported expressions
// inside supported stages fail loud rather than being silently ignored.
func TestExecutePipelineUnsupportedExpressions(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()

	seedBook(t, svc, "b1", map[string]*firestorestore.Value{"title": firestorestore.StringVal("A")})

	cases := []struct {
		name  string
		stage *firestorepb.Pipeline_Stage
		code  codes.Code
	}{
		{
			name: "unsupported where function",
			stage: &firestorepb.Pipeline_Stage{Name: "where", Args: []*firestorepb.Value{
				fnVal("not", fnVal("equal", fieldRef("title"), strVal("A"))),
			}},
			code: codes.Unimplemented,
		},
		{
			name: "computed sort key",
			stage: &firestorepb.Pipeline_Stage{Name: "sort", Args: []*firestorepb.Value{
				mapValue(map[string]*firestorepb.Value{
					"direction":  strVal("ascending"),
					"expression": fnVal("length", fieldRef("title")),
				}),
			}},
			code: codes.Unimplemented,
		},
		{
			name: "computed select",
			stage: &firestorepb.Pipeline_Stage{Name: "select", Args: []*firestorepb.Value{
				mapValue(map[string]*firestorepb.Value{"n": fnVal("length", fieldRef("title"))}),
			}},
			code: codes.Unimplemented,
		},
		{
			name: "where argument is not a function",
			stage: &firestorepb.Pipeline_Stage{Name: "where", Args: []*firestorepb.Value{
				refVal("title"),
			}},
			code: codes.InvalidArgument,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := client.ExecutePipeline(context.Background(), pipelineRequest(
				&firestorepb.Pipeline_Stage{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
				tc.stage,
			))
			if err != nil {
				t.Fatalf("ExecutePipeline: %v", err)
			}
			if _, err := stream.Recv(); status.Code(err) != tc.code {
				t.Fatalf("expected %v, got %v", tc.code, err)
			}
		})
	}
}

func TestPartitionQuerySplitsResults(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"p0", "p1", "p2", "p3", "p4", "p5"} {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "items", id, map[string]*firestorestore.Value{
			"n": firestorestore.IntVal(1),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	resp, err := client.PartitionQuery(ctx, &firestorepb.PartitionQueryRequest{
		Parent: writeParent,
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items", AllDescendants: true}},
				OrderBy: []*firestorepb.StructuredQuery_Order{{
					Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "__name__"},
					Direction: firestorepb.StructuredQuery_ASCENDING,
				}},
			},
		},
		PartitionCount: 3,
	})
	if err != nil {
		t.Fatalf("PartitionQuery: %v", err)
	}
	if len(resp.GetPartitions()) != 2 {
		t.Fatalf("expected 2 partitions for 6 documents / count 3, got %d", len(resp.GetPartitions()))
	}
	for i, part := range resp.GetPartitions() {
		if got := part.GetValues()[0].GetReferenceValue(); got == "" {
			t.Fatalf("partition %d cursor has no reference value", i)
		}
	}
}
