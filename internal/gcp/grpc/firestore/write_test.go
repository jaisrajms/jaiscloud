package firestore

import (
	"context"
	"io"
	"reflect"
	"sort"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

const writeParent = "projects/test/databases/(default)/documents"

// writeClient opens a Firestore Write stream against the test service.
func writeClient(t *testing.T) (firestorepb.Firestore_WriteClient, *firestoreprovider.Service, func()) {
	t.Helper()
	client, svc, cleanup := listenTestClient(t)
	stream, err := client.Write(context.Background())
	if err != nil {
		cleanup()
		t.Fatalf("Write: %v", err)
	}
	return stream, svc, cleanup
}

func updateWrite(doc string, fields map[string]*firestorepb.Value) *firestorepb.Write {
	return &firestorepb.Write{
		Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
			Name:   writeParent + "/" + doc,
			Fields: fields,
		}},
	}
}

func strVal(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

func intVal(n int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: n}}
}

func TestWriteNewStream(t *testing.T) {
	stream, svc, cleanup := writeClient(t)
	defer cleanup()
	ctx := context.Background()

	// First message: handshake.
	if err := stream.Send(&firestorepb.WriteRequest{Database: "projects/test/databases/(default)"}); err != nil {
		t.Fatalf("Send handshake: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv handshake: %v", err)
	}
	if resp.GetStreamId() == "" {
		t.Fatal("expected non-empty stream_id on new stream")
	}
	if len(resp.GetStreamToken()) == 0 {
		t.Fatal("expected non-empty stream_token on handshake")
	}
	if resp.GetCommitTime() != nil {
		t.Fatal("expected no commit_time on handshake response")
	}
	streamID := resp.GetStreamId()
	firstToken := resp.GetStreamToken()

	// Second message: two writes.
	if err := stream.Send(&firestorepb.WriteRequest{
		StreamToken: firstToken,
		Writes: []*firestorepb.Write{
			updateWrite("users/alice", map[string]*firestorepb.Value{"name": strVal("alice")}),
			updateWrite("users/bob", map[string]*firestorepb.Value{"name": strVal("bob")}),
		},
	}); err != nil {
		t.Fatalf("Send writes: %v", err)
	}
	resp, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv writes: %v", err)
	}
	if resp.GetStreamId() != "" {
		t.Fatalf("expected empty stream_id on non-first response, got %q", resp.GetStreamId())
	}
	if len(resp.GetWriteResults()) != 2 {
		t.Fatalf("expected 2 write_results, got %d", len(resp.GetWriteResults()))
	}
	if resp.GetCommitTime() == nil {
		t.Fatal("expected commit_time on write response")
	}
	if resp.GetStreamToken() == nil || len(resp.GetStreamToken()) == 0 {
		t.Fatal("expected non-empty stream_token on write response")
	}
	_ = streamID

	// The writes must be persisted via the shared service.
	doc, err := svc.GetDocument(ctx, writeParent+"/users/alice", nil, nil)
	if err != nil {
		t.Fatalf("GetDocument alice: %v", err)
	}
	if got, _ := doc.Fields["name"].AsString(); got != "alice" {
		t.Fatalf("expected alice, got %q", got)
	}
}

func TestWriteResumeFlow(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	// First stream: handshake + one write.
	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&firestorepb.WriteRequest{Database: "projects/test/databases/(default)"}); err != nil {
		t.Fatalf("Send handshake: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv handshake: %v", err)
	}
	streamID := resp.GetStreamId()
	stream.Send(&firestorepb.WriteRequest{
		StreamToken: resp.GetStreamToken(),
		Writes:      []*firestorepb.Write{updateWrite("items/a", map[string]*firestorepb.Value{"v": strVal("1")})},
	})
	lastResp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv write: %v", err)
	}
	if len(lastResp.GetWriteResults()) != 1 {
		t.Fatalf("expected 1 write result, got %d", len(lastResp.GetWriteResults()))
	}
	lastToken := lastResp.GetStreamToken()

	// Final flush + close.
	if err := stream.Send(&firestorepb.WriteRequest{StreamToken: lastToken}); err != nil {
		t.Fatalf("Send flush: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv flush: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected EOF on clean close, got %v", err)
	}

	// Resume: new stream whose first message sets stream_id + stream_token.
	stream2, err := client.Write(ctx)
	if err != nil {
		t.Fatalf("Write (resume): %v", err)
	}
	if err := stream2.Send(&firestorepb.WriteRequest{
		Database:    "projects/test/databases/(default)",
		StreamId:    streamID,
		StreamToken: lastToken,
	}); err != nil {
		t.Fatalf("Send resume: %v", err)
	}
	resp2, err := stream2.Recv()
	if err != nil {
		t.Fatalf("Recv resume: %v", err)
	}
	if resp2.GetStreamId() != "" {
		t.Fatalf("resume response must not carry stream_id, got %q", resp2.GetStreamId())
	}
	if len(resp2.GetStreamToken()) == 0 {
		t.Fatal("expected non-empty stream_token on resume")
	}

	// The resumed stream continues to accept writes.
	if err := stream2.Send(&firestorepb.WriteRequest{
		StreamToken: resp2.GetStreamToken(),
		Writes:      []*firestorepb.Write{updateWrite("items/b", map[string]*firestorepb.Value{"v": strVal("2")})},
	}); err != nil {
		t.Fatalf("Send after resume: %v", err)
	}
	resp3, err := stream2.Recv()
	if err != nil {
		t.Fatalf("Recv after resume: %v", err)
	}
	if len(resp3.GetWriteResults()) != 1 {
		t.Fatalf("expected 1 write result after resume, got %d", len(resp3.GetWriteResults()))
	}

	doc, err := svc.GetDocument(ctx, writeParent+"/items/b", nil, nil)
	if err != nil {
		t.Fatalf("GetDocument items/b: %v", err)
	}
	if got, _ := doc.Fields["v"].AsString(); got != "2" {
		t.Fatalf("expected v=2, got %q", got)
	}
}

func TestWriteFirstMessageRejectsWrites(t *testing.T) {
	stream, _, cleanup := writeClient(t)
	defer cleanup()

	if err := stream.Send(&firestorepb.WriteRequest{
		Database: "projects/test/databases/(default)",
		Writes:   []*firestorepb.Write{updateWrite("users/alice", map[string]*firestorepb.Value{"name": strVal("alice")})},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestRunAggregationQueryCountAndSum(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "products", "p1", map[string]*firestorestore.Value{
		"price":  firestorestore.IntVal(10),
		"active": firestorestore.BoolVal(true),
	})
	svc.CreateDocument(ctx, "test", "(default)", "products", "p2", map[string]*firestorestore.Value{
		"price":  firestorestore.IntVal(25),
		"active": firestorestore.BoolVal(true),
	})
	svc.CreateDocument(ctx, "test", "(default)", "products", "p3", map[string]*firestorestore.Value{
		"price":  firestorestore.IntVal(100),
		"active": firestorestore.BoolVal(false),
	})

	stream, err := client.RunAggregationQuery(ctx, &firestorepb.RunAggregationQueryRequest{
		Parent: writeParent,
		QueryType: &firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery{
			StructuredAggregationQuery: &firestorepb.StructuredAggregationQuery{
				QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "products"}},
						Where: &firestorepb.StructuredQuery_Filter{
							FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
								FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
									Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "active"},
									Op:    firestorepb.StructuredQuery_FieldFilter_EQUAL,
									Value: &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: true}},
								},
							},
						},
					},
				},
				Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
					{Alias: "count", Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
						Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
					}},
					{Alias: "total", Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Sum_{
						Sum: &firestorepb.StructuredAggregationQuery_Aggregation_Sum{
							Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "price"},
						},
					}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RunAggregationQuery: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	fields := resp.GetResult().GetAggregateFields()
	if got := fields["count"].GetIntegerValue(); got != 2 {
		t.Fatalf("expected count=2, got %d", got)
	}
	if got := fields["total"].GetIntegerValue(); got != 35 {
		t.Fatalf("expected total=35, got %d", got)
	}
	if resp.GetReadTime() == nil {
		t.Fatal("expected read_time")
	}
	// A second Recv should terminate the stream.
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestRunAggregationQueryDefaultAliases(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "products", "p1", map[string]*firestorestore.Value{
		"price": firestorestore.IntVal(10),
	})

	countAgg := func() *firestorepb.StructuredAggregationQuery_Aggregation {
		return &firestorepb.StructuredAggregationQuery_Aggregation{
			Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
				Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
			},
		}
	}
	named := func(alias string) *firestorepb.StructuredAggregationQuery_Aggregation {
		a := countAgg()
		a.Alias = alias
		return a
	}

	run := func(aggs []*firestorepb.StructuredAggregationQuery_Aggregation) []string {
		stream, err := client.RunAggregationQuery(ctx, &firestorepb.RunAggregationQueryRequest{
			Parent: writeParent,
			QueryType: &firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery{
				StructuredAggregationQuery: &firestorepb.StructuredAggregationQuery{
					QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
						StructuredQuery: &firestorepb.StructuredQuery{
							From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "products"}},
						},
					},
					Aggregations: aggs,
				},
			},
		})
		if err != nil {
			t.Fatalf("RunAggregationQuery: %v", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		fields := resp.GetResult().GetAggregateFields()
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return keys
	}

	// [unnamed] -> field_1
	if got := run([]*firestorepb.StructuredAggregationQuery_Aggregation{countAgg()}); !reflect.DeepEqual(got, []string{"field_1"}) {
		t.Fatalf("case [unnamed]: expected [field_1], got %v", got)
	}
	// [named, named, unnamed] -> field_1
	if got := run([]*firestorepb.StructuredAggregationQuery_Aggregation{named("a"), named("b"), countAgg()}); !reflect.DeepEqual(got, []string{"a", "b", "field_1"}) {
		t.Fatalf("case [named, named, unnamed]: expected [a b field_1], got %v", got)
	}
	// [unnamed, unnamed] -> field_1, field_2
	if got := run([]*firestorepb.StructuredAggregationQuery_Aggregation{countAgg(), countAgg()}); !reflect.DeepEqual(got, []string{"field_1", "field_2"}) {
		t.Fatalf("case [unnamed, unnamed]: expected [field_1 field_2], got %v", got)
	}
}

func TestPartitionQueryTooFewDocuments(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()

	resp, err := client.PartitionQuery(context.Background(), &firestorepb.PartitionQueryRequest{
		Parent: writeParent,
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "products"}},
			},
		},
		PartitionCount: 4,
	})
	if err != nil {
		t.Fatalf("PartitionQuery: %v", err)
	}
	if len(resp.GetPartitions()) != 0 {
		t.Fatalf("expected 0 partitions for an empty collection, got %d", len(resp.GetPartitions()))
	}
}
