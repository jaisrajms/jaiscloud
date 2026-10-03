package grpcconformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// firestoreExtraChecks covers the firestore document, transaction, query and
// streaming RPCs on top of the Commit/BatchGetDocuments probes in
// checks_firestore.go.
//
// Every probe drives the generated firestorepb stub directly (dialled with
// insecure credentials) rather than the high-level cloud.google.com/go/firestore
// client: the high-level Doc.Set/Doc.Get/Doc.Delete collapse onto Commit and
// BatchGetDocuments, so they cannot attribute the individual CreateDocument,
// GetDocument, UpdateDocument, DeleteDocument, ListDocuments, ListCollectionIds,
// transaction, query, BatchWrite, Write or Listen RPCs. The two long-lived
// bidirectional streams (Write, Listen) are bounded by a short stream context
// and cancelled as soon as the asserted frame arrives.
//
// Checks run in registry order and share one run-unique collection. Names are
// run-unique via cfg.ResourceName, so a long-lived emulator never sees cross-run
// collisions, and each probe leaves its own fixtures behind only until the
// final DeleteDocument cleanup.
func firestoreExtraChecks() []Check {
	return []Check{
		// ── documents ───────────────────────────────────────────────────────
		{Service: "firestore", RPC: "CreateDocument", Method: "CreateDocument", KeyField: "document.name/fields", Run: checkFirestoreCreateDocument},
		{Service: "firestore", RPC: "GetDocument", Method: "GetDocument", KeyField: "document.name/fields", Run: checkFirestoreGetDocument},
		{Service: "firestore", RPC: "UpdateDocument (update_mask)", Method: "UpdateDocument", KeyField: "masked field updated, unmasked preserved", Run: checkFirestoreUpdateDocument},
		{Service: "firestore", RPC: "ListDocuments", Method: "ListDocuments", KeyField: "documents[] contains fixture", Run: checkFirestoreListDocuments},
		{Service: "firestore", RPC: "ListCollectionIds", Method: "ListCollectionIds", KeyField: "collectionIds[] contains fixture", Run: checkFirestoreListCollectionIds},

		// ── queries ─────────────────────────────────────────────────────────
		{Service: "firestore", RPC: "RunQuery (structured)", Method: "RunQuery", KeyField: "streamed document matches filter", Run: checkFirestoreRunQuery},
		{Service: "firestore", RPC: "RunAggregationQuery (COUNT)", Method: "RunAggregationQuery", KeyField: "aggregate_fields.count", Run: checkFirestoreRunAggregationQuery},
		{Service: "firestore", RPC: "PartitionQuery", Method: "PartitionQuery", KeyField: "partitions[].values reference cursors", Run: checkFirestorePartitionQuery},
		{Service: "firestore", RPC: "ExecutePipeline", Method: "ExecutePipeline", KeyField: "results[] + execution_time", Run: checkFirestoreExecutePipeline},

		// ── writes / transactions ───────────────────────────────────────────
		{Service: "firestore", RPC: "BatchWrite", Method: "BatchWrite", KeyField: "write_results[] + OK statuses", Run: checkFirestoreBatchWrite},
		{Service: "firestore", RPC: "BeginTransaction + Commit", Method: "BeginTransaction", KeyField: "transaction id; committed write persists", Run: checkFirestoreBeginTransaction},
		{Service: "firestore", RPC: "Rollback", Method: "Rollback", KeyField: "rolled-back write absent, committed present", Run: checkFirestoreRollback},
		{Service: "firestore", RPC: "Write", Method: "Write", KeyField: "stream_id/stream_token + write_results", Run: checkFirestoreWrite},
		{Service: "firestore", RPC: "Listen", Method: "Listen", KeyField: "ADD, DocumentChange, CURRENT, NO_CHANGE frames", Run: checkFirestoreListen},

		// ── cleanup ─────────────────────────────────────────────────────────
		{Service: "firestore", RPC: "DeleteDocument", Method: "DeleteDocument", KeyField: "document absent after delete", Run: checkFirestoreDeleteDocument},
	}
}

const (
	firestoreDBName   = "projects/%s/databases/(default)"
	firestoreExtraVal = "grpc-fs-conformance"
)

// ─── fixtures + raw client ───────────────────────────────────────────────────

func fsDatabase(cfg Config) string { return fmt.Sprintf(firestoreDBName, cfg.Project) }

func fsDocuments(cfg Config) string { return fsDatabase(cfg) + "/documents" }

// fsCollection is the shared run-unique collection id.
func fsCollection(cfg Config) string { return cfg.ResourceName("gcpc_grpc_fs") }

// fsDocName is the full document name for collection coll and document id.
func fsDocName(cfg Config, coll, id string) string {
	return fsDocuments(cfg) + "/" + coll + "/" + id
}

// newFirestoreStub dials the emulator with insecure credentials and returns the
// generated firestorepb client. The returned conn must be closed by the caller.
func newFirestoreStub(cfg Config) (firestorepb.FirestoreClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(cfg.GRPCAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", cfg.GRPCAddr(), err)
	}
	return firestorepb.NewFirestoreClient(conn), conn, nil
}

func fsString(v string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: v}}
}

func fsInt(v int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
}

func fsReference(v string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: v}}
}

// fsCreate creates a document with the shared fixture fields, tolerating a
// pre-existing document (AlreadyExists) so repeated checks stay idempotent.
func fsCreate(ctx context.Context, client firestorepb.FirestoreClient, cfg Config, coll, id string) (*firestorepb.Document, error) {
	doc, err := client.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsDocuments(cfg),
		CollectionId: coll,
		DocumentId:   id,
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"name": fsString(firestoreExtraVal),
			"keep": fsString("keep"),
		}},
	})
	if status.Code(err) == codes.AlreadyExists {
		return client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: fsDocName(cfg, coll, id)})
	}
	return doc, err
}

// fsDelete removes a document, ignoring "not found".
func fsDelete(ctx context.Context, client firestorepb.FirestoreClient, name string) {
	_, _ = client.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name})
}

// fsUpdateWrite builds a Write that sets the named field on a document.
func fsUpdateWrite(name, field, value string) *firestorepb.Write {
	return &firestorepb.Write{
		Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{field: fsString(value)},
		}},
	}
}

// ─── documents ───────────────────────────────────────────────────────────────

func checkFirestoreCreateDocument(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "create-doc"
	name := fsDocName(cfg, coll, id)
	doc, err := fsCreate(ctx, client, cfg, coll, id)
	if err != nil {
		return fmt.Errorf("CreateDocument: %w", err)
	}
	if doc.GetName() != name {
		return fmt.Errorf("CreateDocument name = %q, want %q", doc.GetName(), name)
	}
	if got := doc.GetFields()["name"].GetStringValue(); got != firestoreExtraVal {
		return fmt.Errorf("CreateDocument field name = %q, want %q", got, firestoreExtraVal)
	}
	if doc.GetCreateTime() == nil || doc.GetUpdateTime() == nil {
		return fmt.Errorf("CreateDocument returned no create_time/update_time")
	}
	return nil
}

func checkFirestoreGetDocument(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "create-doc"
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	doc, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: fsDocName(cfg, coll, id)})
	if err != nil {
		return fmt.Errorf("GetDocument: %w", err)
	}
	if got := doc.GetFields()["name"].GetStringValue(); got != firestoreExtraVal {
		return fmt.Errorf("GetDocument field name = %q, want %q", got, firestoreExtraVal)
	}
	return nil
}

func checkFirestoreUpdateDocument(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "update-doc"
	name := fsDocName(cfg, coll, id)
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	const updated = "updated-by-mask"
	doc, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"name": fsString(updated)},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"name"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateDocument: %w", err)
	}
	if got := doc.GetFields()["name"].GetStringValue(); got != updated {
		return fmt.Errorf("UpdateDocument name = %q, want %q", got, updated)
	}
	if got := doc.GetFields()["keep"].GetStringValue(); got != "keep" {
		return fmt.Errorf("UpdateDocument clobbered unmasked field keep = %q, want keep", got)
	}
	return nil
}

func checkFirestoreListDocuments(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "create-doc"
	want := fsDocName(cfg, coll, id)
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	resp, err := client.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent: fsDocuments(cfg), CollectionId: coll,
	})
	if err != nil {
		return fmt.Errorf("ListDocuments: %w", err)
	}
	for _, doc := range resp.GetDocuments() {
		if doc.GetName() == want {
			return nil
		}
	}
	return fmt.Errorf("ListDocuments did not include %q", want)
}

func checkFirestoreListCollectionIds(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll := fsCollection(cfg)
	if _, err := fsCreate(ctx, client, cfg, coll, "create-doc"); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	resp, err := client.ListCollectionIds(ctx, &firestorepb.ListCollectionIdsRequest{Parent: fsDocuments(cfg)})
	if err != nil {
		return fmt.Errorf("ListCollectionIds: %w", err)
	}
	for _, id := range resp.GetCollectionIds() {
		if id == coll {
			return nil
		}
	}
	return fmt.Errorf("ListCollectionIds did not include %q (got %v)", coll, resp.GetCollectionIds())
}

// ─── queries ─────────────────────────────────────────────────────────────────

func checkFirestoreRunQuery(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "create-doc"
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	stream, err := client.RunQuery(ctx, &firestorepb.RunQueryRequest{
		Parent: fsDocuments(cfg),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
				Where: &firestorepb.StructuredQuery_Filter{
					FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
						FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
							Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "name"},
							Op:    firestorepb.StructuredQuery_FieldFilter_EQUAL,
							Value: fsString(firestoreExtraVal),
						},
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("RunQuery: %w", err)
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("RunQuery recv: %w", err)
		}
		if resp.GetDone() {
			return nil
		}
		if got := resp.GetDocument().GetFields()["name"].GetStringValue(); got == firestoreExtraVal {
			return nil
		}
	}
	return fmt.Errorf("RunQuery did not return a document with name=%q", firestoreExtraVal)
}

func checkFirestoreRunAggregationQuery(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll := fsCollection(cfg)
	if _, err := fsCreate(ctx, client, cfg, coll, "create-doc"); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	stream, err := client.RunAggregationQuery(ctx, &firestorepb.RunAggregationQueryRequest{
		Parent: fsDocuments(cfg),
		QueryType: &firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery{
			StructuredAggregationQuery: &firestorepb.StructuredAggregationQuery{
				QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
					},
				},
				Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{{
					Alias: "total",
					Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
						Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
					},
				}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("RunAggregationQuery: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("RunAggregationQuery recv: %w", err)
	}
	count := resp.GetResult().GetAggregateFields()["total"].GetIntegerValue()
	if count < 1 {
		return fmt.Errorf("RunAggregationQuery COUNT = %d, want >= 1", count)
	}
	if resp.GetReadTime() == nil {
		return fmt.Errorf("RunAggregationQuery returned no read_time")
	}
	return nil
}

func checkFirestorePartitionQuery(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll := fsCollection(cfg) + "-part"
	ids := []string{"p0", "p1", "p2", "p3", "p4", "p5"}
	for _, id := range ids {
		if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
			return fmt.Errorf("fixture CreateDocument %s: %w", id, err)
		}
	}
	resp, err := client.PartitionQuery(ctx, &firestorepb.PartitionQueryRequest{
		Parent: fsDocuments(cfg),
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll, AllDescendants: true}},
				OrderBy: []*firestorepb.StructuredQuery_Order{{
					Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "__name__"},
					Direction: firestorepb.StructuredQuery_ASCENDING,
				}},
			},
		},
		PartitionCount: 3,
	})
	if err != nil {
		return fmt.Errorf("PartitionQuery: %w", err)
	}
	if len(resp.GetPartitions()) == 0 {
		return fmt.Errorf("PartitionQuery returned no partitions for %d documents", len(ids))
	}
	for _, part := range resp.GetPartitions() {
		if len(part.GetValues()) == 0 || part.GetValues()[0].GetReferenceValue() == "" {
			return fmt.Errorf("PartitionQuery partition cursor = %v, want a reference value", part.GetValues())
		}
	}
	for _, id := range ids {
		fsDelete(ctx, client, fsDocName(cfg, coll, id))
	}
	return nil
}

func checkFirestoreExecutePipeline(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "create-doc"
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	stream, err := client.ExecutePipeline(ctx, &firestorepb.ExecutePipelineRequest{
		Database: fsDatabase(cfg),
		PipelineType: &firestorepb.ExecutePipelineRequest_StructuredPipeline{
			StructuredPipeline: &firestorepb.StructuredPipeline{
				Pipeline: &firestorepb.Pipeline{Stages: []*firestorepb.Pipeline_Stage{
					{Name: "collection", Args: []*firestorepb.Value{fsReference("/" + coll)}},
					{Name: "limit", Args: []*firestorepb.Value{fsInt(1)}},
				}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("ExecutePipeline: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("ExecutePipeline recv: %w", err)
	}
	if resp.GetExecutionTime() == nil {
		return fmt.Errorf("ExecutePipeline returned no execution_time")
	}
	if len(resp.GetResults()) != 1 {
		return fmt.Errorf("ExecutePipeline limit(1) returned %d results, want 1", len(resp.GetResults()))
	}
	if got := resp.GetResults()[0].GetName(); !strings.HasPrefix(got, fsDocuments(cfg)+"/") {
		return fmt.Errorf("ExecutePipeline result name = %q, want a document in %s", got, fsDocuments(cfg))
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("ExecutePipeline trailing recv = %v, want EOF", err)
	}
	return nil
}

// ─── writes / transactions ───────────────────────────────────────────────────

func checkFirestoreBatchWrite(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll := fsCollection(cfg)
	a, b := fsDocName(cfg, coll, "batch-a"), fsDocName(cfg, coll, "batch-b")
	resp, err := client.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: fsDatabase(cfg),
		Writes: []*firestorepb.Write{
			fsUpdateWrite(a, "name", "batch-a-value"),
			fsUpdateWrite(b, "name", "batch-b-value"),
		},
	})
	if err != nil {
		return fmt.Errorf("BatchWrite: %w", err)
	}
	if len(resp.GetWriteResults()) != 2 {
		return fmt.Errorf("BatchWrite returned %d write_results, want 2", len(resp.GetWriteResults()))
	}
	for i, st := range resp.GetStatus() {
		if st.GetCode() != 0 {
			return fmt.Errorf("BatchWrite status[%d] = code %d (%s), want OK", i, st.GetCode(), st.GetMessage())
		}
	}
	for _, name := range []string{a, b} {
		doc, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
		if err != nil {
			return fmt.Errorf("BatchWrite did not persist %q: %w", name, err)
		}
		if doc.GetFields()["name"].GetStringValue() == "" {
			return fmt.Errorf("BatchWrite document %q has no name field", name)
		}
	}
	fsDelete(ctx, client, a)
	fsDelete(ctx, client, b)
	return nil
}

func checkFirestoreBeginTransaction(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	begin, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: fsDatabase(cfg)})
	if err != nil {
		return fmt.Errorf("BeginTransaction: %w", err)
	}
	if len(begin.GetTransaction()) == 0 {
		return fmt.Errorf("BeginTransaction returned an empty transaction id")
	}
	name := fsDocName(cfg, fsCollection(cfg), "txn-committed")
	resp, err := client.Commit(ctx, &firestorepb.CommitRequest{
		Database:    fsDatabase(cfg),
		Transaction: begin.GetTransaction(),
		Writes:      []*firestorepb.Write{fsUpdateWrite(name, "name", "committed")},
	})
	if err != nil {
		return fmt.Errorf("Commit in transaction: %w", err)
	}
	if len(resp.GetWriteResults()) != 1 || resp.GetCommitTime() == nil {
		return fmt.Errorf("Commit returned %d write_results / commit_time=%v, want 1 / set", len(resp.GetWriteResults()), resp.GetCommitTime())
	}
	doc, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDocument after transactional Commit: %w", err)
	}
	if got := doc.GetFields()["name"].GetStringValue(); got != "committed" {
		return fmt.Errorf("transactional write name = %q, want committed", got)
	}
	fsDelete(ctx, client, name)
	return nil
}

func checkFirestoreRollback(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll := fsCollection(cfg)
	committed := fsDocName(cfg, coll, "txn-committed-ok")
	rolledBack := fsDocName(cfg, coll, "txn-rolled-back")

	// A committed transaction persists its write.
	txn1, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: fsDatabase(cfg)})
	if err != nil {
		return fmt.Errorf("BeginTransaction (commit path): %w", err)
	}
	if _, err := client.Commit(ctx, &firestorepb.CommitRequest{
		Database: fsDatabase(cfg), Transaction: txn1.GetTransaction(),
		Writes: []*firestorepb.Write{fsUpdateWrite(committed, "name", "keep")},
	}); err != nil {
		return fmt.Errorf("Commit committed txn: %w", err)
	}
	if _, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: committed}); err != nil {
		return fmt.Errorf("committed document missing: %w", err)
	}

	// A rolled-back transaction must be unusable and its write must not land.
	txn2, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: fsDatabase(cfg)})
	if err != nil {
		return fmt.Errorf("BeginTransaction (rollback path): %w", err)
	}
	if _, err := client.Rollback(ctx, &firestorepb.RollbackRequest{Database: fsDatabase(cfg), Transaction: txn2.GetTransaction()}); err != nil {
		return fmt.Errorf("Rollback: %w", err)
	}
	if _, err := client.Commit(ctx, &firestorepb.CommitRequest{
		Database: fsDatabase(cfg), Transaction: txn2.GetTransaction(),
		Writes: []*firestorepb.Write{fsUpdateWrite(rolledBack, "name", "must-not-land")},
	}); err == nil {
		return fmt.Errorf("Commit after Rollback succeeded, want an inactive-transaction error")
	}
	if _, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: rolledBack}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetDocument after Rollback = %v, want NotFound", err)
	}
	fsDelete(ctx, client, committed)
	return nil
}

func checkFirestoreWrite(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stream, err := client.Write(wctx)
	if err != nil {
		return fmt.Errorf("Write: %w", err)
	}
	if err := stream.Send(&firestorepb.WriteRequest{Database: fsDatabase(cfg)}); err != nil {
		return fmt.Errorf("Write handshake send: %w", err)
	}
	handshake, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("Write handshake recv: %w", err)
	}
	if handshake.GetStreamId() == "" || len(handshake.GetStreamToken()) == 0 {
		return fmt.Errorf("Write handshake stream_id=%q stream_token=%d bytes, want both set",
			handshake.GetStreamId(), len(handshake.GetStreamToken()))
	}

	name := fsDocName(cfg, fsCollection(cfg), "write-doc")
	if err := stream.Send(&firestorepb.WriteRequest{
		StreamToken: handshake.GetStreamToken(),
		Writes:      []*firestorepb.Write{fsUpdateWrite(name, "name", "streamed")},
	}); err != nil {
		return fmt.Errorf("Write send: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("Write recv: %w", err)
	}
	if len(resp.GetWriteResults()) != 1 {
		return fmt.Errorf("Write returned %d write_results, want 1", len(resp.GetWriteResults()))
	}
	if resp.GetCommitTime() == nil || len(resp.GetStreamToken()) == 0 {
		return fmt.Errorf("Write response missing commit_time/stream_token")
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("Write close send: %w", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("Write trailing recv = %v, want EOF", err)
	}

	doc, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDocument after Write: %w", err)
	}
	if got := doc.GetFields()["name"].GetStringValue(); got != "streamed" {
		return fmt.Errorf("Write document name = %q, want streamed", got)
	}
	fsDelete(ctx, client, name)
	return nil
}

func checkFirestoreListen(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "listen-doc"
	want := fsDocName(cfg, coll, id)
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}

	lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		return fmt.Errorf("Listen: %w", err)
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database: fsDatabase(cfg),
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: &firestorepb.Target{
			TargetId: 1,
			TargetType: &firestorepb.Target_Query{Query: &firestorepb.Target_QueryTarget{
				Parent: fsDocuments(cfg),
				QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
					},
				},
			}},
		}},
	}); err != nil {
		return fmt.Errorf("Listen add_target send: %w", err)
	}

	sawADD, sawChange, sawCURRENT := false, false, false
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("Listen recv: %w", err)
		}
		if tc := resp.GetTargetChange(); tc != nil {
			switch tc.GetTargetChangeType() {
			case firestorepb.TargetChange_ADD:
				sawADD = true
			case firestorepb.TargetChange_CURRENT:
				sawCURRENT = true
			case firestorepb.TargetChange_NO_CHANGE:
				if !sawADD || !sawCURRENT || !sawChange {
					return fmt.Errorf("Listen NO_CHANGE with ADD=%t CURRENT=%t DocumentChange=%t, want all true", sawADD, sawCURRENT, sawChange)
				}
				return nil
			}
		}
		if dc := resp.GetDocumentChange(); dc != nil && dc.GetDocument().GetName() == want {
			sawChange = true
		}
	}
}

// ─── cleanup ─────────────────────────────────────────────────────────────────

func checkFirestoreDeleteDocument(ctx context.Context, cfg Config) error {
	client, conn, err := newFirestoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	coll, id := fsCollection(cfg), "create-doc"
	name := fsDocName(cfg, coll, id)
	if _, err := fsCreate(ctx, client, cfg, coll, id); err != nil {
		return fmt.Errorf("fixture CreateDocument: %w", err)
	}
	if _, err := client.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name}); err != nil {
		return fmt.Errorf("DeleteDocument: %w", err)
	}
	if _, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetDocument after delete = %v, want NotFound", err)
	}
	return nil
}
