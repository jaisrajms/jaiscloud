package firestore

import (
	"context"
	"io"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func intPb(v int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
}

// TestUnaryDocumentCRUD exercises the unary document RPCs end to end through the
// generated client/server, so the transport handlers (and their transcoders) are
// covered without depending on an external integration suite.
func TestUnaryDocumentCRUD(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()
	const db = "projects/test/databases/(default)"

	created, err := client.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       listenParent,
		CollectionId: "things",
		DocumentId:   "d1",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"n": intPb(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	if created.GetName() != listenParent+"/things/d1" {
		t.Fatalf("created name = %q", created.GetName())
	}

	got, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: listenParent + "/things/d1"})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got.GetFields()["n"].GetIntegerValue() != 1 {
		t.Fatalf("GetDocument n = %v", got.GetFields()["n"])
	}

	list, err := client.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent: listenParent, CollectionId: "things",
	})
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(list.GetDocuments()) != 1 {
		t.Fatalf("ListDocuments returned %d documents", len(list.GetDocuments()))
	}

	ids, err := client.ListCollectionIds(ctx, &firestorepb.ListCollectionIdsRequest{Parent: listenParent})
	if err != nil {
		t.Fatalf("ListCollectionIds: %v", err)
	}
	if len(ids.GetCollectionIds()) != 1 || ids.GetCollectionIds()[0] != "things" {
		t.Fatalf("ListCollectionIds = %v", ids.GetCollectionIds())
	}

	stream, err := client.RunQuery(ctx, &firestorepb.RunQueryRequest{
		Parent: listenParent,
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
			From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		}},
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	var results int
	for {
		msg, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("RunQuery Recv: %v", rerr)
		}
		if msg.GetDocument() != nil {
			results++
		}
	}
	if results != 1 {
		t.Fatalf("RunQuery returned %d documents, want 1", results)
	}

	upd, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   listenParent + "/things/d1",
			Fields: map[string]*firestorepb.Value{"n": intPb(2)},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"n"}},
	})
	if err != nil {
		t.Fatalf("UpdateDocument: %v", err)
	}
	if upd.GetFields()["n"].GetIntegerValue() != 2 {
		t.Fatalf("UpdateDocument n = %v", upd.GetFields()["n"])
	}

	if _, err := client.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: listenParent + "/things/d1"}); err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}
	_ = db
}

// TestUnaryUpdateDocumentMaskPresence distinguishes the three update_mask
// states through the generated client/server: a populated mask patches only the
// listed paths, a set-but-empty mask is AIP-134's no-op, and an unset mask
// replaces the whole document. It also checks that `mask` (the response
// projection, "fields to return") never doubles as the write mask.
func TestUnaryUpdateDocumentMaskPresence(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()
	name := listenParent + "/things/mask3"

	_, err := client.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       listenParent,
		CollectionId: "things",
		DocumentId:   "mask3",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"n": intPb(1),
			"m": intPb(2),
		}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	// Populated mask: only the listed path changes; the unmasked stored field
	// survives and an unmasked body field is not written.
	patched, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"n": intPb(9), "z": intPb(3)},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"n"}},
	})
	if err != nil {
		t.Fatalf("UpdateDocument (mask n): %v", err)
	}
	if patched.GetFields()["n"].GetIntegerValue() != 9 {
		t.Fatalf("masked update n = %v", patched.GetFields()["n"])
	}
	if patched.GetFields()["m"].GetIntegerValue() != 2 {
		t.Fatalf("masked update clobbered unmasked field m: %v", patched.GetFields())
	}
	if _, ok := patched.GetFields()["z"]; ok {
		t.Fatalf("masked update wrote an unmasked field: %v", patched.GetFields())
	}

	// Present-but-empty mask: no-op — the body is ignored, the document and its
	// updateTime come back unchanged ...
	stamped := patched.GetUpdateTime().AsTime()
	noop, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"n": intPb(77)},
		},
		UpdateMask: &firestorepb.DocumentMask{},
	})
	if err != nil {
		t.Fatalf("UpdateDocument (empty mask): %v", err)
	}
	if noop.GetFields()["n"].GetIntegerValue() != 9 || noop.GetFields()["m"].GetIntegerValue() != 2 {
		t.Fatalf("empty mask wrote fields: %v", noop.GetFields())
	}
	if noop.GetUpdateTime().AsTime().UnixNano() != stamped.UnixNano() {
		t.Fatalf("empty mask rotated updateTime: %v -> %v", stamped, noop.GetUpdateTime().AsTime())
	}
	got, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got.GetFields()["n"].GetIntegerValue() != 9 || got.GetFields()["m"].GetIntegerValue() != 2 {
		t.Fatalf("stored document changed: %v", got.GetFields())
	}

	// ... and `mask` only projects the reply: the same empty-mask no-op with a
	// response mask of ["m"] hides n from the reply, not from the document.
	projected, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"n": intPb(77)},
		},
		UpdateMask: &firestorepb.DocumentMask{},
		Mask:       &firestorepb.DocumentMask{FieldPaths: []string{"m"}},
	})
	if err != nil {
		t.Fatalf("UpdateDocument (response mask): %v", err)
	}
	if _, ok := projected.GetFields()["n"]; ok {
		t.Fatalf("response mask did not project out n: %v", projected.GetFields())
	}
	if projected.GetFields()["m"].GetIntegerValue() != 2 {
		t.Fatalf("response mask dropped m: %v", projected.GetFields())
	}

	// Unset mask: the body replaces every field.
	replaced, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"n": intPb(7)},
		},
	})
	if err != nil {
		t.Fatalf("UpdateDocument (no mask): %v", err)
	}
	if len(replaced.GetFields()) != 1 || replaced.GetFields()["n"].GetIntegerValue() != 7 {
		t.Fatalf("no-mask update did not replace: %v", replaced.GetFields())
	}

	// An empty mask on a missing document still upserts (UpdateDocument creates
	// when absent), with no fields: the mask discards the body.
	missing := listenParent + "/things/mask3-missing"
	createdEmpty, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   missing,
			Fields: map[string]*firestorepb.Value{"n": intPb(5)},
		},
		UpdateMask: &firestorepb.DocumentMask{},
	})
	if err != nil {
		t.Fatalf("UpdateDocument (empty mask, missing doc): %v", err)
	}
	if len(createdEmpty.GetFields()) != 0 {
		t.Fatalf("empty-mask create carries fields: %v", createdEmpty.GetFields())
	}
	if _, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: missing}); err != nil {
		t.Fatalf("empty-mask upsert did not create the document: %v", err)
	}
}

// TestUnaryTransactionsAndBatches exercises the transaction and non-atomic batch
// RPCs through the client.
func TestUnaryTransactionsAndBatches(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()
	const db = "projects/test/databases/(default)"

	// Commit a write inside a started transaction.
	begin, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: db})
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	commit, err := client.Commit(ctx, &firestorepb.CommitRequest{
		Database:    db,
		Transaction: begin.GetTransaction(),
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
				Name:   listenParent + "/things/t1",
				Fields: map[string]*firestorepb.Value{"n": intPb(9)},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(commit.GetWriteResults()) != 1 {
		t.Fatalf("Commit returned %d write results", len(commit.GetWriteResults()))
	}

	// Begin again, then roll it back.
	begin2, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: db})
	if err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if _, err := client.Rollback(ctx, &firestorepb.RollbackRequest{Database: db, Transaction: begin2.GetTransaction()}); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// BatchGet the committed document.
	bg, err := client.BatchGetDocuments(ctx, &firestorepb.BatchGetDocumentsRequest{
		Database:  db,
		Documents: []string{listenParent + "/things/t1"},
	})
	if err != nil {
		t.Fatalf("BatchGetDocuments: %v", err)
	}
	var found int
	for {
		msg, rerr := bg.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("BatchGetDocuments Recv: %v", rerr)
		}
		if msg.GetFound() != nil {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("BatchGetDocuments found %d documents, want 1", found)
	}

	// Non-atomic BatchWrite: one delete.
	bw, err := client.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: db,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Delete{Delete: listenParent + "/things/t1"},
		}},
	})
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}
	if len(bw.GetStatus()) != 1 || bw.GetStatus()[0].GetCode() != 0 {
		t.Fatalf("BatchWrite status = %v", bw.GetStatus())
	}
}
