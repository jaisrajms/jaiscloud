package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const fsDB = "projects/test-project/databases/(default)"
const fsParent = fsDB + "/documents"

var fsClient firestorepb.FirestoreClient

func initFirestore() {
	conn, err := grpc.NewClient(grpcEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	fsClient = firestorepb.NewFirestoreClient(conn)
}

// cname appends the run suffix so each run uses fresh collection names.
func cname(n string) string { return n + suffix }

func strVal(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}
func intVal(n int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: n}}
}
func arrayVal(strs ...string) *firestorepb.Value {
	var vals []*firestorepb.Value
	for _, s := range strs {
		vals = append(vals, strVal(s))
	}
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: vals}}}
}

func fsCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}

func runFirestore() {
	initFirestore()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"TestCreateAndGetDocument", fsCreateAndGetDocument},
		{"TestCreateDocumentAutoID", fsCreateDocumentAutoID},
		{"TestUpdateDocument", fsUpdateDocument},
		{"TestDeleteDocument", fsDeleteDocument},
		{"TestGetNonexistent", fsGetNonexistent},
		{"TestDeleteNonexistent", fsDeleteNonexistent},
		{"TestListDocuments", fsListDocuments},
		{"TestRunQueryEquality", fsRunQueryEquality},
		{"TestRunQueryRangeFilter", fsRunQueryRangeFilter},
		{"TestRunQueryOrderByAndLimit", fsRunQueryOrderByAndLimit},
		{"TestBeginTransactionAndCommit", fsBeginTransactionAndCommit},
		{"TestRollback", fsRollback},
		{"TestNestedCollections", fsNestedCollections},
		{"TestCommitDeleteWrite", fsCommitDeleteWrite},
		{"TestCreateDuplicate", fsCreateDuplicate},
		{"TestQueryIN", fsQueryIN},
		{"TestQueryNOT_IN", fsQueryNOT_IN},
		{"TestQueryARRAY_CONTAINS", fsQueryARRAY_CONTAINS},
		{"TestQueryARRAY_CONTAINS_ANY", fsQueryARRAY_CONTAINS_ANY},
		{"TestQueryARRAY_CONTAINS_NoMatch", fsQueryARRAY_CONTAINS_NoMatch},
		{"TestQueryIN_Empty", fsQueryIN_Empty},
		{"TestQueryARRAY_CONTAINS_NonArrayField", fsQueryARRAY_CONTAINS_NonArrayField},
		{"TestQueryARRAY_CONTAINS_ANY_NoOverlap", fsQueryARRAY_CONTAINS_ANY_NoOverlap},
		{"TestListenCollectionInitialSnapshot", fsListenCollectionInitialSnapshot},
		{"TestListenDocumentTarget", fsListenDocumentTarget},
		{"TestListenRemoveTarget", fsListenRemoveTarget},
		{"TestListenResumeTokenFallsBackToSnapshot", fsListenResumeTokenFallsBackToSnapshot},
		{"TestListenCollectionRealTimeUpdates", fsListenCollectionRealTimeUpdates},
		{"TestListenIgnoresUnrelatedCollections", fsListenIgnoresUnrelatedCollections},
		{"TestListenMultipleClients", fsListenMultipleClients},
		{"TestListenResumeTokenBasic", fsListenResumeTokenBasic},
	}
	for _, c := range cases {
		record("firestore", c.name, c.fn())
	}

	recordNA("firestore", "TestResumeTokenEncodeDecode", "internal Go function (Encode/DecodeResumeToken); not an external gRPC surface")
	recordNA("firestore", "store_test.go (15 cases)", "in-memory Store unit tests (NewStore) — not exposed over gRPC/REST; semantics re-covered at service layer")
}

// --- service_test.go cases ---

func fsCreateAndGetDocument() error {
	ctx, cancel := fsCtx()
	defer cancel()
	created, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent,
		CollectionId: cname("users"),
		DocumentId:   "alice",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"name": strVal("Alice"), "age": intVal(30)}},
	})
	if err != nil {
		return fmt.Errorf("CreateDocument: %v", err)
	}
	wantName := fsParent + "/" + cname("users") + "/alice"
	if created.Name != wantName {
		return fmt.Errorf("name = %q, want %q", created.Name, wantName)
	}
	if created.Fields["name"].GetStringValue() != "Alice" {
		return fmt.Errorf("name field = %q, want Alice", created.Fields["name"].GetStringValue())
	}
	got, err := fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: wantName})
	if err != nil {
		return fmt.Errorf("GetDocument: %v", err)
	}
	if got.Fields["age"].GetIntegerValue() != 30 {
		return fmt.Errorf("age = %d, want 30", got.Fields["age"].GetIntegerValue())
	}
	return nil
}

func fsCreateDocumentAutoID() error {
	ctx, cancel := fsCtx()
	defer cancel()
	created, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent,
		CollectionId: cname("auto-items"),
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"title": strVal("Widget")}},
	})
	if err != nil {
		return fmt.Errorf("CreateDocument: %v", err)
	}
	if created.Name == "" {
		return fmt.Errorf("expected auto-generated name")
	}
	if created.Fields["title"].GetStringValue() != "Widget" {
		return fmt.Errorf("field not preserved")
	}
	return nil
}

func fsUpdateDocument() error {
	ctx, cancel := fsCtx()
	defer cancel()
	name := fsParent + "/" + cname("users") + "/bob"
	_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent,
		CollectionId: cname("users"),
		DocumentId:   "bob",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"name": strVal("Bob"), "age": intVal(25), "email": strVal("bob@example.com")}},
	})
	if err != nil {
		return err
	}
	updated, err := fsClient.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document:   &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"age": intVal(26)}},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"age"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateDocument: %v", err)
	}
	if updated.Fields["age"].GetIntegerValue() != 26 {
		return fmt.Errorf("age = %d, want 26", updated.Fields["age"].GetIntegerValue())
	}
	if updated.Fields["name"].GetStringValue() != "Bob" {
		return fmt.Errorf("name was modified unexpectedly")
	}
	if updated.Fields["email"].GetStringValue() != "bob@example.com" {
		return fmt.Errorf("email was modified unexpectedly")
	}
	return nil
}

func fsDeleteDocument() error {
	ctx, cancel := fsCtx()
	defer cancel()
	name := fsParent + "/" + cname("users") + "/charlie"
	_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("users"), DocumentId: "charlie",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"name": strVal("Charlie")}},
	})
	if err != nil {
		return err
	}
	if _, err := fsClient.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name}); err != nil {
		return fmt.Errorf("DeleteDocument: %v", err)
	}
	_, err = fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("after delete: got %v, want NotFound", err)
	}
	return nil
}

func fsGetNonexistent() error {
	ctx, cancel := fsCtx()
	defer cancel()
	_, err := fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: fsParent + "/" + cname("users") + "/nonexistent"})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("got %v, want NotFound", err)
	}
	return nil
}

func fsDeleteNonexistent() error {
	ctx, cancel := fsCtx()
	defer cancel()
	_, err := fsClient.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: fsParent + "/" + cname("users") + "/nonexistent"})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("got %v, want NotFound", err)
	}
	return nil
}

func fsListDocuments() error {
	ctx, cancel := fsCtx()
	defer cancel()
	for _, id := range []string{"d1", "d2", "d3"} {
		_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       fsParent, CollectionId: cname("list-items"), DocumentId: id,
			Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"id": strVal(id)}},
		})
		if err != nil {
			return err
		}
	}
	resp, err := fsClient.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{Parent: fsParent, CollectionId: cname("list-items")})
	if err != nil {
		return fmt.Errorf("ListDocuments: %v", err)
	}
	if len(resp.Documents) != 3 {
		return fmt.Errorf("got %d documents, want 3", len(resp.Documents))
	}
	return nil
}

func fsQueryStream(ctx context.Context, coll string, where *firestorepb.StructuredQuery_Filter, orderBy []*firestorepb.StructuredQuery_Order, limit int32) ([]*firestorepb.Document, error) {
	q := &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
		Where:   where,
		OrderBy: orderBy,
	}
	if limit > 0 {
		q.Limit = &wrapperspb.Int32Value{Value: limit}
	}
	stream, err := fsClient.RunQuery(ctx, &firestorepb.RunQueryRequest{
		Parent:    fsParent,
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: q},
	})
	if err != nil {
		return nil, err
	}
	var docs []*firestorepb.Document
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if resp.Document != nil {
			docs = append(docs, resp.Document)
		}
	}
	return docs, nil
}

func fsRunQueryEquality() error {
	ctx, cancel := fsCtx()
	defer cancel()
	for _, u := range []struct{ id, city string }{{"u1", "NYC"}, {"u2", "LA"}, {"u3", "NYC"}} {
		_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       fsParent, CollectionId: cname("people"), DocumentId: u.id,
			Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"city": strVal(u.city)}},
		})
		if err != nil {
			return err
		}
	}
	f := fieldFilter("city", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("NYC"))
	docs, err := fsQueryStream(ctx, cname("people"), f, nil, 0)
	if err != nil {
		return err
	}
	if len(docs) != 2 {
		return fmt.Errorf("got %d results, want 2", len(docs))
	}
	return nil
}

func fsRunQueryRangeFilter() error {
	ctx, cancel := fsCtx()
	defer cancel()
	for i := int64(1); i <= 5; i++ {
		_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       fsParent, CollectionId: cname("scores"), DocumentId: fmt.Sprintf("s%d", i),
			Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"score": intVal(i * 10)}},
		})
		if err != nil {
			return err
		}
	}
	f := fieldFilter("score", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL, intVal(30))
	docs, err := fsQueryStream(ctx, cname("scores"), f, nil, 0)
	if err != nil {
		return err
	}
	if len(docs) != 3 {
		return fmt.Errorf("got %d results, want 3 (30,40,50)", len(docs))
	}
	return nil
}

func fsRunQueryOrderByAndLimit() error {
	ctx, cancel := fsCtx()
	defer cancel()
	for _, item := range []struct {
		id    string
		price int64
	}{{"p1", 100}, {"p2", 50}, {"p3", 200}, {"p4", 75}} {
		_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       fsParent, CollectionId: cname("products"), DocumentId: item.id,
			Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"price": intVal(item.price)}},
		})
		if err != nil {
			return err
		}
	}
	order := []*firestorepb.StructuredQuery_Order{{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "price"},
		Direction: firestorepb.StructuredQuery_ASCENDING,
	}}
	docs, err := fsQueryStream(ctx, cname("products"), nil, order, 2)
	if err != nil {
		return err
	}
	if len(docs) != 2 {
		return fmt.Errorf("got %d results, want 2", len(docs))
	}
	if docs[0].Fields["price"].GetIntegerValue() != 50 {
		return fmt.Errorf("first price = %d, want 50", docs[0].Fields["price"].GetIntegerValue())
	}
	if docs[1].Fields["price"].GetIntegerValue() != 75 {
		return fmt.Errorf("second price = %d, want 75", docs[1].Fields["price"].GetIntegerValue())
	}
	return nil
}

func fsBeginTransactionAndCommit() error {
	ctx, cancel := fsCtx()
	defer cancel()
	txResp, err := fsClient.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: fsDB})
	if err != nil {
		return fmt.Errorf("BeginTransaction: %v", err)
	}
	if len(txResp.Transaction) == 0 {
		return fmt.Errorf("empty transaction ID")
	}
	docName := fsParent + "/" + cname("tx-items") + "/item1"
	commitResp, err := fsClient.Commit(ctx, &firestorepb.CommitRequest{
		Database: fsDB, Transaction: txResp.Transaction,
		Writes: []*firestorepb.Write{{Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{Name: docName, Fields: map[string]*firestorepb.Value{"status": strVal("created")}},
		}}},
	})
	if err != nil {
		return fmt.Errorf("Commit: %v", err)
	}
	if len(commitResp.WriteResults) != 1 {
		return fmt.Errorf("got %d write results, want 1", len(commitResp.WriteResults))
	}
	if commitResp.CommitTime == nil {
		return fmt.Errorf("missing commit time")
	}
	got, err := fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName})
	if err != nil {
		return err
	}
	if got.Fields["status"].GetStringValue() != "created" {
		return fmt.Errorf("document not created by commit")
	}
	return nil
}

func fsRollback() error {
	ctx, cancel := fsCtx()
	defer cancel()
	txResp, err := fsClient.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: fsDB})
	if err != nil {
		return err
	}
	if _, err := fsClient.Rollback(ctx, &firestorepb.RollbackRequest{Database: fsDB, Transaction: txResp.Transaction}); err != nil {
		return fmt.Errorf("Rollback: %v", err)
	}
	_, err = fsClient.Commit(ctx, &firestorepb.CommitRequest{Database: fsDB, Transaction: txResp.Transaction})
	if err == nil {
		return fmt.Errorf("expected error committing rolled-back transaction")
	}
	return nil
}

func fsNestedCollections() error {
	ctx, cancel := fsCtx()
	defer cancel()
	_, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("chats"), DocumentId: "chat1",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"title": strVal("General")}},
	})
	if err != nil {
		return err
	}
	nestedParent := fsParent + "/" + cname("chats") + "/chat1"
	_, err = fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       nestedParent, CollectionId: "messages", DocumentId: "msg1",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"text": strVal("Hello, world!")}},
	})
	if err != nil {
		return fmt.Errorf("nested create: %v", err)
	}
	nestedName := nestedParent + "/messages/msg1"
	got, err := fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: nestedName})
	if err != nil {
		return err
	}
	if got.Fields["text"].GetStringValue() != "Hello, world!" {
		return fmt.Errorf("nested field mismatch")
	}
	listResp, err := fsClient.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{Parent: nestedParent, CollectionId: "messages"})
	if err != nil {
		return err
	}
	if len(listResp.Documents) != 1 {
		return fmt.Errorf("got %d nested docs, want 1", len(listResp.Documents))
	}
	return nil
}

func fsCommitDeleteWrite() error {
	ctx, cancel := fsCtx()
	defer cancel()
	docName := fsParent + "/" + cname("del-test") + "/doc1"
	_, err := fsClient.Commit(ctx, &firestorepb.CommitRequest{
		Database: fsDB,
		Writes: []*firestorepb.Write{{Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{Name: docName, Fields: map[string]*firestorepb.Value{"v": intVal(1)}},
		}}},
	})
	if err != nil {
		return err
	}
	if _, err := fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName}); err != nil {
		return err
	}
	_, err = fsClient.Commit(ctx, &firestorepb.CommitRequest{
		Database: fsDB,
		Writes:   []*firestorepb.Write{{Operation: &firestorepb.Write_Delete{Delete: docName}}},
	})
	if err != nil {
		return err
	}
	_, err = fsClient.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("got %v, want NotFound", err)
	}
	return nil
}

func fsCreateDuplicate() error {
	ctx, cancel := fsCtx()
	defer cancel()
	req := &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("dupes"), DocumentId: "same",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
	}
	if _, err := fsClient.CreateDocument(ctx, req); err != nil {
		return fmt.Errorf("first create: %v", err)
	}
	_, err := fsClient.CreateDocument(ctx, req)
	if status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("second create: got %v, want AlreadyExists", err)
	}
	return nil
}

// --- query_test.go cases ---

func fieldFilter(field string, op firestorepb.StructuredQuery_FieldFilter_Operator, val *firestorepb.Value) *firestorepb.StructuredQuery_Filter {
	return &firestorepb.StructuredQuery_Filter{
		FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
			FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
				Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: field},
				Op:    op,
				Value: val,
			},
		},
	}
}

func seedAndCount(ctx context.Context, coll string, f *firestorepb.StructuredQuery_Filter) (int, error) {
	docs := []struct {
		id     string
		fields map[string]*firestorepb.Value
	}{
		{"doc1", map[string]*firestorepb.Value{"status": strVal("active"), "tags": arrayVal("go", "grpc"), "score": intVal(10)}},
		{"doc2", map[string]*firestorepb.Value{"status": strVal("pending"), "tags": arrayVal("python", "rest"), "score": intVal(20)}},
		{"doc3", map[string]*firestorepb.Value{"status": strVal("archived"), "tags": arrayVal("go", "rest"), "score": intVal(30)}},
		{"doc4", map[string]*firestorepb.Value{"status": strVal("active"), "tags": arrayVal("java"), "score": intVal(40)}},
	}
	for _, d := range docs {
		if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       fsParent, CollectionId: coll, DocumentId: d.id,
			Document: &firestorepb.Document{Fields: d.fields},
		}); err != nil {
			return 0, err
		}
	}
	res, err := fsQueryStream(ctx, coll, f, nil, 0)
	if err != nil {
		return 0, err
	}
	return len(res), nil
}

func queryCase(coll string, f *firestorepb.StructuredQuery_Filter, want int) error {
	ctx, cancel := fsCtx()
	defer cancel()
	n, err := seedAndCount(ctx, cname(coll), f)
	if err != nil {
		return err
	}
	if n != want {
		return fmt.Errorf("got %d results, want %d", n, want)
	}
	return nil
}

func fsQueryIN() error {
	return queryCase("q-in", fieldFilter("status", firestorepb.StructuredQuery_FieldFilter_IN, arrayVal("active", "pending")), 3)
}
func fsQueryNOT_IN() error {
	return queryCase("q-notin", fieldFilter("status", firestorepb.StructuredQuery_FieldFilter_NOT_IN, arrayVal("active", "pending")), 1)
}
func fsQueryARRAY_CONTAINS() error {
	return queryCase("q-ac", fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, strVal("go")), 2)
}
func fsQueryARRAY_CONTAINS_ANY() error {
	return queryCase("q-aca", fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY, arrayVal("python", "java")), 2)
}
func fsQueryARRAY_CONTAINS_NoMatch() error {
	return queryCase("q-acnm", fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, strVal("rust")), 0)
}
func fsQueryIN_Empty() error {
	return queryCase("q-inempty", fieldFilter("status", firestorepb.StructuredQuery_FieldFilter_IN, arrayVal()), 0)
}
func fsQueryARRAY_CONTAINS_NonArrayField() error {
	return queryCase("q-acna", fieldFilter("score", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, strVal("go")), 0)
}
func fsQueryARRAY_CONTAINS_ANY_NoOverlap() error {
	return queryCase("q-acano", fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY, arrayVal("rust", "c++")), 0)
}

// --- listen_test.go cases (gRPC-adapted) ---

func listenTargetQuery(coll string, tid int32, resume []byte) *firestorepb.Target {
	t := &firestorepb.Target{
		TargetId: tid,
		TargetType: &firestorepb.Target_Query{
			Query: &firestorepb.Target_QueryTarget{
				Parent: fsParent,
				QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
					},
				},
			},
		},
	}
	if resume != nil {
		t.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: resume}
	}
	return t
}

func recvN(stream firestorepb.Firestore_ListenClient, n int, timeout time.Duration) ([]*firestorepb.ListenResponse, error) {
	var out []*firestorepb.ListenResponse
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		type r struct {
			resp *firestorepb.ListenResponse
			err  error
		}
		ch := make(chan r, 1)
		go func() {
			resp, err := stream.Recv()
			ch <- r{resp, err}
		}()
		select {
		case x := <-ch:
			if x.err != nil {
				return out, fmt.Errorf("Recv %d/%d: %v", i+1, n, x.err)
			}
			out = append(out, x.resp)
		case <-deadline:
			return out, fmt.Errorf("timeout waiting for response %d/%d", i+1, n)
		}
	}
	return out, nil
}

func recvWithTimeout(stream firestorepb.Firestore_ListenClient, timeout time.Duration) (*firestorepb.ListenResponse, error) {
	type r struct {
		resp *firestorepb.ListenResponse
		err  error
	}
	ch := make(chan r, 1)
	go func() {
		resp, err := stream.Recv()
		ch <- r{resp, err}
	}()
	select {
	case x := <-ch:
		return x.resp, x.err
	case <-time.After(timeout):
		return nil, io.ErrUnexpectedEOF
	}
}

func fsListenCollectionInitialSnapshot() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, n := range []string{"alice", "bob"} {
		if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       fsParent, CollectionId: cname("listen-users"), DocumentId: n,
			Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"name": strVal(n)}},
		}); err != nil {
			return err
		}
	}
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return fmt.Errorf("Listen: %v", err)
	}
	err = stream.Send(&firestorepb.ListenRequest{
		Database: fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: listenTargetQuery(cname("listen-users"), 1, nil),
		},
	})
	if err != nil {
		return err
	}
	responses, err := recvN(stream, 4, 5*time.Second)
	if err != nil {
		return err
	}
	if tc := responses[0].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_ADD {
		return fmt.Errorf("expected ADD, got %v", responses[0])
	}
	dc1 := responses[1].GetDocumentChange()
	dc2 := responses[2].GetDocumentChange()
	if dc1 == nil || dc2 == nil {
		return fmt.Errorf("expected DocumentChanges")
	}
	names := []string{dc1.Document.Fields["name"].GetStringValue(), dc2.Document.Fields["name"].GetStringValue()}
	sort.Strings(names)
	if names[0] != "alice" || names[1] != "bob" {
		return fmt.Errorf("expected [alice bob], got %v", names)
	}
	if tc := responses[3].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		return fmt.Errorf("expected CURRENT, got %v", responses[3])
	}
	return nil
}

func fsListenDocumentTarget() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	docPath := fsParent + "/" + cname("listen-config") + "/settings"
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("listen-config"), DocumentId: "settings",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"theme": strVal("dark")}},
	}); err != nil {
		return err
	}
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return err
	}
	err = stream.Send(&firestorepb.ListenRequest{
		Database: fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: 42,
				TargetType: &firestorepb.Target_Documents{
					Documents: &firestorepb.Target_DocumentsTarget{Documents: []string{docPath}},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	responses, err := recvN(stream, 3, 5*time.Second)
	if err != nil {
		return err
	}
	if tc := responses[0].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_ADD {
		return fmt.Errorf("expected ADD")
	}
	dc := responses[1].GetDocumentChange()
	if dc == nil || dc.Document.Fields["theme"].GetStringValue() != "dark" {
		return fmt.Errorf("expected theme=dark")
	}
	if tc := responses[2].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		return fmt.Errorf("expected CURRENT")
	}
	return nil
}

func fsListenRemoveTarget() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("listen-col"), 1, nil)},
	}); err != nil {
		return err
	}
	if _, err := recvN(stream, 2, 5*time.Second); err != nil {
		return err
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: 1},
	}); err != nil {
		return err
	}
	responses, err := recvN(stream, 1, 5*time.Second)
	if err != nil {
		return err
	}
	tc := responses[0].GetTargetChange()
	if tc == nil || tc.TargetChangeType != firestorepb.TargetChange_REMOVE {
		return fmt.Errorf("expected REMOVE, got %v", responses[0])
	}
	return nil
}

func fsListenResumeTokenFallsBackToSnapshot() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("listen-items"), DocumentId: "a",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"v": strVal("1")}},
	}); err != nil {
		return err
	}
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return err
	}
	err = stream.Send(&firestorepb.ListenRequest{
		Database: fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: listenTargetQuery(cname("listen-items"), 1, []byte("bad")),
		},
	})
	if err != nil {
		return err
	}
	responses, err := recvN(stream, 4, 5*time.Second)
	if err != nil {
		return err
	}
	if tc := responses[0].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_ADD {
		return fmt.Errorf("expected ADD")
	}
	// An un-resumable token resets the target before the fresh snapshot (real
	// Firestore emits TargetChange(RESET)).
	if tc := responses[1].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_RESET {
		return fmt.Errorf("expected RESET before re-snapshot, got %v", responses[1])
	}
	dc := responses[2].GetDocumentChange()
	if dc == nil || dc.Document.Fields["v"].GetStringValue() != "1" {
		return fmt.Errorf("expected full snapshot with item a")
	}
	if tc := responses[3].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		return fmt.Errorf("expected CURRENT")
	}
	return nil
}

func fsListenCollectionRealTimeUpdates() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("rt-items"), 1, nil)},
	}); err != nil {
		return err
	}
	responses, err := recvN(stream, 2, 5*time.Second)
	if err != nil {
		return err
	}
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		return fmt.Errorf("expected ADD")
	}
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		return fmt.Errorf("expected CURRENT")
	}
	docPath := fsParent + "/" + cname("rt-items") + "/item1"
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("rt-items"), DocumentId: "item1",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"title": strVal("first item")}},
	}); err != nil {
		return err
	}
	responses, err = recvN(stream, 1, 5*time.Second)
	if err != nil {
		return err
	}
	if dc := responses[0].GetDocumentChange(); dc == nil || dc.Document.Fields["title"].GetStringValue() != "first item" {
		return fmt.Errorf("expected DocumentChange first item, got %v", responses[0])
	}
	if _, err := fsClient.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document:   &firestorepb.Document{Name: docPath, Fields: map[string]*firestorepb.Value{"title": strVal("updated item")}},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"title"}},
	}); err != nil {
		return err
	}
	responses, err = recvN(stream, 1, 5*time.Second)
	if err != nil {
		return err
	}
	if dc := responses[0].GetDocumentChange(); dc == nil || dc.Document.Fields["title"].GetStringValue() != "updated item" {
		return fmt.Errorf("expected MODIFIED updated item, got %v", responses[0])
	}
	if _, err := fsClient.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: docPath}); err != nil {
		return err
	}
	responses, err = recvN(stream, 1, 5*time.Second)
	if err != nil {
		return err
	}
	if dd := responses[0].GetDocumentDelete(); dd == nil || dd.Document != docPath {
		return fmt.Errorf("expected DocumentDelete %s, got %v", docPath, responses[0])
	}
	return nil
}

func fsListenIgnoresUnrelatedCollections() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("lt-users"), 1, nil)},
	}); err != nil {
		return err
	}
	if _, err := recvN(stream, 2, 5*time.Second); err != nil {
		return err
	}
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("lt-posts"), DocumentId: "post1",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"title": strVal("hello")}},
	}); err != nil {
		return err
	}
	resp, err := recvWithTimeout(stream, 500*time.Millisecond)
	if err == nil {
		return fmt.Errorf("expected no event for unrelated collection, got %v", resp)
	}
	return nil
}

func fsListenMultipleClients() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream1, _ := fsClient.Listen(ctx)
	stream2, _ := fsClient.Listen(ctx)
	stream1.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("lt-shared"), 1, nil)},
	})
	stream2.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("lt-shared"), 2, nil)},
	})
	if _, err := recvN(stream1, 2, 5*time.Second); err != nil {
		return err
	}
	if _, err := recvN(stream2, 2, 5*time.Second); err != nil {
		return err
	}
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("lt-shared"), DocumentId: "doc1",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"v": strVal("hello")}},
	}); err != nil {
		return err
	}
	for _, s := range []firestorepb.Firestore_ListenClient{stream1, stream2} {
		r, err := recvN(s, 1, 5*time.Second)
		if err != nil {
			return err
		}
		if r[0].GetDocumentChange() == nil {
			return fmt.Errorf("expected DocumentChange on stream")
		}
	}
	return nil
}

func fsListenResumeTokenBasic() error {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("lt-tasks"), DocumentId: "t1",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"status": strVal("open")}},
	}); err != nil {
		return err
	}
	stream, err := fsClient.Listen(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("lt-tasks"), 1, nil)},
	}); err != nil {
		return err
	}
	responses, err := recvN(stream, 3, 5*time.Second)
	if err != nil {
		return err
	}
	tc := responses[2].GetTargetChange()
	if tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		return fmt.Errorf("expected CURRENT")
	}
	if len(tc.ResumeToken) == 0 {
		return fmt.Errorf("expected non-empty resume token")
	}
	resumeToken := tc.ResumeToken
	if _, err := fsClient.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       fsParent, CollectionId: cname("lt-tasks"), DocumentId: "t2",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"status": strVal("pending")}},
	}); err != nil {
		return err
	}
	if _, err := recvN(stream, 1, 5*time.Second); err != nil {
		return err
	}
	stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: 1},
	})
	if _, err := recvN(stream, 1, 5*time.Second); err != nil {
		return err
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     fsDB,
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: listenTargetQuery(cname("lt-tasks"), 2, resumeToken)},
	}); err != nil {
		return err
	}
	responses, err = recvN(stream, 3, 5*time.Second)
	if err != nil {
		return err
	}
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		return fmt.Errorf("expected ADD")
	}
	dc := responses[1].GetDocumentChange()
	if dc == nil {
		return fmt.Errorf("expected DocumentChange, got %v", responses[1])
	}
	if dc.Document.Fields["status"].GetStringValue() != "pending" {
		return fmt.Errorf("expected t2 (pending), got %s", dc.Document.Fields["status"].GetStringValue())
	}
	if responses[2].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		return fmt.Errorf("expected CURRENT")
	}
	return nil
}
