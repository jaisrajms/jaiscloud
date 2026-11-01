package firestore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

const listenParent = "projects/test/databases/(default)/documents"

func listenTestClient(t *testing.T) (firestorepb.FirestoreClient, *firestoreprovider.Service, func()) {
	t.Helper()
	store := firestorestore.NewMemoryStore()
	providerSvc := firestoreprovider.New(store, nil).Service
	grpcSvc := NewService(providerSvc, "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	firestorepb.RegisterFirestoreServer(srv, grpcSvc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	client := firestorepb.NewFirestoreClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return client, providerSvc, cleanup
}

func queryTarget(tid int32, collection string) *firestorepb.Target {
	return &firestorepb.Target{
		TargetId: tid,
		TargetType: &firestorepb.Target_Query{
			Query: &firestorepb.Target_QueryTarget{
				Parent: listenParent,
				QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{
							{CollectionId: collection},
						},
					},
				},
			},
		},
	}
}

func addTargetReq(t *firestorepb.Target) *firestorepb.ListenRequest {
	return &firestorepb.ListenRequest{
		Database:     "projects/test/databases/(default)",
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: t},
	}
}

func removeTargetReq(tid int32) *firestorepb.ListenRequest {
	return &firestorepb.ListenRequest{
		Database:     "projects/test/databases/(default)",
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: tid},
	}
}

func recvN(t *testing.T, stream firestorepb.Firestore_ListenClient, n int, timeout time.Duration) []*firestorepb.ListenResponse {
	t.Helper()
	var responses []*firestorepb.ListenResponse
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		type result struct {
			resp *firestorepb.ListenResponse
			err  error
		}
		ch := make(chan result, 1)
		go func() {
			resp, err := stream.Recv()
			ch <- result{resp, err}
		}()
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("Recv %d/%d: %v", i+1, n, r.err)
			}
			responses = append(responses, r.resp)
		case <-deadline:
			t.Fatalf("timeout waiting for response %d/%d", i+1, n)
		}
	}
	return responses
}

func recvWithTimeout(stream firestorepb.Firestore_ListenClient, timeout time.Duration) (*firestorepb.ListenResponse, error) {
	type result struct {
		resp *firestorepb.ListenResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := stream.Recv()
		ch <- result{resp, err}
	}()
	select {
	case r := <-ch:
		return r.resp, r.err
	case <-time.After(timeout):
		return nil, io.ErrUnexpectedEOF
	}
}

func TestListenCollectionInitialSnapshot(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "users", "alice", map[string]*firestorestore.Value{
		"name": firestorestore.StringVal("alice"),
	})
	svc.CreateDocument(ctx, "test", "(default)", "users", "bob", map[string]*firestorestore.Value{
		"name": firestorestore.StringVal("bob"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "users"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	responses := recvN(t, stream, 5, 3*time.Second)
	if tc := responses[0].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_ADD {
		t.Fatalf("expected ADD, got %v", responses[0])
	}
	dc1, dc2 := responses[1].GetDocumentChange(), responses[2].GetDocumentChange()
	if dc1 == nil || dc2 == nil {
		t.Fatalf("expected DocumentChanges, got %v and %v", responses[1], responses[2])
	}
	if dc1.Document.Fields["name"].GetStringValue() != "alice" {
		t.Fatalf("expected alice first, got %s", dc1.Document.Fields["name"].GetStringValue())
	}
	if dc2.Document.Fields["name"].GetStringValue() != "bob" {
		t.Fatalf("expected bob second, got %s", dc2.Document.Fields["name"].GetStringValue())
	}
	if tc := responses[3].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		t.Fatalf("expected CURRENT, got %v", responses[3])
	}
	if tc := responses[4].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", responses[4])
	}
}

func TestListenCollectionRealTimeUpdates(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "items"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	responses := recvN(t, stream, 3, 3*time.Second) // ADD + CURRENT + NO_CHANGE (empty)
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatal("expected ADD")
	}
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatal("expected CURRENT")
	}
	if responses[2].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatal("expected NO_CHANGE")
	}

	// Create → DocumentChange + NO_CHANGE.
	svc.CreateDocument(ctx, "test", "(default)", "items", "item1", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("first item"),
	})
	responses = recvN(t, stream, 2, 3*time.Second)
	dc := responses[0].GetDocumentChange()
	if dc == nil || dc.Document.Fields["title"].GetStringValue() != "first item" {
		t.Fatalf("expected create DocumentChange, got %v", responses[0])
	}
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE after create, got %v", responses[1])
	}

	// Update → DocumentChange + NO_CHANGE.
	svc.PatchDocument(ctx, "test", "(default)", "items/item1", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("updated item"),
	}, nil, nil)
	responses = recvN(t, stream, 2, 3*time.Second)
	dc = responses[0].GetDocumentChange()
	if dc == nil || dc.Document.Fields["title"].GetStringValue() != "updated item" {
		t.Fatalf("expected update DocumentChange, got %v", responses[0])
	}
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE after update, got %v", responses[1])
	}

	// Delete → DocumentDelete + NO_CHANGE.
	svc.DeleteDocument(ctx, "test", "(default)", "items/item1", nil)
	responses = recvN(t, stream, 2, 3*time.Second)
	dd := responses[0].GetDocumentDelete()
	if dd == nil || dd.Document != listenParent+"/items/item1" {
		t.Fatalf("expected DocumentDelete, got %v", responses[0])
	}
	if !containsTargetID(dd.RemovedTargetIds, 1) {
		t.Fatalf("expected removed_target_ids to contain 1, got %v", dd.RemovedTargetIds)
	}
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE after delete, got %v", responses[1])
	}
}

func containsTargetID(ids []int32, want int32) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestListenDocumentDeleteRemovedTargetIds asserts that a delete which drops a
// document out of a query target's result set names that target in
// DocumentDelete.removed_target_ids, so a client can prune the document from
// that target's result set (real Firestore semantics).
func TestListenDocumentDeleteRemovedTargetIds(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	const tid int32 = 7
	if err := stream.Send(addTargetReq(queryTarget(tid, "items"))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvN(t, stream, 3, 3*time.Second) // ADD + CURRENT + NO_CHANGE

	svc.CreateDocument(ctx, "test", "(default)", "items", "item1", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("first item"),
	})
	responses := recvN(t, stream, 2, 3*time.Second) // DocumentChange + NO_CHANGE
	if responses[0].GetDocumentChange() == nil {
		t.Fatalf("expected DocumentChange, got %v", responses[0])
	}

	svc.DeleteDocument(ctx, "test", "(default)", "items/item1", nil)
	responses = recvN(t, stream, 2, 3*time.Second) // DocumentDelete + NO_CHANGE
	dd := responses[0].GetDocumentDelete()
	if dd == nil {
		t.Fatalf("expected DocumentDelete, got %v", responses[0])
	}
	if dd.Document != listenParent+"/items/item1" {
		t.Fatalf("expected %q, got %q", listenParent+"/items/item1", dd.Document)
	}
	if !containsTargetID(dd.RemovedTargetIds, tid) {
		t.Fatalf("expected removed_target_ids to contain %d, got %v", tid, dd.RemovedTargetIds)
	}
}

func TestListenDocumentTarget(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	docPath := listenParent + "/config/settings"
	svc.CreateDocument(ctx, "test", "(default)", "config", "settings", map[string]*firestorestore.Value{
		"theme": firestorestore.StringVal("dark"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	err = stream.Send(&firestorepb.ListenRequest{
		Database: "projects/test/databases/(default)",
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
		t.Fatalf("Send: %v", err)
	}

	responses := recvN(t, stream, 4, 3*time.Second)
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatal("expected ADD")
	}
	dc := responses[1].GetDocumentChange()
	if dc == nil || dc.Document.Fields["theme"].GetStringValue() != "dark" {
		t.Fatal("expected document with theme=dark")
	}
	if responses[2].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatal("expected CURRENT")
	}
	if responses[3].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatal("expected NO_CHANGE")
	}
}

func TestListenRemoveTarget(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "col"))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvN(t, stream, 3, 3*time.Second) // ADD + CURRENT + NO_CHANGE

	if err := stream.Send(&firestorepb.ListenRequest{
		Database:     "projects/test/databases/(default)",
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: 1},
	}); err != nil {
		t.Fatalf("Send remove: %v", err)
	}

	responses := recvN(t, stream, 1, 3*time.Second)
	tc := responses[0].GetTargetChange()
	if tc == nil || tc.TargetChangeType != firestorepb.TargetChange_REMOVE {
		t.Fatalf("expected REMOVE, got %v", responses[0])
	}
}

func TestListenIgnoresUnrelatedCollections(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "users"))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvN(t, stream, 3, 3*time.Second) // ADD + CURRENT + NO_CHANGE

	svc.CreateDocument(ctx, "test", "(default)", "posts", "post1", map[string]*firestorestore.Value{
		"title": firestorestore.StringVal("hello"),
	})

	time.Sleep(200 * time.Millisecond)
	if resp, err := recvWithTimeout(stream, 300*time.Millisecond); err == nil {
		t.Fatalf("expected no event for unrelated collection, got %v", resp)
	}
}

func TestListenMultipleClients(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	stream1, _ := client.Listen(ctx1)
	stream2, _ := client.Listen(ctx2)

	stream1.Send(addTargetReq(queryTarget(1, "shared")))
	stream2.Send(addTargetReq(queryTarget(2, "shared")))

	recvN(t, stream1, 3, 3*time.Second) // ADD + CURRENT + NO_CHANGE
	recvN(t, stream2, 3, 3*time.Second)

	svc.CreateDocument(ctx, "test", "(default)", "shared", "doc1", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("hello"),
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if r := recvN(t, stream1, 1, 3*time.Second); r[0].GetDocumentChange() == nil {
			t.Error("stream1: expected DocumentChange")
		}
	}()
	go func() {
		defer wg.Done()
		if r := recvN(t, stream2, 1, 3*time.Second); r[0].GetDocumentChange() == nil {
			t.Error("stream2: expected DocumentChange")
		}
	}()
	wg.Wait()
}

func TestListenResumeTokenBasic(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "tasks", "t1", map[string]*firestorestore.Value{
		"status": firestorestore.StringVal("open"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "tasks"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	responses := recvN(t, stream, 4, 3*time.Second) // ADD, t1, CURRENT, NO_CHANGE
	tc := responses[2].GetTargetChange()
	if tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		t.Fatal("expected CURRENT")
	}
	tok, ok := parseResumeToken(tc.ResumeToken)
	if !ok || !tok.bound || tok.seq != 1 {
		t.Fatalf("CURRENT token = %v ok=%v, want bound seq 1", tok, ok)
	}
	resumeToken := tc.ResumeToken

	svc.CreateDocument(ctx, "test", "(default)", "tasks", "t2", map[string]*firestorestore.Value{
		"status": firestorestore.StringVal("pending"),
	})
	recvN(t, stream, 2, 3*time.Second) // consume real-time event (DocumentChange + NO_CHANGE)

	stream.Send(&firestorepb.ListenRequest{
		Database:     "projects/test/databases/(default)",
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: 1},
	})
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	rt := queryTarget(2, "tasks")
	rt.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: resumeToken}
	if err := stream.Send(addTargetReq(rt)); err != nil {
		t.Fatalf("Send resume: %v", err)
	}

	responses = recvN(t, stream, 4, 3*time.Second) // ADD, t2 only, CURRENT, NO_CHANGE
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatal("expected ADD")
	}
	dc := responses[1].GetDocumentChange()
	if dc == nil || dc.Document.Fields["status"].GetStringValue() != "pending" {
		t.Fatalf("expected t2 (pending) incremental, got %v", responses[1])
	}
	if responses[2].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatal("expected CURRENT")
	}
	if responses[3].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatal("expected NO_CHANGE")
	}
}

func TestListenResumeTokenFallsBackToSnapshot(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "items", "a", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("1"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	rt := queryTarget(1, "items")
	rt.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: []byte("bad")}
	if err := stream.Send(addTargetReq(rt)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	responses := recvN(t, stream, 5, 3*time.Second) // ADD, RESET, a (full snapshot), CURRENT, NO_CHANGE
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatal("expected ADD")
	}
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_RESET {
		t.Fatalf("expected RESET for un-honorable token, got %v", responses[1])
	}
	dc := responses[2].GetDocumentChange()
	if dc == nil || dc.Document.Fields["v"].GetStringValue() != "1" {
		t.Fatal("expected full snapshot with item a")
	}
	if responses[3].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatal("expected CURRENT")
	}
	if responses[4].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatal("expected NO_CHANGE")
	}
}

func TestResumeTokenEncodeDecode(t *testing.T) {
	for _, seq := range []uint64{0, 1, 42, 1<<32 - 1, 1 << 63} {
		token := EncodeResumeToken(seq)
		got, ok := DecodeResumeToken(token)
		if !ok || got != seq {
			t.Fatalf("round-trip failed for seq=%d: got %d ok=%v", seq, got, ok)
		}
		if len(token) != 8 {
			t.Fatalf("legacy token for seq=%d is %d bytes, want 8", seq, len(token))
		}
		if parsed, ok := parseResumeToken(token); !ok || parsed.bound {
			t.Fatalf("legacy token for seq=%d parsed as %+v ok=%v, want unbound", seq, parsed, ok)
		}
	}
	if _, ok := DecodeResumeToken(nil); ok {
		t.Fatal("expected decode failure for nil")
	}
	if _, ok := DecodeResumeToken([]byte("short")); ok {
		t.Fatal("expected decode failure for wrong length")
	}
}

// TestResumeTokenBoundFormat pins the versioned, scope-bound per-target token
// format and its backwards compatibility with the legacy 8-byte bare sequence.
func TestResumeTokenBoundFormat(t *testing.T) {
	var scopeA, scopeB [resumeScopeLen]byte
	scopeA[0], scopeB[0] = 0xAA, 0xBB

	tokenA := encodeBoundResumeToken(42, scopeA)
	tokenB := encodeBoundResumeToken(42, scopeB)
	if len(tokenA) != boundResumeTokenLen {
		t.Fatalf("bound token is %d bytes, want %d", len(tokenA), boundResumeTokenLen)
	}
	if bytes.Equal(tokenA, tokenB) {
		t.Fatal("tokens for different scopes are identical")
	}
	if bytes.Equal(tokenA, EncodeResumeToken(42)) {
		t.Fatal("bound token must not collide with the legacy 8-byte token")
	}

	parsed, ok := parseResumeToken(tokenA)
	if !ok || !parsed.bound || parsed.seq != 42 || parsed.scope != scopeA {
		t.Fatalf("parsed token = %+v ok=%v, want bound seq 42 scope %x", parsed, ok, scopeA)
	}
	if seq, ok := DecodeResumeToken(tokenA); !ok || seq != 42 {
		t.Fatalf("DecodeResumeToken = %d ok=%v, want 42", seq, ok)
	}

	// A versioned token with the wrong magic or length is malformed.
	for _, bad := range [][]byte{
		append([]byte("BAD!"), tokenA[4:]...),
		tokenA[:boundResumeTokenLen-1],
		append(tokenA, 0),
	} {
		if _, ok := parseResumeToken(bad); ok {
			t.Fatalf("expected parse failure for %d-byte token %q", len(bad), bad)
		}
	}
}

// TestTargetScopeFingerprint asserts the fingerprint identifies a target's
// query scope only: target_id, once, expected_count, and the resume token do not
// affect it, while a different query or document list does.
func TestTargetScopeFingerprint(t *testing.T) {
	base, ok := targetScopeFingerprint(queryTarget(1, "items"))
	if !ok {
		t.Fatal("fingerprint of a query target failed")
	}

	resumed := queryTarget(2, "items")
	resumed.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: EncodeResumeToken(7)}
	resumed.Once = true
	resumed.ExpectedCount = wrapperspb.Int32(3)
	same, ok := targetScopeFingerprint(resumed)
	if !ok || same != base {
		t.Fatalf("fingerprint changed with target_id/resume/once/expected_count: %x vs %x", same, base)
	}

	filtered, ok := targetScopeFingerprint(whereQueryTarget(1, "items", "active", pbBool(true)))
	if !ok || filtered == base {
		t.Fatalf("where filter did not change the fingerprint: %x vs %x", filtered, base)
	}
	otherCollection, _ := targetScopeFingerprint(queryTarget(1, "others"))
	if otherCollection == base {
		t.Fatal("different collection did not change the fingerprint")
	}

	docsA, _ := targetScopeFingerprint(documentTarget(1, listenParent+"/items/b", listenParent+"/items/a"))
	docsB, _ := targetScopeFingerprint(documentTarget(9, listenParent+"/items/a", listenParent+"/items/b"))
	if docsA != docsB {
		t.Fatalf("document list order changed the fingerprint: %x vs %x", docsA, docsB)
	}
	docsC, _ := targetScopeFingerprint(documentTarget(1, listenParent+"/items/a"))
	if docsC == docsA {
		t.Fatal("different document list did not change the fingerprint")
	}

	if _, ok := targetScopeFingerprint(&firestorepb.Target{TargetId: 1}); ok {
		t.Fatal("expected no fingerprint for a target without a query or documents")
	}
	if _, ok := targetScopeFingerprint(&firestorepb.Target{
		TargetType: &firestorepb.Target_Documents{Documents: nil},
	}); !ok {
		t.Fatal("expected a fingerprint for a document target with an empty documents message")
	}
}

func TestListenNoChangeAfterCurrent(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "users", "alice", map[string]*firestorestore.Value{
		"name": firestorestore.StringVal("alice"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "users"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	responses := recvN(t, stream, 4, 3*time.Second) // ADD, DocumentChange, CURRENT, NO_CHANGE
	if tc := responses[0].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_ADD {
		t.Fatalf("frame 0: expected ADD, got %v", responses[0])
	}
	if dc := responses[1].GetDocumentChange(); dc == nil || dc.Document.Fields["name"].GetStringValue() != "alice" {
		t.Fatalf("frame 1: expected DocumentChange, got %v", responses[1])
	}
	if tc := responses[2].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_CURRENT {
		t.Fatalf("frame 2: expected CURRENT, got %v", responses[2])
	}
	nc := responses[3].GetTargetChange()
	if nc == nil || nc.TargetChangeType != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("frame 3: expected NO_CHANGE, got %v", responses[3])
	}
	if len(nc.TargetIds) != 0 {
		t.Fatalf("NO_CHANGE must have empty target_ids, got %v", nc.TargetIds)
	}
	if nc.ReadTime == nil {
		t.Fatal("NO_CHANGE must have a non-nil read_time")
	}
	if len(nc.ResumeToken) == 0 {
		t.Fatal("NO_CHANGE must have a non-empty resume_token")
	}
}

func TestListenReadTimeMonotonic(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "users", "alice", map[string]*firestorestore.Value{
		"name": firestorestore.StringVal("alice"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	noChangeReadTime := func(tid int32) time.Time {
		if err := stream.Send(addTargetReq(queryTarget(tid, "users"))); err != nil {
			t.Fatalf("Send: %v", err)
		}
		rs := recvN(t, stream, 4, 3*time.Second)
		nc := rs[3].GetTargetChange()
		if nc == nil || nc.TargetChangeType != firestorepb.TargetChange_NO_CHANGE {
			t.Fatalf("expected NO_CHANGE, got %v", rs[3])
		}
		if nc.ReadTime == nil {
			t.Fatal("expected non-nil read_time")
		}
		return nc.ReadTime.AsTime()
	}

	rt1 := noChangeReadTime(1)
	rt2 := noChangeReadTime(2)

	if rt2.Before(rt1) {
		t.Fatalf("read_time went backwards: first=%v second=%v", rt1, rt2)
	}
}

func TestPublishChangeNonBlocking(t *testing.T) {
	store := firestorestore.NewMemoryStore()
	svc := firestoreprovider.New(store, nil).Service
	ctx := context.Background()

	sub := svc.SubscribeChange()
	defer sub.Cancel()

	// Fill the subscriber's buffered channel (capacity 1024) without draining.
	const n = 1024
	for i := 0; i < n; i++ {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "bulk", fmt.Sprintf("d%03d", i), map[string]*firestorestore.Value{
			"v": firestorestore.StringVal("x"),
		}); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}

	// The buffer is now full. A subsequent publish must return promptly: it
	// drops the event (and detaches the subscriber) rather than blocking.
	done := make(chan struct{})
	go func() {
		_, _ = svc.CreateDocument(ctx, "test", "(default)", "bulk", "overflow", map[string]*firestorestore.Value{
			"v": firestorestore.StringVal("y"),
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publishChange blocked on a full subscriber channel")
	}
}

// TestListenMultiTargetPerTargetResumeTokens asserts that each target tracks its
// own delivered sequence and is resumed from its own resume token. Because a
// single empty-target_ids NO_CHANGE token cannot carry one cursor per target,
// the stream-wide token is the minimum of the live cursors (documented
// approximation); this test pins that behavior and shows each target replaying
// only its own deltas when resumed from its own token.
func TestListenMultiTargetPerTargetResumeTokens(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	// Seed one doc in "a" so target 1 snapshots at head 1.
	svc.CreateDocument(ctx, "test", "(default)", "a", "a1", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("1"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Target 1 on "a": ADD, a1, CURRENT, NO_CHANGE. Its own token is seq 1.
	if err := stream.Send(addTargetReq(queryTarget(1, "a"))); err != nil {
		t.Fatalf("send target 1: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second)
	t1Token := rs[2].GetTargetChange().GetResumeToken()
	if seq, ok := DecodeResumeToken(t1Token); !ok || seq != 1 {
		t.Fatalf("target 1 CURRENT token seq = %d ok = %v, want 1", seq, ok)
	}

	// Seed a doc in "b" (head 2) and add target 2: its cursor is 2, distinct
	// from target 1's 1.
	svc.CreateDocument(ctx, "test", "(default)", "b", "b1", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("1"),
	})
	if err := stream.Send(addTargetReq(queryTarget(2, "b"))); err != nil {
		t.Fatalf("send target 2: %v", err)
	}
	rs = recvN(t, stream, 4, 3*time.Second)
	t2Token := rs[2].GetTargetChange().GetResumeToken()
	if seq, ok := DecodeResumeToken(t2Token); !ok || seq != 2 {
		t.Fatalf("target 2 CURRENT token seq = %d ok = %v, want 2", seq, ok)
	}

	// A new "a" doc (seq 3) matches only target 1, leaving target 2 at 2: the
	// NO_CHANGE token is min(3, 2) = 2, not the global head 3.
	svc.CreateDocument(ctx, "test", "(default)", "a", "a2", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("2"),
	})
	rs = recvN(t, stream, 2, 3*time.Second)
	if dc := rs[0].GetDocumentChange(); dc == nil || !containsTargetID(dc.TargetIds, 1) {
		t.Fatalf("expected DocumentChange for target 1, got %v", rs[0])
	}
	if seq, ok := DecodeResumeToken(rs[1].GetTargetChange().GetResumeToken()); !ok || seq != 2 {
		t.Fatalf("stream NO_CHANGE token after a2 = seq %d ok = %v, want 2 (min cursor)", seq, ok)
	}

	// A new "b" doc (seq 4) matches only target 2: min cursor becomes 3.
	svc.CreateDocument(ctx, "test", "(default)", "b", "b2", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("2"),
	})
	rs = recvN(t, stream, 2, 3*time.Second)
	if dc := rs[0].GetDocumentChange(); dc == nil || !containsTargetID(dc.TargetIds, 2) {
		t.Fatalf("expected DocumentChange for target 2, got %v", rs[0])
	}
	if seq, ok := DecodeResumeToken(rs[1].GetTargetChange().GetResumeToken()); !ok || seq != 3 {
		t.Fatalf("stream NO_CHANGE token after b2 = seq %d ok = %v, want 3 (min cursor)", seq, ok)
	}

	// Resume target 1 from its own token (seq 1): it must replay only a2 (seq 3),
	// not target 2's b2.
	if err := stream.Send(removeTargetReq(1)); err != nil {
		t.Fatalf("remove target 1: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second)
	t1Resume := queryTarget(3, "a")
	t1Resume.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: t1Token}
	if err := stream.Send(addTargetReq(t1Resume)); err != nil {
		t.Fatalf("resume target 1: %v", err)
	}
	rs = recvN(t, stream, 4, 3*time.Second) // ADD, a2, CURRENT, NO_CHANGE
	dc := rs[1].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/a/a2" {
		t.Fatalf("target 1 resume replayed %v, want only a2", rs[1])
	}
	if !containsTargetID(dc.TargetIds, 3) {
		t.Fatalf("target 1 resume change target_ids = %v, want [3]", dc.TargetIds)
	}

	// Resume target 2 from its own token (seq 2): it must replay only b2 (seq 4).
	if err := stream.Send(removeTargetReq(2)); err != nil {
		t.Fatalf("remove target 2: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second)
	t2Resume := queryTarget(4, "b")
	t2Resume.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: t2Token}
	if err := stream.Send(addTargetReq(t2Resume)); err != nil {
		t.Fatalf("resume target 2: %v", err)
	}
	rs = recvN(t, stream, 4, 3*time.Second) // ADD, b2, CURRENT, NO_CHANGE
	dc = rs[1].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/b/b2" {
		t.Fatalf("target 2 resume replayed %v, want only b2", rs[1])
	}
}

// TestListenResumeTokenBelowFloorFallsBackToSnapshot asserts that a resume token
// whose deltas have been evicted (below the retention floor) is treated as
// expired: the target gets a fresh snapshot instead of replaying from a partial
// history. Deltas that would have matched are gone, so a stale replay would be
// silently lossy.
func TestListenResumeTokenBelowFloorFallsBackToSnapshot(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	// Two docs in the watched collection establish a resumable position at seq 2.
	for _, id := range []string{"a1", "a2"} {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "a", id, map[string]*firestorestore.Value{
			"v": firestorestore.StringVal(id),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	lctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(queryTarget(1, "a"))); err != nil {
		t.Fatalf("send target: %v", err)
	}
	rs := recvN(t, stream, 5, 5*time.Second) // ADD, a1, a2, CURRENT, NO_CHANGE
	resumeToken := rs[3].GetTargetChange().GetResumeToken()
	if seq, ok := DecodeResumeToken(resumeToken); !ok || seq != 2 {
		t.Fatalf("CURRENT resume token seq = %d ok = %v, want 2", seq, ok)
	}

	// Evict the resume position by writing past the change-feed retention window.
	// The unrelated "z" writes still advance the floor.
	const maxWrites = 50_000
	for i := 0; svc.ChangeFloor() <= 2 && i < maxWrites; i++ {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "z", fmt.Sprintf("z%05d", i), nil); err != nil {
			t.Fatalf("bulk write %d: %v", i, err)
		}
	}
	if floor := svc.ChangeFloor(); floor <= 2 {
		t.Fatalf("change floor = %d, want > 2 after eviction", floor)
	}

	// Resume from the now-expired token. A correct server resets the target and
	// falls back to a full snapshot (a1 + a2); a stale incremental replay would
	// send no deltas at all because no "a" write happened after seq 2.
	if err := stream.Send(removeTargetReq(1)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 5*time.Second)

	rt := queryTarget(2, "a")
	rt.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: resumeToken}
	if err := stream.Send(addTargetReq(rt)); err != nil {
		t.Fatalf("resume target: %v", err)
	}
	rs = recvN(t, stream, 6, 10*time.Second) // ADD, RESET, a1, a2, CURRENT, NO_CHANGE
	if rs[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatalf("frame 0: expected ADD, got %v", rs[0])
	}
	if rs[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_RESET {
		t.Fatalf("frame 1: expected RESET, got %v", rs[1])
	}
	got := map[string]bool{}
	for _, r := range rs[2:4] {
		dc := r.GetDocumentChange()
		if dc == nil {
			t.Fatalf("expected snapshot DocumentChange, got %v", r)
		}
		got[dc.Document.Name] = true
	}
	if !got[listenParent+"/a/a1"] || !got[listenParent+"/a/a2"] {
		t.Fatalf("snapshot documents = %v, want a1 and a2", got)
	}
}

func pbString(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

func pbBool(b bool) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: b}}
}

// documentTarget builds a target watching an explicit set of document names.
func documentTarget(tid int32, names ...string) *firestorepb.Target {
	return &firestorepb.Target{
		TargetId: tid,
		TargetType: &firestorepb.Target_Documents{
			Documents: &firestorepb.Target_DocumentsTarget{Documents: names},
		},
	}
}

// whereQueryTarget builds a single-collection query target with an EQUAL field
// filter, so live deltas must honor the where predicate.
func whereQueryTarget(tid int32, collection, field string, val *firestorepb.Value) *firestorepb.Target {
	return &firestorepb.Target{
		TargetId: tid,
		TargetType: &firestorepb.Target_Query{
			Query: &firestorepb.Target_QueryTarget{
				Parent: listenParent,
				QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: collection}},
						Where: &firestorepb.StructuredQuery_Filter{
							FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
								FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
									Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: field},
									Op:    firestorepb.StructuredQuery_FieldFilter_EQUAL,
									Value: val,
								},
							},
						},
					},
				},
			},
		},
	}
}

// orderedLimitedQueryTarget builds a single-collection query target ordered by
// one field (descending when desc) with a document limit, so deltas and resume
// replay must honor order_by and limit, not just collection scope.
func orderedLimitedQueryTarget(tid int32, collection, field string, desc bool, limit int32) *firestorepb.Target {
	direction := firestorepb.StructuredQuery_ASCENDING
	if desc {
		direction = firestorepb.StructuredQuery_DESCENDING
	}
	return &firestorepb.Target{
		TargetId: tid,
		TargetType: &firestorepb.Target_Query{
			Query: &firestorepb.Target_QueryTarget{
				Parent: listenParent,
				QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: collection}},
						OrderBy: []*firestorepb.StructuredQuery_Order{{
							Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: field},
							Direction: direction,
						}},
						Limit: wrapperspb.Int32(limit),
					},
				},
			},
		},
	}
}

// currentToken returns the per-target resume token from the first CURRENT frame
// in rs.
func currentToken(t *testing.T, rs []*firestorepb.ListenResponse) []byte {
	t.Helper()
	for _, r := range rs {
		if tc := r.GetTargetChange(); tc != nil && tc.TargetChangeType == firestorepb.TargetChange_CURRENT {
			if len(tc.ResumeToken) == 0 {
				t.Fatal("CURRENT frame carries no resume token")
			}
			return tc.ResumeToken
		}
	}
	t.Fatalf("no CURRENT frame in %d responses", len(rs))
	return nil
}

// resumeTargetWithToken re-adds a target using tok as its resume token.
func resumeTargetWithToken(t *firestorepb.Target, tok []byte) *firestorepb.Target {
	t.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: tok}
	return t
}

// assertResumeReplayFrames asserts the common ADD -> ... -> CURRENT -> NO_CHANGE
// envelope around a replay, returning the frames between ADD and CURRENT.
func assertResumeReplayFrames(t *testing.T, rs []*firestorepb.ListenResponse) []*firestorepb.ListenResponse {
	t.Helper()
	if got := rs[0].GetTargetChange().GetTargetChangeType(); got != firestorepb.TargetChange_ADD {
		t.Fatalf("frame 0: expected ADD, got %v", rs[0])
	}
	last := len(rs) - 1
	if got := rs[last].GetTargetChange().GetTargetChangeType(); got != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("last frame: expected NO_CHANGE, got %v", rs[last])
	}
	if got := rs[last-1].GetTargetChange().GetTargetChangeType(); got != firestorepb.TargetChange_CURRENT {
		t.Fatalf("penultimate frame: expected CURRENT, got %v", rs[last-1])
	}
	return rs[1 : last-1]
}

// TestListenQueryWhereLiveUpdateEmitsDocumentRemove asserts that a live update
// which makes a document stop matching a query target's where predicate emits a
// DocumentRemove naming that target, rather than a DocumentChange.
func TestListenQueryWhereLiveUpdateEmitsDocumentRemove(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 11
	for _, id := range []string{"leave", "stay"} {
		svc.CreateDocument(ctx, "test", "(default)", "items", id, map[string]*firestorestore.Value{
			"active": firestorestore.BoolVal(true),
		})
	}

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rs := recvN(t, stream, 5, 3*time.Second) // ADD, leave, stay, CURRENT, NO_CHANGE
	if rs[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatalf("expected ADD, got %v", rs[0])
	}
	visible := map[string]bool{}
	for _, r := range rs[1:3] {
		dc := r.GetDocumentChange()
		if dc == nil {
			t.Fatalf("expected snapshot DocumentChange, got %v", r)
		}
		visible[dc.Document.Name] = true
	}
	if !visible[listenParent+"/items/leave"] || !visible[listenParent+"/items/stay"] {
		t.Fatalf("snapshot = %v, want leave and stay", visible)
	}

	svc.PatchDocument(ctx, "test", "(default)", "items/leave", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	}, nil, nil)

	rs = recvN(t, stream, 2, 3*time.Second) // DocumentRemove + NO_CHANGE
	dr := rs[0].GetDocumentRemove()
	if dr == nil {
		t.Fatalf("expected DocumentRemove, got %v", rs[0])
	}
	if dr.Document != listenParent+"/items/leave" {
		t.Fatalf("DocumentRemove for %q, want %q", dr.Document, listenParent+"/items/leave")
	}
	if !containsTargetID(dr.RemovedTargetIds, tid) {
		t.Fatalf("removed_target_ids = %v, want [%d]", dr.RemovedTargetIds, tid)
	}
	if rs[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[1])
	}
}

// TestListenQueryWhereLiveDeleteEmitsDocumentDelete asserts that deleting a
// document that was in a where-filtered target's view emits DocumentDelete (not
// DocumentRemove).
func TestListenQueryWhereLiveDeleteEmitsDocumentDelete(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 12
	svc.CreateDocument(ctx, "test", "(default)", "items", "one", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	recvN(t, stream, 4, 3*time.Second) // ADD, one, CURRENT, NO_CHANGE

	svc.DeleteDocument(ctx, "test", "(default)", "items/one", nil)

	rs := recvN(t, stream, 2, 3*time.Second) // DocumentDelete + NO_CHANGE
	dd := rs[0].GetDocumentDelete()
	if dd == nil {
		t.Fatalf("expected DocumentDelete, got %v", rs[0])
	}
	if dd.Document != listenParent+"/items/one" {
		t.Fatalf("DocumentDelete for %q, want %q", dd.Document, listenParent+"/items/one")
	}
	if !containsTargetID(dd.RemovedTargetIds, tid) {
		t.Fatalf("removed_target_ids = %v, want [%d]", dd.RemovedTargetIds, tid)
	}
	if rs[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[1])
	}
}

// TestListenQueryWhereLiveUpdateEntersView asserts the live counterpart of the
// where-exit case: an update that makes a document start matching the target's
// where predicate emits a DocumentChange for it and adds it to the view.
func TestListenQueryWhereLiveUpdateEntersView(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 13
	svc.CreateDocument(ctx, "test", "(default)", "items", "sleeping", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if rs := recvN(t, stream, 3, 3*time.Second); rs[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatalf("expected empty snapshot (ADD, CURRENT, NO_CHANGE), got %v", rs)
	}

	svc.PatchDocument(ctx, "test", "(default)", "items/sleeping", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	}, nil, nil)

	rs := recvN(t, stream, 2, 3*time.Second) // DocumentChange + NO_CHANGE
	dc := rs[0].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/items/sleeping" {
		t.Fatalf("expected DocumentChange for sleeping, got %v", rs[0])
	}
	if !containsTargetID(dc.TargetIds, tid) {
		t.Fatalf("target_ids = %v, want [%d]", dc.TargetIds, tid)
	}
	if rs[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[1])
	}
}

// TestListenQueryLimitPushOutAndEntry asserts that an ordered, limited target
// tracks its window live: a new document that enters the limit pushes the old
// one out (DocumentRemove), and deleting the entrant lets the old document back
// in (DocumentChange).
func TestListenQueryLimitPushOutAndEntry(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 14
	for name, score := range map[string]int64{"a": 10, "b": 20} {
		svc.CreateDocument(ctx, "test", "(default)", "items", name, map[string]*firestorestore.Value{
			"score": firestorestore.IntVal(score),
		})
	}

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(orderedLimitedQueryTarget(tid, "items", "score", true, 1))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second) // ADD, b (top of the limit), CURRENT, NO_CHANGE
	dc := rs[1].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/items/b" {
		t.Fatalf("snapshot = %v, want only b", rs[1])
	}

	// c enters the limit and pushes b out of it.
	svc.CreateDocument(ctx, "test", "(default)", "items", "c", map[string]*firestorestore.Value{
		"score": firestorestore.IntVal(30),
	})
	rs = recvN(t, stream, 3, 3*time.Second) // DocumentRemove(b), DocumentChange(c), NO_CHANGE
	byName := map[string]*firestorepb.ListenResponse{}
	for _, r := range rs[:2] {
		switch {
		case r.GetDocumentChange() != nil:
			byName[r.GetDocumentChange().Document.Name] = r
		case r.GetDocumentRemove() != nil:
			byName[r.GetDocumentRemove().Document] = r
		}
	}
	if r := byName[listenParent+"/items/b"]; r == nil || r.GetDocumentRemove() == nil ||
		!containsTargetID(r.GetDocumentRemove().RemovedTargetIds, tid) {
		t.Fatalf("expected DocumentRemove for b naming target %d, got %v", tid, rs)
	}
	if r := byName[listenParent+"/items/c"]; r == nil || r.GetDocumentChange() == nil {
		t.Fatalf("expected DocumentChange for c, got %v", rs)
	}
	if rs[2].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[2])
	}

	// Deleting the entrant lets b back into the limit.
	svc.DeleteDocument(ctx, "test", "(default)", "items/c", nil)
	rs = recvN(t, stream, 3, 3*time.Second) // DocumentChange(b), DocumentDelete(c), NO_CHANGE
	var sawEnter, sawDelete bool
	for _, r := range rs[:2] {
		if dc := r.GetDocumentChange(); dc != nil && dc.Document.Name == listenParent+"/items/b" {
			sawEnter = true
		}
		if dd := r.GetDocumentDelete(); dd != nil && dd.Document == listenParent+"/items/c" {
			sawDelete = true
		}
	}
	if !sawEnter || !sawDelete {
		t.Fatalf("expected DocumentChange(b) and DocumentDelete(c), got %v", rs)
	}
}

// TestListenResumeScopeChangeEmitsReset asserts that a per-target resume token
// replayed against a different query scope is not honored: the target is reset
// before its fresh snapshot, so the client does not keep a result set the new
// query never selected.
func TestListenResumeScopeChangeEmitsReset(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 21
	svc.CreateDocument(ctx, "test", "(default)", "items", "open", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})
	svc.CreateDocument(ctx, "test", "(default)", "items", "closed", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	token := currentToken(t, recvN(t, stream, 4, 3*time.Second)) // ADD, open, CURRENT, NO_CHANGE
	if tok, ok := parseResumeToken(token); !ok || !tok.bound {
		t.Fatalf("CURRENT token = %v ok=%v, want scope-bound", tok, ok)
	}

	if err := stream.Send(removeTargetReq(tid)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	// Same target id, different query: the token describes a different result
	// set, so the target must be reset and resnapshotted.
	changed := resumeTargetWithToken(whereQueryTarget(tid, "items", "active", pbBool(false)), token)
	if err := stream.Send(addTargetReq(changed)); err != nil {
		t.Fatalf("Send changed scope: %v", err)
	}
	rs := recvN(t, stream, 5, 3*time.Second) // ADD, RESET, closed, CURRENT, NO_CHANGE
	if rs[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatalf("frame 0: expected ADD, got %v", rs[0])
	}
	if tc := rs[1].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_RESET {
		t.Fatalf("frame 1: expected RESET for a changed query scope, got %v", rs[1])
	}
	dc := rs[2].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/items/closed" {
		t.Fatalf("frame 2: expected snapshot DocumentChange for closed, got %v", rs[2])
	}
	if rs[3].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatalf("frame 3: expected CURRENT, got %v", rs[3])
	}
	if rs[4].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("frame 4: expected NO_CHANGE, got %v", rs[4])
	}
}

// TestListenReplayQueryFiltersCatchUp asserts that catch-up replay runs through
// the target's query: a retained event for a document outside the where
// predicate is not replayed, and a document that now matches is.
func TestListenReplayQueryFiltersCatchUp(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 31
	svc.CreateDocument(ctx, "test", "(default)", "items", "keep", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})
	svc.CreateDocument(ctx, "test", "(default)", "items", "noise", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	token := currentToken(t, recvN(t, stream, 4, 3*time.Second)) // ADD, keep, CURRENT, NO_CHANGE

	if err := stream.Send(removeTargetReq(tid)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	// Writes after the token: a non-matching update, a new non-matching document,
	// and a new matching one.
	svc.PatchDocument(ctx, "test", "(default)", "items/noise", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
		"note":   firestorestore.StringVal("still out"),
	}, nil, nil)
	svc.CreateDocument(ctx, "test", "(default)", "items", "fresh", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	})
	svc.CreateDocument(ctx, "test", "(default)", "items", "join", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	resumed := resumeTargetWithToken(whereQueryTarget(tid, "items", "active", pbBool(true)), token)
	if err := stream.Send(addTargetReq(resumed)); err != nil {
		t.Fatalf("Send resume: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second) // ADD, join, CURRENT, NO_CHANGE
	mid := assertResumeReplayFrames(t, rs)
	if len(mid) != 1 {
		t.Fatalf("replay emitted %d frames (%v), want only join", len(mid), mid)
	}
	dc := mid[0].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/items/join" {
		t.Fatalf("replayed %v, want DocumentChange for join", mid[0])
	}
	if !containsTargetID(dc.TargetIds, tid) {
		t.Fatalf("target_ids = %v, want [%d]", dc.TargetIds, tid)
	}
}

// TestListenReplayQueryWhereExitEmitsDocumentRemove asserts the replay mirror of
// the live where-exit case: a document that matched at the resume position and
// no longer matches is removed from the target during catch-up.
func TestListenReplayQueryWhereExitEmitsDocumentRemove(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 32
	for _, name := range []string{"leave", "stay"} {
		svc.CreateDocument(ctx, "test", "(default)", "items", name, map[string]*firestorestore.Value{
			"active": firestorestore.BoolVal(true),
		})
	}

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	token := currentToken(t, recvN(t, stream, 5, 3*time.Second)) // ADD, leave, stay, CURRENT, NO_CHANGE

	if err := stream.Send(removeTargetReq(tid)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	svc.PatchDocument(ctx, "test", "(default)", "items/leave", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	}, nil, nil)

	resumed := resumeTargetWithToken(whereQueryTarget(tid, "items", "active", pbBool(true)), token)
	if err := stream.Send(addTargetReq(resumed)); err != nil {
		t.Fatalf("Send resume: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second) // ADD, DocumentRemove, CURRENT, NO_CHANGE
	mid := assertResumeReplayFrames(t, rs)
	if len(mid) != 1 {
		t.Fatalf("replay emitted %d frames (%v), want only the removal", len(mid), mid)
	}
	dr := mid[0].GetDocumentRemove()
	if dr == nil || dr.Document != listenParent+"/items/leave" {
		t.Fatalf("replayed %v, want DocumentRemove for leave", mid[0])
	}
	if !containsTargetID(dr.RemovedTargetIds, tid) {
		t.Fatalf("removed_target_ids = %v, want [%d]", dr.RemovedTargetIds, tid)
	}
}

// TestListenReplayQueryWhereEntryEmitsDocumentChange asserts that a document
// which did not match at the resume position but matches now is added during
// catch-up.
func TestListenReplayQueryWhereEntryEmitsDocumentChange(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 33
	svc.CreateDocument(ctx, "test", "(default)", "items", "sleeping", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	token := currentToken(t, recvN(t, stream, 3, 3*time.Second)) // ADD, CURRENT, NO_CHANGE

	if err := stream.Send(removeTargetReq(tid)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	svc.PatchDocument(ctx, "test", "(default)", "items/sleeping", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	}, nil, nil)

	resumed := resumeTargetWithToken(whereQueryTarget(tid, "items", "active", pbBool(true)), token)
	if err := stream.Send(addTargetReq(resumed)); err != nil {
		t.Fatalf("Send resume: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second) // ADD, DocumentChange, CURRENT, NO_CHANGE
	mid := assertResumeReplayFrames(t, rs)
	if len(mid) != 1 {
		t.Fatalf("replay emitted %d frames (%v), want only sleeping", len(mid), mid)
	}
	dc := mid[0].GetDocumentChange()
	if dc == nil || dc.Document.Name != listenParent+"/items/sleeping" {
		t.Fatalf("replayed %v, want DocumentChange for sleeping", mid[0])
	}
}

// TestListenReplayQueryDeleteEmitsDocumentDelete asserts that a document in the
// target's view at the resume position that was deleted afterwards is replayed
// as a DocumentDelete.
func TestListenReplayQueryDeleteEmitsDocumentDelete(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 34
	svc.CreateDocument(ctx, "test", "(default)", "items", "gone", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	token := currentToken(t, recvN(t, stream, 4, 3*time.Second)) // ADD, gone, CURRENT, NO_CHANGE

	if err := stream.Send(removeTargetReq(tid)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	svc.DeleteDocument(ctx, "test", "(default)", "items/gone", nil)

	resumed := resumeTargetWithToken(whereQueryTarget(tid, "items", "active", pbBool(true)), token)
	if err := stream.Send(addTargetReq(resumed)); err != nil {
		t.Fatalf("Send resume: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second) // ADD, DocumentDelete, CURRENT, NO_CHANGE
	mid := assertResumeReplayFrames(t, rs)
	if len(mid) != 1 {
		t.Fatalf("replay emitted %d frames (%v), want only the delete", len(mid), mid)
	}
	dd := mid[0].GetDocumentDelete()
	if dd == nil || dd.Document != listenParent+"/items/gone" {
		t.Fatalf("replayed %v, want DocumentDelete for gone", mid[0])
	}
	if !containsTargetID(dd.RemovedTargetIds, tid) {
		t.Fatalf("removed_target_ids = %v, want [%d]", dd.RemovedTargetIds, tid)
	}
}

// TestListenReplayLegacyUnboundToken asserts that the legacy 8-byte bare
// sequence (still used by the stream-wide NO_CHANGE token) is accepted: it
// carries no target identity, so it replays against the added target, filtered
// by that target's query.
func TestListenReplayLegacyUnboundToken(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const tid int32 = 35
	svc.CreateDocument(ctx, "test", "(default)", "items", "join", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	rs := recvN(t, stream, 4, 3*time.Second) // ADD, join, CURRENT, NO_CHANGE
	// The stream-wide token is the legacy 8-byte sequence.
	nc := rs[3].GetTargetChange()
	if nc == nil || len(nc.ResumeToken) != 8 {
		t.Fatalf("NO_CHANGE token = %v, want the 8-byte stream-wide sequence", nc)
	}
	token := nc.ResumeToken

	if err := stream.Send(removeTargetReq(tid)); err != nil {
		t.Fatalf("remove target: %v", err)
	}
	recvN(t, stream, 1, 3*time.Second) // REMOVE

	resumed := resumeTargetWithToken(whereQueryTarget(tid, "items", "active", pbBool(true)), token)
	if err := stream.Send(addTargetReq(resumed)); err != nil {
		t.Fatalf("Send resume: %v", err)
	}
	// An unbound token can never trigger a scope-change reset; the replay is
	// simply empty because nothing matched the query after the token.
	rs = recvN(t, stream, 3, 3*time.Second) // ADD, CURRENT, NO_CHANGE
	assertResumeReplayFrames(t, rs)
	if rs[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatalf("expected CURRENT immediately after ADD, got %v", rs[1])
	}
}

func TestListenMultiTargetAggregatesDocumentChangeRemovedTargetIds(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const (
		docTargetID   int32 = 1
		queryTargetID int32 = 2
	)
	leavePath := listenParent + "/items/leave"
	svc.CreateDocument(ctx, "test", "(default)", "items", "leave", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	if err := stream.Send(addTargetReq(documentTarget(docTargetID, leavePath))); err != nil {
		t.Fatalf("send document target: %v", err)
	}
	recvN(t, stream, 4, 3*time.Second) // ADD, leave, CURRENT, NO_CHANGE

	if err := stream.Send(addTargetReq(whereQueryTarget(queryTargetID, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("send query target: %v", err)
	}
	recvN(t, stream, 4, 3*time.Second) // ADD, leave, CURRENT, NO_CHANGE

	// leave stops matching the query's where predicate but is still present, so
	// it stays in the document target and leaves the query target.
	svc.PatchDocument(ctx, "test", "(default)", "items/leave", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	}, nil, nil)

	rs := recvN(t, stream, 2, 3*time.Second) // DocumentChange + NO_CHANGE
	dc := rs[0].GetDocumentChange()
	if dc == nil {
		t.Fatalf("expected a single DocumentChange, got %v", rs[0])
	}
	if dc.Document.Name != leavePath {
		t.Fatalf("DocumentChange for %q, want %q", dc.Document.Name, leavePath)
	}
	if len(dc.TargetIds) != 1 || dc.TargetIds[0] != docTargetID {
		t.Fatalf("target_ids = %v, want [%d]", dc.TargetIds, docTargetID)
	}
	if len(dc.RemovedTargetIds) != 1 || dc.RemovedTargetIds[0] != queryTargetID {
		t.Fatalf("removed_target_ids = %v, want [%d]", dc.RemovedTargetIds, queryTargetID)
	}
	if tc := rs[1].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[1])
	}
}

// TestListenMultiTargetAggregatesDocumentRemove asserts that when a document
// leaves the result set of every target watching it, the epoch emits one
// DocumentRemove whose removed_target_ids aggregates all of them.
func TestListenMultiTargetAggregatesDocumentRemove(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const (
		t1 int32 = 1
		t2 int32 = 2
	)
	leavePath := listenParent + "/items/leave"
	svc.CreateDocument(ctx, "test", "(default)", "items", "leave", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	for _, tid := range []int32{t1, t2} {
		if err := stream.Send(addTargetReq(whereQueryTarget(tid, "items", "active", pbBool(true)))); err != nil {
			t.Fatalf("send query target %d: %v", tid, err)
		}
		recvN(t, stream, 4, 3*time.Second) // ADD, leave, CURRENT, NO_CHANGE
	}

	svc.PatchDocument(ctx, "test", "(default)", "items/leave", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(false),
	}, nil, nil)

	rs := recvN(t, stream, 2, 3*time.Second) // DocumentRemove + NO_CHANGE
	dr := rs[0].GetDocumentRemove()
	if dr == nil {
		t.Fatalf("expected a single DocumentRemove, got %v", rs[0])
	}
	if dr.Document != leavePath {
		t.Fatalf("DocumentRemove for %q, want %q", dr.Document, leavePath)
	}
	want := []int32{t1, t2}
	if len(dr.RemovedTargetIds) != len(want) {
		t.Fatalf("removed_target_ids = %v, want %v", dr.RemovedTargetIds, want)
	}
	for i, id := range want {
		if dr.RemovedTargetIds[i] != id {
			t.Fatalf("removed_target_ids = %v, want %v", dr.RemovedTargetIds, want)
		}
	}
	if tc := rs[1].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[1])
	}
}

// TestListenMultiTargetAggregatesDocumentDelete asserts that deleting a
// document watched by both a document target and a matching query target
// emits one DocumentDelete whose removed_target_ids aggregates both targets.
func TestListenMultiTargetAggregatesDocumentDelete(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const (
		docTargetID   int32 = 1
		queryTargetID int32 = 2
	)
	onePath := listenParent + "/items/one"
	svc.CreateDocument(ctx, "test", "(default)", "items", "one", map[string]*firestorestore.Value{
		"active": firestorestore.BoolVal(true),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	if err := stream.Send(addTargetReq(documentTarget(docTargetID, onePath))); err != nil {
		t.Fatalf("send document target: %v", err)
	}
	recvN(t, stream, 4, 3*time.Second) // ADD, one, CURRENT, NO_CHANGE

	if err := stream.Send(addTargetReq(whereQueryTarget(queryTargetID, "items", "active", pbBool(true)))); err != nil {
		t.Fatalf("send query target: %v", err)
	}
	recvN(t, stream, 4, 3*time.Second) // ADD, one, CURRENT, NO_CHANGE

	svc.DeleteDocument(ctx, "test", "(default)", "items/one", nil)

	rs := recvN(t, stream, 2, 3*time.Second) // DocumentDelete + NO_CHANGE
	dd := rs[0].GetDocumentDelete()
	if dd == nil {
		t.Fatalf("expected a single DocumentDelete, got %v", rs[0])
	}
	if dd.Document != onePath {
		t.Fatalf("DocumentDelete for %q, want %q", dd.Document, onePath)
	}
	want := []int32{docTargetID, queryTargetID}
	if len(dd.RemovedTargetIds) != len(want) {
		t.Fatalf("removed_target_ids = %v, want %v", dd.RemovedTargetIds, want)
	}
	for i, id := range want {
		if dd.RemovedTargetIds[i] != id {
			t.Fatalf("removed_target_ids = %v, want %v", dd.RemovedTargetIds, want)
		}
	}
	if tc := rs[1].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[1])
	}
}

// TestListenUnresumableTokenEmitsReset asserts that a target whose resume token
// cannot be honored is reset (TargetChange_RESET) before its fresh snapshot,
// instead of silently falling back.
func TestListenUnresumableTokenEmitsReset(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	svc.CreateDocument(ctx, "test", "(default)", "items", "a", map[string]*firestorestore.Value{
		"v": firestorestore.StringVal("1"),
	})

	lctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Listen(lctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	rt := queryTarget(1, "items")
	rt.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: []byte("bad")}
	if err := stream.Send(addTargetReq(rt)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rs := recvN(t, stream, 5, 3*time.Second) // ADD, RESET, a, CURRENT, NO_CHANGE
	if rs[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatalf("expected ADD, got %v", rs[0])
	}
	if tc := rs[1].GetTargetChange(); tc == nil || tc.TargetChangeType != firestorepb.TargetChange_RESET {
		t.Fatalf("expected RESET, got %v", rs[1])
	}
	if dc := rs[2].GetDocumentChange(); dc == nil || dc.Document.Name != listenParent+"/items/a" {
		t.Fatalf("expected snapshot DocumentChange for a, got %v", rs[2])
	}
	if rs[3].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatalf("expected CURRENT, got %v", rs[3])
	}
	if rs[4].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE, got %v", rs[4])
	}
}

// fakeListenServer captures the frames handleChange emits without a real gRPC
// stream, so the exactly-once dedupe can be exercised deterministically (the
// race it closes depends on goroutine scheduling and cannot be reproduced
// reliably through a live stream).
type fakeListenServer struct {
	grpc.ServerStream
	mu   sync.Mutex
	sent []*firestorepb.ListenResponse
}

func (f *fakeListenServer) Send(r *firestorepb.ListenResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeListenServer) Recv() (*firestorepb.ListenRequest, error) { return nil, io.EOF }

func (f *fakeListenServer) Context() context.Context { return context.Background() }

func (f *fakeListenServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// TestListenHandleChangeDropsAlreadyDeliveredSeq pins the exactly-once dedupe: a
// live event at or below a target's delivered cursor was already represented in
// its snapshot or resume replay and must not be emitted again; an event above
// the cursor must be delivered and advance the cursor.
func TestListenHandleChangeDropsAlreadyDeliveredSeq(t *testing.T) {
	store := firestorestore.NewMemoryStore()
	providerSvc := firestoreprovider.New(store, nil).Service
	grpcSvc := NewService(providerSvc, "test")

	fake := &fakeListenServer{}
	const tid int32 = 1
	docName := listenParent + "/items/x"
	lt := &listenTarget{
		documents: []string{docName},
		seq:       5,
		view:      map[string]struct{}{docName: {}},
	}
	ls := &listenSession{
		srv:          grpcSvc,
		stream:       fake,
		targets:      map[int32]*listenTarget{tid: lt},
		lastReadTime: time.Now(),
	}

	newDoc := func(v string) *firestorestore.Document {
		return &firestorestore.Document{
			Name:   docName,
			Fields: map[string]*firestorestore.Value{"v": firestorestore.StringVal(v)},
		}
	}

	// A write that raced the snapshot/replay is at or below the cursor: dropped.
	ls.handleChange(firestoreprovider.ChangeEvent{Seq: 5, Name: docName, Doc: newDoc("dup")})
	if got := fake.count(); got != 0 {
		t.Fatalf("event at the cursor delivered %d frames, want 0", got)
	}

	// A genuinely new event is delivered (DocumentChange + NO_CHANGE).
	ls.handleChange(firestoreprovider.ChangeEvent{Seq: 6, Name: docName, Doc: newDoc("new")})
	if got := fake.count(); got == 0 {
		t.Fatal("event above the cursor was not delivered")
	}
	if lt.seq != 6 {
		t.Fatalf("target seq = %d, want 6", lt.seq)
	}
}
