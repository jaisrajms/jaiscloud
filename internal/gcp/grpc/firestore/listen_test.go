package firestore

import (
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
	if responses[1].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
		t.Fatalf("expected NO_CHANGE after delete, got %v", responses[1])
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
	if len(tc.ResumeToken) != 8 {
		t.Fatalf("expected 8-byte resume token, got %d bytes", len(tc.ResumeToken))
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

	responses := recvN(t, stream, 4, 3*time.Second) // ADD, a (full snapshot), CURRENT, NO_CHANGE
	if responses[0].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_ADD {
		t.Fatal("expected ADD")
	}
	dc := responses[1].GetDocumentChange()
	if dc == nil || dc.Document.Fields["v"].GetStringValue() != "1" {
		t.Fatal("expected full snapshot with item a")
	}
	if responses[2].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_CURRENT {
		t.Fatal("expected CURRENT")
	}
	if responses[3].GetTargetChange().GetTargetChangeType() != firestorepb.TargetChange_NO_CHANGE {
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
	}
	if _, ok := DecodeResumeToken(nil); ok {
		t.Fatal("expected decode failure for nil")
	}
	if _, ok := DecodeResumeToken([]byte("short")); ok {
		t.Fatal("expected decode failure for wrong length")
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
