package firestore

import (
	"context"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// listenDocTarget opens a Listen stream on one document and drains the
// snapshot handshake (ADD, the document itself, CURRENT and the trailing
// NO_CHANGE) so the caller observes only post-subscription events.
func listenDocTarget(t *testing.T, client firestorepb.FirestoreClient, doc string) (firestorepb.Firestore_ListenClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream, err := client.Listen(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Listen: %v", err)
	}
	if err := stream.Send(&firestorepb.ListenRequest{
		Database: "projects/test/databases/(default)",
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: &firestorepb.Target{
			TargetId:   7,
			TargetType: &firestorepb.Target_Documents{Documents: &firestorepb.Target_DocumentsTarget{Documents: []string{doc}}},
		}},
	}); err != nil {
		cancel()
		t.Fatalf("Send add target: %v", err)
	}
	sawCurrent := false
	for i := 0; i < 10; i++ {
		resp, err := recvWithTimeout(stream, 3*time.Second)
		if err != nil {
			cancel()
			t.Fatalf("draining snapshot: %v", err)
		}
		tc := resp.GetTargetChange()
		if tc == nil {
			continue
		}
		if tc.GetTargetChangeType() == firestorepb.TargetChange_CURRENT {
			sawCurrent = true
		}
		if sawCurrent && tc.GetTargetChangeType() == firestorepb.TargetChange_NO_CHANGE {
			return stream, cancel
		}
	}
	cancel()
	t.Fatal("snapshot handshake never reached CURRENT")
	return nil, nil
}

// TestBatchWriteNotifiesListeners pins the gRPC half of documents:batchWrite's
// listener fan-out: a write the service applies is reported to a Listen
// subscriber (real Firestore does this), and the empty-mask no-op is not.
func TestBatchWriteNotifiesListeners(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	docPath := writeParent + "/items/item1"
	if _, err := svc.CreateDocument(ctx, "test", "(default)", "items", "item1", map[string]*firestorestore.Value{
		"a": firestorestore.IntVal(1),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	stream, cancel := listenDocTarget(t, client, docPath)
	defer cancel()

	// A real update through batchWrite → DocumentChange carrying the new field.
	resp, err := client.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: "projects/test/databases/(default)",
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
				Name:   docPath,
				Fields: map[string]*firestorepb.Value{"a": intVal(2)},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("BatchWrite update: %v", err)
	}
	if code := resp.Status[0].Code; code != 0 {
		t.Fatalf("BatchWrite update status = %v", resp.Status[0])
	}
	firstUpdateTime := resp.WriteResults[0].UpdateTime.AsTime()

	change := awaitDocumentChange(t, stream, 3*time.Second)
	if change == nil {
		t.Fatal("batchWrite update did not notify the listener")
	}
	if got := change.Document.Fields["a"].GetIntegerValue(); got != 2 {
		t.Errorf("DocumentChange field a = %d, want 2", got)
	}

	// The empty-mask no-op → no DocumentChange, and the write result echoes the
	// stored update_time.
	resp, err = client.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: "projects/test/databases/(default)",
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
				Name:   docPath,
				Fields: map[string]*firestorepb.Value{"a": intVal(99)},
			}},
			UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{}},
		}},
	})
	if err != nil {
		t.Fatalf("BatchWrite no-op: %v", err)
	}
	if code := resp.Status[0].Code; code != 0 {
		t.Fatalf("BatchWrite no-op status = %v", resp.Status[0])
	}
	if got := resp.WriteResults[0].UpdateTime.AsTime(); !got.Equal(firstUpdateTime) {
		t.Errorf("no-op write result updateTime = %v, want the stored %v", got, firstUpdateTime)
	}
	if change := awaitDocumentChange(t, stream, 400*time.Millisecond); change != nil {
		t.Errorf("empty-mask no-op notified the listener: %v", change.Document.Name)
	}

	stored, err := svc.GetDocument(ctx, docPath, nil, nil)
	if err != nil {
		t.Fatalf("get after no-op: %v", err)
	}
	if v, _ := stored.Fields["a"].AsInt64(); v != 2 {
		t.Errorf("no-op stored a = %d, want 2", v)
	}
}

// awaitDocumentChange consumes Listen responses until a DocumentChange arrives
// or the timeout expires; ordinary TargetChange traffic (NO_CHANGE) is skipped.
func awaitDocumentChange(t *testing.T, stream firestorepb.Firestore_ListenClient, timeout time.Duration) *firestorepb.DocumentChange {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		resp, err := recvWithTimeout(stream, remaining)
		if err != nil {
			return nil
		}
		if dc := resp.GetDocumentChange(); dc != nil {
			return dc
		}
	}
}
