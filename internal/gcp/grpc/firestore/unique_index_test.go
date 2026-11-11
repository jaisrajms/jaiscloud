package firestore

import (
	"context"
	"net"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/store"
)

func strPb(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

// newUniqueDataPlaneClient starts the data-plane gRPC service on an Enterprise
// edition database with a unique index on users.email, returning the client and
// the database's documents parent. It mirrors listenTestClient but attaches a
// resource store, which the index and database control plane need.
func newUniqueDataPlaneClient(t *testing.T) (firestorepb.FirestoreClient, string, func()) {
	t.Helper()
	docs := firestorestore.NewMemoryStore()
	svc := firestoreprovider.New(docs, store.NewMemoryResourceStore()).Service
	grpcSvc := NewService(svc, "test")

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
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	client := firestorepb.NewFirestoreClient(conn)

	ctx := context.Background()
	const db = "uniqdb"
	if _, _, err := svc.CreateDatabaseDef(ctx, "test", db, firestoreprovider.DatabaseDef{
		LocationID:      "us-central1",
		DatabaseEdition: "ENTERPRISE",
	}); err != nil {
		cleanup()
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, _, err := svc.CreateIndexDef(ctx, "test", db, "users", firestoreprovider.IndexDef{
		QueryScope: "COLLECTION",
		Unique:     true,
		Fields:     []firestoreprovider.IndexField{{FieldPath: "email", Order: "ASCENDING"}},
	}); err != nil {
		cleanup()
		t.Fatalf("CreateIndexDef: %v", err)
	}
	return client, "projects/test/databases/" + db + "/documents", cleanup
}

func uniqueEmailWrite(name, email string) *firestorepb.Write {
	return &firestorepb.Write{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
		Name:   name,
		Fields: map[string]*firestorepb.Value{"email": strPb(email)},
	}}}
}

// TestUniqueIndexGRPCCreateAndCommit pins the gRPC transport's envelope: a
// duplicate create and a duplicate commit both surface codes.AlreadyExists,
// keeping the two transports identical.
func TestUniqueIndexGRPCCreateAndCommit(t *testing.T) {
	client, parent, cleanup := newUniqueDataPlaneClient(t)
	defer cleanup()
	ctx := context.Background()
	db := parent[:len(parent)-len("/documents")]

	create := func(id string) error {
		_, err := client.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       parent,
			CollectionId: "users",
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"email": strPb("create@example.com")}},
		})
		return err
	}
	if err := create("a"); err != nil {
		t.Fatalf("create users/a: %v", err)
	}
	if err := create("b"); grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate create = %v (code %v), want AlreadyExists", err, grpcstatus.Code(err))
	}

	commit := func(id string) error {
		_, err := client.Commit(ctx, &firestorepb.CommitRequest{
			Database: db,
			Writes:   []*firestorepb.Write{uniqueEmailWrite(parent+"/users/"+id, "commit@example.com")},
		})
		return err
	}
	if err := commit("c"); err != nil {
		t.Fatalf("commit users/c: %v", err)
	}
	if err := commit("d"); grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate commit = %v (code %v), want AlreadyExists", err, grpcstatus.Code(err))
	}
}

// TestUniqueIndexGRPCBatchWriteStatus pins batchWrite's per-write contract over
// gRPC: the duplicate write reports google.rpc code 6, its neighbours succeed.
func TestUniqueIndexGRPCBatchWriteStatus(t *testing.T) {
	client, parent, cleanup := newUniqueDataPlaneClient(t)
	defer cleanup()
	ctx := context.Background()
	db := parent[:len(parent)-len("/documents")]

	resp, err := client.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: db,
		Writes: []*firestorepb.Write{
			uniqueEmailWrite(parent+"/users/a", "same@example.com"),
			uniqueEmailWrite(parent+"/users/b", "same@example.com"),
			uniqueEmailWrite(parent+"/users/c", "other@example.com"),
		},
	})
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}
	if len(resp.GetStatus()) != 3 {
		t.Fatalf("BatchWrite returned %d statuses, want 3", len(resp.GetStatus()))
	}
	wantCodes := []int32{0, 6, 0}
	for i, want := range wantCodes {
		if got := resp.GetStatus()[i].GetCode(); got != want {
			t.Fatalf("status[%d].code = %d, want %d (%v)", i, got, want, resp.GetStatus()[i])
		}
	}
	// The rejected document was not written.
	_, err = client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: parent + "/users/b"})
	if grpcstatus.Code(err) != codes.NotFound {
		t.Fatalf("users/b after its rejected write = %v (code %v), want NotFound", err, grpcstatus.Code(err))
	}
}

// TestUniqueIndexGRPCWriteStream pins the streaming Write path, which funnels
// through the same core: the duplicate batch fails the stream with
// ALREADY_EXISTS.
func TestUniqueIndexGRPCWriteStream(t *testing.T) {
	client, parent, cleanup := newUniqueDataPlaneClient(t)
	defer cleanup()
	ctx := context.Background()
	db := parent[:len(parent)-len("/documents")]

	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&firestorepb.WriteRequest{Database: db}); err != nil {
		t.Fatalf("handshake send: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("handshake recv: %v", err)
	}

	// The first batch lands; the second repeats the value and must fail.
	if err := stream.Send(&firestorepb.WriteRequest{
		StreamToken: resp.GetStreamToken(),
		Writes:      []*firestorepb.Write{uniqueEmailWrite(parent+"/users/a", "stream@example.com")},
	}); err != nil {
		t.Fatalf("first batch send: %v", err)
	}
	resp, err = stream.Recv()
	if err != nil {
		t.Fatalf("first batch recv: %v", err)
	}
	if err := stream.Send(&firestorepb.WriteRequest{
		StreamToken: resp.GetStreamToken(),
		Writes:      []*firestorepb.Write{uniqueEmailWrite(parent+"/users/b", "stream@example.com")},
	}); err != nil {
		t.Fatalf("second batch send: %v", err)
	}
	if _, err := stream.Recv(); err == nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate stream batch = %v, want AlreadyExists", err)
	}
}
