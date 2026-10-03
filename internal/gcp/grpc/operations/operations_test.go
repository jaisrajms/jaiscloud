package operations

import (
	"context"
	"net"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// operationsTestService dials a real in-process gRPC server backed by the
// Operations service and returns the generated client.
func operationsTestService(t *testing.T, resolvers ...Resolver) (longrunningpb.OperationsClient, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	longrunningpb.RegisterOperationsServer(srv, New(resolvers...))
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
	return longrunningpb.NewOperationsClient(conn), cleanup
}

const operationName = "projects/test/locations/global/operations/op-1"

// TestGetOperation verifies the stub reports any operation name as done.
func TestGetOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	op, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: operationName})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if op.GetName() != operationName {
		t.Errorf("GetOperation name = %q, want %q", op.GetName(), operationName)
	}
	if !op.GetDone() {
		t.Errorf("GetOperation done = false, want true")
	}
}

// TestListOperations verifies the stub returns a well-formed empty list for any
// parent rather than erroring.
func TestListOperations(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{
		Name: "projects/test/locations/global",
	})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if n := len(resp.GetOperations()); n != 0 {
		t.Errorf("ListOperations returned %d operations, want 0", n)
	}
	if resp.GetNextPageToken() != "" {
		t.Errorf("ListOperations next_page_token = %q, want empty", resp.GetNextPageToken())
	}
}

// TestDeleteOperation verifies the no-op delete succeeds.
func TestDeleteOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: operationName}); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
}

// TestCancelOperation verifies the no-op cancel succeeds.
func TestCancelOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: operationName}); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}
}

// TestWaitOperation verifies the synchronous stub returns the requested name
// already done (there is never an in-flight operation to block on).
func TestWaitOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	op, err := client.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: operationName})
	if err != nil {
		t.Fatalf("WaitOperation: %v", err)
	}
	if op.GetName() != operationName {
		t.Errorf("WaitOperation name = %q, want %q", op.GetName(), operationName)
	}
	if !op.GetDone() {
		t.Errorf("WaitOperation done = false, want true")
	}
}

// fakeResolver owns a single operation name and delegates everything else.
type fakeResolver struct {
	name string
	op   *longrunningpb.Operation
	err  error
}

func (f fakeResolver) ResolveOperation(_ context.Context, name string) (*longrunningpb.Operation, bool, error) {
	if name != f.name {
		return nil, false, nil
	}
	return f.op, true, f.err
}

// TestGetOperationDelegatesToResolver verifies a registered resolver's in-flight
// operation is served ahead of the terminal stub.
func TestGetOperationDelegatesToResolver(t *testing.T) {
	dpName := "projects/p/regions/us-central1/operations/op-dp"
	client, cleanup := operationsTestService(t, fakeResolver{
		name: dpName,
		op:   &longrunningpb.Operation{Name: dpName, Done: false},
	})
	defer cleanup()

	op, err := client.GetOperation(context.Background(), &longrunningpb.GetOperationRequest{Name: dpName})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if op.GetName() != dpName || op.GetDone() {
		t.Fatalf("operation = %+v, want in-flight %s", op, dpName)
	}
}

// TestGetOperationResolverError verifies a resolver's own error (e.g. NotFound)
// is returned rather than silently falling through to the terminal stub.
func TestGetOperationResolverError(t *testing.T) {
	dpName := "projects/p/regions/us-central1/operations/missing"
	client, cleanup := operationsTestService(t, fakeResolver{
		name: dpName,
		err:  status.Error(codes.NotFound, "operation not found"),
	})
	defer cleanup()

	_, err := client.GetOperation(context.Background(), &longrunningpb.GetOperationRequest{Name: dpName})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetOperation err = %v, want NotFound", err)
	}
}

// TestGetOperationFallsBackToStub verifies a name no resolver owns is still
// reported terminal.
func TestGetOperationFallsBackToStub(t *testing.T) {
	client, cleanup := operationsTestService(t, fakeResolver{name: "projects/p/regions/us-central1/operations/op-dp"})
	defer cleanup()

	op, err := client.GetOperation(context.Background(), &longrunningpb.GetOperationRequest{Name: operationName})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("fallback operation = %+v, want done", op)
	}
}
