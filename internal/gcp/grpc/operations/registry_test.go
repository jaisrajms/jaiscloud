package operations

import (
	"context"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRegistry is a Registry that owns a fixed set of operation names and
// optionally claims every List parent. It records the delete/cancel calls so a
// test can assert the shared service delegated to it.
type fakeRegistry struct {
	ops        map[string]*longrunningpb.Operation
	listPage   *longrunningpb.ListOperationsResponse
	listAck    bool
	listFilter string
	cancelled  []string
	deleted    []string
}

func (f *fakeRegistry) ResolveOperation(_ context.Context, name string) (*longrunningpb.Operation, bool, error) {
	op, ok := f.ops[name]
	if !ok {
		return nil, false, nil
	}
	return op, true, nil
}

func (f *fakeRegistry) ListOperations(_ context.Context, _ string, _ int32, _, filter string) (*longrunningpb.ListOperationsResponse, bool, error) {
	if !f.listAck {
		return nil, false, nil
	}
	f.listFilter = filter
	return f.listPage, true, nil
}

func (f *fakeRegistry) CancelOperation(_ context.Context, name string) (bool, error) {
	if _, ok := f.ops[name]; !ok {
		return false, nil
	}
	f.cancelled = append(f.cancelled, name)
	return true, nil
}

func (f *fakeRegistry) DeleteOperation(_ context.Context, name string) (bool, error) {
	if _, ok := f.ops[name]; !ok {
		return false, nil
	}
	f.deleted = append(f.deleted, name)
	return true, nil
}

const registryOp = "operations/registry-op"

// TestStrictUnknownNameIsNotFound verifies the opt-in async contract: a name no
// resolver or registry owns is NotFound over Get/Wait/Cancel/Delete instead of
// the lenient terminal stub / no-op success.
func TestStrictUnknownNameIsNotFound(t *testing.T) {
	svc := New()
	svc.SetStrict(true)
	ctx := context.Background()

	if _, err := svc.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetOperation = %v, want NotFound", err)
	}
	if _, err := svc.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("WaitOperation = %v, want NotFound", err)
	}
	if _, err := svc.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("CancelOperation = %v, want NotFound", err)
	}
	if _, err := svc.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("DeleteOperation = %v, want NotFound", err)
	}
}

// TestLenientUnknownNameIsTerminal pins the default synchronous contract: an
// unowned name is still a done operation and delete/cancel still succeed.
func TestLenientUnknownNameIsTerminal(t *testing.T) {
	svc := New()
	ctx := context.Background()

	op, err := svc.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/missing"})
	if err != nil || !op.GetDone() {
		t.Fatalf("GetOperation = %v, %v; want terminal", op, err)
	}
	if _, err := svc.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: "operations/missing"}); err != nil {
		t.Fatalf("CancelOperation = %v, want no-op success", err)
	}
	if _, err := svc.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: "operations/missing"}); err != nil {
		t.Fatalf("DeleteOperation = %v, want no-op success", err)
	}
}

// TestListOperationsDelegatesToRegistry verifies a registry's page is served.
func TestListOperationsDelegatesToRegistry(t *testing.T) {
	reg := &fakeRegistry{
		listAck:  true,
		listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: registryOp, Done: true}}},
	}
	client, cleanup := operationsTestService(t, reg)
	defer cleanup()

	resp, err := client.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{Name: "operations"})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(resp.GetOperations()) != 1 || resp.GetOperations()[0].GetName() != registryOp {
		t.Fatalf("ListOperations = %+v, want the registry page", resp)
	}
}

// TestListOperationsFallsThroughToNextRegistry verifies a registry that declines
// the parent yields to the next registry, then to an empty page.
func TestListOperationsFallsThroughToNextRegistry(t *testing.T) {
	declining := &fakeRegistry{listAck: false}
	claiming := &fakeRegistry{
		listAck:  true,
		listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: registryOp}}},
	}
	client, cleanup := operationsTestService(t, declining, claiming)
	defer cleanup()

	resp, err := client.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{Name: "operations"})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(resp.GetOperations()) != 1 {
		t.Fatalf("ListOperations = %+v, want the claiming registry page", resp)
	}
}

// TestListOperationsUnownedParentIsEmpty verifies an unowned parent returns a
// well-formed empty page in both modes (never an error).
func TestListOperationsUnownedParentIsEmpty(t *testing.T) {
	svc := New(&fakeRegistry{listAck: false})
	svc.SetStrict(true)
	ctx := context.Background()

	resp, err := svc.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: "projects/p/locations/us/operations"})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(resp.GetOperations()) != 0 || resp.GetNextPageToken() != "" {
		t.Fatalf("ListOperations = %+v, want empty page", resp)
	}
}

// TestCancelOperationDelegatesToRegistry verifies a registry-owned operation is
// cancelled through the registry.
func TestCancelOperationDelegatesToRegistry(t *testing.T) {
	reg := &fakeRegistry{ops: map[string]*longrunningpb.Operation{registryOp: {Name: registryOp}}}
	svc := New(reg)
	svc.SetStrict(true)

	if _, err := svc.CancelOperation(context.Background(), &longrunningpb.CancelOperationRequest{Name: registryOp}); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}
	if len(reg.cancelled) != 1 || reg.cancelled[0] != registryOp {
		t.Fatalf("registry.cancelled = %v, want [%s]", reg.cancelled, registryOp)
	}
}

// TestCancelOperationGetOnlyResolverSucceeds verifies the Get-based fallback: an
// operation served by a Get-only resolver (no Registry) still cancels, even in
// strict mode, because it exists.
func TestCancelOperationGetOnlyResolverSucceeds(t *testing.T) {
	name := "projects/p/locations/us/operations/get-only"
	svc := New(fakeResolver{name: name, op: &longrunningpb.Operation{Name: name, Done: true}})
	svc.SetStrict(true)

	if _, err := svc.CancelOperation(context.Background(), &longrunningpb.CancelOperationRequest{Name: name}); err != nil {
		t.Fatalf("CancelOperation = %v, want success for an existing op", err)
	}
}

// TestLenientCancelDeleteResolverOwnedAbsentStaysNoOp pins the default
// synchronous contract: a resolver that owns a name but reports NotFound (like
// Cloud Functions v1's top-level resolver) must not leak that error through the
// lenient Cancel/Delete, which stay unconditional no-op successes.
func TestLenientCancelDeleteResolverOwnedAbsentStaysNoOp(t *testing.T) {
	name := "operations/functions-owned-absent"
	owner := fakeResolver{name: name, err: status.Error(codes.NotFound, "operation not found")}
	svc := New(owner)
	ctx := context.Background()

	if _, err := svc.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: name}); err != nil {
		t.Fatalf("lenient CancelOperation = %v, want no-op success", err)
	}
	if _, err := svc.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: name}); err != nil {
		t.Fatalf("lenient DeleteOperation = %v, want no-op success", err)
	}

	svc.SetStrict(true)
	if _, err := svc.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Fatalf("strict CancelOperation = %v, want NotFound", err)
	}
	if _, err := svc.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Fatalf("strict DeleteOperation = %v, want NotFound", err)
	}
}

// TestListOperationsForwardsFilter verifies the shared service forwards the
// standard filter request parameter to the owning ListRegistry instead of
// blanket-rejecting it: the registry decides whether it can evaluate it.
func TestListOperationsForwardsFilter(t *testing.T) {
	reg := &fakeRegistry{listAck: true, listPage: &longrunningpb.ListOperationsResponse{}}
	svc := New(reg)

	if _, err := svc.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{
		Name:   "operations",
		Filter: "done=true",
	}); err != nil {
		t.Fatalf("ListOperations(filter): %v", err)
	}
	if reg.listFilter != "done=true" {
		t.Fatalf("registry filter = %q, want done=true", reg.listFilter)
	}
}

// TestListOperationsReturnPartialSuccessUnimplemented verifies the unsupported
// returnPartialSuccess request parameter fails loud for every parent, matching
// the canonical Operations.List proto ("UNIMPLEMENTED if set unless explicitly
// documented otherwise").
func TestListOperationsReturnPartialSuccessUnimplemented(t *testing.T) {
	svc := New(&fakeRegistry{listAck: true, listPage: &longrunningpb.ListOperationsResponse{}})

	_, err := svc.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{
		Name:                 "operations",
		ReturnPartialSuccess: true,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("ListOperations(returnPartialSuccess) = %v, want Unimplemented", err)
	}
}

// TestListOperationsRegistryErrorPropagates verifies a ListRegistry that owns
// the parent and fails loud (e.g. Service Usage given a filter it cannot
// evaluate) surfaces its error rather than being swallowed.
func TestListOperationsRegistryErrorPropagates(t *testing.T) {
	svc := New(errorListRegistry{err: status.Error(codes.Unimplemented, "filter not supported")})

	_, err := svc.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{Name: "operations", Filter: "done=true"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("ListOperations(filter) = %v, want Unimplemented", err)
	}
}

// errorListRegistry is a ListRegistry that claims its parent and always returns
// the configured error.
type errorListRegistry struct{ err error }

func (errorListRegistry) ResolveOperation(context.Context, string) (*longrunningpb.Operation, bool, error) {
	return nil, false, nil
}

func (e errorListRegistry) ListOperations(context.Context, string, int32, string, string) (*longrunningpb.ListOperationsResponse, bool, error) {
	return nil, true, e.err
}

// TestDeleteOperationDelegatesToRegistry verifies a registry-owned operation is
// deleted through the registry.
func TestDeleteOperationDelegatesToRegistry(t *testing.T) {
	reg := &fakeRegistry{ops: map[string]*longrunningpb.Operation{registryOp: {Name: registryOp}}}
	svc := New(reg)
	svc.SetStrict(true)

	if _, err := svc.DeleteOperation(context.Background(), &longrunningpb.DeleteOperationRequest{Name: registryOp}); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
	if len(reg.deleted) != 1 || reg.deleted[0] != registryOp {
		t.Fatalf("registry.deleted = %v, want [%s]", reg.deleted, registryOp)
	}
}

// TestRegistryOwnershipFallthrough verifies a declining registry yields to the
// next resolver, so Service Usage and Cloud Functions can share a namespace.
func TestRegistryOwnershipFallthrough(t *testing.T) {
	foreign := "operations/foreign"
	declining := &fakeRegistry{ops: map[string]*longrunningpb.Operation{}}
	owned := &fakeRegistry{ops: map[string]*longrunningpb.Operation{foreign: {Name: foreign, Done: false}}}
	svc := New(declining, owned)

	op, err := svc.GetOperation(context.Background(), &longrunningpb.GetOperationRequest{Name: foreign})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if op.GetName() != foreign || op.GetDone() {
		t.Fatalf("GetOperation = %+v, want the second registry's in-flight op", op)
	}
}
