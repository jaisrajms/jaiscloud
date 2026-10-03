package serviceusage

import (
	"context"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/grpc/operations"
	"jaiscloud/internal/gcp/lro"
	core "jaiscloud/internal/gcp/service/serviceusage"
	"jaiscloud/internal/store"
)

// newAsyncRegistryService builds a Service Usage gRPC service over an async core
// and enables one API, returning the service and the in-flight operation.
func newAsyncRegistryService(t *testing.T) (*Service, core.Operation) {
	t.Helper()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	s := NewService(core.NewService(store.NewMemoryResourceStore(),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "proj")
	_, op, err := s.core.EnableAPI(context.Background(), "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	return s, op
}

// TestRegistryListOperations verifies the generic Operations registry lists
// Service Usage's top-level operations, rendering the in-flight op and the
// settled typed response after the delay.
func TestRegistryListOperations(t *testing.T) {
	s, op := newAsyncRegistryService(t)
	ctx := context.Background()

	resp, handled, err := s.ListOperations(ctx, "operations", 0, "", "")
	if err != nil || !handled {
		t.Fatalf("ListOperations: handled=%v err=%v", handled, err)
	}
	if len(resp.GetOperations()) != 1 || resp.GetOperations()[0].GetName() != op.Name {
		t.Fatalf("ListOperations = %+v, want the in-flight op %s", resp, op.Name)
	}
	if resp.GetOperations()[0].GetDone() {
		t.Fatalf("listed operation must be in flight: %+v", resp.GetOperations()[0])
	}

	clock.SetGlobalClock(clock.FixedClock{T: time.Date(2026, 5, 1, 12, 0, 30, 0, time.UTC)})
	resp, handled, err = s.ListOperations(ctx, "", 0, "", "")
	if err != nil || !handled {
		t.Fatalf("ListOperations after delay: handled=%v err=%v", handled, err)
	}
	if len(resp.GetOperations()) != 1 || !resp.GetOperations()[0].GetDone() {
		t.Fatalf("settled listed operation = %+v, want done with a response", resp.GetOperations())
	}
}

// TestRegistryListOperationsDeclinesForeignParent verifies a location-scoped
// parent is not claimed by Service Usage (its operations have no parent).
func TestRegistryListOperationsDeclinesForeignParent(t *testing.T) {
	s, _ := newAsyncRegistryService(t)
	if _, handled, err := s.ListOperations(context.Background(), "projects/p/locations/us/operations", 0, "", ""); handled || err != nil {
		t.Fatalf("foreign parent: handled=%v err=%v, want declined", handled, err)
	}
}

// TestRegistryListOperationsFilterFailsLoud verifies Service Usage does not
// silently ignore a filter the emulator cannot evaluate for its operations.
func TestRegistryListOperationsFilterFailsLoud(t *testing.T) {
	s, _ := newAsyncRegistryService(t)
	_, handled, err := s.ListOperations(context.Background(), "operations", 0, "", "done=true")
	if !handled || status.Code(err) != codes.Unimplemented {
		t.Fatalf("filtered ListOperations: handled=%v err=%v, want handled Unimplemented", handled, err)
	}
}

// TestRegistryCancelAndDelete verifies a Service Usage operation cancels and
// deletes, and the deleted operation is no longer resolvable, while ids Service
// Usage does not own are declined (the Functions ownership handshake).
func TestRegistryCancelAndDelete(t *testing.T) {
	s, op := newAsyncRegistryService(t)
	ctx := context.Background()

	if handled, err := s.CancelOperation(ctx, op.Name); !handled || err != nil {
		t.Fatalf("CancelOperation: handled=%v err=%v", handled, err)
	}
	if _, handled, _ := s.ResolveOperation(ctx, op.Name); !handled {
		t.Fatalf("cancelled operation must still resolve")
	}
	if handled, err := s.DeleteOperation(ctx, op.Name); !handled || err != nil {
		t.Fatalf("DeleteOperation: handled=%v err=%v", handled, err)
	}
	if _, handled, _ := s.ResolveOperation(ctx, op.Name); handled {
		t.Fatalf("deleted operation must no longer resolve")
	}
}

// TestRegistryDeclinesUnownedOperation verifies unknown and foreign names are
// declined rather than reported NotFound, so the shared service keeps its
// lenient/ordering contract.
func TestRegistryDeclinesUnownedOperation(t *testing.T) {
	s, _ := newAsyncRegistryService(t)
	ctx := context.Background()

	if handled, err := s.CancelOperation(ctx, "operations/missing"); handled || err != nil {
		t.Fatalf("cancel unknown: handled=%v err=%v, want declined", handled, err)
	}
	if handled, err := s.DeleteOperation(ctx, "operations/missing"); handled || err != nil {
		t.Fatalf("delete unknown: handled=%v err=%v, want declined", handled, err)
	}
	if handled, err := s.DeleteOperation(ctx, "projects/p/locations/us/operations/x"); handled || err != nil {
		t.Fatalf("delete foreign: handled=%v err=%v, want declined", handled, err)
	}
}

// TestStrictGenericServiceThroughRegistry verifies the wiring contract: with the
// registry registered, the generic service lists Service Usage operations and,
// in strict (async) mode, reports an unowned name NotFound; the lenient default
// keeps the terminal stub.
func TestStrictGenericServiceThroughRegistry(t *testing.T) {
	s, op := newAsyncRegistryService(t)
	ctx := context.Background()

	ops := operations.New(s)
	ops.SetStrict(true)

	if _, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name}); err != nil {
		t.Fatalf("GetOperation(owned) = %v", err)
	}
	if _, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetOperation(unknown) = %v, want NotFound", err)
	}
	resp, err := ops.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: "operations"})
	if err != nil || len(resp.GetOperations()) != 1 {
		t.Fatalf("ListOperations = %+v, %v; want one operation", resp, err)
	}
	if _, err := ops.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: op.Name}); err != nil {
		t.Fatalf("DeleteOperation(owned) = %v", err)
	}
	if _, err := ops.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("DeleteOperation(unknown) = %v, want NotFound", err)
	}

	lenient := operations.New(s)
	if got, err := lenient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/missing"}); err != nil || !got.GetDone() {
		t.Fatalf("lenient GetOperation(unknown) = %v, %v; want terminal", got, err)
	}
}
