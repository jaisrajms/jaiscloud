package serviceusage

import (
	"context"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/grpc/operations"
	"jaiscloud/internal/gcp/lro"
	fncore "jaiscloud/internal/gcp/service/functions"
	core "jaiscloud/internal/gcp/service/serviceusage"
	fnstore "jaiscloud/internal/gcp/store/functions"
	grpcfunctions "jaiscloud/internal/gcp/transport/grpc/functions"
	"jaiscloud/internal/store"
)

// TestResolveOperationAsync verifies the generic Operations resolver serves a
// Service Usage top-level operation, omits the response while in flight, settles
// it after the delay, and declines names it does not own.
func TestResolveOperationAsync(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	ctx := context.Background()
	s := NewService(core.NewService(store.NewMemoryResourceStore(),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "proj")

	op, err := s.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: "projects/proj/services/run.googleapis.com"})
	if err != nil {
		t.Fatalf("EnableService: %v", err)
	}
	if op.GetDone() {
		t.Fatalf("async enable must be in flight: %v", op)
	}
	if op.GetResult() != nil {
		t.Fatalf("in-flight operation must carry no result: %v", op.GetResult())
	}

	resolved, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if resolved.GetDone() {
		t.Fatalf("resolved operation before delay must be in flight")
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err = s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation after delay: handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() || resolved.GetResponse() == nil {
		t.Fatalf("resolved operation = %v, want done with a response", resolved)
	}
	var resp serviceusagepb.EnableServiceResponse
	if err := resolved.GetResponse().UnmarshalTo(&resp); err != nil {
		t.Fatalf("unmarshal EnableServiceResponse: %v", err)
	}
	if resp.GetService().GetName() != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("resolved service name = %q", resp.GetService().GetName())
	}
	if resp.GetService().GetState() != serviceusagepb.State_ENABLED {
		t.Fatalf("resolved service state = %v", resp.GetService().GetState())
	}

	// An unknown top-level id is declined (functions owns the namespace).
	if _, handled, err := s.ResolveOperation(ctx, "operations/missing"); handled || err != nil {
		t.Fatalf("unknown id: handled=%v err=%v, want declined", handled, err)
	}
	// A location-scoped name is not this service's shape.
	if _, handled, _ := s.ResolveOperation(ctx, "projects/p/locations/us/operations/x"); handled {
		t.Fatalf("location-scoped name must be declined")
	}
}

// TestOperationsResolverOrdering pins the load-bearing registration order: the
// Service Usage resolver is consulted before Cloud Functions v1, so a Service
// Usage operation resolves while an unknown top-level id still yields Functions'
// NotFound (not the shared terminal stub).
func TestOperationsResolverOrdering(t *testing.T) {
	ctx := context.Background()
	suCore := core.NewService(store.NewMemoryResourceStore())
	suSvc := NewService(suCore, "proj")
	fnCore := fncore.NewService(fnstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		fncore.WithBlobs(blobfs.NewMemoryBlobStore()))
	fnSvc := grpcfunctions.NewService(fnCore, "proj", "")
	ops := operations.New(suSvc, fnSvc)

	if _, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown top-level op = %v, want NotFound (functions owns it)", err)
	}

	_, op, err := suCore.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	got, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name})
	if err != nil {
		t.Fatalf("GetOperation serviceusage op: %v", err)
	}
	if !got.GetDone() || got.GetName() != op.Name {
		t.Fatalf("GetOperation = %v, want done %s", got, op.Name)
	}
}
