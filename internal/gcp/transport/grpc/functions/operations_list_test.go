package functions

import (
	"context"
	"testing"

	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
)

func createV2Op(t *testing.T, s *ServiceV2, id string) *longrunningpb.Operation {
	t.Helper()
	op, err := s.CreateFunction(context.Background(), &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: id,
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return op
}

// TestV2ListOperationsRegistry verifies the v2 functions gRPC service serves the
// shared google.longrunning.Operations List surface for a location parent,
// forwards the filter, and renders typed v2 operations.
func TestV2ListOperationsRegistry(t *testing.T) {
	ctx := context.Background()
	s := newTestV2()
	createV2Op(t, s, "one")
	createV2Op(t, s, "two")

	// Through the shared Operations service, as a longrunning client would call it.
	svc := grpcoperations.New(s)

	resp, err := svc.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(resp.GetOperations()) != 2 {
		t.Fatalf("ListOperations = %d ops, want 2: %+v", len(resp.GetOperations()), resp.GetOperations())
	}
	if !resp.GetOperations()[0].GetDone() || resp.GetOperations()[0].GetResponse() == nil {
		t.Fatalf("listed operation must be done with a typed response: %+v", resp.GetOperations()[0])
	}
	var fn apiv2functionspb.Function
	if err := resp.GetOperations()[0].GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("unmarshal listed response: %v", err)
	}

	// The filter is forwarded and evaluated (done=false matches nothing here).
	filtered, err := svc.ListOperations(ctx, &longrunningpb.ListOperationsRequest{
		Name:   "projects/proj/locations/us-central1",
		Filter: "done=false",
	})
	if err != nil {
		t.Fatalf("ListOperations(filter): %v", err)
	}
	if len(filtered.GetOperations()) != 0 {
		t.Fatalf("done=false = %+v, want empty", filtered.GetOperations())
	}

	// An unparseable filter fails loud.
	if _, err := svc.ListOperations(ctx, &longrunningpb.ListOperationsRequest{
		Name:   "projects/proj/locations/us-central1",
		Filter: "bogus=1",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad filter = %v, want InvalidArgument", err)
	}
}

// TestV2ListOperationsDeclinesForeignParent verifies a parent the functions
// registry does not own is declined so another registry (or the empty page)
// answers. A location parent is shared with other location-scoped services, so
// an empty functions location is declined too rather than shadowing them.
func TestV2ListOperationsDeclinesForeignParent(t *testing.T) {
	s := newTestV2()
	for _, parent := range []string{
		"operations",
		"",
		"projects/proj/locations/us-central1/operations",
		"projects/proj/locations/us-central1", // valid shape, but no operations here
	} {
		if _, handled, err := s.ListOperations(context.Background(), parent, 0, "", ""); handled || err != nil {
			t.Errorf("parent %q: handled=%v err=%v, want declined", parent, handled, err)
		}
	}
}
