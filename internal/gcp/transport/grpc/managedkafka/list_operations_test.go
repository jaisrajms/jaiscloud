package managedkafka

import (
	"context"
	"testing"

	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestListOperations verifies the shared Operations ListRegistry surface: a
// location parent lists the service's persisted operations, a parent that holds
// none or is not a location parent declines so the shared service can answer,
// and a non-empty filter fails loud rather than returning unfiltered results.
func TestListOperations(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	parent := "projects/proj/locations/us-central1"

	op, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    parent,
		ClusterId: "c1",
		Cluster:   &managedkafkapb.Cluster{},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	page, handled, err := s.ListOperations(ctx, parent, 0, "", "")
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if !handled {
		t.Fatalf("ListOperations handled = false, want true for the owning parent")
	}
	if n := len(page.GetOperations()); n != 1 {
		t.Fatalf("ListOperations returned %d operations, want 1", n)
	}
	if got := page.GetOperations()[0].GetName(); got != op.GetName() {
		t.Fatalf("listed operation = %q, want %q", got, op.GetName())
	}

	// A location with no operations declines so the shared service (or another
	// service) can answer when the request is not endpoint-scoped.
	if _, handled, err := s.ListOperations(ctx, "projects/proj/locations/europe-west1", 0, "", ""); err != nil || handled {
		t.Fatalf("empty location: handled=%v err=%v, want declined", handled, err)
	}
	// A resource name (not a location parent) declines.
	if _, handled, err := s.ListOperations(ctx, parent+"/clusters/c1", 0, "", ""); err != nil || handled {
		t.Fatalf("resource name: handled=%v err=%v, want declined", handled, err)
	}
	// A non-empty filter fails loud.
	if _, _, err := s.ListOperations(ctx, parent, 0, "", "done=true"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("filter err = %v, want InvalidArgument", err)
	}
}
