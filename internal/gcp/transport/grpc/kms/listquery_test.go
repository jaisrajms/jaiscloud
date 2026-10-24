package kms

import (
	"context"
	"testing"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGRPCListFilterOrderBy verifies the gRPC KMS list methods honor the
// documented filter/order_by request fields, in parity with the REST adapter.
func TestGRPCListFilterOrderBy(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"kr-a", "kr-b", "cloud-kr"} {
		createKeyRing(t, client, id)
	}

	// Filter on the full resource name.
	resp, err := client.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{
		Parent: "projects/test/locations/global", Filter: `name:"cloud"`,
	})
	if err != nil {
		t.Fatalf("filtered ListKeyRings: %v", err)
	}
	if len(resp.GetKeyRings()) != 1 || resp.GetKeyRings()[0].GetName() != "projects/test/locations/global/keyRings/cloud-kr" {
		t.Fatalf("filter name:\"cloud\" = %v, want only cloud-kr", resp.GetKeyRings())
	}
	if resp.GetTotalSize() != 1 {
		t.Errorf("TotalSize = %d, want the matched count 1", resp.GetTotalSize())
	}

	// order_by name desc.
	resp, err = client.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{
		Parent: "projects/test/locations/global", OrderBy: "name desc",
	})
	if err != nil {
		t.Fatalf("ordered ListKeyRings: %v", err)
	}
	got := resp.GetKeyRings()
	if len(got) != 3 || got[0].GetName() != "projects/test/locations/global/keyRings/kr-b" ||
		got[2].GetName() != "projects/test/locations/global/keyRings/cloud-kr" {
		t.Fatalf("order_by name desc = %v, want kr-b, kr-a, cloud-kr", got)
	}

	// An unsupported filter is INVALID_ARGUMENT.
	_, err = client.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{
		Parent: "projects/test/locations/global", Filter: `bogus=1`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid filter code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}
