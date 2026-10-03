package grpc

import (
	"context"
	"strings"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubIAM records which handler served a call, keyed by the resource name the
// handler claims to own.
type stubIAM struct {
	iampb.UnimplementedIAMPolicyServer
	prefix string
}

func (s *stubIAM) Owns(resource string) bool { return strings.HasPrefix(resource, s.prefix) }

func (s *stubIAM) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return &iampb.Policy{Version: 1}, nil
}

func (s *stubIAM) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return &iampb.Policy{Version: 1}, nil
}

func (s *stubIAM) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return &iampb.TestIamPermissionsResponse{Permissions: req.GetPermissions()}, nil
}

func TestIAMRouterDispatchesByOwnership(t *testing.T) {
	router := NewIAMRouter(&stubIAM{prefix: "projects/p/topics/"}, &stubIAM{prefix: "projects/p/locations/"})

	ctx := context.Background()

	// A topic is owned by the first handler.
	if _, err := router.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/p/topics/t"}); err != nil {
		t.Fatalf("GetIamPolicy topic: %v", err)
	}
	// A KMS resource is owned by the second handler.
	if _, err := router.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: "projects/p/locations/global/keyRings/kr"}); err != nil {
		t.Fatalf("SetIamPolicy keyring: %v", err)
	}
	if _, err := router.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: "projects/p/locations/global/keyRings/kr", Permissions: []string{"a"}}); err != nil {
		t.Fatalf("TestIamPermissions keyring: %v", err)
	}

	// An unrecognized resource is rejected.
	if _, err := router.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/p/secrets/s"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetIamPolicy unknown err = %v, want InvalidArgument", err)
	}
}

// TestSharedIAMPolicyEtagMismatchMapsToAborted locks in the etag-OCC contract
// for the shared IAM policy envelope: a stale-etag setIamPolicy is google.rpc
// ABORTED on the gRPC path, not an incidental mapping through the HTTP 409
// fallback.
func TestSharedIAMPolicyEtagMismatchMapsToAborted(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()

	if _, err := policy.Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
		"policy": map[string]any{"bindings": []any{}},
	}); err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	_, err := policy.Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
		"policy": map[string]any{"etag": "stale", "bindings": []any{}},
	})
	if err == nil {
		t.Fatal("expected etag-mismatch error")
	}
	if got := status.Code(GRPCStatus(err)); got != codes.Aborted {
		t.Fatalf("GRPCStatus(etag mismatch) = %v, want Aborted", got)
	}
}
