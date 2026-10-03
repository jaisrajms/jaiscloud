package resourcemanager

import (
	"context"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/store"
)

func newGRPCService() *Service {
	return NewService(core.NewService(store.NewMemoryResourceStore()), "proj")
}

func TestGetProject(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	got, err := s.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: "projects/proj"})
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if got.GetName() != "projects/proj" {
		t.Errorf("name = %q, want projects/proj", got.GetName())
	}
	if got.GetProjectId() != "proj" {
		t.Errorf("projectId = %q, want proj", got.GetProjectId())
	}
	if got.GetDisplayName() != "proj" {
		t.Errorf("displayName = %q, want proj", got.GetDisplayName())
	}
	if got.GetState() != resourcemanagerpb.Project_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", got.GetState())
	}
	if got.GetEtag() == "" {
		t.Error("etag must be non-empty")
	}
	if got.GetCreateTime() != nil {
		t.Errorf("createTime = %v, want unset", got.GetCreateTime())
	}
}

func TestGetProjectInvalidName(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	_, err := s.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: "projects/a/b"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestIamPolicyRoundTripAndOcc(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	resource := "projects/proj"

	got, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if got.GetEtag() == nil {
		t.Fatal("default policy has no etag")
	}

	got, err = s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Etag:     got.GetEtag(),
			Bindings: []*iampb.Binding{{Role: "roles/owner", Members: []string{"user:a@example.com"}}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if len(got.GetBindings()) != 1 || got.GetBindings()[0].GetRole() != "roles/owner" {
		t.Fatalf("bindings = %v", got.GetBindings())
	}

	// A stale etag is rejected with ABORTED.
	_, err = s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy:   &iampb.Policy{Etag: []byte("stale"), Bindings: []*iampb.Binding{}},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale etag err = %v, want Aborted", err)
	}
}

func TestTestIamPermissions(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	got, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    "projects/proj",
		Permissions: []string{"resourcemanager.projects.get", "resourcemanager.projects.setIamPolicy"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(got.GetPermissions()) != 2 {
		t.Fatalf("permissions = %v, want 2", got.GetPermissions())
	}
}
