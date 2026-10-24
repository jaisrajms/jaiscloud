package resourcemanager

import (
	"context"
	"strings"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
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

// newAsyncGRPCService returns a gRPC service whose core uses the opt-in async
// LRO mode with a 30s delay.
func newAsyncGRPCService() *Service {
	coreSvc := core.NewService(store.NewMemoryResourceStore(),
		core.WithKnownProjects("proj", nil),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}),
	)
	return NewService(coreSvc, "proj")
}

func TestProjectLifecycleRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	op, err := s.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: "grpc-proj-1234", DisplayName: "Grpc Proj"},
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if !op.GetDone() || op.GetName() == "" {
		t.Fatalf("create operation = %+v, want done with a name", op)
	}
	if got := op.GetMetadata().GetTypeUrl(); !strings.HasSuffix(got, "CreateProjectMetadata") {
		t.Errorf("metadata type = %q, want CreateProjectMetadata", got)
	}
	var created resourcemanagerpb.Project
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unpack create response: %v", err)
	}
	if created.GetProjectId() != "grpc-proj-1234" || created.GetState() != resourcemanagerpb.Project_ACTIVE {
		t.Errorf("created = %+v, want ACTIVE grpc-proj-1234", &created)
	}

	got, err := s.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: "projects/grpc-proj-1234"})
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if got.GetDisplayName() != "Grpc Proj" {
		t.Errorf("displayName = %q, want Grpc Proj", got.GetDisplayName())
	}

	// ListProjects ignores the required parent and returns the created project.
	list, err := s.ListProjects(ctx, &resourcemanagerpb.ListProjectsRequest{Parent: "organizations/123"})
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(list.GetProjects()) == 0 {
		t.Fatal("ListProjects returned no projects")
	}

	// A duplicate create is AlreadyExists.
	if _, err := s.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: "grpc-proj-1234"},
	}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("duplicate create err = %v, want AlreadyExists", err)
	}

	// Delete marks DELETE_REQUESTED; a second delete is idempotent (v3).
	del, err := s.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: "projects/grpc-proj-1234"})
	if err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}
	var deleted resourcemanagerpb.Project
	if err := del.GetResponse().UnmarshalTo(&deleted); err != nil {
		t.Fatalf("unpack delete response: %v", err)
	}
	if deleted.GetState() != resourcemanagerpb.Project_DELETE_REQUESTED {
		t.Errorf("deleted state = %v, want DELETE_REQUESTED", deleted.GetState())
	}
	if _, err := s.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: "projects/grpc-proj-1234"}); err != nil {
		t.Fatalf("second DeleteProject must be idempotent: %v", err)
	}

	// The project is hidden from the default list, then restored by undelete.
	list, err = s.ListProjects(ctx, &resourcemanagerpb.ListProjectsRequest{})
	if err != nil {
		t.Fatalf("ListProjects after delete: %v", err)
	}
	if protoHasProject(list, "grpc-proj-1234") {
		t.Error("DELETE_REQUESTED project must be omitted from the default list")
	}
	und, err := s.UndeleteProject(ctx, &resourcemanagerpb.UndeleteProjectRequest{Name: "projects/grpc-proj-1234"})
	if err != nil {
		t.Fatalf("UndeleteProject: %v", err)
	}
	var restored resourcemanagerpb.Project
	if err := und.GetResponse().UnmarshalTo(&restored); err != nil {
		t.Fatalf("unpack undelete response: %v", err)
	}
	if restored.GetState() != resourcemanagerpb.Project_ACTIVE {
		t.Errorf("restored state = %v, want ACTIVE", restored.GetState())
	}
}

func TestProjectLifecycleErrors(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	if _, err := s.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: "bad"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("invalid id err = %v, want InvalidArgument", err)
	}
	if _, err := s.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: "projects/never-created"}); status.Code(err) != codes.NotFound {
		t.Errorf("delete unknown err = %v, want NotFound", err)
	}
	if _, err := s.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: "projects/a/b"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("delete malformed name err = %v, want InvalidArgument", err)
	}
}

func TestResolveOperationAsyncSettles(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	s := newAsyncGRPCService()
	op, err := s.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: "async-proj-123"},
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if op.GetDone() {
		t.Fatal("async create must return done=false")
	}

	resolved, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation handled=%v err=%v", handled, err)
	}
	if resolved.GetDone() {
		t.Error("operation must still be pending before the delay")
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err = s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation(after delay) handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() {
		t.Fatal("operation must settle after the delay")
	}
	var proj resourcemanagerpb.Project
	if err := resolved.GetResponse().UnmarshalTo(&proj); err != nil {
		t.Fatalf("unpack settled response: %v", err)
	}
	if proj.GetProjectId() != "async-proj-123" {
		t.Errorf("settled response projectId = %q", proj.GetProjectId())
	}

	// Names this service does not own are declined.
	if _, handled, _ := s.ResolveOperation(ctx, "projects/p/locations/l/operations/x"); handled {
		t.Error("location-scoped name must not be handled")
	}
	if _, handled, err := s.ResolveOperation(ctx, "operations/missing"); handled || err != nil {
		t.Errorf("unknown id handled=%v err=%v, want false/nil", handled, err)
	}
}

func TestSearchUpdateMoveProject(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	// Seed a project to administer.
	if _, err := s.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: "admin-proj-123", DisplayName: "Admin Proj", Parent: "organizations/111"},
	}); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// UpdateProject: display_name + labels via the field mask.
	up, err := s.UpdateProject(ctx, &resourcemanagerpb.UpdateProjectRequest{
		Project: &resourcemanagerpb.Project{
			Name: "projects/admin-proj-123", DisplayName: "Renamed Proj",
			Labels: map[string]string{"env": "prod"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateProject: %v", err)
	}
	if !up.GetDone() {
		t.Fatalf("update operation = %+v, want done", up)
	}
	if got := up.GetMetadata().GetTypeUrl(); !strings.HasSuffix(got, "UpdateProjectMetadata") {
		t.Errorf("update metadata type = %q, want UpdateProjectMetadata", got)
	}
	var updated resourcemanagerpb.Project
	if err := up.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("unpack update response: %v", err)
	}
	if updated.GetDisplayName() != "Renamed Proj" || updated.GetLabels()["env"] != "prod" {
		t.Errorf("updated = %+v, want renamed with env=prod", &updated)
	}

	// MoveProject reparents the project.
	mv, err := s.MoveProject(ctx, &resourcemanagerpb.MoveProjectRequest{
		Name: "projects/admin-proj-123", DestinationParent: "folders/222",
	})
	if err != nil {
		t.Fatalf("MoveProject: %v", err)
	}
	if got := mv.GetMetadata().GetTypeUrl(); !strings.HasSuffix(got, "MoveProjectMetadata") {
		t.Errorf("move metadata type = %q, want MoveProjectMetadata", got)
	}
	var moved resourcemanagerpb.Project
	if err := mv.GetResponse().UnmarshalTo(&moved); err != nil {
		t.Fatalf("unpack move response: %v", err)
	}
	if moved.GetParent() != "folders/222" {
		t.Errorf("moved parent = %q, want folders/222", moved.GetParent())
	}

	// SearchProjects finds the project by id.
	res, err := s.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{Query: "id:admin-proj-123"})
	if err != nil {
		t.Fatalf("SearchProjects: %v", err)
	}
	if len(res.GetProjects()) != 1 || res.GetProjects()[0].GetProjectId() != "admin-proj-123" {
		t.Errorf("search = %+v, want the one admin project", res.GetProjects())
	}

	// Errors: invalid query, missing update name, unknown project, bad move dest.
	if _, err := s.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{Query: "bogus"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("invalid query err = %v, want InvalidArgument", err)
	}
	if _, err := s.UpdateProject(ctx, &resourcemanagerpb.UpdateProjectRequest{Project: &resourcemanagerpb.Project{DisplayName: "No Name"}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("missing update name err = %v, want InvalidArgument", err)
	}
	if _, err := s.UpdateProject(ctx, &resourcemanagerpb.UpdateProjectRequest{
		Project: &resourcemanagerpb.Project{Name: "projects/never-created", DisplayName: "X"},
	}); status.Code(err) != codes.NotFound {
		t.Errorf("update unknown err = %v, want NotFound", err)
	}
	if _, err := s.MoveProject(ctx, &resourcemanagerpb.MoveProjectRequest{Name: "projects/admin-proj-123", DestinationParent: "projects/1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad move dest err = %v, want InvalidArgument", err)
	}
	// An empty name must not fall back to the default project.
	if _, err := s.MoveProject(ctx, &resourcemanagerpb.MoveProjectRequest{DestinationParent: "folders/1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty move name err = %v, want InvalidArgument", err)
	}
	// A `*` update mask applies every mutable field.
	star, err := s.UpdateProject(ctx, &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: "projects/admin-proj-123", DisplayName: "Star Proj", Labels: map[string]string{"star": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("UpdateProject(*): %v", err)
	}
	var starred resourcemanagerpb.Project
	if err := star.GetResponse().UnmarshalTo(&starred); err != nil {
		t.Fatalf("unpack star response: %v", err)
	}
	if starred.GetDisplayName() != "Star Proj" || starred.GetLabels()["star"] != "yes" {
		t.Errorf("star update = %+v, want Star Proj with star=yes", &starred)
	}
}

func protoHasProject(resp *resourcemanagerpb.ListProjectsResponse, id string) bool {
	for _, p := range resp.GetProjects() {
		if p.GetProjectId() == id {
			return true
		}
	}
	return false
}
