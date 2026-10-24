package run

import (
	"context"
	"strings"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/store"
)

const testParent = "projects/proj/locations/us-central1"

func newTestAdapter() *Service {
	c := core.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return NewService(c, "proj")
}

func createReq(parent, id string) *runpb.CreateServiceRequest {
	return &runpb.CreateServiceRequest{
		Parent:    parent,
		ServiceId: id,
		Service: &runpb.Service{
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{Image: "nginx:latest", Ports: []*runpb.ContainerPort{{ContainerPort: 80}}}},
			},
		},
	}
}

func TestCreateGetServiceOverGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()

	op, err := s.CreateService(ctx, createReq(testParent, "svc"))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("create operation not done")
	}
	if !strings.HasPrefix(op.GetName(), testParent+"/operations/operation-run-") {
		t.Fatalf("operation name = %q", op.GetName())
	}
	if got := op.GetResponse().GetTypeUrl(); !strings.HasSuffix(got, "google.cloud.run.v2.Service") {
		t.Fatalf("response type url = %q", got)
	}

	got, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: testParent + "/services/svc"})
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if got.GetName() != testParent+"/services/svc" {
		t.Errorf("name = %q", got.GetName())
	}
	if got.GetUri() == "" || !strings.Contains(got.GetLatestReadyRevision(), "/revisions/") {
		t.Errorf("service = uri %q latestReady %q", got.GetUri(), got.GetLatestReadyRevision())
	}
	if c := got.GetTerminalCondition(); c.GetType() != "Ready" || c.GetState() != runpb.Condition_CONDITION_SUCCEEDED {
		t.Errorf("terminalCondition = %v/%v", c.GetType(), c.GetState())
	}
}

func TestGetServiceNotFoundMapsGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	_, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: testParent + "/services/missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetService missing code = %v, want NotFound", status.Code(err))
	}
}

func TestUpdateAndListRevisionsOverGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, err := s.CreateService(ctx, createReq(testParent, "svc")); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	op, err := s.UpdateService(ctx, &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name:     testParent + "/services/svc",
			Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: "nginx:1.27"}}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"template"}},
	})
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("update operation not done")
	}

	resp, err := s.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: testParent + "/services/svc"})
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if len(resp.GetRevisions()) != 2 {
		t.Fatalf("revisions = %d, want 2", len(resp.GetRevisions()))
	}
	rev, err := s.GetRevision(ctx, &runpb.GetRevisionRequest{Name: testParent + "/services/svc/revisions/svc-00002"})
	if err != nil {
		t.Fatalf("GetRevision: %v", err)
	}
	if rev.GetService() != testParent+"/services/svc" {
		t.Errorf("revision service = %q", rev.GetService())
	}
	if len(rev.GetContainers()) != 1 || rev.GetContainers()[0].GetImage() != "nginx:1.27" {
		t.Errorf("revision containers = %v", rev.GetContainers())
	}
}

func TestListRevisionsWildcardOverGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, err := s.CreateService(ctx, createReq(testParent, "svc-a")); err != nil {
		t.Fatalf("CreateService svc-a: %v", err)
	}
	if _, err := s.CreateService(ctx, createReq(testParent, "svc-b")); err != nil {
		t.Fatalf("CreateService svc-b: %v", err)
	}
	resp, err := s.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: testParent + "/services/-"})
	if err != nil {
		t.Fatalf("ListRevisions wildcard: %v", err)
	}
	if len(resp.GetRevisions()) != 2 {
		t.Fatalf("wildcard revisions = %d, want 2", len(resp.GetRevisions()))
	}
}

func TestDeleteRevisionRetiredOverGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, err := s.CreateService(ctx, createReq(testParent, "svc")); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, err := s.UpdateService(ctx, &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name:     testParent + "/services/svc",
			Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: "nginx:1.27"}}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"template"}},
	}); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	retired := testParent + "/services/svc/revisions/svc-00001"
	op, err := s.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: retired})
	if err != nil {
		t.Fatalf("DeleteRevision: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("delete operation not done")
	}
	if got := op.GetResponse().GetTypeUrl(); !strings.HasSuffix(got, "google.cloud.run.v2.Revision") {
		t.Errorf("delete response type url = %q", got)
	}
	if _, err := s.GetRevision(ctx, &runpb.GetRevisionRequest{Name: retired}); status.Code(err) != codes.NotFound {
		t.Errorf("GetRevision after delete code = %v, want NotFound", status.Code(err))
	}
}

func TestDeleteServingRevisionFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, err := s.CreateService(ctx, createReq(testParent, "svc")); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	_, err := s.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: testParent + "/services/svc/revisions/svc-00001"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteRevision serving code = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestIAMOverGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, err := s.CreateService(ctx, createReq(testParent, "svc")); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	resource := testParent + "/services/svc"
	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/run.invoker", Members: []string{"allUsers"}}}},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	pol, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/run.invoker" {
		t.Errorf("bindings = %v", pol.GetBindings())
	}
	perms, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: resource, Permissions: []string{"run.services.get"}})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(perms.GetPermissions()) != 1 {
		t.Errorf("permissions = %v", perms.GetPermissions())
	}
}

func TestResolveOperationClaimsOnlyRunIDs(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	op, err := s.CreateService(ctx, createReq(testParent, "svc"))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	got, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation = %v, %v, %v", got, handled, err)
	}
	if got.GetName() != op.GetName() {
		t.Errorf("resolved name = %q, want %q", got.GetName(), op.GetName())
	}
	if _, handled, _ := s.ResolveOperation(ctx, testParent+"/operations/functions-op"); handled {
		t.Errorf("ResolveOperation claimed a non-run operation")
	}
}

func TestListOperationsDeclinesEmptyLocation(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, handled, err := s.ListOperations(ctx, testParent, 0, "", ""); handled || err != nil {
		t.Fatalf("ListOperations empty = handled %v, err %v", handled, err)
	}
	op, err := s.CreateService(ctx, createReq(testParent, "svc"))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	resp, handled, err := s.ListOperations(ctx, testParent, 0, "", "")
	if err != nil || !handled {
		t.Fatalf("ListOperations = %v, %v", handled, err)
	}
	found := false
	for _, o := range resp.GetOperations() {
		if o.GetName() == op.GetName() {
			found = true
		}
	}
	if !found {
		t.Errorf("ListOperations did not include %q", op.GetName())
	}
	if _, handled, _ := s.ListOperations(ctx, testParent, 0, "", "name=x"); !handled {
		t.Errorf("ListOperations with filter should be handled")
	}
}

// TestServiceMutationFlagsOverGRPC proves the proto request fields reach the
// core: validateOnly is a dry run, an etag mismatch is Aborted, and
// allowMissing upserts.
func TestServiceMutationFlagsOverGRPC(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()

	// validateOnly create previews but persists nothing.
	dry := createReq(testParent, "svc")
	dry.ValidateOnly = true
	if op, err := s.CreateService(ctx, dry); err != nil {
		t.Fatalf("validateOnly CreateService: %v", err)
	} else if !op.GetDone() {
		t.Fatalf("validateOnly create operation not done")
	}
	if _, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: testParent + "/services/svc"}); status.Code(err) != codes.NotFound {
		t.Fatalf("validateOnly create persisted the service (%v)", err)
	}

	if _, err := s.CreateService(ctx, createReq(testParent, "svc")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A stale etag is Aborted and leaves the service.
	_, err := s.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: testParent + "/services/svc", Etag: "stale-token"})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale-etag DeleteService code = %v, want Aborted", status.Code(err))
	}
	if _, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: testParent + "/services/svc"}); err != nil {
		t.Fatalf("stale-etag delete removed the service: %v", err)
	}

	// validateOnly delete keeps the service.
	if _, err := s.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: testParent + "/services/svc", ValidateOnly: true}); err != nil {
		t.Fatalf("validateOnly DeleteService: %v", err)
	}
	if _, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: testParent + "/services/svc"}); err != nil {
		t.Fatalf("validateOnly delete removed the service: %v", err)
	}

	// allowMissing update upserts the missing service.
	if _, err := s.UpdateService(ctx, &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name:     testParent + "/services/upsert",
			Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: "nginx:latest"}}},
		},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"template"}},
		AllowMissing: true,
	}); err != nil {
		t.Fatalf("allowMissing UpdateService: %v", err)
	}
	if _, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: testParent + "/services/upsert"}); err != nil {
		t.Fatalf("allowMissing update did not create the service: %v", err)
	}
}

// TestListNegativePageSizeRejected pins the shared paging contract on Cloud
// Run's gRPC list surfaces: a negative pageSize is INVALID_ARGUMENT. A created
// service (and its operation) makes the list non-empty, though the cursor
// rejects before paging either way.
func TestListNegativePageSizeRejected(t *testing.T) {
	ctx := context.Background()
	s := newTestAdapter()
	if _, err := s.CreateService(ctx, createReq(testParent, "svc")); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	cases := []struct {
		name string
		call func() error
	}{
		{"ListServices", func() error {
			_, err := s.ListServices(ctx, &runpb.ListServicesRequest{Parent: testParent, PageSize: -1})
			return err
		}},
		{"ListRevisions", func() error {
			_, err := s.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: testParent + "/services/svc", PageSize: -1})
			return err
		}},
	}
	for _, tc := range cases {
		if err := tc.call(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s(pageSize=-1) = %v, want InvalidArgument", tc.name, err)
		}
	}

	// run.projects.locations.operations.list documents "If not set or set to
	// less than or equal to 0, the default page size is 100" — a documented
	// exception to the reject-negative rule, so a negative size takes the
	// default rather than raising InvalidArgument.
	if _, _, err := s.ListOperations(ctx, testParent, -1, "", ""); err != nil {
		t.Errorf("ListOperations(pageSize=-1) = %v, want nil (documented default)", err)
	}
}
