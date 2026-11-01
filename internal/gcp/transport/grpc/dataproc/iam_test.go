package dataproc

import (
	"context"
	"testing"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	exprpb "google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestIAMOwns covers the router dispatch: only workflow-template names are
// claimed.
func TestIAMOwns(t *testing.T) {
	s := newTestService()
	if !s.Owns("projects/proj/regions/us-central1/workflowTemplates/t1") {
		t.Fatal("workflow template name not owned")
	}
	for _, name := range []string{
		"projects/proj/regions/us-central1/clusters/c1",
		"projects/proj/regions/us-central1/jobs/j1",
		"projects/proj/locations/us-central1/services/s1",
	} {
		if s.Owns(name) {
			t.Errorf("Owns(%q) = true, want false", name)
		}
	}
}

// TestIAMConditionRoundTrip proves an IAM condition survives the proto -> core
// -> proto path in both directions and the stored version is 3.
func TestIAMConditionRoundTrip(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	resource := "projects/proj/regions/us-central1/workflowTemplates/t1"

	if _, err := s.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent:   "projects/proj/regions/us-central1",
		Template: wfTemplateProto("t1"),
	}); err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}

	condition := &exprpb.Expr{
		Title:       "expires-soon",
		Description: "temporary access",
		Expression:  `request.time < timestamp("2027-01-01T00:00:00Z")`,
	}
	set, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Version: 3,
			Bindings: []*iampb.Binding{{
				Role:      "roles/dataproc.worker",
				Members:   []string{"user:a@example.com"},
				Condition: condition,
			}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if set.GetVersion() != 3 {
		t.Fatalf("set version = %d, want 3", set.GetVersion())
	}
	if len(set.GetBindings()) != 1 || set.GetBindings()[0].GetCondition() == nil {
		t.Fatalf("set did not return the condition: %+v", set.GetBindings())
	}

	got, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if got.GetVersion() != 3 {
		t.Fatalf("get version = %d, want 3", got.GetVersion())
	}
	if len(got.GetBindings()) != 1 {
		t.Fatalf("bindings = %+v", got.GetBindings())
	}
	b := got.GetBindings()[0]
	if b.GetRole() != "roles/dataproc.worker" || len(b.GetMembers()) != 1 || b.GetMembers()[0] != "user:a@example.com" {
		t.Fatalf("binding did not round-trip: %+v", b)
	}
	c := b.GetCondition()
	if c == nil {
		t.Fatal("condition was dropped on the get path")
	}
	if c.GetTitle() != condition.Title || c.GetExpression() != condition.Expression || c.GetDescription() != condition.Description {
		t.Fatalf("condition = %+v, want %+v", c, condition)
	}

	// A conditional set through the condition-preserving path keeps the etag
	// usable for a follow-up unconditional write.
	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Etag:     got.GetEtag(),
			Version:  3,
			Bindings: got.GetBindings(),
		},
	}); err != nil {
		t.Fatalf("SetIamPolicy with returned etag: %v", err)
	}
}

// TestIAMMissingTemplate: the three verbs report NotFound and no policy exists.
func TestIAMMissingTemplate(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	resource := "projects/proj/regions/us-central1/workflowTemplates/absent"

	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIamPolicy(missing) = %v, want NotFound", err)
	}
	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: resource, Policy: &iampb.Policy{
		Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@example.com"}}},
	}}); status.Code(err) != codes.NotFound {
		t.Fatalf("SetIamPolicy(missing) = %v, want NotFound", err)
	}
	if _, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: resource, Permissions: []string{"dataproc.workflowTemplates.get"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("TestIamPermissions(missing) = %v, want NotFound", err)
	}
}

// TestIAMWrongShape: a non-template name is InvalidArgument without an
// existence lookup.
func TestIAMWrongShape(t *testing.T) {
	s := newTestService()
	_, err := s.GetIamPolicy(context.Background(), &iampb.GetIamPolicyRequest{
		Resource: "projects/proj/regions/us-central1/clusters/c1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetIamPolicy(cluster) = %v, want InvalidArgument", err)
	}
}
