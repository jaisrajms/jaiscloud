package workflows

import (
	"context"
	"strings"
	"testing"

	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

const source = "main:\n  steps:\n    - r:\n        return: 1\n"

func newService() *Service {
	return NewService(core.NewService(workflowsstore.NewMemoryStore()), "default-proj")
}

const workflowName = "projects/proj/locations/us-central1/workflows/wf1"

func TestCreateGetListProto(t *testing.T) {
	ctx := context.Background()
	s := newService()

	op, err := s.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     "projects/proj/locations/us-central1",
		WorkflowId: "wf1",
		Workflow: &workflowspb.Workflow{
			Description:    "hello",
			SourceCode:     &workflowspb.Workflow_SourceContents{SourceContents: source},
			Labels:         map[string]string{"k": "v"},
			CallLogLevel:   workflowspb.Workflow_LOG_ALL_CALLS,
			UserEnvVars:    map[string]string{"MY_VAR": "hello"},
			Tags:           map[string]string{"env": "test"},
			ServiceAccount: "sa@p.iam.gserviceaccount.com",
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("operation not done: %v", op)
	}
	if !strings.HasPrefix(op.GetName(), "projects/proj/locations/us-central1/operations/") {
		t.Fatalf("operation name = %q", op.GetName())
	}
	meta := &workflowspb.OperationMetadata{}
	if err := op.GetMetadata().UnmarshalTo(meta); err != nil {
		t.Fatalf("unmarshal OperationMetadata: %v", err)
	}
	if meta.GetVerb() != "create" || meta.GetTarget() != workflowName {
		t.Fatalf("metadata = %+v", meta)
	}
	created := &workflowspb.Workflow{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("unmarshal Workflow response: %v", err)
	}
	if created.GetName() != workflowName {
		t.Fatalf("created name = %q, want %q", created.GetName(), workflowName)
	}
	if created.GetState() != workflowspb.Workflow_ACTIVE {
		t.Fatalf("created state = %v, want ACTIVE", created.GetState())
	}
	if created.GetSourceContents() != source {
		t.Fatalf("created source = %q", created.GetSourceContents())
	}
	if !strings.HasPrefix(created.GetRevisionId(), "000001-") {
		t.Fatalf("created revisionId = %q", created.GetRevisionId())
	}
	if created.GetCallLogLevel() != workflowspb.Workflow_LOG_ALL_CALLS {
		t.Fatalf("created callLogLevel = %v", created.GetCallLogLevel())
	}
	if created.GetUserEnvVars()["MY_VAR"] != "hello" || created.GetTags()["env"] != "test" {
		t.Fatalf("created userEnvVars/tags lost: %+v", created)
	}

	got, err := s.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: workflowName})
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.GetRevisionId() != created.GetRevisionId() || got.GetServiceAccount() != "sa@p.iam.gserviceaccount.com" {
		t.Fatalf("GetWorkflow = %+v", got)
	}

	list, err := s.ListWorkflows(ctx, &workflowspb.ListWorkflowsRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(list.GetWorkflows()) != 1 || list.GetWorkflows()[0].GetName() != workflowName {
		t.Fatalf("ListWorkflows = %+v", list.GetWorkflows())
	}
}

// TestUpdateMaskProto verifies a masked update preserves unspecified fields and
// bumps the revision only when the source changes.
func TestUpdateMaskProto(t *testing.T) {
	ctx := context.Background()
	s := newService()

	op, err := s.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     "projects/proj/locations/us-central1",
		WorkflowId: "wf1",
		Workflow: &workflowspb.Workflow{
			Description: "orig",
			SourceCode:  &workflowspb.Workflow_SourceContents{SourceContents: source},
			Labels:      map[string]string{"k": "v"},
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	created := &workflowspb.Workflow{}
	_ = op.GetResponse().UnmarshalTo(created)

	uop, err := s.UpdateWorkflow(ctx, &workflowspb.UpdateWorkflowRequest{
		Workflow:   &workflowspb.Workflow{Name: workflowName, Description: "new"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		t.Fatalf("UpdateWorkflow: %v", err)
	}
	updated := &workflowspb.Workflow{}
	if err := uop.GetResponse().UnmarshalTo(updated); err != nil {
		t.Fatalf("unmarshal updated Workflow: %v", err)
	}
	if updated.GetDescription() != "new" {
		t.Fatalf("description = %q, want new", updated.GetDescription())
	}
	if updated.GetSourceContents() != source {
		t.Fatalf("source should be preserved: %q", updated.GetSourceContents())
	}
	if updated.GetLabels()["k"] != "v" {
		t.Fatalf("labels should be preserved: %v", updated.GetLabels())
	}
	if updated.GetRevisionId() != created.GetRevisionId() {
		t.Fatalf("revision should not change for a description-only update")
	}

	// Changing sourceContents bumps the revision.
	uop2, err := s.UpdateWorkflow(ctx, &workflowspb.UpdateWorkflowRequest{
		Workflow: &workflowspb.Workflow{
			Name:       workflowName,
			SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: "main:\n  steps:\n    - r:\n        return: 2\n"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"source_code.source_contents"}},
	})
	if err != nil {
		t.Fatalf("UpdateWorkflow source: %v", err)
	}
	updated2 := &workflowspb.Workflow{}
	_ = uop2.GetResponse().UnmarshalTo(updated2)
	if updated2.GetRevisionId() == created.GetRevisionId() {
		t.Fatalf("revision should change when sourceContents changes")
	}
}

func TestDeleteProto(t *testing.T) {
	ctx := context.Background()
	s := newService()

	if _, err := s.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     "projects/proj/locations/us-central1",
		WorkflowId: "wf1",
		Workflow:   &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: source}},
	}); err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	op, err := s.DeleteWorkflow(ctx, &workflowspb.DeleteWorkflowRequest{Name: workflowName})
	if err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}
	if !op.GetDone() || op.GetResponse() == nil {
		t.Fatalf("delete operation = %v", op)
	}
	if _, err := s.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: workflowName}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetWorkflow after delete code = %v, want NotFound", status.Code(err))
	}
}

func TestGetWorkflowMissing(t *testing.T) {
	s := newService()
	_, err := s.GetWorkflow(context.Background(), &workflowspb.GetWorkflowRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty name code = %v, want InvalidArgument", status.Code(err))
	}
	_, err = s.GetWorkflow(context.Background(), &workflowspb.GetWorkflowRequest{Name: workflowName})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing workflow code = %v, want NotFound", status.Code(err))
	}
}

// TestProjectFallsBackToDefault verifies that a parent with no project segment
// falls back to the configured default project.
func TestProjectFallsBackToDefault(t *testing.T) {
	ctx := context.Background()
	s := newService()
	op, err := s.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     "locations/us-central1",
		WorkflowId: "wf1",
		Workflow:   &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: source}},
	})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	created := &workflowspb.Workflow{}
	_ = op.GetResponse().UnmarshalTo(created)
	if created.GetName() != "projects/default-proj/locations/us-central1/workflows/wf1" {
		t.Fatalf("fallback name = %q", created.GetName())
	}
}

func TestListWorkflowRevisionsUnimplemented(t *testing.T) {
	s := newService()
	_, err := s.ListWorkflowRevisions(context.Background(), &workflowspb.ListWorkflowRevisionsRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("ListWorkflowRevisions code = %v, want Unimplemented", status.Code(err))
	}
}
