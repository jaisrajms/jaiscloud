package dataproc

import (
	"context"
	"testing"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func wfTemplateProto(id string) *dataprocpb.WorkflowTemplate {
	return &dataprocpb.WorkflowTemplate{
		Id: id,
		Placement: &dataprocpb.WorkflowTemplatePlacement{
			Placement: &dataprocpb.WorkflowTemplatePlacement_ManagedCluster{
				ManagedCluster: &dataprocpb.ManagedCluster{ClusterName: "c"},
			},
		},
		Jobs: []*dataprocpb.OrderedJob{{
			StepId:  "a",
			JobType: &dataprocpb.OrderedJob_PysparkJob{PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://b/a.py"}},
		}},
	}
}

func TestWorkflowTemplateGRPCRoundTrip(t *testing.T) {
	s := newTestService()
	ctx := context.Background()

	created, err := s.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent:   "projects/proj/regions/us-central1",
		Template: wfTemplateProto("t1"),
	})
	if err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}
	if created.GetName() != "projects/proj/regions/us-central1/workflowTemplates/t1" || created.GetVersion() != 1 {
		t.Fatalf("created = %+v", created)
	}

	got, err := s.GetWorkflowTemplate(ctx, &dataprocpb.GetWorkflowTemplateRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetWorkflowTemplate: %v", err)
	}
	if len(got.GetJobs()) != 1 || got.GetJobs()[0].GetStepId() != "a" {
		t.Fatalf("jobs = %+v", got.GetJobs())
	}

	listed, err := s.ListWorkflowTemplates(ctx, &dataprocpb.ListWorkflowTemplatesRequest{Parent: "projects/proj/regions/us-central1"})
	if err != nil {
		t.Fatalf("ListWorkflowTemplates: %v", err)
	}
	if len(listed.GetTemplates()) != 1 {
		t.Fatalf("templates = %d, want 1", len(listed.GetTemplates()))
	}

	updated, err := s.UpdateWorkflowTemplate(ctx, &dataprocpb.UpdateWorkflowTemplateRequest{
		Template: &dataprocpb.WorkflowTemplate{Name: created.GetName(), Version: 1, Id: "t1", Placement: wfTemplateProto("t1").Placement, Jobs: wfTemplateProto("t1").Jobs},
	})
	if err != nil {
		t.Fatalf("UpdateWorkflowTemplate: %v", err)
	}
	if updated.GetVersion() != 2 {
		t.Fatalf("version = %d, want 2", updated.GetVersion())
	}

	if _, err := s.DeleteWorkflowTemplate(ctx, &dataprocpb.DeleteWorkflowTemplateRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("DeleteWorkflowTemplate: %v", err)
	}
	_, err = s.GetWorkflowTemplate(ctx, &dataprocpb.GetWorkflowTemplateRequest{Name: created.GetName()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Get after delete = %v, want NotFound", err)
	}
}

func TestInstantiateInlineWorkflowGRPC(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	op, err := s.InstantiateInlineWorkflowTemplate(ctx, &dataprocpb.InstantiateInlineWorkflowTemplateRequest{
		Parent:   "projects/proj/regions/us-central1",
		Template: wfTemplateProto("ignored"),
	})
	if err != nil {
		t.Fatalf("InstantiateInlineWorkflowTemplate: %v", err)
	}
	if op.GetDone() {
		t.Fatal("instantiate returned a done operation; want in-flight")
	}
	var meta dataprocpb.WorkflowMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetState() != dataprocpb.WorkflowMetadata_RUNNING {
		t.Fatalf("workflow state = %v, want RUNNING", meta.GetState())
	}

	for i := 0; i < 32; i++ {
		polled, handled, err := s.ResolveOperation(ctx, op.GetName())
		if err != nil || !handled {
			t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
		}
		if !polled.GetDone() {
			continue
		}
		var done dataprocpb.WorkflowMetadata
		if err := polled.GetMetadata().UnmarshalTo(&done); err != nil {
			t.Fatalf("done metadata: %v", err)
		}
		if done.GetState() != dataprocpb.WorkflowMetadata_DONE {
			t.Fatalf("workflow state = %v, want DONE", done.GetState())
		}
		nodes := done.GetGraph().GetNodes()
		if len(nodes) != 1 || nodes[0].GetState() != dataprocpb.WorkflowNode_COMPLETED {
			t.Fatalf("nodes = %+v", nodes)
		}
		return
	}
	t.Fatal("workflow operation did not complete")
}
