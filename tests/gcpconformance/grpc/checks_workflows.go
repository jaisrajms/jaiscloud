package grpcconformance

import (
	"context"
	"fmt"

	workflows "cloud.google.com/go/workflows/apiv1"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// workflowsChecks covers the Cloud Workflows v1 management surface
// (google.cloud.workflows.v1.Workflows) via the official generated
// cloud.google.com/go/workflows/apiv1 client: the CRUD RPCs whose
// create/update/delete forms return a done long-running operation the client's
// Wait observes without polling. GetOperation is the shared
// google.longrunning.Operations service and ListWorkflowRevisions is an
// explicit Unimplemented stub, so neither is probed here.
//
// Every probe is self-contained and run-unique (cfg.ResourceName), so a
// long-lived emulator never sees cross-run collisions.
func workflowsChecks() []Check {
	return []Check{
		{Service: "workflows", RPC: "CreateWorkflow", Method: "CreateWorkflow", KeyField: "LRO done + ACTIVE workflow with revision", Run: checkWorkflowsCreate},
		{Service: "workflows", RPC: "GetWorkflow", Method: "GetWorkflow", KeyField: "name/state/source round-trip", Run: checkWorkflowsGet},
		{Service: "workflows", RPC: "ListWorkflows", Method: "ListWorkflows", KeyField: "created workflow present in list", Run: checkWorkflowsList},
		{Service: "workflows", RPC: "UpdateWorkflow", Method: "UpdateWorkflow", KeyField: "LRO done + description updated, source preserved", Run: checkWorkflowsUpdate},
		{Service: "workflows", RPC: "DeleteWorkflow", Method: "DeleteWorkflow", KeyField: "LRO done + subsequent get NotFound", Run: checkWorkflowsDelete},
	}
}

// workflowLocation is the region the probe workflows are deployed to.
const workflowLocation = "us-central1"

// workflowSource is a deterministic trivial workflow.
const workflowSource = "main:\n  steps:\n    - r:\n        return: 1\n"

// newWorkflowsClient dials the emulator and returns the official generated
// Workflows client.
func newWorkflowsClient(ctx context.Context, cfg Config) (*workflows.Client, error) {
	return workflows.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// workflowParent is the project/location parent the probe workflows live under.
func workflowParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, workflowLocation)
}

// workflowName is the run-unique full resource name of a probe workflow.
func workflowName(cfg Config, prefix string) string {
	return fmt.Sprintf("%s/workflows/%s", workflowParent(cfg), cfg.ResourceName(prefix))
}

// createWorkflow deploys a run-unique workflow over gRPC and returns the
// terminal workflow from the create LRO.
func createWorkflow(ctx context.Context, client *workflows.Client, cfg Config, prefix, description string) (*workflowspb.Workflow, error) {
	op, err := client.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     workflowParent(cfg),
		WorkflowId: cfg.ResourceName(prefix),
		Workflow: &workflowspb.Workflow{
			Description: description,
			SourceCode:  &workflowspb.Workflow_SourceContents{SourceContents: workflowSource},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("CreateWorkflow: %w", err)
	}
	w, err := op.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("CreateWorkflow Wait: %w", err)
	}
	return w, nil
}

// Check 1: CreateWorkflow returns a done LRO whose response is an ACTIVE
// workflow with a revision and the source echoed back.
func checkWorkflowsCreate(ctx context.Context, cfg Config) error {
	client, err := newWorkflowsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	w, err := createWorkflow(ctx, client, cfg, "gcpc-grpc-wf-create", "hello")
	if err != nil {
		return err
	}
	if w.GetState() != workflowspb.Workflow_ACTIVE {
		return fmt.Errorf("CreateWorkflow state = %v, want ACTIVE", w.GetState())
	}
	if w.GetName() != workflowName(cfg, "gcpc-grpc-wf-create") {
		return fmt.Errorf("CreateWorkflow name = %q, want %q", w.GetName(), workflowName(cfg, "gcpc-grpc-wf-create"))
	}
	if w.GetRevisionId() == "" {
		return fmt.Errorf("CreateWorkflow revisionId is empty")
	}
	if w.GetSourceContents() != workflowSource {
		return fmt.Errorf("CreateWorkflow sourceContents = %q", w.GetSourceContents())
	}
	return nil
}

// Check 2: GetWorkflow returns the created workflow.
func checkWorkflowsGet(ctx context.Context, cfg Config) error {
	client, err := newWorkflowsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createWorkflow(ctx, client, cfg, "gcpc-grpc-wf-get", "get")
	if err != nil {
		return err
	}
	got, err := client.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("GetWorkflow: %w", err)
	}
	if got.GetName() != created.GetName() {
		return fmt.Errorf("GetWorkflow name = %q, want %q", got.GetName(), created.GetName())
	}
	if got.GetState() != workflowspb.Workflow_ACTIVE {
		return fmt.Errorf("GetWorkflow state = %v, want ACTIVE", got.GetState())
	}
	if got.GetSourceContents() != workflowSource {
		return fmt.Errorf("GetWorkflow sourceContents = %q", got.GetSourceContents())
	}
	return nil
}

// Check 3: ListWorkflows includes the created workflow.
func checkWorkflowsList(ctx context.Context, cfg Config) error {
	client, err := newWorkflowsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createWorkflow(ctx, client, cfg, "gcpc-grpc-wf-list", "list")
	if err != nil {
		return err
	}
	it := client.ListWorkflows(ctx, &workflowspb.ListWorkflowsRequest{Parent: workflowParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListWorkflows did not include %q", created.GetName())
		}
		if err != nil {
			return fmt.Errorf("ListWorkflows: %w", err)
		}
		if got.GetName() == created.GetName() {
			if got.GetState() != workflowspb.Workflow_ACTIVE {
				return fmt.Errorf("ListWorkflows state = %v, want ACTIVE", got.GetState())
			}
			return nil
		}
	}
}

// Check 4: UpdateWorkflow returns a done LRO whose response has the new
// description and preserves the source.
func checkWorkflowsUpdate(ctx context.Context, cfg Config) error {
	client, err := newWorkflowsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createWorkflow(ctx, client, cfg, "gcpc-grpc-wf-update", "orig")
	if err != nil {
		return err
	}
	op, err := client.UpdateWorkflow(ctx, &workflowspb.UpdateWorkflowRequest{
		Workflow:   &workflowspb.Workflow{Name: created.GetName(), Description: "updated"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateWorkflow: %w", err)
	}
	updated, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("UpdateWorkflow Wait: %w", err)
	}
	if updated.GetDescription() != "updated" {
		return fmt.Errorf("UpdateWorkflow description = %q, want updated", updated.GetDescription())
	}
	if updated.GetSourceContents() != workflowSource {
		return fmt.Errorf("UpdateWorkflow sourceContents = %q, want preserved", updated.GetSourceContents())
	}
	return nil
}

// Check 5: DeleteWorkflow returns a done LRO and the workflow is then gone.
func checkWorkflowsDelete(ctx context.Context, cfg Config) error {
	client, err := newWorkflowsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createWorkflow(ctx, client, cfg, "gcpc-grpc-wf-delete", "delete")
	if err != nil {
		return err
	}
	op, err := client.DeleteWorkflow(ctx, &workflowspb.DeleteWorkflowRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("DeleteWorkflow: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteWorkflow Wait: %w", err)
	}
	if _, err := client.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetWorkflow after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}
