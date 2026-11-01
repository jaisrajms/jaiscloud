package grpcconformance

import (
	"context"
	"fmt"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	exprpb "google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// dataprocIAMChecks covers the google.iam.v1.IAMPolicy mixin for Dataproc
// workflow templates. The pinned dataproc proto defines no IAM RPCs, so real GCP
// serves the mixin through the standalone IAMPolicy service; these probes dial
// it with a workflow-template resource name to prove the shared router
// dispatches dataproc IAM (and that the condition survives the protojson
// transcode, which the older hand-written converters dropped). They are
// attributed to the "iam" fidelity service and back themselves with a
// run-unique workflow template.
func dataprocIAMChecks() []Check {
	return []Check{
		{Service: "iam", RPC: "GetIamPolicy (dataproc)", Method: "GetIamPolicy", KeyField: "workflow template policy present, empty by default", Run: checkDPIAMGetPolicy},
		{Service: "iam", RPC: "SetIamPolicy (dataproc)", Method: "SetIamPolicy", KeyField: "conditional binding round-trips at version 3", Run: checkDPIAMSetPolicy},
		{Service: "iam", RPC: "TestIamPermissions (dataproc)", Method: "TestIamPermissions", KeyField: "requested permissions echoed back for a workflow template", Run: checkDPIAMTestPermissions},
	}
}

// dataprocIAMResource ensures a run-unique workflow template exists and returns
// its resource name, the IAMPolicy resource these probes target.
func dataprocIAMResource(ctx context.Context, cfg Config) (string, error) {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("new workflow template client: %w", err)
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpiam")
	if _, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent: dataprocWorkflowParent(cfg), Template: dataprocWorkflowTemplate(id),
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return "", fmt.Errorf("create workflow template: %w", err)
	}
	return dataprocWorkflowParent(cfg) + "/workflowTemplates/" + id, nil
}

func checkDPIAMGetPolicy(ctx context.Context, cfg Config) error {
	resource, err := dataprocIAMResource(ctx, cfg)
	if err != nil {
		return err
	}
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", resource, err)
	}
	if pol == nil {
		return fmt.Errorf("GetIamPolicy(%s) returned a nil policy", resource)
	}
	return nil
}

func checkDPIAMSetPolicy(ctx context.Context, cfg Config) error {
	resource, err := dataprocIAMResource(ctx, cfg)
	if err != nil {
		return err
	}
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	const (
		role   = "roles/dataproc.worker"
		member = "user:probe@example.com"
	)
	condition := &exprpb.Expr{
		Title:       "probe-window",
		Description: "time bounded",
		Expression:  `request.time < timestamp("2027-01-01T00:00:00Z")`,
	}
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: role, Members: []string{member}, Condition: condition}},
		},
	}); err != nil {
		return fmt.Errorf("SetIamPolicy(%s): %w", resource, err)
	}
	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", resource, err)
	}
	if got.GetVersion() != 3 {
		return fmt.Errorf("GetIamPolicy(%s) version = %d, want 3", resource, got.GetVersion())
	}
	for _, b := range got.GetBindings() {
		if b.GetRole() != role {
			continue
		}
		for _, m := range b.GetMembers() {
			if m != member {
				continue
			}
			if c := b.GetCondition(); c == nil || c.GetExpression() != condition.Expression || c.GetTitle() != condition.Title {
				return fmt.Errorf("SetIamPolicy(%s) condition did not round-trip: %+v", resource, c)
			}
			return nil
		}
	}
	return fmt.Errorf("SetIamPolicy(%s) binding did not round-trip: %+v", resource, got.GetBindings())
}

func checkDPIAMTestPermissions(ctx context.Context, cfg Config) error {
	resource, err := dataprocIAMResource(ctx, cfg)
	if err != nil {
		return err
	}
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	perms := []string{"dataproc.workflowTemplates.get", "dataproc.workflowTemplates.update"}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    resource,
		Permissions: perms,
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions(%s): %w", resource, err)
	}
	if len(resp.GetPermissions()) != len(perms) {
		return fmt.Errorf("TestIamPermissions(%s) = %v, want %v", resource, resp.GetPermissions(), perms)
	}
	return nil
}
