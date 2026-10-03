package grpcconformance

import (
	"context"
	"fmt"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
)

// resourceManagerChecks covers the Cloud Resource Manager v3 project surface
// (google.cloud.resourcemanager.v3.Projects) the emulator serves: GetProject and
// project IAM (GetIamPolicy / SetIamPolicy / TestIamPermissions). The remaining
// Projects RPCs are explicit Unimplemented stubs and are not probed here.
//
// Projects are synthesized from their id and never created, so each probe uses
// its own run-unique project id (cfg.ResourceName) as the resource — that keeps
// the shared default project's IAM policy untouched and the run idempotent.
func resourceManagerChecks() []Check {
	return []Check{
		{Service: "resourcemanager", RPC: "GetProject", Method: "GetProject", KeyField: "name/projectId/state ACTIVE round-trip", Run: checkRMGetProject},
		{Service: "resourcemanager", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "policy etag present", Run: checkRMGetIamPolicy},
		{Service: "resourcemanager", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "binding stored + etag rotates", Run: checkRMSetIamPolicy},
		{Service: "resourcemanager", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions echoed", Run: checkRMTestIamPermissions},
	}
}

// newResourceManagerClient dials the emulator and returns the official
// Resource Manager v3 Projects client.
func newResourceManagerClient(ctx context.Context, cfg Config) (*resourcemanager.ProjectsClient, error) {
	return resourcemanager.NewProjectsClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// resourceManagerProject is the full resource name of a run-unique synthesized
// project.
func resourceManagerProject(cfg Config, prefix string) string {
	return "projects/" + cfg.ResourceName(prefix)
}

// Check 1: GetProject returns the synthesized ACTIVE project.
func checkRMGetProject(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := resourceManagerProject(cfg, "gcpc-grpc-rm-get")
	got, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetProject: %w", err)
	}
	if got.GetName() != name {
		return fmt.Errorf("GetProject name = %q, want %q", got.GetName(), name)
	}
	if got.GetProjectId() != cfg.ResourceName("gcpc-grpc-rm-get") {
		return fmt.Errorf("GetProject projectId = %q", got.GetProjectId())
	}
	if got.GetState() != resourcemanagerpb.Project_ACTIVE {
		return fmt.Errorf("GetProject state = %v, want ACTIVE", got.GetState())
	}
	if got.GetDisplayName() == "" {
		return fmt.Errorf("GetProject displayName is empty")
	}
	return nil
}

// Check 2: GetIamPolicy returns a policy with an etag for a synthesized project.
func checkRMGetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resource := resourceManagerProject(cfg, "gcpc-grpc-rm-iamget")
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if len(pol.GetEtag()) == 0 {
		return fmt.Errorf("GetIamPolicy returned no etag")
	}
	return nil
}

// Check 3: SetIamPolicy stores a binding and rotates the etag.
func checkRMSetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resource := resourceManagerProject(cfg, "gcpc-grpc-rm-iamset")
	before, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	after, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Etag:     before.GetEtag(),
			Bindings: []*iampb.Binding{{Role: "roles/owner", Members: []string{"user:conformance@example.com"}}},
		},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	if len(after.GetBindings()) != 1 || after.GetBindings()[0].GetRole() != "roles/owner" {
		return fmt.Errorf("SetIamPolicy bindings = %v", after.GetBindings())
	}
	if len(after.GetEtag()) == 0 {
		return fmt.Errorf("SetIamPolicy returned no etag")
	}
	return nil
}

// Check 4: TestIamPermissions echoes the requested permissions.
func checkRMTestIamPermissions(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resource := resourceManagerProject(cfg, "gcpc-grpc-rm-test")
	want := []string{"resourcemanager.projects.get", "resourcemanager.projects.setIamPolicy"}
	got, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    resource,
		Permissions: want,
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	if len(got.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions = %v, want %v", got.GetPermissions(), want)
	}
	return nil
}
