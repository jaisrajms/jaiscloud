package grpcconformance

import (
	"context"
	"fmt"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
)

// metastoreIAMChecks covers the google.iam.v1.IAMPolicy mixin for Dataproc
// Metastore resources. The pinned metastore proto defines no IAM RPCs, so real
// GCP serves the mixin through the standalone IAMPolicy service; these probes
// dial it with a metastore service resource name to prove the shared router
// dispatches metastore IAM as well as Pub/Sub/KMS/Eventarc. They are attributed
// to the "iam" fidelity service (the standalone service's cells) and back
// themselves with a run-unique metastore service.
func metastoreIAMChecks() []Check {
	return []Check{
		{Service: "iam", RPC: "GetIamPolicy (metastore)", Method: "GetIamPolicy", KeyField: "metastore resource policy present, empty by default", Run: checkMSIAMGetPolicy},
		{Service: "iam", RPC: "SetIamPolicy (metastore)", Method: "SetIamPolicy", KeyField: "metastore role+member binding round-trips", Run: checkMSIAMSetPolicy},
		{Service: "iam", RPC: "TestIamPermissions (metastore)", Method: "TestIamPermissions", KeyField: "requested permissions echoed back for a metastore resource", Run: checkMSIAMTestPermissions},
	}
}

// metastoreIAMResource ensures a run-unique metastore service exists and
// returns its resource name, the IAMPolicy resource these probes target.
func metastoreIAMResource(ctx context.Context, cfg Config) (string, error) {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("new metastore client: %w", err)
	}
	defer client.Close()
	return ensureMetastoreService(ctx, client, cfg)
}

func checkMSIAMGetPolicy(ctx context.Context, cfg Config) error {
	resource, err := metastoreIAMResource(ctx, cfg)
	if err != nil {
		return err
	}
	client, err := newMetastoreIAMPolicyClient(ctx, cfg)
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

func checkMSIAMSetPolicy(ctx context.Context, cfg Config) error {
	resource, err := metastoreIAMResource(ctx, cfg)
	if err != nil {
		return err
	}
	client, err := newMetastoreIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	const (
		role   = "roles/metastore.admin"
		member = "user:probe@example.com"
	)
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: role, Members: []string{member}}},
		},
	}); err != nil {
		return fmt.Errorf("SetIamPolicy(%s): %w", resource, err)
	}
	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", resource, err)
	}
	for _, b := range got.GetBindings() {
		if b.GetRole() != role {
			continue
		}
		for _, m := range b.GetMembers() {
			if m == member {
				return nil
			}
		}
	}
	return fmt.Errorf("SetIamPolicy(%s) binding did not round-trip: %+v", resource, got.GetBindings())
}

func checkMSIAMTestPermissions(ctx context.Context, cfg Config) error {
	resource, err := metastoreIAMResource(ctx, cfg)
	if err != nil {
		return err
	}
	client, err := newMetastoreIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	perms := []string{"metastore.services.get", "metastore.services.update"}
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
