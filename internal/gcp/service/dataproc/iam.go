package dataproc

import (
	"context"
	"strings"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
)

// WorkflowTemplatePolicyType is the shared ResourceStore type that holds a
// workflow template's IAM policy. One type for the templates collection keeps
// their policies distinct from any other Dataproc resource's.
const WorkflowTemplatePolicyType = "gcp_dataproc_workflow_template_policy"

// IAMResource is a parsed workflow-template IAM target. The Dataproc Discovery
// surface exposes getIamPolicy/setIamPolicy/testIamPermissions on
// projects/{p}/regions/{r}/workflowTemplates/{id} only.
type IAMResource struct {
	Project  string
	Region   string
	Template string
}

// Key is the ResourceStore id (relative to the project) under which the
// template's policy is stored. Like the Metastore and Eventarc policy planes it
// is the region-prefixed relative name, so the shared policy store is keyed
// consistently.
func (r IAMResource) Key() string { return r.Region + "/" + r.Template }

// ParseIAMResource parses a canonical workflow-template resource name
// projects/{p}/regions/{r}/workflowTemplates/{id}. ok is false for anything
// else (clusters/jobs have no IAM surface here).
func ParseIAMResource(name string) (IAMResource, bool) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "regions" || parts[4] != "workflowTemplates" {
		return IAMResource{}, false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return IAMResource{}, false
	}
	// A custom-method suffix (":getIamPolicy") is not part of the resource name.
	if strings.Contains(name, ":") {
		return IAMResource{}, false
	}
	return IAMResource{Project: parts[1], Region: parts[3], Template: parts[5]}, true
}

// InvalidIAMResource builds the canonical InvalidArgument error reported when a
// name is not a workflow-template resource. It is exported so both transports
// report the same code.
func InvalidIAMResource() error {
	return model.NewProviderError("InvalidArgument", "invalid dataproc workflow template IAM resource name", 400)
}

// iamTarget parses an IAM resource name and verifies the template exists. The
// existence check runs BEFORE the policy plane is loaded or written, so a
// missing template is NotFound for all three verbs and never creates a policy
// row.
func (s *Service) iamTarget(ctx context.Context, resource string) (IAMResource, error) {
	r, ok := ParseIAMResource(resource)
	if !ok {
		return IAMResource{}, InvalidIAMResource()
	}
	if _, err := s.GetWorkflowTemplate(ctx, r.Project, r.Region, r.Template, 0); err != nil {
		return IAMResource{}, err
	}
	return r, nil
}

// GetIamPolicy returns the stored IAM policy for a workflow template, or the
// empty default policy when none was set.
func (s *Service) GetIamPolicy(ctx context.Context, resource string) (policy.Policy, error) {
	if s.resources == nil {
		return policy.Policy{}, InvalidIAMResource()
	}
	r, err := s.iamTarget(ctx, resource)
	if err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, r.Project, WorkflowTemplatePolicyType, r.Key()), nil
}

// SetIamPolicy stores a workflow template's IAM policy (etag OCC).
func (s *Service) SetIamPolicy(ctx context.Context, resource string, body map[string]any) (policy.Policy, error) {
	if s.resources == nil {
		return policy.Policy{}, InvalidIAMResource()
	}
	r, err := s.iamTarget(ctx, resource)
	if err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, r.Project, WorkflowTemplatePolicyType, r.Key(), body)
}

// TestIamPermissions reports the requested permissions the caller holds. The
// emulator does not enforce IAM, so every permission is granted.
func (s *Service) TestIamPermissions(ctx context.Context, resource string, perms []string) ([]string, error) {
	if s.resources == nil {
		return nil, InvalidIAMResource()
	}
	if _, err := s.iamTarget(ctx, resource); err != nil {
		return nil, err
	}
	return policy.TestPermissions(perms), nil
}
