package dataproc

import (
	"context"
	"strings"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// The per-collection ResourceStore types that hold a Dataproc resource's IAM
// policy. One type per collection keeps a cluster's policy distinct from a job
// or workflow template that happens to share its region and id.
const (
	ClusterPolicyType          = "gcp_dataproc_cluster_policy"
	JobPolicyType              = "gcp_dataproc_job_policy"
	WorkflowTemplatePolicyType = "gcp_dataproc_workflow_template_policy"
)

// IAMKind identifies one of the three Dataproc collections that publish the
// google.iam.v1.IAMPolicy mixin in the v1 Discovery document:
// projects/{p}/regions/{r}/{clusters|jobs|workflowTemplates}/{id}.
type IAMKind string

const (
	IAMCluster          IAMKind = "clusters"
	IAMJob              IAMKind = "jobs"
	IAMWorkflowTemplate IAMKind = "workflowTemplates"
)

// IAMResource is a parsed regional Dataproc IAM target.
type IAMResource struct {
	Project string
	Region  string
	Kind    IAMKind
	ID      string
}

// Key is the ResourceStore id (relative to the project) under which the
// resource's policy is stored. Like the Metastore and Eventarc policy planes it
// is the region-prefixed relative name, so the shared policy store is keyed
// consistently.
func (r IAMResource) Key() string { return r.Region + "/" + r.ID }

// PolicyType is the ResourceStore type holding this collection's policies.
func (r IAMResource) PolicyType() string {
	switch r.Kind {
	case IAMCluster:
		return ClusterPolicyType
	case IAMJob:
		return JobPolicyType
	default:
		return WorkflowTemplatePolicyType
	}
}

// ParseIAMResource parses a canonical Dataproc resource name
// projects/{p}/regions/{r}/{collection}/{id}. ok is false for anything else
// (locations-scoped names, the autoscaling-policy and operations collections,
// malformed paths). The three collections here are the ones the provider's
// google_dataproc_{cluster,job}_iam_* resources target.
func ParseIAMResource(name string) (IAMResource, bool) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "regions" {
		return IAMResource{}, false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return IAMResource{}, false
	}
	// A custom-method suffix (":getIamPolicy") is not part of the resource name.
	if strings.Contains(name, ":") {
		return IAMResource{}, false
	}
	var kind IAMKind
	switch parts[4] {
	case string(IAMCluster):
		kind = IAMCluster
	case string(IAMJob):
		kind = IAMJob
	case string(IAMWorkflowTemplate):
		kind = IAMWorkflowTemplate
	default:
		return IAMResource{}, false
	}
	return IAMResource{Project: parts[1], Region: parts[3], Kind: kind, ID: parts[5]}, true
}

// InvalidIAMResource builds the canonical InvalidArgument error reported when a
// name is not a supported Dataproc resource. It is exported so both transports
// report the same code.
func InvalidIAMResource() error {
	return model.NewProviderError("InvalidArgument", "invalid dataproc IAM resource name", 400)
}

// iamTarget parses an IAM resource name and verifies the resource exists. The
// existence check runs BEFORE the policy plane is loaded or written, so a
// missing cluster/job/template is NotFound for all three verbs and never
// creates a policy row. It reads the store directly (not GetCluster/GetJob) so
// an IAM read never advances the lazy cluster/job state machine.
func (s *Service) iamTarget(ctx context.Context, resource string) (IAMResource, error) {
	r, ok := ParseIAMResource(resource)
	if !ok {
		return IAMResource{}, InvalidIAMResource()
	}
	var err error
	switch r.Kind {
	case IAMCluster:
		_, err = s.store.GetCluster(ctx, r.Project, r.Region, r.ID)
	case IAMJob:
		_, err = s.store.GetJob(ctx, r.Project, r.Region, r.ID)
	case IAMWorkflowTemplate:
		_, err = s.store.GetWorkflowTemplate(ctx, r.Project, r.Region, r.ID)
	}
	if err != nil {
		return IAMResource{}, mapErr(err)
	}
	return r, nil
}

// GetIamPolicy returns the stored IAM policy for a cluster, job or workflow
// template, or the empty default policy when none was set.
func (s *Service) GetIamPolicy(ctx context.Context, resource string) (policy.Policy, error) {
	if s.resources == nil {
		return policy.Policy{}, InvalidIAMResource()
	}
	r, err := s.iamTarget(ctx, resource)
	if err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, r.Project, r.PolicyType(), r.Key()), nil
}

// SetIamPolicy stores a resource's IAM policy (etag OCC).
func (s *Service) SetIamPolicy(ctx context.Context, resource string, body map[string]any) (policy.Policy, error) {
	if s.resources == nil {
		return policy.Policy{}, InvalidIAMResource()
	}
	r, err := s.iamTarget(ctx, resource)
	if err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, r.Project, r.PolicyType(), r.Key(), body)
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

// purgeIAMPolicy drops a deleted resource's stored policy. Real GCP binds the
// policy to the resource's lifetime, so a resource re-created under the same
// name starts with the empty default instead of inheriting the old grants.
func (s *Service) purgeIAMPolicy(ctx context.Context, r IAMResource) {
	if s.resources == nil {
		return
	}
	_ = s.resources.Delete(ctx, r.Project, store.GlobalRegion, r.PolicyType(), r.Key())
}
