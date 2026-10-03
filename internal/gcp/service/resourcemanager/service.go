// Package resourcemanager is the transport-neutral core of Cloud Resource
// Manager (cloudresourcemanager.googleapis.com).
//
// The emulator's REST surface is the legacy v1 project API
// (cloudresourcemanager.googleapis.com/v1), while the proto-defined gRPC
// surface is v3 (google.cloud.resourcemanager.v3.Projects). This core owns the
// canonical v3 project semantics and both transports map onto it deliberately:
//
//   - v3 gRPC (internal/gcp/transport/grpc/resourcemanager) maps Project to
//     resourcemanagerpb.Project and addresses it by "projects/{id}".
//   - v1 REST (internal/gcp/transport/rest/resourcemanager) maps Project to the
//     v1 Project schema: projectId, projectNumber, name = DisplayName, and
//     lifecycleState = State. The v1 API has no separate resource name or etag
//     field, so the REST adapter emits only the v1 fields.
//
// Multi-tenancy is keyed by project id and the emulator never creates or
// deletes projects, so every project id resolves to an ACTIVE, synthesized
// project with a stable synthetic projectNumber and a stable etag. Project
// create/update/delete lifecycle times are not modelled (the fields are exposed
// for v3 shape parity but stay zero). IAM policies are stored in the shared
// ResourceStore through internal/gcp/policy (etag optimistic concurrency
// control, fresh etag per set), so memory and PostgreSQL backends behave
// identically and no provider-level Reset/Snapshotter is needed. Bindings are
// not enforced — they never restrict access to emulated resources. As with the
// other GCP IAM surfaces in this emulator, only role+members bindings are
// modelled: binding conditions are not preserved and the reported policy
// version stays at the default.
package resourcemanager

import (
	"context"
	"strings"
	"time"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// rtProjectPolicy is the resource type for a project's IAM policy in the shared
// ResourceStore. The entry id and owning account are both the project id.
const rtProjectPolicy = "gcp_resourcemanager_project_iam"

// StateActive is the v3 Project state the emulator reports for every project.
const StateActive = "ACTIVE"

// Service is the transport-neutral Cloud Resource Manager core.
type Service struct {
	resources store.ResourceStore
}

// NewService returns a Service backed by the shared ResourceStore.
func NewService(resources store.ResourceStore) *Service {
	return &Service{resources: resources}
}

// Project is the canonical v3 project resource. The zero CreateTime/UpdateTime/
// DeleteTime, empty Parent, and nil Labels reflect that the emulator does not
// model project lifecycle or metadata — only the identity and IAM policy.
type Project struct {
	ProjectID     string
	ProjectNumber string
	DisplayName   string
	State         string
	Etag          string
	Parent        string
	CreateTime    time.Time
	UpdateTime    time.Time
	DeleteTime    time.Time
	Labels        map[string]string
}

// ProjectName formats a project's canonical v3 resource name
// ("projects/{id}") through the shared resource formatter.
func ProjectName(project string) string {
	return resource.ResourceID(project)("project", "")
}

// ParseProjectName returns the project id from a "projects/{id}" resource name.
// A bare id (no prefix) is accepted so callers holding only an id work too; any
// other shape (empty, extra segments) reports ok=false.
func ParseProjectName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	rest := name
	if strings.HasPrefix(name, "projects/") {
		rest = name[len("projects/"):]
	}
	if rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	return rest, true
}

// GetProject returns the synthesized project resource for an id.
func (s *Service) GetProject(_ context.Context, project string) (Project, error) {
	if project == "" {
		return Project{}, invalidArgument("project is required")
	}
	return Project{
		ProjectID:     project,
		ProjectNumber: resource.ProjectNumber(project),
		DisplayName:   project,
		State:         StateActive,
		Etag:          policy.Etag(ProjectName(project)),
	}, nil
}

// GetIamPolicy returns the stored project policy (or an empty default policy).
func (s *Service) GetIamPolicy(ctx context.Context, project string) (policy.Policy, error) {
	if project == "" {
		return policy.Policy{}, invalidArgument("project is required")
	}
	return policy.Load(ctx, s.resources, project, rtProjectPolicy, project), nil
}

// PolicyInput carries the caller-supplied IAM policy fields of a setIamPolicy.
type PolicyInput struct {
	Bindings []any
	Etag     string
	Version  int
}

// SetIamPolicy stores the project policy, enforcing etag OCC (a stale etag is
// rejected with ABORTED/409). A fresh etag is derived from the new bindings.
func (s *Service) SetIamPolicy(ctx context.Context, project string, in PolicyInput) (policy.Policy, error) {
	if project == "" {
		return policy.Policy{}, invalidArgument("project is required")
	}
	body := map[string]any{"bindings": in.Bindings}
	if in.Etag != "" {
		body["etag"] = in.Etag
	}
	if in.Version != 0 {
		body["version"] = in.Version
	}
	return policy.Set(ctx, s.resources, project, rtProjectPolicy, project, body)
}

// TestIamPermissions echoes the requested permissions (the emulator treats the
// caller as owner — no authz enforcement).
func (s *Service) TestIamPermissions(_ context.Context, project string, permissions []string) ([]string, error) {
	if project == "" {
		return nil, invalidArgument("project is required")
	}
	return policy.TestPermissions(permissions), nil
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}
