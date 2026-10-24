// Package resourcemanager is the gRPC transport for Cloud Resource Manager v3
// (google.cloud.resourcemanager.v3.Projects). It is a thin proto adapter over
// the transport-neutral core in internal/gcp/service/resourcemanager: it
// transcodes between the generated protobuf messages and the core's typed API,
// and maps core errors to gRPC status codes. It owns no business logic.
//
// The project lookup (GetProject), project lifecycle (CreateProject /
// ListProjects / DeleteProject / UndeleteProject), project administration
// (SearchProjects / UpdateProject / MoveProject), and project IAM
// (GetIamPolicy / SetIamPolicy / TestIamPermissions) are implemented. The
// Folders, Organizations, and Tag* services are not registered.
//
// Project mutations return google.longrunning.Operations whose names are
// top-level (operations/{id}); ResolveOperation lets the shared
// google.longrunning.Operations service settle a poll, matching the core's LRO
// timing mode. In the default synchronous mode every returned operation is
// already done, so a poll is only exercised in the opt-in async mode.
package resourcemanager

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/resourcemanager"

	"jaiscloud/internal/model"
)

// Service implements resourcemanagerpb.ProjectsServer over the shared core.
type Service struct {
	resourcemanagerpb.UnimplementedProjectsServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Resource Manager gRPC service wrapping the core.
// defaultProj is the config-default project used when a request carries no
// project in its resource name.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// GetProject returns the synthesized v3 project.
func (s *Service) GetProject(ctx context.Context, req *resourcemanagerpb.GetProjectRequest) (*resourcemanagerpb.Project, error) {
	project, ok := s.projectFor(ctx, req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid project name", 400))
	}
	p, err := s.core.GetProject(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	return projectToProto(p), nil
}

// ListProjects returns a page of projects. The emulator does not model an
// org/folder hierarchy, so the required parent is ignored and every project is
// listed (recorded as a deferral).
func (s *Service) ListProjects(ctx context.Context, req *resourcemanagerpb.ListProjectsRequest) (*resourcemanagerpb.ListProjectsResponse, error) {
	page, next, err := s.core.ListProjects(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetShowDeleted(), "")
	if err != nil {
		return nil, mapError(err)
	}
	out := &resourcemanagerpb.ListProjectsResponse{NextPageToken: next}
	for _, p := range page {
		out.Projects = append(out.Projects, projectToProto(p))
	}
	return out, nil
}

// CreateProject registers a project and returns the long-running operation the
// v3 API specifies.
func (s *Service) CreateProject(ctx context.Context, req *resourcemanagerpb.CreateProjectRequest) (*longrunningpb.Operation, error) {
	p := req.GetProject()
	_, op, err := s.core.CreateProject(ctx, core.CreateProjectInput{
		ProjectID:   p.GetProjectId(),
		DisplayName: p.GetDisplayName(),
		Parent:      p.GetParent(),
		Labels:      p.GetLabels(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

// SearchProjects returns a page of projects matching the query expression. The
// query grammar is the same bounded field:value grammar ListProjects accepts
// (projectadmin.go), evaluated by the core; an invalid query is InvalidArgument.
func (s *Service) SearchProjects(ctx context.Context, req *resourcemanagerpb.SearchProjectsRequest) (*resourcemanagerpb.SearchProjectsResponse, error) {
	page, next, err := s.core.SearchProjects(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetQuery())
	if err != nil {
		return nil, mapError(err)
	}
	out := &resourcemanagerpb.SearchProjectsResponse{NextPageToken: next}
	for _, p := range page {
		out.Projects = append(out.Projects, projectToProto(p))
	}
	return out, nil
}

// UpdateProject applies a masked metadata update (display_name/labels) and
// returns the long-running operation. The project is identified by the request
// project's resource name, which is required.
func (s *Service) UpdateProject(ctx context.Context, req *resourcemanagerpb.UpdateProjectRequest) (*longrunningpb.Operation, error) {
	in := req.GetProject()
	if in.GetName() == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "project name is required", 400))
	}
	project, ok := s.projectFor(ctx, in.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid project name", 400))
	}
	_, op, err := s.core.UpdateProject(ctx, project, core.UpdateProjectInput{
		DisplayName: in.GetDisplayName(),
		Labels:      in.GetLabels(),
		UpdateMask:  updateMaskPaths(req.GetUpdateMask()),
		Etag:        in.GetEtag(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

// MoveProject reparents a project and returns the long-running operation. The
// project name is required (an empty name must not fall back to the default
// project, which would silently reparent it).
func (s *Service) MoveProject(ctx context.Context, req *resourcemanagerpb.MoveProjectRequest) (*longrunningpb.Operation, error) {
	if req.GetName() == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "project name is required", 400))
	}
	project, ok := s.projectFor(ctx, req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid project name", 400))
	}
	_, op, err := s.core.MoveProject(ctx, project, req.GetDestinationParent())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

// DeleteProject marks a project DELETE_REQUESTED and returns the operation.
func (s *Service) DeleteProject(ctx context.Context, req *resourcemanagerpb.DeleteProjectRequest) (*longrunningpb.Operation, error) {
	project, ok := s.projectFor(ctx, req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid project name", 400))
	}
	_, op, err := s.core.DeleteProject(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

// UndeleteProject restores a DELETE_REQUESTED project and returns the operation.
func (s *Service) UndeleteProject(ctx context.Context, req *resourcemanagerpb.UndeleteProjectRequest) (*longrunningpb.Operation, error) {
	project, ok := s.projectFor(ctx, req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid project name", 400))
	}
	_, op, err := s.core.UndeleteProject(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

// ResolveOperation implements the generic google.longrunning.Operations
// resolver for project mutations' top-level operation names (operations/{id}).
// It shares that namespace with Cloud Functions v1, so an id absent from the
// project operation store returns handled=false — the caller then consults the
// next resolver (functions), preserving functions' NotFound for its own unknown
// ids. It must therefore be registered BEFORE the functions resolver in main.go.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	if !isTopLevelOperationName(name) {
		return nil, false, nil
	}
	project, _ := s.projectFor(ctx, "")
	op, err := s.core.GetOperation(ctx, project, name)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, true, mapError(err)
	}
	out, err := operationToProto(op)
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// GetIamPolicy returns the stored project policy.
func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, ok := s.projectFor(ctx, req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	pol, err := s.core.GetIamPolicy(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

// SetIamPolicy stores the project policy (etag OCC).
func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, ok := s.projectFor(ctx, req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	pol, err := s.core.SetIamPolicy(ctx, project, policyFromProto(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

// TestIamPermissions echoes the requested permissions.
func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, ok := s.projectFor(ctx, req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	perms, err := s.core.TestIamPermissions(ctx, project, req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// compile-time assertion that Service implements the generated server.
var _ resourcemanagerpb.ProjectsServer = (*Service)(nil)
