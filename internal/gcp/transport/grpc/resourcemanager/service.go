// Package resourcemanager is the gRPC transport for Cloud Resource Manager v3
// (google.cloud.resourcemanager.v3.Projects). It is a thin proto adapter over
// the transport-neutral core in internal/gcp/service/resourcemanager: it
// transcodes between the generated protobuf messages and the core's typed API,
// and maps core errors to gRPC status codes. It owns no business logic.
//
// Only the project lookup (GetProject) and project IAM (GetIamPolicy /
// SetIamPolicy / TestIamPermissions) are implemented; the remaining Projects
// RPCs (List/Search/Create/Update/Move/Delete/Undelete) are the embedded
// Unimplemented stubs. The Folders, Organizations, and Tag* services are not
// registered.
package resourcemanager

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
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
