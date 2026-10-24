// Package run is the gRPC transport for Cloud Run Admin v2
// (google.cloud.run.v2.Services and google.cloud.run.v2.Revisions). It is a
// thin proto adapter over the transport-neutral core in
// internal/gcp/service/run: it transcodes between the generated protobuf
// messages and the core's typed API, and maps core errors to gRPC status
// codes. It owns no business logic and no state beyond its default project.
//
// The adapter renders responses through the same Discovery-shaped maps the REST
// codec emits (core.ServiceJSON / RevisionJSON / OperationJSON) and transcodes
// them with protojson, so the two transports cannot drift. Service mutations
// and revision deletes return a google.longrunning.Operation whose response is
// a typed Any (Service or Revision); a gRPC client that polls
// google.longrunning.Operations is served by the shared Operations service via
// this adapter's Resolver/ListRegistry (run operation ids are prefixed
// "operation-run-" so they cannot be confused with Cloud Functions v2's).
package run

import (
	"context"
	"strings"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcutil "jaiscloud/internal/gcp/grpc"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	"jaiscloud/internal/gcp/paging"
	core "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
)

// operationIDPrefix is the run-owned google.longrunning operation id prefix.
// Cloud Functions v2 publishes location-scoped operations at the same
// projects/{p}/locations/{l}/operations path, so run claims only its own ids.
const operationIDPrefix = "operation-run-"

// Service implements runpb.ServicesServer and runpb.RevisionsServer over the
// shared core.
type Service struct {
	runpb.UnimplementedServicesServer
	runpb.UnimplementedRevisionsServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Cloud Run gRPC service wrapping the core. defaultProj is
// the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func invalid(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

// project resolves the owning project, falling back to the gRPC routing
// metadata then the configured default.
func (s *Service) project(ctx context.Context, project string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Services ─────────────────────────────────────────────────────────────────

func (s *Service) CreateService(ctx context.Context, req *runpb.CreateServiceRequest) (*longrunningpb.Operation, error) {
	project, location, ok := core.ParseParent(req.GetParent())
	if !ok {
		return nil, invalid("invalid parent: " + req.GetParent())
	}
	op, err := s.core.CreateService(ctx, s.project(ctx, project), location,
		req.GetServiceId(), protojsonToMap(req.GetService()), req.GetValidateOnly())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) GetService(ctx context.Context, req *runpb.GetServiceRequest) (*runpb.Service, error) {
	project, location, id, ok := core.ParseServiceName(req.GetName())
	if !ok {
		return nil, invalid("invalid service name: " + req.GetName())
	}
	svc, err := s.core.GetService(ctx, s.project(ctx, project), location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return serviceToProto(svc), nil
}

func (s *Service) ListServices(ctx context.Context, req *runpb.ListServicesRequest) (*runpb.ListServicesResponse, error) {
	project, location, ok := core.ParseParent(req.GetParent())
	if !ok {
		return nil, invalid("invalid parent: " + req.GetParent())
	}
	svcs, err := s.core.ListServices(ctx, s.project(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	page, next, err := paging.Page(svcs, func(s runstore.Service) string { return s.ID }, pageParams(req.GetPageSize(), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := &runpb.ListServicesResponse{NextPageToken: next}
	for _, svc := range page {
		out.Services = append(out.Services, serviceToProto(svc))
	}
	return out, nil
}

func (s *Service) UpdateService(ctx context.Context, req *runpb.UpdateServiceRequest) (*longrunningpb.Operation, error) {
	project, location, id, ok := core.ParseServiceName(req.GetService().GetName())
	if !ok {
		return nil, invalid("invalid service name: " + req.GetService().GetName())
	}
	mask := strings.Join(req.GetUpdateMask().GetPaths(), ",")
	op, err := s.core.UpdateService(ctx, s.project(ctx, project), location, id,
		protojsonToMap(req.GetService()), mask, req.GetValidateOnly(), req.GetAllowMissing())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) DeleteService(ctx context.Context, req *runpb.DeleteServiceRequest) (*longrunningpb.Operation, error) {
	project, location, id, ok := core.ParseServiceName(req.GetName())
	if !ok {
		return nil, invalid("invalid service name: " + req.GetName())
	}
	op, err := s.core.DeleteService(ctx, s.project(ctx, project), location, id, req.GetValidateOnly(), req.GetEtag())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, ok := core.ParseServiceName(req.GetResource())
	if !ok {
		return nil, invalid("invalid resource: " + req.GetResource())
	}
	pol, err := s.core.ServiceGetIamPolicy(ctx, s.project(ctx, project), location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, ok := core.ParseServiceName(req.GetResource())
	if !ok {
		return nil, invalid("invalid resource: " + req.GetResource())
	}
	pol, err := s.core.ServiceSetIamPolicy(ctx, s.project(ctx, project), location, id,
		protojsonToMap(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, location, id, ok := core.ParseServiceName(req.GetResource())
	if !ok {
		return nil, invalid("invalid resource: " + req.GetResource())
	}
	perms, err := s.core.ServiceTestIamPermissions(ctx, s.project(ctx, project), location, id, req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// ─── Revisions ────────────────────────────────────────────────────────────────

func (s *Service) GetRevision(ctx context.Context, req *runpb.GetRevisionRequest) (*runpb.Revision, error) {
	project, location, service, id, ok := core.ParseRevisionName(req.GetName())
	if !ok {
		return nil, invalid("invalid revision name: " + req.GetName())
	}
	rev, err := s.core.GetRevision(ctx, s.project(ctx, project), location, service, id)
	if err != nil {
		return nil, mapError(err)
	}
	return revisionToProto(rev), nil
}

// ListRevisions lists a service's revisions. The parent is a service
// (projects/{p}/locations/{l}/services/{s}); the documented wildcard
// projects/{p}/locations/{l}/services/- aggregates every service's revisions.
func (s *Service) ListRevisions(ctx context.Context, req *runpb.ListRevisionsRequest) (*runpb.ListRevisionsResponse, error) {
	project, location, service, ok := core.ParseServiceName(req.GetParent())
	if !ok {
		return nil, invalid("invalid parent: " + req.GetParent())
	}
	project = s.project(ctx, project)
	if service != "-" {
		revs, err := s.core.ListRevisions(ctx, project, location, service)
		if err != nil {
			return nil, mapError(err)
		}
		return s.pageRevisions(req, revs)
	}
	svcs, err := s.core.ListServices(ctx, project, location)
	if err != nil {
		return nil, mapError(err)
	}
	var all []runstore.Revision
	for _, svc := range svcs {
		revs, err := s.core.ListRevisions(ctx, project, location, svc.ID)
		if err != nil {
			return nil, mapError(err)
		}
		all = append(all, revs...)
	}
	return s.pageRevisions(req, all)
}

func (s *Service) pageRevisions(req *runpb.ListRevisionsRequest, revs []runstore.Revision) (*runpb.ListRevisionsResponse, error) {
	page, next, err := paging.Page(revs, func(r runstore.Revision) string { return r.ID }, pageParams(req.GetPageSize(), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := &runpb.ListRevisionsResponse{NextPageToken: next}
	for _, rev := range page {
		out.Revisions = append(out.Revisions, revisionToProto(rev))
	}
	return out, nil
}

func (s *Service) DeleteRevision(ctx context.Context, req *runpb.DeleteRevisionRequest) (*longrunningpb.Operation, error) {
	project, location, service, id, ok := core.ParseRevisionName(req.GetName())
	if !ok {
		return nil, invalid("invalid revision name: " + req.GetName())
	}
	op, err := s.core.DeleteRevision(ctx, s.project(ctx, project), location, service, id, req.GetValidateOnly(), req.GetEtag())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

// ─── google.longrunning.Operations resolver ──────────────────────────────────

// ResolveOperation resolves a run operation name
// (projects/{p}/locations/{l}/operations/operation-run-*). A name without the
// run prefix belongs to another service (Cloud Functions v2 shares the
// location-scoped path), so it is declined; a prefixed id the run store does
// not hold is unambiguously run's and is NotFound.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	project, location, id, ok := core.ParseOperationName(name)
	if !ok || !strings.HasPrefix(id, operationIDPrefix) {
		return nil, false, nil
	}
	op, err := s.core.GetOperation(ctx, s.project(ctx, project), location, id)
	if err != nil {
		return nil, true, mapError(err)
	}
	return operationToProto(op), true, nil
}

// ListOperations implements the shared Operations ListRegistry surface for run
// operations. The parent must be a location parent; a location that holds no
// run operations is declined so the shared stub (or another service) answers.
// The standard filter parameter is not modelled; a non-empty filter fails loud
// rather than silently returning unfiltered results.
func (s *Service) ListOperations(ctx context.Context, parent string, pageSize int32, pageToken, filter string) (*longrunningpb.ListOperationsResponse, bool, error) {
	project, location, ok := core.ParseParent(parent)
	if !ok {
		return nil, false, nil
	}
	project = s.project(ctx, project)
	owned, err := s.core.ListOperations(ctx, project, location)
	if err != nil {
		return nil, true, mapError(err)
	}
	if len(owned) == 0 {
		return nil, false, nil
	}
	if filter != "" {
		return nil, true, status.Error(codes.InvalidArgument, "operations filter is not supported")
	}
	// run.projects.locations.operations.list documents "If not set or set to
	// less than or equal to 0, the default page size is 100" — a documented
	// exception to AIP-158's reject-negative rule, so a non-positive size is
	// cleared to the default rather than raising InvalidArgument.
	if pageSize < 0 {
		pageSize = 0
	}
	page, next, err := paging.Page(owned, func(o runstore.Operation) string { return o.ID }, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, true, mapError(err)
	}
	out := &longrunningpb.ListOperationsResponse{NextPageToken: next}
	for _, op := range page {
		out.Operations = append(out.Operations, operationToProto(op))
	}
	return out, true, nil
}

// pageParams builds the paging.Page parameter map from a gRPC list request.
func pageParams(pageSize int32, pageToken string) map[string]any {
	return map[string]any{"pageSize": int(pageSize), "pageToken": pageToken}
}

// compile-time assertions that the adapter implements the generated servers and
// the shared google.longrunning.Operations surfaces it registers.
var (
	_ runpb.ServicesServer        = (*Service)(nil)
	_ runpb.RevisionsServer       = (*Service)(nil)
	_ grpcoperations.Resolver     = (*Service)(nil)
	_ grpcoperations.ListRegistry = (*Service)(nil)
)
