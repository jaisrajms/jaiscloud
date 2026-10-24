// Package serviceusage is the gRPC transport for Service Usage v1
// (google.api.serviceusage.v1.ServiceUsage). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/serviceusage: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// EnableService, DisableService, and BatchEnableServices return a done
// google.longrunning.Operation with the response (a Service, or a list of
// Services) packed as a typed Any, so the generated client's Wait observes it
// without polling.
package serviceusage

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcutil "jaiscloud/internal/gcp/grpc"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	core "jaiscloud/internal/gcp/service/serviceusage"
)

// Service implements serviceusagepb.ServiceUsageServer over the shared core.
type Service struct {
	serviceusagepb.UnimplementedServiceUsageServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Service Usage gRPC service wrapping the core. defaultProj
// is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func (s *Service) GetService(ctx context.Context, req *serviceusagepb.GetServiceRequest) (*serviceusagepb.Service, error) {
	project, service := parseName(req.GetName())
	api, err := s.core.GetAPI(ctx, s.projectFor(ctx, project), service)
	if err != nil {
		return nil, mapError(err)
	}
	return apiToProto(api), nil
}

func (s *Service) ListServices(ctx context.Context, req *serviceusagepb.ListServicesRequest) (*serviceusagepb.ListServicesResponse, error) {
	project, _ := parseName(req.GetParent())
	filter, err := core.ParseFilter(req.GetFilter())
	if err != nil {
		return nil, mapError(err)
	}
	page, next, err := s.core.ListAPIs(ctx, s.projectFor(ctx, project), filter, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &serviceusagepb.ListServicesResponse{NextPageToken: next}
	for _, a := range page {
		out.Services = append(out.Services, apiToProto(a))
	}
	return out, nil
}

func (s *Service) BatchGetServices(ctx context.Context, req *serviceusagepb.BatchGetServicesRequest) (*serviceusagepb.BatchGetServicesResponse, error) {
	project, _ := parseName(req.GetParent())
	apis, err := s.core.BatchGetAPIs(ctx, s.projectFor(ctx, project), req.GetNames())
	if err != nil {
		return nil, mapError(err)
	}
	out := &serviceusagepb.BatchGetServicesResponse{}
	for _, a := range apis {
		out.Services = append(out.Services, apiToProto(a))
	}
	return out, nil
}

func (s *Service) EnableService(ctx context.Context, req *serviceusagepb.EnableServiceRequest) (*longrunningpb.Operation, error) {
	project, service := parseName(req.GetName())
	_, op, err := s.core.EnableAPI(ctx, s.projectFor(ctx, project), service)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, operationResponseProto(op))
}

func (s *Service) DisableService(ctx context.Context, req *serviceusagepb.DisableServiceRequest) (*longrunningpb.Operation, error) {
	project, service := parseName(req.GetName())
	_, op, err := s.core.DisableAPI(ctx, s.projectFor(ctx, project), service)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, operationResponseProto(op))
}

func (s *Service) BatchEnableServices(ctx context.Context, req *serviceusagepb.BatchEnableServicesRequest) (*longrunningpb.Operation, error) {
	project, _ := parseName(req.GetParent())
	_, op, err := s.core.BatchEnableAPIs(ctx, s.projectFor(ctx, project), req.GetServiceIds())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, operationResponseProto(op))
}

// ResolveOperation implements the generic google.longrunning.Operations
// resolver for Service Usage's top-level operation names (operations/{id}). It
// shares that namespace with Cloud Functions v1, so an id absent from the
// serviceusage store returns handled=false — the caller then consults the next
// resolver (functions), preserving functions' NotFound for its own unknown ids.
// It must therefore be registered BEFORE the functions resolver in main.go.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	if !core.IsTopLevelOperationName(name) {
		return nil, false, nil
	}
	op, err := s.core.GetOperation(ctx, s.projectFor(ctx, ""), name)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, mapError(err)
	}
	out, err := operationToProto(op, operationResponseProto(op))
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// isTopLevelOperationsParent reports whether a ListOperations parent names the
// top-level operations collection Service Usage owns. Service Usage operations
// are named operations/{id} with no parent, so only the empty root (or the
// literal "operations" collection name) is claimed; a location- or region-scoped
// parent belongs to another service and is declined.
func isTopLevelOperationsParent(parent string) bool {
	switch strings.Trim(parent, "/") {
	case "", "operations":
		return true
	default:
		return false
	}
}

// ListOperations implements the generic google.longrunning.Operations
// ListRegistry surface for Service Usage's top-level operations. It claims only
// the top-level parent; a location-scoped parent is declined so another
// service's registry (or the empty page) answers. A top-level operations/{id}
// name carries no project segment, and the generated longrunning client's
// routing metadata is `name=<parent>` (which contains no project), so the
// project resolves from the bearer token or the configured default — the same
// fallback ListServices uses — which is why `make test-lro-async-gcp` starts the
// emulator with JAISCLOUD_GCP_PROJECT_ID matching the create project.
//
// Service Usage does not evaluate an AIP-160 filter, so a non-empty filter
// fails loud (Unimplemented) rather than silently listing everything.
// returnPartialSuccess is rejected once by the shared Operations service.
func (s *Service) ListOperations(ctx context.Context, parent string, pageSize int32, pageToken, filter string) (*longrunningpb.ListOperationsResponse, bool, error) {
	if !isTopLevelOperationsParent(parent) {
		return nil, false, nil
	}
	if filter != "" {
		return nil, true, status.Error(codes.Unimplemented, "ListOperations filter is not supported for Service Usage operations")
	}
	ops, next, err := s.core.ListOperations(ctx, s.projectFor(ctx, ""), int(pageSize), pageToken)
	if err != nil {
		return nil, true, mapError(err)
	}
	out := &longrunningpb.ListOperationsResponse{NextPageToken: next}
	for _, op := range ops {
		p, err := operationToProto(op, operationResponseProto(op))
		if err != nil {
			return nil, true, err
		}
		out.Operations = append(out.Operations, p)
	}
	return out, true, nil
}

// CancelOperation implements the generic google.longrunning.Operations registry.
// The emulator does not model cancellation, so a known Service Usage operation
// is a no-op success; an id that is not a Service Usage operation is declined
// (handled=false) so the shared service keeps its ownership handshake with
// Cloud Functions v1 and the lenient terminal contract is unchanged.
func (s *Service) CancelOperation(ctx context.Context, name string) (bool, error) {
	if !core.IsTopLevelOperationName(name) {
		return false, nil
	}
	if err := s.core.CancelOperation(ctx, s.projectFor(ctx, ""), name); err != nil {
		if core.IsNotFound(err) {
			return false, nil
		}
		return true, mapError(err)
	}
	return true, nil
}

// DeleteOperation implements the generic google.longrunning.Operations registry,
// removing a persisted Service Usage operation. Like CancelOperation an id not
// owned by Service Usage is declined rather than reported NotFound, so functions
// keeps its namespace and the lenient contract is unchanged.
func (s *Service) DeleteOperation(ctx context.Context, name string) (bool, error) {
	if !core.IsTopLevelOperationName(name) {
		return false, nil
	}
	if err := s.core.DeleteOperation(ctx, s.projectFor(ctx, ""), name); err != nil {
		if core.IsNotFound(err) {
			return false, nil
		}
		return true, mapError(err)
	}
	return true, nil
}

// compile-time assertions: the generated server, the generic Operations
// resolver, and the richer registry surface.
var (
	_ serviceusagepb.ServiceUsageServer = (*Service)(nil)
	_ grpcoperations.Resolver           = (*Service)(nil)
	_ grpcoperations.Registry           = (*Service)(nil)
	_ grpcoperations.ListRegistry       = (*Service)(nil)
)
