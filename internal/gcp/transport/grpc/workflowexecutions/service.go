// Package workflowexecutions is the gRPC transport for Cloud Workflow
// Executions v1 (google.cloud.workflows.executions.v1.Executions). It is a thin
// proto adapter over the transport-neutral core in
// internal/gcp/service/workflowexecutions: it transcodes between the generated
// protobuf messages and the core's typed API, and maps core errors to gRPC
// status codes. It owns no business logic and no state beyond its default
// project.
package workflowexecutions

import (
	"context"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/workflowexecutions"
)

// Service implements executionspb.ExecutionsServer over the shared core.
type Service struct {
	executionspb.UnimplementedExecutionsServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Workflow Executions gRPC service wrapping the core.
// defaultProj is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// project resolves the owning project: the resource name's project when
// present, else the gRPC metadata routing header, else the configured default.
func (s *Service) project(ctx context.Context, name string) string {
	if p := core.ProjectFromName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func (s *Service) CreateExecution(ctx context.Context, req *executionspb.CreateExecutionRequest) (*executionspb.Execution, error) {
	location, workflowID, _ := core.ParseName(req.GetParent())
	project := s.project(ctx, req.GetParent())

	in := req.GetExecution()
	e, err := s.core.CreateExecution(ctx, project, location, workflowID, core.CreateExecutionInput{
		Argument:     in.GetArgument(),
		CallLogLevel: callLogLevelName(in.GetCallLogLevel()),
		Labels:       in.GetLabels(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return executionToProto(e, project), nil
}

func (s *Service) GetExecution(ctx context.Context, req *executionspb.GetExecutionRequest) (*executionspb.Execution, error) {
	location, workflowID, executionID := core.ParseName(req.GetName())
	project := s.project(ctx, req.GetName())
	e, err := s.core.GetExecution(ctx, project, location, workflowID, executionID, viewFromProto(req.GetView(), core.ViewFull))
	if err != nil {
		return nil, mapError(err)
	}
	return executionToProto(e, project), nil
}

func (s *Service) ListExecutions(ctx context.Context, req *executionspb.ListExecutionsRequest) (*executionspb.ListExecutionsResponse, error) {
	location, workflowID, _ := core.ParseName(req.GetParent())
	project := s.project(ctx, req.GetParent())
	page, next, err := s.core.ListExecutions(ctx, project, location, workflowID,
		viewFromProto(req.GetView(), core.ViewBasic), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &executionspb.ListExecutionsResponse{NextPageToken: next}
	for _, e := range page {
		out.Executions = append(out.Executions, executionToProto(e, project))
	}
	return out, nil
}

func (s *Service) CancelExecution(ctx context.Context, req *executionspb.CancelExecutionRequest) (*executionspb.Execution, error) {
	location, workflowID, executionID := core.ParseName(req.GetName())
	project := s.project(ctx, req.GetName())
	e, err := s.core.CancelExecution(ctx, project, location, workflowID, executionID)
	if err != nil {
		return nil, mapError(err)
	}
	return executionToProto(e, project), nil
}

// compile-time assertion that Service implements the generated server.
var _ executionspb.ExecutionsServer = (*Service)(nil)
