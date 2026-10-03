// Package workflows is the gRPC transport for Cloud Workflows v1
// (google.cloud.workflows.v1.Workflows). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/workflows: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// CreateWorkflow, UpdateWorkflow, and DeleteWorkflow return a
// google.longrunning.Operation with the response (a Workflow, or Empty for
// delete) packed as a typed Any. By default the operation is done and the
// generated client's Wait observes it without polling; in the opt-in async mode
// it is returned in flight and the client's poll resolves it through
// ResolveOperation (registered with the shared google.longrunning.Operations
// service in main.go). ListWorkflowRevisions is not implemented (Unimplemented),
// matching the emulator's control-plane scope.
package workflows

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	core "jaiscloud/internal/gcp/service/workflows"
)

// Service implements workflowspb.WorkflowsServer over the shared core.
type Service struct {
	workflowspb.UnimplementedWorkflowsServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Cloud Workflows gRPC service wrapping the core.
// defaultProj is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// projectFor resolves the owning project: the name/parent's project when
// present, else the gRPC metadata routing header, else the configured default.
func (s *Service) projectFor(ctx context.Context, name string) string {
	if p := core.ProjectFromName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

func (s *Service) ListWorkflows(ctx context.Context, req *workflowspb.ListWorkflowsRequest) (*workflowspb.ListWorkflowsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	page, next, err := s.core.ListWorkflows(ctx, project, core.LocationFromName(req.GetParent()),
		int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &workflowspb.ListWorkflowsResponse{NextPageToken: next}
	for _, w := range page {
		out.Workflows = append(out.Workflows, workflowToProto(w, project))
	}
	return out, nil
}

func (s *Service) GetWorkflow(ctx context.Context, req *workflowspb.GetWorkflowRequest) (*workflowspb.Workflow, error) {
	project := s.projectFor(ctx, req.GetName())
	w, err := s.core.GetWorkflow(ctx, project, core.LocationFromName(req.GetName()), core.WorkflowIDFromName(req.GetName()))
	if err != nil {
		return nil, mapError(err)
	}
	return workflowToProto(w, project), nil
}

func (s *Service) CreateWorkflow(ctx context.Context, req *workflowspb.CreateWorkflowRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	id := req.GetWorkflowId()
	if id == "" {
		id = core.WorkflowIDFromName(req.GetWorkflow().GetName())
	}
	w, op, err := s.core.CreateWorkflow(ctx, project, core.LocationFromName(req.GetParent()), createInputFromProto(req.GetWorkflow(), id))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, workflowToProto(w, project))
}

func (s *Service) UpdateWorkflow(ctx context.Context, req *workflowspb.UpdateWorkflowRequest) (*longrunningpb.Operation, error) {
	name := req.GetWorkflow().GetName()
	project := s.projectFor(ctx, name)
	id := core.WorkflowIDFromName(name)
	w, op, err := s.core.UpdateWorkflow(ctx, project, core.LocationFromName(name), updateInputFromProto(req.GetWorkflow(), req.GetUpdateMask(), id))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, workflowToProto(w, project))
}

func (s *Service) DeleteWorkflow(ctx context.Context, req *workflowspb.DeleteWorkflowRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	op, err := s.core.DeleteWorkflow(ctx, project, core.LocationFromName(req.GetName()), core.WorkflowIDFromName(req.GetName()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, emptyResponse())
}

// ResolveOperation implements the google.longrunning.Operations resolver for
// Cloud Workflows. Names are location-scoped
// (projects/{p}/locations/{l}/operations/{id}); a name outside that shape — or
// an id the workflows store does not know, since a location-scoped name is
// shared with other services — is not ours (handled=false) so the generic
// terminal stub keeps serving it done=true.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	project, location, id, ok := parseOperationName(name)
	if !ok {
		return nil, false, nil
	}
	op, err := s.core.GetOperation(ctx, project, location, id)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, mapError(err)
	}
	out, err := operationToProto(op, project, operationResponse(op))
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// parseOperationName parses a Cloud Workflows location operation name
// projects/{project}/locations/{location}/operations/{id}.
func parseOperationName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "operations" {
		return "", "", "", false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// compile-time assertions that Service implements the generated server and the
// operations resolver.
var (
	_ workflowspb.WorkflowsServer = (*Service)(nil)
	_ grpcoperations.Resolver     = (*Service)(nil)
)
