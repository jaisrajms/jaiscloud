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
// service in main.go). ListWorkflowRevisions serves the workflow revision
// history, and GetWorkflow's optional revision_id selects one revision.
//
// The shared google.longrunning.Operations surface is served through the
// Resolver (GetOperation), ListRegistry (ListOperations) and Registry
// (DeleteOperation) seams: ListOperations lists a location's persisted
// operations, DeleteOperation removes one, and CancelOperation declines (the
// Discovery declares no cancel for Cloud Workflows operations).
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
		req.GetFilter(), req.GetOrderBy(),
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
	r, err := s.core.GetWorkflowRevision(ctx, project, core.LocationFromName(req.GetName()),
		core.WorkflowIDFromName(req.GetName()), req.GetRevisionId())
	if err != nil {
		return nil, mapError(err)
	}
	return revisionToProto(r, project), nil
}

func (s *Service) ListWorkflowRevisions(ctx context.Context, req *workflowspb.ListWorkflowRevisionsRequest) (*workflowspb.ListWorkflowRevisionsResponse, error) {
	project := s.projectFor(ctx, req.GetName())
	page, next, err := s.core.ListWorkflowRevisions(ctx, project, core.LocationFromName(req.GetName()),
		core.WorkflowIDFromName(req.GetName()), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &workflowspb.ListWorkflowRevisionsResponse{NextPageToken: next}
	for _, r := range page {
		out.Workflows = append(out.Workflows, revisionToProto(r, project))
	}
	return out, nil
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

// ListOperations implements the shared google.longrunning.Operations
// ListRegistry surface for Cloud Workflows. The parent must be a location
// parent (projects/{project}/locations/{location}); a location that holds no
// Cloud Workflows operations is declined so the shared stub (or another
// service) answers when the request is not endpoint-scoped. When the request is
// addressed to the Cloud Workflows endpoint the page is authoritative, so an
// empty page is returned rather than falling through. The standard `filter`
// parameter is not modelled; the core fails loud on a non-empty filter rather
// than silently returning unfiltered results.
func (s *Service) ListOperations(ctx context.Context, parent string, pageSize int32, pageToken, filter string) (*longrunningpb.ListOperationsResponse, bool, error) {
	project, location, ok := parseOperationParent(parent)
	if !ok {
		return nil, false, nil
	}
	page, next, err := s.core.ListOperations(ctx, project, location,
		int(pageSize), pageToken, filter)
	if err != nil {
		return nil, true, mapError(err)
	}
	if len(page) == 0 {
		return nil, false, nil
	}
	out := &longrunningpb.ListOperationsResponse{NextPageToken: next}
	for _, op := range page {
		pb, err := operationToProto(op, project, operationResponse(op))
		if err != nil {
			return nil, true, err
		}
		out.Operations = append(out.Operations, pb)
	}
	return out, true, nil
}

// DeleteOperation implements the shared google.longrunning.Operations Registry
// surface for Cloud Workflows: a location-scoped operation the workflows store
// owns is removed; a name in the shared namespace that workflows does not own
// (a Dataproc Metastore or Managed Kafka operation) is declined (handled=false)
// so their registry — or the shared terminal stub — answers.
func (s *Service) DeleteOperation(ctx context.Context, name string) (bool, error) {
	project, location, id, ok := parseOperationName(name)
	if !ok {
		return false, nil
	}
	if err := s.core.DeleteOperation(ctx, project, location, id); err != nil {
		if core.IsNotFound(err) {
			return false, nil
		}
		return true, mapError(err)
	}
	return true, nil
}

// CancelOperation declines ownership: the Cloud Workflows Discovery declares
// operations.get/list/delete but no cancel, so a cancel is not workflows'. The
// shared Operations service's documented no-op (or strict-mode Get check)
// handles it exactly as before the Registry seam.
func (s *Service) CancelOperation(_ context.Context, _ string) (bool, error) {
	return false, nil
}

// parseOperationParent parses a location parent name
// projects/{project}/locations/{location} for the operations List surface.
func parseOperationParent(parent string) (project, location string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(parent, "/"), "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "locations" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
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
	_ grpcoperations.ListRegistry = (*Service)(nil)
	_ grpcoperations.Registry     = (*Service)(nil)
)
