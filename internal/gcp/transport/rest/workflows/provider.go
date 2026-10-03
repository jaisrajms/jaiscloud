// Package workflows is the REST transport for Cloud Workflows v1
// (workflows.googleapis.com).
//
// Real GCP serves the proto-defined v1 management API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document. The implemented methods are the REST mirrors of the
// cloud.google.com/go/workflows/apiv1 Workflows RPCs plus GetOperation:
//
//	GET    /v1/projects/{p}/locations/{l}/workflows                     workflows.list
//	POST   /v1/projects/{p}/locations/{l}/workflows?workflowId=…        workflows.create
//	GET    /v1/projects/{p}/locations/{l}/workflows/{w}                 workflows.get
//	PATCH  /v1/projects/{p}/locations/{l}/workflows/{w}?updateMask=…    workflows.update
//	DELETE /v1/projects/{p}/locations/{l}/workflows/{w}                 workflows.delete
//	GET    /v1/projects/{p}/locations/{l}/operations/{id}               operations.get
//
// It keeps the generic JSONCodec{Service: "workflows"} descriptor (the codec
// owns path/action decoding); this package only adapts the NormalizedRequest
// params into the shared core's typed API. Neither owns business logic — both
// delegate to the single core Service shared with the gRPC transport (see
// internal/gcp/service/workflows).
package workflows

import (
	"context"

	core "jaiscloud/internal/gcp/service/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Workflows v1 REST control plane. It is a thin
// adapter: every handler resolves the NormalizedRequest params into the core's
// typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
	// operationResolvers resolves location-scoped operations owned by other
	// services. The path /v1/projects/{p}/locations/{l}/operations/{id} is
	// decoded as a Workflows operation, so without them a Metastore/Managed
	// Kafka operation poll 404s. Empty (the default) leaves the surface
	// unchanged; main wires them only in the opt-in async LRO mode.
	operationResolvers []OperationResolver
}

// OperationResolver resolves a location-scoped operation name owned by another
// service that shares the google.longrunning.Operations
// projects/{p}/locations/{l}/operations/{id} namespace with Cloud Workflows.
// ResolveOperation returns handled=false when the name is not owned by the
// implementing service, so the caller can fall through to the canonical
// NotFound. It mirrors the gRPC operations.Resolver contract.
type OperationResolver interface {
	ResolveOperation(ctx context.Context, project, location, opID string) (map[string]any, bool, error)
}

// NewProvider returns a Cloud Workflows REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// SetOperationResolvers wires cross-service operation resolution. It is called
// only when the opt-in async LRO mode is enabled: in the default synchronous
// mode the create response is already done and no client polls, so the REST
// contract is left byte-for-byte unchanged.
func (p *Provider) SetOperationResolvers(rs ...OperationResolver) {
	p.operationResolvers = rs
}

// Routes maps "Workflow.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Workflow.ListWorkflows":  p.ListWorkflows,
		"Workflow.GetWorkflow":    p.GetWorkflow,
		"Workflow.CreateWorkflow": p.CreateWorkflow,
		"Workflow.UpdateWorkflow": p.UpdateWorkflow,
		"Workflow.DeleteWorkflow": p.DeleteWorkflow,
		"Workflow.GetOperation":   p.GetOperation,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

func (p *Provider) ListWorkflows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListWorkflows(ctx, project, strParam(nr, "location"),
		intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, w := range page {
		items = append(items, core.WorkflowJSON(w, project))
	}
	out := map[string]any{"workflows": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) GetWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = core.LocationFromName(name)
	}
	w, err := p.core.GetWorkflow(ctx, project, location, core.WorkflowIDFromName(name))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.WorkflowJSON(w, project)), nil
}

func (p *Provider) CreateWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	id := strParam(nr, "workflowId")
	if id == "" {
		id = core.WorkflowIDFromName(strFrom(body["name"]))
	}
	_, op, err := p.core.CreateWorkflow(ctx, project, strParam(nr, "location"), core.CreateInput{
		ID:             id,
		Description:    strFrom(body["description"]),
		Labels:         stringMapFrom(body["labels"]),
		ServiceAccount: strFrom(body["serviceAccount"]),
		SourceContents: strFrom(body["sourceContents"]),
		CallLogLevel:   strFrom(body["callLogLevel"]),
		UserEnvVars:    stringMapFrom(body["userEnvVars"]),
		Tags:           stringMapFrom(body["tags"]),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) UpdateWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = core.LocationFromName(name)
	}
	body := bodyOf(nr)
	_, op, err := p.core.UpdateWorkflow(ctx, project, location, core.UpdateInput{
		ID:             core.WorkflowIDFromName(name),
		UpdateMask:     strParam(nr, "updateMask"),
		Description:    strFrom(body["description"]),
		Labels:         stringMapFrom(body["labels"]),
		UserEnvVars:    stringMapFrom(body["userEnvVars"]),
		ServiceAccount: strFrom(body["serviceAccount"]),
		SourceContents: strFrom(body["sourceContents"]),
		CallLogLevel:   strFrom(body["callLogLevel"]),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) DeleteWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = core.LocationFromName(name)
	}
	op, err := p.core.DeleteWorkflow(ctx, project, location, core.WorkflowIDFromName(name))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = core.LocationFromName(name)
	}
	opID := core.OperationIDFromName(name)
	op, err := p.core.GetOperation(ctx, project, location, opID)
	if err != nil {
		// A location-scoped operation name is shared with the other regional
		// services (Dataproc Metastore, Managed Kafka). Ask their resolvers
		// before surfacing the Workflows NotFound.
		if core.IsNotFound(err) {
			for _, r := range p.operationResolvers {
				resolved, handled, rerr := r.ResolveOperation(ctx, project, location, opID)
				if rerr != nil {
					return nil, rerr
				}
				if handled {
					return provider.OK(resolved), nil
				}
			}
		}
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}
