// Package workflows is the transport-neutral core of the Cloud Workflows v1
// management service (workflows.googleapis.com). It owns all workflow business
// logic — List/Get/Create/Update/DeleteWorkflow and the GetOperation surface for
// the long-running operations those methods return — over the shared Cloud
// Workflows store (internal/gcp/store/workflows, shared with the Workflow
// Executions service; the store is not duplicated).
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/workflows) and the REST transport
// (internal/gcp/transport/rest/workflows) both transcode their wire format into
// this package's typed API and then call the SAME Service instance. That is the
// dual-protocol invariant: one core, one piece of state, so the transports
// cannot drift.
//
// Create/Update/Delete complete synchronously and return a done=true
// google.longrunning.Operation (mirroring the GCP REST LRO convention used by
// Firestore CreateIndex). The operation is persisted so GetOperation can return
// it. The LRO timing is opt-in via WithLROMode: when enabled, operations are
// stored done=false and settle lazily on read (see settle); the default remains
// synchronous.
package workflows

import (
	"context"
	"errors"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/paging"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
)

// Service is the transport-neutral Cloud Workflows v1 service over the shared
// workflows store.
type Service struct {
	workflows workflowsstore.Store
	// lroMode controls operation timing. The zero value is synchronous: every
	// operation is stored done=true inline, matching the v1.1.0 contract. An
	// enabled mode stores operations done=false and settles them lazily on read.
	lroMode lro.Mode
}

// Option configures Service.
type Option func(*Service)

// WithLROMode sets the long-running-operation timing mode. The zero value is
// synchronous; Mode{Enabled: true, Delay: d} stores create/update/delete
// operations done=false and settles them on read once d has elapsed.
func WithLROMode(m lro.Mode) Option {
	return func(s *Service) { s.lroMode = m }
}

// NewService returns a Cloud Workflows core backed by the given store.
func NewService(workflows workflowsstore.Store, opts ...Option) *Service {
	s := &Service{workflows: workflows}
	for _, o := range opts {
		o(s)
	}
	return s
}

// CreateInput carries the caller-supplied fields of a workflow create. The
// transports resolve the owning project/location and the workflow ID.
type CreateInput struct {
	ID             string
	Description    string
	Labels         map[string]string
	ServiceAccount string
	SourceContents string
	CallLogLevel   string
	UserEnvVars    map[string]string
	Tags           map[string]string
}

// UpdateInput carries the caller-supplied fields of a workflow update. The
// UpdateMask is a comma-separated field list ("" means every field). Labels and
// UserEnvVars are nil when the request omitted the map (as opposed to supplying
// an empty one), so an omitted map never clears the stored value.
type UpdateInput struct {
	ID             string
	UpdateMask     string
	Description    string
	Labels         map[string]string
	UserEnvVars    map[string]string
	ServiceAccount string
	SourceContents string
	CallLogLevel   string
}

// ListWorkflows returns a cursor page of the workflows in a location. filter
// and orderBy are accepted by the wire but ignored (documented limitation).
func (s *Service) ListWorkflows(ctx context.Context, project, location string, pageSize int, pageToken string) ([]workflowsstore.Workflow, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	wfs, err := s.workflows.ListWorkflows(ctx, project, location)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	params := map[string]any{"pageSize": clampPageSize(pageSize)}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next := paging.Page(wfs, func(w workflowsstore.Workflow) string { return w.ID }, params)
	return page, next, nil
}

// GetWorkflow returns one workflow, or NotFound.
func (s *Service) GetWorkflow(ctx context.Context, project, location, id string) (workflowsstore.Workflow, error) {
	if location == "" || id == "" {
		return workflowsstore.Workflow{}, invalidArgument("missing workflow name")
	}
	w, err := s.workflows.GetWorkflow(ctx, project, location, id)
	if err != nil {
		return workflowsstore.Workflow{}, mapStoreError(err)
	}
	return w, nil
}

// CreateWorkflow creates a workflow and returns it with the done operation that
// carries it as the response, or AlreadyExists.
func (s *Service) CreateWorkflow(ctx context.Context, project, location string, in CreateInput) (workflowsstore.Workflow, workflowsstore.Operation, error) {
	if location == "" {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, invalidArgument("missing location")
	}
	if in.ID == "" {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, invalidArgument("missing workflowId")
	}
	now := clock.Now().UTC()
	w := workflowsstore.Workflow{
		ID:             in.ID,
		Location:       location,
		Description:    in.Description,
		Labels:         in.Labels,
		ServiceAccount: in.ServiceAccount,
		SourceContents: in.SourceContents,
		State:          "ACTIVE",
		RevisionID:     nextRevision(""),
		CreateTime:     now,
		UpdateTime:     now,
		CallLogLevel:   in.CallLogLevel,
		UserEnvVars:    in.UserEnvVars,
		Tags:           in.Tags,
	}
	if err := s.workflows.CreateWorkflow(ctx, project, location, in.ID, w); err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, mapStoreError(err)
	}
	target := WorkflowName(project, location, in.ID)
	op, err := s.storeOperation(ctx, project, location, "create", target, workflowOpResponse(w, project))
	if err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, mapStoreError(err)
	}
	return w, op, nil
}

// UpdateWorkflow applies a masked update atomically and returns the updated
// workflow with the done operation that carries it as the response. A change to
// sourceContents or serviceAccount bumps the revision.
func (s *Service) UpdateWorkflow(ctx context.Context, project, location string, in UpdateInput) (workflowsstore.Workflow, workflowsstore.Operation, error) {
	if location == "" || in.ID == "" {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, invalidArgument("missing workflow name")
	}
	masked, err := maskedFields(in.UpdateMask)
	if err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, err
	}
	apply := func(field string) bool {
		if masked == nil {
			return true // no mask: the entire workflow is updated
		}
		return masked[field]
	}
	w, err := s.workflows.UpdateWorkflowAtomic(ctx, project, location, in.ID, func(w workflowsstore.Workflow) (workflowsstore.Workflow, error) {
		sourceChanged := false
		if apply("description") {
			w.Description = in.Description
		}
		if apply("labels") {
			if in.Labels != nil {
				w.Labels = in.Labels
			}
		}
		if apply("userEnvVars") {
			if in.UserEnvVars != nil {
				w.UserEnvVars = in.UserEnvVars
			}
		}
		if apply("serviceAccount") {
			if in.ServiceAccount != w.ServiceAccount {
				w.ServiceAccount = in.ServiceAccount
				sourceChanged = true
			}
		}
		if apply("sourceContents") {
			if in.SourceContents != w.SourceContents {
				w.SourceContents = in.SourceContents
				sourceChanged = true
			}
		}
		if apply("callLogLevel") {
			w.CallLogLevel = in.CallLogLevel
		}
		if sourceChanged {
			w.RevisionID = nextRevision(w.RevisionID)
		}
		w.UpdateTime = clock.Now().UTC()
		return w, nil
	})
	if err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, mapStoreError(err)
	}
	target := WorkflowName(project, location, in.ID)
	op, err := s.storeOperation(ctx, project, location, "update", target, workflowOpResponse(w, project))
	if err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, mapStoreError(err)
	}
	return w, op, nil
}

// DeleteWorkflow deletes a workflow and returns the done delete operation. The
// delete LRO response type is google.protobuf.Empty.
func (s *Service) DeleteWorkflow(ctx context.Context, project, location, id string) (workflowsstore.Operation, error) {
	if location == "" || id == "" {
		return workflowsstore.Operation{}, invalidArgument("missing workflow name")
	}
	if err := s.workflows.DeleteWorkflow(ctx, project, location, id); err != nil {
		return workflowsstore.Operation{}, mapStoreError(err)
	}
	target := WorkflowName(project, location, id)
	op, err := s.storeOperation(ctx, project, location, "delete", target, map[string]any{})
	if err != nil {
		return workflowsstore.Operation{}, mapStoreError(err)
	}
	return op, nil
}

// GetOperation returns one persisted long-running operation, or NotFound.
func (s *Service) GetOperation(ctx context.Context, project, location, opID string) (workflowsstore.Operation, error) {
	if location == "" || opID == "" {
		return workflowsstore.Operation{}, invalidArgument("missing operation name")
	}
	op, err := s.workflows.GetOperation(ctx, project, location, opID)
	if err != nil {
		return workflowsstore.Operation{}, mapStoreError(err)
	}
	return s.settle(op), nil
}

// IsNotFound reports whether err is the canonical NotFound provider error (as
// returned by GetOperation for an absent operation), so a transport's
// ResolveOperation can decline an unknown name instead of surfacing an error.
func IsNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "NotFound"
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// workflowUpdateFields maps every accepted updateMask path to its canonical
// workflow field name. Both the proto snake_case and the JSON camelCase forms
// are accepted (a FieldMask carries the path verbatim), as is the oneof path
// "source_code.source_contents".
var workflowUpdateFields = map[string]string{
	"description":              "description",
	"labels":                   "labels",
	"userenvvars":              "userEnvVars",
	"serviceaccount":           "serviceAccount",
	"sourcecontents":           "sourceContents",
	"sourcecodesourcecontents": "sourceContents",
	"callloglevel":             "callLogLevel",
}

// canonicalMaskField normalizes an updateMask path to its canonical workflow
// field name. A leading "workflow." (the proto request wraps the resource) is
// stripped, and underscores/dots removed, so snake_case ("source_contents"),
// camelCase ("sourceContents"), and the oneof path
// ("source_code.source_contents") all normalize alike.
func canonicalMaskField(path string) (string, bool) {
	p := strings.ToLower(strings.TrimSpace(path))
	p = strings.TrimPrefix(p, "workflow.")
	p = strings.NewReplacer("_", "", ".", "").Replace(p)
	f, ok := workflowUpdateFields[p]
	return f, ok
}

// maskedFields parses an updateMask into the set of canonical fields to apply.
// An empty mask returns nil, meaning every field is applied. An unrecognized
// path fails loud rather than being silently ignored.
func maskedFields(mask string) (map[string]bool, error) {
	if strings.TrimSpace(mask) == "" {
		return nil, nil
	}
	out := map[string]bool{}
	for _, m := range strings.Split(mask, ",") {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		f, ok := canonicalMaskField(m)
		if !ok {
			return nil, model.NewProviderError("Unimplemented", "unsupported updateMask field "+m, 501)
		}
		out[f] = true
	}
	return out, nil
}

// clampPageSize applies ListWorkflows' paging contract: a default of 500 when
// unspecified, and a maximum of 1000.
func clampPageSize(n int) int {
	switch {
	case n <= 0:
		return 500
	case n > 1000:
		return 1000
	default:
		return n
	}
}

// mapStoreError maps a workflows store error onto a canonical provider error.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, workflowsstore.ErrNoSuchWorkflow):
		return model.NewProviderError("NotFound", "workflow not found", 404)
	case errors.Is(err, workflowsstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, workflowsstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "workflow already exists", 409)
	default:
		return err
	}
}
