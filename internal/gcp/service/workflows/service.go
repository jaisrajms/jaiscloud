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
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/operationfilter"
	"jaiscloud/internal/gcp/paging"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
)

// Service is the transport-neutral Cloud Workflows v1 service over the shared
// workflows store.
type Service struct {
	workflows workflowsstore.Store
	// lroMode controls operation timing. The zero value is synchronous: every
	// operation is stored done=true inline, matching the default synchronous contract. An
	// enabled mode stores operations done=false and settles them lazily on read.
	lroMode lro.Mode
	// tracker publishes the one-time "operation settled" event when a lazily
	// settled operation completes. Nil (no bus) makes it inert.
	tracker *lro.Tracker
}

// Option configures Service.
type Option func(*Service)

// WithLROMode sets the long-running-operation timing mode. The zero value is
// synchronous; Mode{Enabled: true, Delay: d} stores create/update/delete
// operations done=false and settles them on read once d has elapsed.
func WithLROMode(m lro.Mode) Option {
	return func(s *Service) { s.lroMode = m }
}

// WithEventBus wires the shared event bus the console's live status stream
// subscribes to. Nil (the default) disables status events.
func WithEventBus(bus *events.EventBus) Option {
	return func(s *Service) {
		if bus != nil {
			s.tracker = lro.NewTracker(bus)
		}
	}
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

// ListWorkflows returns a cursor page of the workflows in a location, applying
// the documented AIP-160 filter and orderBy. An empty filter matches every
// workflow; a filter or orderBy outside the supported grammar is
// InvalidArgument (never silently ignored). A negative pageSize is rejected.
func (s *Service) ListWorkflows(ctx context.Context, project, location, filter, orderBy string, pageSize int, pageToken string) ([]workflowsstore.Workflow, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	nameOf := func(w workflowsstore.Workflow) string { return WorkflowName(project, location, w.ID) }
	flt, err := compileWorkflowFilter(filter, nameOf)
	if err != nil {
		return nil, "", err
	}
	order, err := workflowOrderSpec.Parse(orderBy)
	if err != nil {
		return nil, "", invalidArgument("workflows ListWorkflows orderBy: " + err.Error())
	}
	wfs, err := s.workflows.ListWorkflows(ctx, project, location)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	out := make([]workflowsstore.Workflow, 0, len(wfs))
	for _, w := range wfs {
		if flt.match(w) {
			out = append(out, w)
		}
	}
	order.Sort(out)
	params := map[string]any{"pageSize": clampPageSize(pageSize)}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next, err := paging.PageOrdered(out, params, order.Cursor, order.AfterCursor)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// ListWorkflowsByProject lists every workflow in a project across all
// locations, sorted by location then ID. It backs the location-optional console
// list, which aggregates locations rather than requiring a location picker.
func (s *Service) ListWorkflowsByProject(ctx context.Context, project string) ([]workflowsstore.Workflow, error) {
	wfs, err := s.workflows.ListWorkflowsByProject(ctx, project)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return wfs, nil
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

// Revision is a workflow revision rendered together with the workflow-wide
// fields it belongs to. Transports render it with the same shape as a Workflow
// (WorkflowJSON / workflowToProto); only the revision-scoped fields differ.
type Revision struct {
	Workflow           workflowsstore.Workflow // live workflow-wide fields (name, description, labels, create/update time)
	RevisionID         string                  // output-only revision
	RevisionCreateTime time.Time               // when this revision was created
	State              string                  // deployment state at this revision
	SourceContents     string                  // workflow YAML
	ServiceAccount     string                  // runtime identity
	CallLogLevel       string                  // call log level
	UserEnvVars        map[string]string       // user-defined environment variables
}

// liveRevisionView renders the current workflow as the "latest revision" view.
func liveRevisionView(w workflowsstore.Workflow) Revision {
	return Revision{
		Workflow:           w,
		RevisionID:         w.RevisionID,
		RevisionCreateTime: liveRevisionCreateTime(w),
		State:              w.State,
		SourceContents:     w.SourceContents,
		ServiceAccount:     w.ServiceAccount,
		CallLogLevel:       w.CallLogLevel,
		UserEnvVars:        w.UserEnvVars,
	}
}

// revisionView combines a stored revision snapshot with the workflow-wide
// fields of the live workflow it belongs to.
func revisionView(w workflowsstore.Workflow, r workflowsstore.Revision) Revision {
	return Revision{
		Workflow:           w,
		RevisionID:         r.RevisionID,
		RevisionCreateTime: r.RevisionCreateTime,
		State:              r.State,
		SourceContents:     r.SourceContents,
		ServiceAccount:     r.ServiceAccount,
		CallLogLevel:       r.CallLogLevel,
		UserEnvVars:        r.UserEnvVars,
	}
}

// GetWorkflowRevision returns a workflow as of a specific revision. An empty
// revisionID returns the current workflow; an unknown workflow or revision is
// NotFound.
func (s *Service) GetWorkflowRevision(ctx context.Context, project, location, id, revisionID string) (Revision, error) {
	if location == "" || id == "" {
		return Revision{}, invalidArgument("missing workflow name")
	}
	w, err := s.workflows.GetWorkflow(ctx, project, location, id)
	if err != nil {
		return Revision{}, mapStoreError(err)
	}
	if revisionID == "" || revisionID == w.RevisionID {
		// The current revision is the live workflow: rendering it from the
		// stored snapshot would report a stale userEnvVars/callLogLevel after a
		// workflow-wide (non-revision) update. See ListWorkflowRevisions.
		return liveRevisionView(w), nil
	}
	r, err := s.workflows.GetRevision(ctx, project, location, id, revisionID)
	if err != nil {
		return Revision{}, mapStoreError(err)
	}
	return revisionView(w, r), nil
}

// ListWorkflowRevisions returns a cursor page of a workflow's revision history,
// newest first. pageSize defaults to 20 and is capped at 100 (the Discovery
// contract for :listRevisions). An unknown workflow is NotFound.
func (s *Service) ListWorkflowRevisions(ctx context.Context, project, location, id string, pageSize int, pageToken string) ([]Revision, string, error) {
	if location == "" || id == "" {
		return nil, "", invalidArgument("missing workflow name")
	}
	// This surface has its own newest-first cursor (pageRevisions), not the
	// shared pager, so it validates the AIP-158 negative-pageSize rule itself.
	if pageSize < 0 {
		return nil, "", invalidArgument("pageSize must not be negative")
	}
	w, err := s.workflows.GetWorkflow(ctx, project, location, id)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	revs, err := s.workflows.ListRevisions(ctx, project, location, id)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	views := make([]Revision, 0, len(revs))
	for _, r := range revs {
		// The newest revision is the live workflow. Render it from the live
		// record so its revision-scoped fields (userEnvVars, callLogLevel,
		// state) and revisionCreateTime match GetWorkflow after a workflow-wide
		// update that did not mint a new revision; older revisions keep their
		// immutable snapshots.
		if r.RevisionID == w.RevisionID {
			views = append(views, liveRevisionView(w))
			continue
		}
		views = append(views, revisionView(w, r))
	}
	if len(views) == 0 {
		// Legacy data (a store written before revisions were modelled, or a
		// snapshot without them): report the current revision rather than an
		// empty history.
		views = append(views, liveRevisionView(w))
	}
	page, next := pageRevisions(views, pageSize, pageToken)
	return page, next, nil
}

// pageRevisions applies the :listRevisions paging contract to an already
// newest-first list. The opaque pageToken is the base64 revision ID of the last
// item of the previous page (the same convention as paging.Page).
func pageRevisions(revs []Revision, pageSize int, pageToken string) ([]Revision, string) {
	size := clampRevisionPageSize(pageSize)
	start := 0
	if pageToken != "" {
		if b, err := base64.RawURLEncoding.DecodeString(pageToken); err == nil {
			cursor := string(b)
			for start < len(revs) && revs[start].RevisionID != cursor {
				start++
			}
			if start < len(revs) {
				start++ // resume after the cursor revision
			}
		}
	}
	if start >= len(revs) {
		return []Revision{}, ""
	}
	end := start + size
	if end >= len(revs) {
		return revs[start:], ""
	}
	return revs[start:end], base64.RawURLEncoding.EncodeToString([]byte(revs[end-1].RevisionID))
}

// CreateWorkflow creates a workflow and returns it with the done operation that
// carries it as the response, or AlreadyExists.
func (s *Service) CreateWorkflow(ctx context.Context, project, location string, in CreateInput) (workflowsstore.Workflow, workflowsstore.Operation, error) {
	if location == "" {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, invalidArgument("missing location")
	}
	if err := validateWorkflowID(in.ID); err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, err
	}
	if err := validateSource(in.SourceContents); err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, err
	}
	now := clock.Now().UTC()
	w := workflowsstore.Workflow{
		ID:                 in.ID,
		Location:           location,
		Description:        in.Description,
		Labels:             in.Labels,
		ServiceAccount:     in.ServiceAccount,
		SourceContents:     in.SourceContents,
		State:              "ACTIVE",
		RevisionID:         nextRevision(""),
		CreateTime:         now,
		UpdateTime:         now,
		RevisionCreateTime: now,
		CallLogLevel:       in.CallLogLevel,
		UserEnvVars:        in.UserEnvVars,
		Tags:               in.Tags,
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
	if err := validateWorkflowID(in.ID); err != nil {
		return workflowsstore.Workflow{}, workflowsstore.Operation{}, err
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
	if apply("sourceContents") {
		if err := validateSource(in.SourceContents); err != nil {
			return workflowsstore.Workflow{}, workflowsstore.Operation{}, err
		}
	}
	w, err := s.workflows.UpdateWorkflowAtomic(ctx, project, location, in.ID, func(w workflowsstore.Workflow) (workflowsstore.Workflow, error) {
		now := clock.Now().UTC()
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
			w.RevisionCreateTime = now
		}
		w.UpdateTime = now
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

// ListOperations returns a cursor page of the long-running operations persisted
// for a location, in the deterministic order of the shared pagination cursor
// (operation ID). Each returned operation is settled against the configured
// timing mode, so a client polling the page sees the same state GetOperation
// reports. It backs both the REST projects.locations.operations.list surface
// (gcloud workflows operations list / the Go SDK) and the shared gRPC
// google.longrunning.Operations.List.
//
// The standard google.longrunning.Operations `filter` is evaluated with the
// shared AIP-160 operations compiler over name, done, verb, target and state
// (including their metadata.* aliases); the filter runs on the settled
// operation and before pagination, so a page is always a page of matches.
func (s *Service) ListOperations(ctx context.Context, project, location string, pageSize int, pageToken, filter string) ([]workflowsstore.Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	compiled, err := operationfilter.Compile(filter,
		"verb", "target", "state",
		"metadata.verb", "metadata.target", "metadata.state",
		"createTime", "endTime", "metadata.createTime", "metadata.endTime",
	)
	if err != nil {
		return nil, "", invalidArgument("workflows ListOperations filter: " + err.Error())
	}
	ops, err := s.workflows.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	if len(ops) == 0 {
		// nil means "this location has no operations" (the gRPC ListRegistry
		// declines so another service can answer); a non-nil empty page below
		// means the location is owned but the filter matched nothing.
		return nil, "", nil
	}
	matched := make([]workflowsstore.Operation, 0, len(ops))
	for _, stored := range ops {
		op := s.settle(stored)
		if compiled.Match(workflowOperationRecord(project, op)) {
			matched = append(matched, op)
		}
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next, err := paging.Page(matched, func(op workflowsstore.Operation) string { return op.ID }, params)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// workflowOperationRecord renders one settled operation into the shared filter
// view. state is the generic DONE/RUNNING projection of the done flag; the
// metadata.* aliases mirror the rendered google.longrunning.Operation shape.
func workflowOperationRecord(project string, op workflowsstore.Operation) operationfilter.Record {
	state := "RUNNING"
	if op.Done {
		state = "DONE"
	}
	created := op.CreateTime.UTC().Format(time.RFC3339Nano)
	fields := map[string]string{
		"verb":                op.Verb,
		"target":              op.Target,
		"state":               state,
		"metadata.verb":       op.Verb,
		"metadata.target":     op.Target,
		"metadata.state":      state,
		"createtime":          created,
		"metadata.createtime": created,
	}
	if !op.EndTime.IsZero() {
		end := op.EndTime.UTC().Format(time.RFC3339Nano)
		fields["endtime"] = end
		fields["metadata.endtime"] = end
	}
	return operationfilter.Record{
		Name:   OperationName(project, op.Location, op.ID),
		Done:   op.Done,
		Fields: fields,
	}
}

// DeleteOperation removes a persisted long-running operation, or NotFound when
// the id is unknown. Per the Discovery, deleting an operation only records that
// the client is no longer interested in the result; it does not cancel it.
func (s *Service) DeleteOperation(ctx context.Context, project, location, id string) error {
	if location == "" || id == "" {
		return invalidArgument("missing operation name")
	}
	if err := s.workflows.DeleteOperation(ctx, project, location, id); err != nil {
		return mapStoreError(err)
	}
	return nil
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
// unspecified, and a maximum of 1000. A negative size is passed through so the
// shared cursor pager rejects it with INVALID_ARGUMENT.
func clampPageSize(n int) int {
	switch {
	case n == 0:
		return 500
	case n > 1000:
		return 1000
	default:
		return n
	}
}

// clampRevisionPageSize applies ListWorkflowRevisions' paging contract: a
// default of 20 when unspecified, and a maximum of 100 (per the Discovery doc).
// ListWorkflowRevisions rejects a negative pageSize before reaching here, so the
// clamp only ever sees 0 (unset) or a positive size.
func clampRevisionPageSize(n int) int {
	switch {
	case n <= 0:
		return 20
	case n > 100:
		return 100
	default:
		return n
	}
}

// mapStoreError maps a workflows store error onto a canonical provider error.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, workflowsstore.ErrNoSuchWorkflow):
		return model.NewProviderError("NotFound", "workflow not found", 404)
	case errors.Is(err, workflowsstore.ErrNoSuchRevision):
		return model.NewProviderError("NotFound", "workflow revision not found", 404)
	case errors.Is(err, workflowsstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, workflowsstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "workflow already exists", 409)
	default:
		return err
	}
}
