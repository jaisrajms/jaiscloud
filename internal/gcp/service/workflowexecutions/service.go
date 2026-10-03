// Package workflowexecutions is the transport-neutral core of the Cloud
// Workflow Executions v1 service (workflowexecutions.googleapis.com). It owns
// all execution business logic — creating an execution (which the emulator runs
// to completion synchronously), reading/listing executions, and cancelling one —
// over the shared Cloud Workflows store (internal/gcp/store/workflows,
// shared with the Workflows management service; the store is not duplicated).
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/workflowexecutions) and the REST
// transport (internal/gcp/transport/rest/workflowexecutions) both transcode
// their wire format into this package's typed API and then call the SAME
// Service instance. That is the dual-protocol invariant: one core, one piece of
// state, so the transports cannot drift.
//
// The engine executes synchronously and the emulator returns the terminal
// execution from CreateExecution (a documented simplification of GCP's async
// model), so CancelExecution is a no-op except for a (never-observed) ACTIVE
// transition.
package workflowexecutions

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"

	"github.com/google/uuid"
)

// Service is the transport-neutral Cloud Workflow Executions service over the
// shared workflows store and workflow engine.
type Service struct {
	workflows workflowsstore.Store
	engine    *workflowengine.Engine
}

// NewService returns a Workflow Executions core backed by the given store and
// engine.
func NewService(workflows workflowsstore.Store, eng *workflowengine.Engine) *Service {
	return &Service{workflows: workflows, engine: eng}
}

// CreateExecutionInput carries the caller-supplied fields of an execution. The
// argument is the raw JSON string from the wire (validated here); the transports
// resolve the owning project/workflow and pass only this payload.
type CreateExecutionInput struct {
	Argument     string
	CallLogLevel string
	Labels       map[string]string
}

// CreateExecution runs the workflow to completion synchronously and persists
// the terminal execution. It returns InvalidArgument for a malformed argument
// JSON or an invalid workflow definition, and NotFound for a missing workflow.
func (s *Service) CreateExecution(ctx context.Context, project, location, workflowID string, in CreateExecutionInput) (workflowsstore.Execution, error) {
	if location == "" || workflowID == "" {
		return workflowsstore.Execution{}, invalidArgument("missing workflow in parent")
	}
	wf, err := s.workflows.GetWorkflow(ctx, project, location, workflowID)
	if err != nil {
		return workflowsstore.Execution{}, mapStoreError(err)
	}

	var argument any
	if in.Argument != "" {
		if err := json.Unmarshal([]byte(in.Argument), &argument); err != nil {
			return workflowsstore.Execution{}, invalidArgument("argument must be valid JSON")
		}
	}

	executionID := uuid.New().String()
	now := clock.Now().UTC()

	// Run the workflow to completion synchronously (documented simplification).
	// Inject the emulator's project/location/workflow context so GCP built-in
	// environment variables (GOOGLE_CLOUD_*) resolve via sys.get_env.
	res := s.engine.ExecuteWithContext(ctx, wf.SourceContents, argument, workflowengine.WorkflowContext{
		ProjectID:      project,
		Location:       location,
		WorkflowID:     workflowID,
		RevisionID:     wf.RevisionID,
		ServiceAccount: wf.ServiceAccount,
		ExecutionID:    executionID,
	})

	e := workflowsstore.Execution{
		ID:                 executionID,
		WorkflowID:         workflowID,
		Location:           location,
		Argument:           in.Argument,
		StartTime:          now,
		WorkflowRevisionID: wf.RevisionID,
		CallLogLevel:       in.CallLogLevel,
		Labels:             in.Labels,
	}

	if res.Step != "" {
		e.CurrentSteps = []workflowsstore.Step{{Routine: "main", Step: res.Step}}
	}

	if res.Err != nil {
		var ve *workflowengine.ValidationError
		if errors.As(res.Err, &ve) {
			return workflowsstore.Execution{}, invalidArgument(ve.Error())
		}
		e.State = "FAILED"
		e.Error = &workflowsstore.ExecutionError{
			Payload: errorPayload(res.Err, res.Step),
			Context: res.Err.Error(),
		}
	} else {
		e.State = "SUCCEEDED"
		resultJSON, _ := json.Marshal(res.Value)
		e.Result = string(resultJSON)
	}
	e.EndTime = clock.Now().UTC()
	e.Duration = durationString(e.EndTime.Sub(e.StartTime))

	if err := s.workflows.CreateExecution(ctx, project, location, workflowID, executionID, e); err != nil {
		return workflowsstore.Execution{}, mapStoreError(err)
	}
	return e, nil
}

// GetExecution returns one execution, projected to the requested view.
func (s *Service) GetExecution(ctx context.Context, project, location, workflowID, executionID string, view View) (workflowsstore.Execution, error) {
	if location == "" || workflowID == "" || executionID == "" {
		return workflowsstore.Execution{}, invalidArgument("missing execution name")
	}
	e, err := s.workflows.GetExecution(ctx, project, location, workflowID, executionID)
	if err != nil {
		return workflowsstore.Execution{}, mapStoreError(err)
	}
	return applyView(e, view), nil
}

// ListExecutions returns a cursor page of executions for a workflow, projected
// to the requested view. The page token is the shared cursor encoding used by
// the other GCP v1 list methods.
//
// Documented approximation: the emulator paginates on the execution ID rather
// than the (startTime, id) cursor real GCP uses, so a page is ordered by ID
// instead of start-time-newest-first. The result set and page boundaries are
// otherwise correct.
func (s *Service) ListExecutions(ctx context.Context, project, location, workflowID string, view View, pageSize int, pageToken string) ([]workflowsstore.Execution, string, error) {
	if location == "" || workflowID == "" {
		return nil, "", invalidArgument("missing workflow in parent")
	}
	execs, err := s.workflows.ListExecutions(ctx, project, location, workflowID)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next := paging.Page(execs, func(e workflowsstore.Execution) string { return e.ID }, params)
	out := make([]workflowsstore.Execution, 0, len(page))
	for _, e := range page {
		out = append(out, applyView(e, view))
	}
	return out, next, nil
}

// CancelExecution stops an ACTIVE execution. Executions complete synchronously
// in the emulator, so cancel is a no-op for a terminal execution and returns it
// unchanged.
func (s *Service) CancelExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	if location == "" || workflowID == "" || executionID == "" {
		return workflowsstore.Execution{}, invalidArgument("missing execution name")
	}
	e, err := s.workflows.GetExecution(ctx, project, location, workflowID, executionID)
	if err != nil {
		return workflowsstore.Execution{}, mapStoreError(err)
	}
	if e.State == "ACTIVE" {
		e.State = "CANCELLED"
		e.EndTime = clock.Now().UTC()
		e.Error = &workflowsstore.ExecutionError{Payload: errorPayload(errors.New("execution cancelled"), "")}
		if err := s.workflows.UpdateExecution(ctx, project, location, workflowID, executionID, e); err != nil {
			return workflowsstore.Execution{}, mapStoreError(err)
		}
	}
	return applyView(e, ViewFull), nil
}

// View selects which execution fields the wire response carries. BASIC carries
// only the summary metadata (name, times, duration, state, revision); FULL
// additionally carries argument/result/error/labels/status/callLogLevel.
type View int

const (
	// ViewBasic is the proto ExecutionView_BASIC projection.
	ViewBasic View = iota
	// ViewFull is the proto ExecutionView_FULL projection.
	ViewFull
)

// applyView projects an execution to the requested view. FULL (and the
// transport-resolved default) returns the execution unchanged.
func applyView(e workflowsstore.Execution, view View) workflowsstore.Execution {
	if view == ViewFull {
		return e
	}
	e.Argument = ""
	e.Result = ""
	e.Error = nil
	e.CallLogLevel = ""
	e.Labels = nil
	e.CurrentSteps = nil
	return e
}

// ParseName extracts the location, workflow ID, and execution ID from a
// relative or full execution resource name
// (locations/{l}/workflows/{w}[/executions[/{e}]] or the projects/{p}/-prefixed
// form). Empty segments are returned when absent.
func ParseName(name string) (location, workflowID, executionID string) {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		switch p {
		case "locations":
			if i+1 < len(parts) {
				location = parts[i+1]
			}
		case "workflows":
			if i+1 < len(parts) {
				workflowID = parts[i+1]
			}
		case "executions":
			if i+1 < len(parts) {
				executionID = parts[i+1]
			}
		}
	}
	return location, workflowID, executionID
}

// ProjectFromName returns the project id from a "projects/{p}/..." resource
// name, or "" when the name is not project-scoped.
func ProjectFromName(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		if p == "projects" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// durationString renders a proto3 Duration JSON string (e.g. "0.5s").
func durationString(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

// errorPayload renders an engine error as the execution error payload JSON
// ({"message":..., "tags":[step]}).
func errorPayload(err error, step string) string {
	tags := []any{}
	if step != "" {
		tags = append(tags, step)
	}
	msg := err.Error()
	payload, _ := json.Marshal(map[string]any{"message": msg, "tags": tags})
	return string(payload)
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// mapStoreError maps a workflows store error onto a canonical provider error.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, workflowsstore.ErrNoSuchExecution):
		return model.NewProviderError("NotFound", "execution not found", 404)
	case errors.Is(err, workflowsstore.ErrNoSuchWorkflow):
		return model.NewProviderError("NotFound", "workflow not found", 404)
	case errors.Is(err, workflowsstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "execution already exists", 409)
	default:
		return err
	}
}
