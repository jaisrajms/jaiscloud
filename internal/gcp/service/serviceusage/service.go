// Package serviceusage is the transport-neutral core of the Service Usage v1
// service (serviceusage.googleapis.com): a project's enabled service APIs.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/serviceusage) and the REST
// transport (internal/gcp/transport/rest/serviceusage) both transcode their wire
// format into this package's typed API and then call the SAME Service instance.
// That is the dual-protocol invariant: one core, one piece of state, so the
// transports cannot drift.
//
// It is an accept-and-succeed control plane, matching the emulator's posture:
// enabling a service flips it to ENABLED, disabling reverses it, and get/list
// echo that state. There is no real API gating or dependency resolution — a
// service works whether or not it was "enabled". State lives in the shared
// ResourceStore (memory + PostgreSQL backends), so no provider-level Reset or
// Snapshotter is needed.
//
// Mutations return a google.longrunning.Operation. By default it is done
// inline (jaiscloud completes operations synchronously), so SDK/terraform
// operation futures resolve immediately. The LRO timing is opt-in via
// WithLROMode: when enabled, operations are persisted done=false and settle
// lazily on read (see settle); the default remains synchronous. The operation
// is always persisted under the shared ResourceStore so Operations.Get can read
// it back over both transports.
package serviceusage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// rtService is the resource type in the shared ResourceStore. The entry id is
// the bare service name (e.g. "run.googleapis.com"); the account scope is the
// project.
const rtService = "gcp_serviceusage_service"

// rtOperation is the resource type for a persisted google.longrunning.Operation.
// The entry id is the bare operation id (without the "operations/" prefix); the
// account scope is the project. Operations live in the same shared store as the
// service states, so memory + PostgreSQL backends behave identically and a
// poll survives a restart under --dsn.
const rtOperation = "gcp_serviceusage_operation"

// maxBatchEnable mirrors real GCP's per-request batchEnable cap.
const maxBatchEnable = 20

// ListServices page-size contract: the default is 50 and the maximum is 200
// (serviceusage.googleapis.com Discovery, services.list pageSize).
const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// State is a service's enablement state, per GoogleApiServiceusageV1Service.state.
type State string

const (
	// StateEnabled means the service is enabled for the consumer.
	StateEnabled State = "ENABLED"
	// StateDisabled means the service has been explicitly disabled or never enabled.
	StateDisabled State = "DISABLED"
)

// StateFilter narrows a ListAPIs result to one enablement state.
type StateFilter int

const (
	// FilterAll lists every tracked service.
	FilterAll StateFilter = iota
	// FilterEnabled lists only ENABLED services.
	FilterEnabled
	// FilterDisabled lists only DISABLED services.
	FilterDisabled
)

// API is the transport-neutral form of a GoogleApiServiceusageV1Service: the
// resource name, its consumer project, the configured DNS name, and the state.
type API struct {
	// Name is the full resource name: projects/{project}/services/{service}.
	Name string
	// Parent is the consumer project name: projects/{project}.
	Parent string
	// ConfigName is the service DNS name (e.g. "run.googleapis.com").
	ConfigName string
	// State is the enablement state.
	State State
}

// Operation is a persisted google.longrunning.Operation. In the default
// synchronous mode it is stored done=true with EndTime set; in async mode it is
// stored done=false and settles lazily on read. The transport renders the
// verb-appropriate response payload from Services; the metadata carries the
// affected resource names, matching google.api.serviceusage.v1.OperationMetadata.
type Operation struct {
	// Name is the operation resource name: operations/{id}.
	Name string
	// Verb is the mutation that produced the operation: "enable", "disable",
	// or "batchEnable".
	Verb string
	// ResourceNames are the full names of the services the operation touched.
	ResourceNames []string
	// Done reports whether the operation has completed.
	Done bool
	// CreateTime is when the operation was created.
	CreateTime time.Time
	// EndTime is when the operation completed (zero while in flight).
	EndTime time.Time
	// Services is the transport-neutral snapshot of the affected services, used
	// to reconstruct the typed response on a poll.
	Services []API
}

// Service is the transport-neutral Service Usage v1 service over the shared
// ResourceStore.
type Service struct {
	resources store.ResourceStore
	// lroMode controls operation timing. The zero value is synchronous: every
	// operation is stored done=true inline, matching the v1.1.0 contract. An
	// enabled mode stores operations done=false and settles them lazily on read.
	lroMode lro.Mode
}

// Option configures Service.
type Option func(*Service)

// WithLROMode sets the long-running-operation timing mode. The zero value is
// synchronous; Mode{Enabled: true, Delay: d} stores mutation operations
// done=false and settles them on read once d has elapsed.
func WithLROMode(m lro.Mode) Option {
	return func(s *Service) { s.lroMode = m }
}

// NewService returns a Service Usage core backed by the shared ResourceStore.
func NewService(resources store.ResourceStore, opts ...Option) *Service {
	s := &Service{resources: resources}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ParseFilter maps a wire filter string ("state:ENABLED", "state:DISABLED", or
// empty) to a StateFilter. Real GCP accepts only the two state filters; anything
// else is an InvalidArgument.
func ParseFilter(filter string) (StateFilter, error) {
	switch strings.TrimSpace(filter) {
	case "":
		return FilterAll, nil
	case "state:ENABLED":
		return FilterEnabled, nil
	case "state:DISABLED":
		return FilterDisabled, nil
	default:
		return FilterAll, invalidArgument(fmt.Sprintf(
			"Invalid filter %s. The allowed filter strings are state:ENABLED and state:DISABLED.", filter))
	}
}

// ListAPIs returns a cursor page of the tracked services for the project that
// match the filter, plus the next-page token (empty when exhausted).
func (s *Service) ListAPIs(ctx context.Context, project string, filter StateFilter, pageSize int, pageToken string) ([]API, string, error) {
	if project == "" {
		return nil, "", invalidArgument("missing project")
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtService, "")
	if err != nil {
		return nil, "", err
	}
	apis := make([]API, 0, len(entries))
	for _, e := range entries {
		st := decodeRecord(e.Data).State
		if !filter.matches(st) {
			continue
		}
		apis = append(apis, buildAPI(project, e.ID, st))
	}
	// The API contract is: pageSize defaults to 50 and cannot exceed 200.
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next := paging.Page(apis, func(a API) string { return a.Name }, params)
	return page, next, nil
}

// GetAPI returns one service. An unknown service resolves to DISABLED rather
// than NotFound: real GCP lists every public API, and the emulator has no
// catalog, so a never-enabled id is simply disabled.
func (s *Service) GetAPI(ctx context.Context, project, service string) (API, error) {
	if project == "" || service == "" {
		return API{}, invalidArgument("missing project or service")
	}
	st, err := s.readState(ctx, project, service)
	if err != nil {
		return API{}, err
	}
	return buildAPI(project, service, st), nil
}

// EnableAPI flips a service to ENABLED and returns it with the operation.
func (s *Service) EnableAPI(ctx context.Context, project, service string) (API, Operation, error) {
	if project == "" || service == "" {
		return API{}, Operation{}, invalidArgument("missing project or service")
	}
	api, err := s.setServiceState(ctx, project, service, StateEnabled)
	if err != nil {
		return API{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, project, "enable", []API{api})
	if err != nil {
		return API{}, Operation{}, err
	}
	return api, op, nil
}

// DisableAPI flips an ENABLED service to DISABLED and returns it with the done
// operation. Disabling a service that is not enabled is a FailedPrecondition,
// matching real GCP.
func (s *Service) DisableAPI(ctx context.Context, project, service string) (API, Operation, error) {
	if project == "" || service == "" {
		return API{}, Operation{}, invalidArgument("missing project or service")
	}
	st, err := s.readState(ctx, project, service)
	if err != nil {
		return API{}, Operation{}, err
	}
	if st != StateEnabled {
		return API{}, Operation{}, model.NewProviderError("FailedPrecondition",
			fmt.Sprintf("Service %s is not enabled for consumer projects/%s.", service, project), 400)
	}
	api, err := s.setServiceState(ctx, project, service, StateDisabled)
	if err != nil {
		return API{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, project, "disable", []API{api})
	if err != nil {
		return API{}, Operation{}, err
	}
	return api, op, nil
}

// BatchEnableAPIs enables up to maxBatchEnable services and returns them with
// the operation. An empty or oversized batch is an InvalidArgument.
func (s *Service) BatchEnableAPIs(ctx context.Context, project string, serviceIDs []string) ([]API, Operation, error) {
	if project == "" {
		return nil, Operation{}, invalidArgument("missing project")
	}
	if len(serviceIDs) == 0 {
		return nil, Operation{}, invalidArgument("serviceIds must not be empty")
	}
	if len(serviceIDs) > maxBatchEnable {
		return nil, Operation{}, invalidArgument(fmt.Sprintf(
			"A single request can enable a maximum of %d services at a time.", maxBatchEnable))
	}
	apis := make([]API, 0, len(serviceIDs))
	for _, id := range serviceIDs {
		// serviceIds are bare DNS identifiers (e.g. "run.googleapis.com"), not
		// resource names. A slash or an empty id is a malformed request; reject
		// it rather than persisting a bogus projects/{p}/services/... name.
		if id == "" || strings.Contains(id, "/") {
			return nil, Operation{}, invalidArgument(fmt.Sprintf("Invalid service id %q.", id))
		}
		api, err := s.setServiceState(ctx, project, id, StateEnabled)
		if err != nil {
			return nil, Operation{}, err
		}
		apis = append(apis, api)
	}
	op, err := s.newOperation(ctx, project, "batchEnable", apis)
	if err != nil {
		return nil, Operation{}, err
	}
	return apis, op, nil
}

// setServiceState persists a service's state and returns its typed form. A
// storage failure is propagated (never swallowed) so an outage surfaces as a
// 5xx rather than a false done:true operation.
func (s *Service) setServiceState(ctx context.Context, project, service string, state State) (API, error) {
	data, err := json.Marshal(record{State: state})
	if err != nil {
		return API{}, err
	}
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion,
		store.ResourceEntry{Type: rtService, ID: service, Data: data}); err != nil {
		return API{}, err
	}
	return buildAPI(project, service, state), nil
}

// readState returns the stored state, defaulting to DISABLED only when the
// service has never been enabled. A non-NotFound storage error is propagated.
func (s *Service) readState(ctx context.Context, project, service string) (State, error) {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtService, service)
	if errors.Is(err, store.ErrNotFound) {
		return StateDisabled, nil
	}
	if err != nil {
		return "", err
	}
	if st := decodeRecord(e.Data).State; st != "" {
		return st, nil
	}
	return StateDisabled, nil
}

// newOperation builds and persists the operation descriptor for a mutation. In
// the default synchronous mode it is stored done=true with EndTime=now, exactly
// as before; in async mode it is stored done=false with a zero EndTime and
// settles lazily on read.
func (s *Service) newOperation(ctx context.Context, project, verb string, services []API) (Operation, error) {
	now := clock.Now().UTC()
	names := make([]string, 0, len(services))
	for _, a := range services {
		names = append(names, a.Name)
	}
	id := randomHex(12)
	op := Operation{
		Name:          OperationName(id),
		Verb:          verb,
		ResourceNames: names,
		CreateTime:    now,
		Services:      services,
	}
	if s.lroMode.Async() {
		op.Done = false
	} else {
		op.Done = true
		op.EndTime = now
	}
	if err := s.persistOperation(ctx, project, id, op); err != nil {
		return Operation{}, err
	}
	return op, nil
}

// persistOperation records a mutation operation so operations.get/list and REST
// :wait can read it back after the mutation returns (and across a restart under
// --dsn). The transport-neutral service snapshot is stored and rendered on read.
func (s *Service) persistOperation(ctx context.Context, project, id string, op Operation) error {
	data, err := json.Marshal(operationRecord{
		Verb:          op.Verb,
		ResourceNames: op.ResourceNames,
		Done:          op.Done,
		CreateTime:    op.CreateTime,
		EndTime:       op.EndTime,
		Services:      op.Services,
	})
	if err != nil {
		return err
	}
	return s.resources.Upsert(ctx, project, store.GlobalRegion,
		store.ResourceEntry{Type: rtOperation, ID: id, Data: data})
}

// GetOperation returns a persisted operation by its full name (operations/{id}).
// An unknown id is NotFound. This is the read side both transports poll: done
// inline in the default synchronous mode, or settled lazily once the async delay
// has elapsed.
func (s *Service) GetOperation(ctx context.Context, project, name string) (Operation, error) {
	id, err := operationIDFromName(name)
	if err != nil {
		return Operation{}, err
	}
	return s.loadOperation(ctx, project, id)
}

// loadOperation reads and settles a persisted operation by its bare id.
func (s *Service) loadOperation(ctx context.Context, project, id string) (Operation, error) {
	e, err := s.lookupOperation(ctx, project, id)
	if err != nil {
		return Operation{}, err
	}
	op, err := operationFromEntry(e)
	if err != nil {
		return Operation{}, err
	}
	return s.settle(op), nil
}

// lookupOperation finds a persisted operation by its globally unique id. A
// top-level operations/{id} name is polled without a project segment, so after
// the project-scoped lookup misses it scans across scopes (the shared
// ResourceStore supports account=""+region="" scans on both the memory and
// PostgreSQL backends). This mirrors Cloud Functions v1's GetOperationByID.
func (s *Service) lookupOperation(ctx context.Context, project, id string) (store.ResourceEntry, error) {
	if project != "" {
		e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtOperation, id)
		if err == nil {
			return e, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return store.ResourceEntry{}, err
		}
	}
	entries, err := s.resources.List(ctx, "", "", rtOperation, id)
	if err != nil {
		return store.ResourceEntry{}, err
	}
	for _, e := range entries {
		if e.ID == id {
			return e, nil
		}
	}
	return store.ResourceEntry{}, notFound(fmt.Sprintf("Operation %s not found.", OperationName(id)))
}

// operationFromEntry decodes a persisted ResourceEntry into an Operation. A
// malformed record is an error rather than a silently settled empty operation.
func operationFromEntry(e store.ResourceEntry) (Operation, error) {
	var rec operationRecord
	if err := json.Unmarshal(e.Data, &rec); err != nil {
		return Operation{}, err
	}
	return Operation{
		Name:          OperationName(e.ID),
		Verb:          rec.Verb,
		ResourceNames: rec.ResourceNames,
		Done:          rec.Done,
		CreateTime:    rec.CreateTime,
		EndTime:       rec.EndTime,
		Services:      rec.Services,
	}, nil
}

// ListOperations returns a cursor page of the persisted operations for the
// project, plus the next-page token (empty when exhausted).
func (s *Service) ListOperations(ctx context.Context, project string, pageSize int, pageToken string) ([]Operation, string, error) {
	if project == "" {
		return nil, "", invalidArgument("missing project")
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtOperation, "")
	if err != nil {
		return nil, "", err
	}
	ops := make([]Operation, 0, len(entries))
	for _, e := range entries {
		op, err := operationFromEntry(e)
		if err != nil {
			return nil, "", err
		}
		ops = append(ops, s.settle(op))
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next := paging.Page(ops, func(op Operation) string { return op.Name }, params)
	return page, next, nil
}

// CancelOperation validates that the operation exists. The emulator does not
// model cancellation, so a known operation is a no-op success and an unknown one
// is NotFound.
func (s *Service) CancelOperation(ctx context.Context, project, name string) error {
	_, err := s.GetOperation(ctx, project, name)
	return err
}

// DeleteOperation removes a persisted operation. An unknown operation is
// NotFound, matching real google.longrunning.Operations.DeleteOperation.
func (s *Service) DeleteOperation(ctx context.Context, project, name string) error {
	id, err := operationIDFromName(name)
	if err != nil {
		return err
	}
	if project != "" {
		if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtOperation, id); err == nil {
			return nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	// Not in the caller's project scope: a top-level operation is stored under
	// its creating project, so resolve it across scopes and delete it there.
	e, err := s.lookupOperation(ctx, "", id)
	if err != nil {
		return err
	}
	return s.resources.Delete(ctx, e.Account, store.GlobalRegion, rtOperation, id)
}

// settle derives the rendered state of a persisted operation from its stored
// done flag and the configured timing mode. An in-flight operation (done=false)
// becomes done once the delay has elapsed, with a deterministic EndTime of
// createTime+delay (a zero delay yields createTime). The flip is derived on read
// rather than written back; the persisted flag is an input while the settled
// state is a pure function of it and the clock. This mirrors the workflows and
// functions cores.
func (s *Service) settle(op Operation) Operation {
	if op.Done || s.lroMode.Pending(op.CreateTime) {
		return op
	}
	op.Done = true
	op.EndTime = op.CreateTime.Add(s.lroMode.Delay)
	return op
}

// IsNotFound reports whether err is the canonical NotFound provider error (as
// returned by GetOperation for an absent operation).
func IsNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "NotFound"
}

// operationIDFromName validates an operation resource name (operations/{id})
// and returns the bare id.
func operationIDFromName(name string) (string, error) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) == 2 && parts[0] == "operations" && parts[1] != "" {
		return parts[1], nil
	}
	return "", invalidArgument("invalid operation name")
}

// IsTopLevelOperationName reports whether name is the top-level
// "operations/{id}" shape Service Usage shares with Cloud Functions v1. Both
// transports use it to scope their cross-service operation resolvers, so the
// ownership rule lives in one place.
func IsTopLevelOperationName(name string) bool {
	_, err := operationIDFromName(name)
	return err == nil
}

// operationRecord is the persisted per-operation state.
type operationRecord struct {
	Verb          string    `json:"verb"`
	ResourceNames []string  `json:"resourceNames"`
	Done          bool      `json:"done"`
	CreateTime    time.Time `json:"createTime"`
	EndTime       time.Time `json:"endTime,omitempty"`
	Services      []API     `json:"services,omitempty"`
}

// matches reports whether a state passes the filter.
func (f StateFilter) matches(st State) bool {
	switch f {
	case FilterEnabled:
		return st == StateEnabled
	case FilterDisabled:
		return st == StateDisabled
	default:
		return true
	}
}

// record is the stored per-service state.
type record struct {
	State State `json:"state"`
}

func decodeRecord(data json.RawMessage) record {
	var r record
	_ = json.Unmarshal(data, &r)
	return r
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// notFound builds the canonical NotFound provider error.
func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

// randomHex returns n random hexadecimal characters (zero-filled on RNG
// failure, which never blocks a mutation).
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
