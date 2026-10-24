// Package run is the transport-neutral core for Cloud Run Admin v2
// (run.googleapis.com). It implements the behavioural control plane the emulator
// exposes — services CRUD, revisions, service IAM, and google.longrunning
// operations — over a Store. The REST adapter and the gRPC adapter
// (internal/gcp/transport/grpc/run) share one instance, so they cannot drift.
//
// The runtime is behind a RuntimeManager seam whose default implementation is
// the MockRuntime: a service is a stored record, not a running container; the
// k8s executor (internal/gcp/runexec) launches the template image instead.
package run

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/policy"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// servicePolicy is the policy resource type for a Cloud Run service IAM policy.
const servicePolicy = "run-service-policy"

// validServiceID is Cloud Run's service-id grammar (RFC 1035 label).
var validServiceID = regexp.MustCompile(`^[a-z](?:[a-z0-9-]{0,47}[a-z0-9])?$`)

// Cloud Run traffic-tag rules, probed against real GCP (the API answers 400
// INVALID_ARGUMENT otherwise). A tag is an RFC 1035 label — lowercase letters,
// digits and hyphens, beginning with a letter and not ending with a hyphen — 3
// to 47 characters long, and the tag and the service id together may not exceed
// 46 characters. Tags must also be unique within a service. The tag becomes the
// leading DNS label of the tag-qualified host ("{tag}---{service-host}").
const (
	trafficTagMinLen      = 3
	trafficTagMaxLen      = 47
	trafficTagCombinedMax = 46
)

// validTrafficTag is the RFC 1035 label shape of a traffic tag.
var validTrafficTag = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)

// Service is the transport-neutral Cloud Run core.
type Service struct {
	store     runstore.Store
	resources store.ResourceStore
	lro       lro.Mode
	runtime   RuntimeManager
	host      HostConfig
	// eventBus publishes cloud-neutral status events for the console's live
	// stream. Nil disables them (unit tests that only exercise the control
	// plane).
	eventBus *events.EventBus
	// tracker publishes the one-time "operation settled" event when a lazily
	// settled operation completes. Nil (no bus) makes it inert.
	tracker *lro.Tracker
}

// Option customises a Service.
type Option func(*Service)

// WithLROMode selects the long-running-operation timing mode (default sync).
func WithLROMode(m lro.Mode) Option {
	return func(s *Service) { s.lro = m }
}

// WithRuntimeManager overrides the runtime seam. A nil manager is ignored,
// leaving the mock in place.
func WithRuntimeManager(m RuntimeManager) Option {
	return func(s *Service) {
		if m != nil {
			s.runtime = m
		}
	}
}

// WithURLSuffix sets the DNS suffix used to synthesize a service uri, leaving
// the scheme and port from the process-wide default.
func WithURLSuffix(suffix string) Option {
	return func(s *Service) {
		if suffix != "" {
			s.host.Suffix = strings.TrimPrefix(suffix, ".")
		}
	}
}

// WithInvocationHost overrides the full invocation host config (scheme, suffix,
// port) for this core. It is used by k8s execution mode so the synthesized uri
// is the http authority the client can send back as a Host header.
func WithInvocationHost(c HostConfig) Option {
	return func(s *Service) { s.host = normalizeHostConfig(c) }
}

// WithEventBus wires the shared event bus the console's live status stream
// subscribes to. Nil (the default) disables status events.
func WithEventBus(bus *events.EventBus) Option {
	return func(s *Service) {
		if bus != nil {
			s.eventBus = bus
			s.tracker = lro.NewTracker(bus)
		}
	}
}

// NewService returns a Cloud Run core over the store using the mock runtime.
func NewService(s runstore.Store, resources store.ResourceStore, opts ...Option) *Service {
	svc := &Service{
		store:     s,
		resources: resources,
		runtime:   MockRuntime{},
		host:      invocationHost,
	}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

// Reset clears all services, revisions and operations (/_jaiscloud/reset).
func (s *Service) Reset(ctx context.Context) {
	s.runtime.Reset(ctx)
	s.store.Reset(ctx)
}

// CreateService validates and stores a new service and its first revision, and
// returns the create operation (done inline in the default synchronous mode).
// validateOnly validates the request, including the duplicate-id check, and
// returns an unpersisted operation whose response previews the would-be service
// without touching the store or the runtime.
func (s *Service) CreateService(ctx context.Context, project, location, serviceID string, body map[string]any, validateOnly bool) (runstore.Operation, error) {
	return s.createService(ctx, project, location, serviceID, body, validateOnly, "create")
}

// createService is the shared create implementation for CreateService and
// UpdateService's allowMissing upsert. verb labels the operation ("create", or
// "update" for the upsert) but does not affect the wire shape.
func (s *Service) createService(ctx context.Context, project, location, serviceID string, body map[string]any, validateOnly bool, verb string) (runstore.Operation, error) {
	if project == "" || location == "" {
		return runstore.Operation{}, invalidArgument("project and location are required")
	}
	if serviceID == "" {
		serviceID = lastSegment(str(body, "name"))
	}
	if serviceID == "" {
		return runstore.Operation{}, invalidArgument("serviceId is required")
	}
	if !validServiceID.MatchString(serviceID) {
		return runstore.Operation{}, invalidArgument("invalid service id: " + serviceID)
	}
	if _, err := s.store.GetService(ctx, project, location, serviceID); err == nil {
		return runstore.Operation{}, model.NewProviderError("AlreadyExists", "service already exists: "+serviceID, 409)
	} else if !errors.Is(err, runstore.ErrNoSuchService) {
		return runstore.Operation{}, err
	}
	if err := validateTraffic(serviceID, body); err != nil {
		return runstore.Operation{}, err
	}

	now := clock.Now()
	revID := serviceID + "-00001"
	svc := runstore.Service{
		ProjectID:             project,
		Location:              location,
		ID:                    serviceID,
		UID:                   newUUID(),
		Generation:            1,
		Etag:                  newUUID(),
		Uri:                   s.uri(project, location, serviceID),
		LatestReadyRevision:   RevisionName(project, location, serviceID, revID),
		LatestCreatedRevision: RevisionName(project, location, serviceID, revID),
		CreateTime:            now,
		UpdateTime:            now,
		Data:                  writableData(body),
	}
	rev := runstore.Revision{
		ProjectID:  project,
		Location:   location,
		Service:    serviceID,
		ID:         revID,
		UID:        newUUID(),
		Generation: 1,
		Etag:       newUUID(),
		CreateTime: now,
		UpdateTime: now,
		Data:       revisionData(body),
	}

	if !validateOnly {
		if err := s.runtime.EnsureRevision(ctx, svc, rev); err != nil {
			return runstore.Operation{}, err
		}
		if err := s.store.CreateService(ctx, project, location, svc); err != nil {
			return runstore.Operation{}, mapStoreErr(err)
		}
		if err := s.store.CreateRevision(ctx, project, location, serviceID, rev); err != nil {
			return runstore.Operation{}, mapStoreErr(err)
		}
	}
	return s.recordOperation(ctx, project, location, verb, svc, validateOnly)
}

// GetService returns a service by id.
func (s *Service) GetService(ctx context.Context, project, location, id string) (runstore.Service, error) {
	svc, err := s.store.GetService(ctx, project, location, id)
	if err != nil {
		return runstore.Service{}, mapStoreErr(err)
	}
	return svc, nil
}

// ListServices lists services under a location, sorted by id.
func (s *Service) ListServices(ctx context.Context, project, location string) ([]runstore.Service, error) {
	svcs, err := s.store.ListServices(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return svcs, nil
}

// ListAllServices lists every service in a project across all locations,
// sorted by location then id. It backs the region-optional console list.
func (s *Service) ListAllServices(ctx context.Context, project string) ([]runstore.Service, error) {
	svcs, err := s.store.ListServicesByProject(ctx, project)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return svcs, nil
}

// UpdateService applies an update mask and returns the update operation. A
// template change mints the next revision. The request body's top-level etag is
// an optimistic-concurrency precondition (a mismatch is Aborted). allowMissing
// upserts: a missing service is created instead of NotFound. validateOnly
// validates and previews the result without persisting or touching the runtime.
func (s *Service) UpdateService(ctx context.Context, project, location, id string, body map[string]any, updateMask string, validateOnly, allowMissing bool) (runstore.Operation, error) {
	existing, err := s.store.GetService(ctx, project, location, id)
	if err != nil {
		if allowMissing && errors.Is(err, runstore.ErrNoSuchService) {
			return s.createService(ctx, project, location, id, body, validateOnly, "update")
		}
		return runstore.Operation{}, mapStoreErr(err)
	}
	if err := checkEtag(str(body, "etag"), existing.Etag); err != nil {
		return runstore.Operation{}, err
	}
	now := clock.Now()
	// Copy the writable Data map before merging: a store may hand back a struct
	// that aliases the stored map (memory), so applyUpdate on the shared map
	// would mutate persisted state even on a validate-only request and would
	// rewrite the map an earlier operation snapshot points at.
	updated := existing
	updated.Data = cloneMap(existing.Data)
	applyUpdate(updated.Data, body, updateMask)
	if err := validateTraffic(id, updated.Data); err != nil {
		return runstore.Operation{}, err
	}

	templateChanged := templateMasked(updateMask) || (strings.TrimSpace(updateMask) == "" && bodyHasTemplate(body))
	updated.Generation++
	updated.Etag = newUUID()
	updated.UpdateTime = now

	var rev runstore.Revision
	if templateChanged {
		// Tear down the revision currently serving traffic before standing up
		// its replacement. Revisions are always-on, so the old runtime is only
		// removed here (and on service delete/reset/orphan sweep). Teardown is
		// best-effort: a failure must not block the update. A validate-only
		// request previews the new revision but touches nothing.
		if !validateOnly {
			s.removeServingRevision(ctx, existing)
		}
		revID := nextRevisionID(id, existing.LatestCreatedRevision)
		rev = runstore.Revision{
			ProjectID:  project,
			Location:   location,
			Service:    id,
			ID:         revID,
			UID:        newUUID(),
			Generation: 1,
			Etag:       newUUID(),
			CreateTime: now,
			UpdateTime: now,
			Data:       revisionData(body),
		}
		updated.LatestCreatedRevision = RevisionName(project, location, id, revID)
		updated.LatestReadyRevision = updated.LatestCreatedRevision
		if !validateOnly {
			if err := s.runtime.EnsureRevision(ctx, updated, rev); err != nil {
				return runstore.Operation{}, err
			}
		}
	}
	if !validateOnly {
		if err := s.store.UpdateService(ctx, project, location, updated); err != nil {
			return runstore.Operation{}, mapStoreErr(err)
		}
		if templateChanged {
			if err := s.store.CreateRevision(ctx, project, location, id, rev); err != nil {
				return runstore.Operation{}, mapStoreErr(err)
			}
		}
	}
	return s.recordOperation(ctx, project, location, "update", updated, validateOnly)
}

// removeServingRevision tears down the runtime of the revision currently
// serving a service (best-effort). It is called when a template-changing update
// replaces the serving revision.
func (s *Service) removeServingRevision(ctx context.Context, svc runstore.Service) {
	name := svc.LatestReadyRevision
	if name == "" {
		name = svc.LatestCreatedRevision
	}
	project, location, service, id, ok := ParseRevisionName(name)
	if !ok {
		return
	}
	_ = s.runtime.RemoveRevision(ctx, runstore.Revision{
		ProjectID: project,
		Location:  location,
		Service:   service,
		ID:        id,
	})
}

// DeleteService removes a service and its revisions and returns the delete
// operation. The operation's response/metadata is the removed service snapshot.
// etag is an optimistic-concurrency precondition (a mismatch is Aborted).
// validateOnly validates the request without removing the service.
func (s *Service) DeleteService(ctx context.Context, project, location, id string, validateOnly bool, etag string) (runstore.Operation, error) {
	existing, err := s.store.GetService(ctx, project, location, id)
	if err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	if err := checkEtag(etag, existing.Etag); err != nil {
		return runstore.Operation{}, err
	}
	deleted := existing
	if !validateOnly {
		if err := s.runtime.RemoveService(ctx, existing); err != nil {
			return runstore.Operation{}, err
		}
		deleted.DeleteTime = clock.Now()
		deleted.UpdateTime = deleted.DeleteTime
		if err := s.store.DeleteService(ctx, project, location, id); err != nil {
			return runstore.Operation{}, mapStoreErr(err)
		}
	}
	return s.recordOperation(ctx, project, location, "delete", deleted, validateOnly)
}

// GetRevision returns a revision by id.
func (s *Service) GetRevision(ctx context.Context, project, location, service, id string) (runstore.Revision, error) {
	if _, err := s.store.GetService(ctx, project, location, service); err != nil {
		return runstore.Revision{}, mapStoreErr(err)
	}
	rev, err := s.store.GetRevision(ctx, project, location, service, id)
	if err != nil {
		return runstore.Revision{}, mapStoreErr(err)
	}
	return rev, nil
}

// ListRevisions lists a service's revisions, sorted by id.
func (s *Service) ListRevisions(ctx context.Context, project, location, service string) ([]runstore.Revision, error) {
	if _, err := s.store.GetService(ctx, project, location, service); err != nil {
		return nil, mapStoreErr(err)
	}
	revs, err := s.store.ListRevisions(ctx, project, location, service)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return revs, nil
}

// DeleteRevision removes a retired revision and returns the delete operation
// (its response/metadata is the removed Revision). Cloud Run only permits
// deleting retired revisions — the revision currently serving the service (the
// latest ready/created one) is a FailedPrecondition. etag is an
// optimistic-concurrency precondition (a mismatch is Aborted). validateOnly
// validates the request without deleting or recording an operation.
func (s *Service) DeleteRevision(ctx context.Context, project, location, service, id string, validateOnly bool, etag string) (runstore.Operation, error) {
	svc, err := s.store.GetService(ctx, project, location, service)
	if err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	rev, err := s.store.GetRevision(ctx, project, location, service, id)
	if err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	if err := checkEtag(etag, rev.Etag); err != nil {
		return runstore.Operation{}, err
	}
	revName := RevisionName(project, location, service, id)
	if revName == svc.LatestReadyRevision || revName == svc.LatestCreatedRevision {
		return runstore.Operation{}, model.NewProviderError("FailedPrecondition",
			"only retired revisions can be deleted: "+revName, 400)
	}
	if !validateOnly {
		// A retired revision has no live runtime, so teardown is best-effort.
		_ = s.runtime.RemoveRevision(ctx, rev)
		if err := s.store.DeleteRevision(ctx, project, location, service, id); err != nil {
			return runstore.Operation{}, mapStoreErr(err)
		}
	}
	return s.recordRevisionOperation(ctx, project, location, svc.ID, rev, validateOnly)
}

// recordRevisionOperation builds (and, unless the request was validate-only,
// persists) the operation for a revision mutation. The response snapshot is the
// Revision, matching the proto's operation_info response_type.
func (s *Service) recordRevisionOperation(ctx context.Context, project, location, service string, rev runstore.Revision, validateOnly bool) (runstore.Operation, error) {
	now := clock.Now()
	if !validateOnly {
		rev.DeleteTime = now
	}
	op := runstore.Operation{
		ProjectID:  project,
		Location:   location,
		ID:         "operation-run-" + newUUID(),
		Verb:       "delete",
		Target:     RevisionName(project, location, service, rev.ID),
		CreateTime: now,
		Revision:   &rev,
	}
	// A validate-only operation is never persisted, so it must be terminal:
	// an in-flight op the client cannot poll would be a dead end.
	if validateOnly || !s.lro.Async() {
		op.Done = true
		op.EndTime = now
	}
	if validateOnly {
		return op, nil
	}
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
}

// --- IAM ---

// ServiceGetIamPolicy returns the service's IAM policy (404 when absent).
func (s *Service) ServiceGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	if err := s.requireService(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, project, servicePolicy, location+"/"+id), nil
}

// ServiceSetIamPolicy replaces the service's IAM policy with etag OCC.
func (s *Service) ServiceSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	if err := s.requireService(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, project, servicePolicy, location+"/"+id, body)
}

// ServiceTestIamPermissions reports the permissions the caller holds.
func (s *Service) ServiceTestIamPermissions(ctx context.Context, project, location, id string, perms []string) ([]string, error) {
	if err := s.requireService(ctx, project, location, id); err != nil {
		return nil, err
	}
	return policy.TestPermissions(perms), nil
}

func (s *Service) requireService(ctx context.Context, project, location, id string) error {
	if location == "" || id == "" {
		return invalidArgument("missing service name or location")
	}
	if _, err := s.store.GetService(ctx, project, location, id); err != nil {
		return mapStoreErr(err)
	}
	return nil
}

// --- operations ---

// GetOperation returns an operation by id, settled against the timing mode.
func (s *Service) GetOperation(ctx context.Context, project, location, id string) (runstore.Operation, error) {
	op, err := s.store.GetOperation(ctx, project, location, id)
	if err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	return s.settle(op), nil
}

// WaitOperation is the read side of the REST :wait custom method; identical to
// GetOperation.
func (s *Service) WaitOperation(ctx context.Context, project, location, id string) (runstore.Operation, error) {
	return s.GetOperation(ctx, project, location, id)
}

// ListOperations lists a location's operations, settled and sorted by id.
func (s *Service) ListOperations(ctx context.Context, project, location string) ([]runstore.Operation, error) {
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	for i := range ops {
		ops[i] = s.settle(ops[i])
	}
	return ops, nil
}

// DeleteOperation removes an operation (404 when unknown).
func (s *Service) DeleteOperation(ctx context.Context, project, location, id string) error {
	if err := s.store.DeleteOperation(ctx, project, location, id); err != nil {
		return mapStoreErr(err)
	}
	return nil
}

// CancelOperation validates that the operation exists. The emulator does not
// model cancellation, so a known operation is a no-op success.
func (s *Service) CancelOperation(ctx context.Context, project, location, id string) error {
	if _, err := s.store.GetOperation(ctx, project, location, id); err != nil {
		return mapStoreErr(err)
	}
	return nil
}

// --- runtime ---

// Invoke forwards a data-plane request through the runtime seam.
func (s *Service) Invoke(ctx context.Context, req InvocationRequest) (Invocation, error) {
	return s.runtime.Invoke(ctx, req)
}

// URLSuffix returns the configured synthesized-uri DNS suffix.
func (s *Service) URLSuffix() string { return s.host.Suffix }

// ServiceAuthority returns the invocation authority (host[:port]) of a service
// under this core's configuration. The runtime manager registers revisions
// under the same authority so Invoke can resolve a request Host to a revision.
func (s *Service) ServiceAuthority(project, location, id string) string {
	host := id + "-" + projectToken(project) + "." + strings.ToLower(location) + "." + s.host.Suffix
	if s.host.Port != "" {
		host += ":" + s.host.Port
	}
	return host
}

// uri synthesizes the service uri. Mock mode uses the real
// https://{id}-{token}.{location}.{suffix} form; k8s execution mode overrides
// the scheme/port via WithInvocationHost.
func (s *Service) uri(project, location, id string) string {
	return s.host.Scheme + "://" + s.ServiceAuthority(project, location, id)
}

// projectToken is the 12-hex-char SHA-256 prefix of the project, matching the
// real Cloud Run URL token shape.
func projectToken(project string) string {
	sum := sha256Sum(project)
	return hex.EncodeToString(sum)[:12]
}

// --- helpers ---

// recordOperation builds and persists the operation for a service mutation.
// A validateOnly request returns the operation unpersisted and emits no status
// event, so a dry run mutates nothing observable.
func (s *Service) recordOperation(ctx context.Context, project, location, verb string, svc runstore.Service, validateOnly bool) (runstore.Operation, error) {
	now := clock.Now()
	op := runstore.Operation{
		ProjectID:  project,
		Location:   location,
		ID:         "operation-run-" + newUUID(),
		Verb:       verb,
		Target:     ServiceName(project, location, svc.ID),
		CreateTime: now,
		Service:    &svc,
	}
	// A validate-only operation is never persisted, so it must be terminal:
	// an in-flight op the client cannot poll would be a dead end.
	if validateOnly || !s.lro.Async() {
		op.Done = true
		op.EndTime = now
	}
	if validateOnly {
		return op, nil
	}
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	s.emitStatus(verb, svc)
	return op, nil
}

// emitStatus publishes a console status event for a service mutation on the
// shared event bus. It is best-effort: a nil bus is a no-op, so the control
// plane never fails because of the UI stream.
func (s *Service) emitStatus(verb string, svc runstore.Service) {
	if s.eventBus == nil {
		return
	}
	state := "READY"
	if verb == "delete" {
		state = "DELETED"
	}
	s.eventBus.Publish(events.Event{
		Type: events.EventStatus,
		Payload: events.StatusEvent{
			Cloud:    model.CloudGCP,
			Keys:     []string{"gcp", "run"},
			Resource: "gcp-run-service",
			ID:       svc.ID,
			State:    state,
			Detail:   verb + " " + ServiceName(svc.ProjectID, svc.Location, svc.ID),
		},
	})
}

// settle derives the rendered state of a persisted operation from its stored
// done flag and the configured timing mode. An in-flight operation becomes done
// once the delay has elapsed, with a deterministic EndTime of createTime+delay.
func (s *Service) settle(op runstore.Operation) runstore.Operation {
	if op.Done || s.lro.Pending(op.CreateTime) {
		return op
	}
	op.Done = true
	op.EndTime = op.CreateTime.Add(s.lro.Delay)
	s.tracker.EmitOperation("run", op.ID, op.ID, op.Verb)
	return op
}

// outputOnlyKeys are Service fields the emulator derives; a caller-supplied
// value is dropped so read-back is stable.
var outputOnlyKeys = []string{
	"name", "uid", "generation", "observedGeneration", "createTime", "updateTime",
	"deleteTime", "etag", "uri", "urls", "conditions", "terminalCondition",
	"latestReadyRevision", "latestCreatedRevision", "reconciling", "trafficStatuses",
	"creator", "lastModifier", "expireTime", "satisfiesPzs",
}

// writableData returns the caller-supplied service JSON with output-only fields
// removed, so it can be echoed back and merged with derived fields on render.
func writableData(body map[string]any) map[string]any {
	out := cloneMap(body)
	for _, k := range outputOnlyKeys {
		delete(out, k)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// revisionData returns the revision body derived from a service request: the
// template object plus the service labels/annotations.
func revisionData(body map[string]any) map[string]any {
	out := map[string]any{}
	if t, ok := body["template"].(map[string]any); ok {
		out = cloneMap(t)
	}
	if v, ok := body["labels"]; ok {
		out["labels"] = cloneJSON(v)
	}
	if v, ok := body["annotations"]; ok {
		out["annotations"] = cloneJSON(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// applyUpdate merges body into data honoring a comma-separated updateMask. An
// empty mask replaces the writable fields; otherwise only the named top-level
// paths are copied.
func applyUpdate(data, body map[string]any, updateMask string) {
	mask := splitMask(updateMask)
	if len(mask) == 0 {
		for k, v := range writableData(body) {
			data[k] = v
		}
		return
	}
	writable := writableData(body)
	for _, path := range mask {
		top := path
		if i := strings.IndexByte(top, '.'); i >= 0 {
			top = top[:i]
		}
		if v, ok := writable[top]; ok {
			data[top] = v
		}
	}
}

// splitMask splits and normalizes a comma-separated field mask.
func splitMask(mask string) []string {
	if strings.TrimSpace(mask) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// templateMasked reports whether the update mask references the template field.
func templateMasked(mask string) bool {
	for _, p := range splitMask(mask) {
		if p == "template" || strings.HasPrefix(p, "template.") {
			return true
		}
	}
	return false
}

func bodyHasTemplate(body map[string]any) bool {
	_, ok := body["template"]
	return ok
}

// nextRevisionID returns the next revision id for a service, deriving the
// counter from the existing latest-created revision name ("{id}-00001").
func nextRevisionID(serviceID, current string) string {
	base := serviceID + "-"
	next := 1
	if i := strings.LastIndex(current, "/revisions/"); i >= 0 {
		if tail := current[i+len("/revisions/"):]; strings.HasPrefix(tail, base) {
			if n, err := strconv.Atoi(strings.TrimPrefix(tail, base)); err == nil {
				next = n + 1
			}
		}
	}
	return fmt.Sprintf("%s%05d", base, next)
}

// --- small helpers ---

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// validateTraffic checks every traffic target's tag against the rules real
// Cloud Run enforces, answering 400 INVALID_ARGUMENT for a violation. Probed
// against the live API: the tag is an RFC 1035 label (3–47 characters, lowercase
// letters/digits/hyphens, beginning with a letter and not ending with a hyphen),
// the tag and the service id together may not exceed 46 characters, and tags are
// unique within a service. An absent traffic list, a non-array value, a
// malformed entry or an empty tag are tolerated here — the tag is the only field
// this validates.
func validateTraffic(serviceID string, data map[string]any) error {
	targets, ok := data["traffic"].([]any)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	for _, t := range targets {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		tag, _ := m["tag"].(string)
		if tag == "" {
			continue
		}
		if n := len(tag); n < trafficTagMinLen || n > trafficTagMaxLen {
			return invalidArgument(fmt.Sprintf(
				"invalid traffic tag %q: length must be between %d and %d characters",
				tag, trafficTagMinLen, trafficTagMaxLen))
		}
		if !validTrafficTag.MatchString(tag) {
			return invalidArgument("invalid traffic tag: must conform to RFC 1035: " +
				"only lowercase, digits, and hyphens; must begin with letter, and cannot end with hyphen")
		}
		if serviceID != "" && len(tag)+len(serviceID) > trafficTagCombinedMax {
			return invalidArgument(fmt.Sprintf(
				"traffic tag %q and service name %q together are too long: combined length cannot exceed %d characters",
				tag, serviceID, trafficTagCombinedMax))
		}
		if seen[tag] {
			return invalidArgument("traffic targets may not contain a duplicate tag: " + tag)
		}
		seen[tag] = true
	}
	return nil
}

// abortedEtagMismatch is the etag-precondition failure, returned as HTTP 409
// with the ABORTED google.rpc status (matching the shared-IAM/Eventarc OCC
// contract).
func abortedEtagMismatch() error {
	return &model.ProviderError{
		Code:       "Aborted",
		Message:    "etag mismatch: optimistic concurrency control failed",
		HTTPStatus: 409,
		Status:     "ABORTED",
	}
}

// checkEtag enforces a request-supplied etag precondition against the stored
// value. An empty request etag is a no-op (the caller opted out of OCC).
func checkEtag(reqEtag, storedEtag string) error {
	if reqEtag != "" && reqEtag != storedEtag {
		return abortedEtagMismatch()
	}
	return nil
}

func mapStoreErr(err error) error {
	switch {
	case errors.Is(err, runstore.ErrNoSuchService):
		return model.NewProviderError("NotFound", "service not found", 404)
	case errors.Is(err, runstore.ErrNoSuchRevision):
		return model.NewProviderError("NotFound", "revision not found", 404)
	case errors.Is(err, runstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, runstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	default:
		return err
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func sha256Sum(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// cloneMap deep-copies a JSON object.
func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneJSON(v)
	}
	return out
}

// cloneJSON deep-copies a decoded JSON value (objects, arrays, scalars).
func cloneJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneMap(t)
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = cloneJSON(item)
		}
		return out
	default:
		return v
	}
}
