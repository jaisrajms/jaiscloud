// Package run is the transport-neutral core for Cloud Run Admin v2
// (run.googleapis.com). It implements the behavioural control plane the emulator
// exposes — services CRUD, revisions, service IAM, and google.longrunning
// operations — over a Store. Both the REST adapter and any future transport
// share one instance, so they cannot drift.
//
// The runtime is behind a RuntimeManager seam whose W1.1 implementation is the
// MockRuntime: a service is a stored record, not a running container. W1.2 adds
// a k8s runtime that actually launches the template image. REST-first (gRPC is
// deferred; see the wave plan CR4).
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
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/policy"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// DefaultURLSuffix is the DNS suffix of a synthesized service uri when
// JAISCLOUD_CLOUDRUN_URL_SUFFIX is unset. It matches the real *.run.app host.
const DefaultURLSuffix = "run.app"

// servicePolicy is the policy resource type for a Cloud Run service IAM policy.
const servicePolicy = "run-service-policy"

// validServiceID is Cloud Run's service-id grammar (RFC 1035 label).
var validServiceID = regexp.MustCompile(`^[a-z](?:[a-z0-9-]{0,47}[a-z0-9])?$`)

// Service is the transport-neutral Cloud Run core.
type Service struct {
	store     runstore.Store
	resources store.ResourceStore
	lro       lro.Mode
	runtime   RuntimeManager
	urlSuffix string
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

// WithURLSuffix sets the DNS suffix used to synthesize a service uri.
func WithURLSuffix(suffix string) Option {
	return func(s *Service) {
		if suffix != "" {
			s.urlSuffix = strings.TrimPrefix(suffix, ".")
		}
	}
}

// NewService returns a Cloud Run core over the store using the mock runtime.
func NewService(s runstore.Store, resources store.ResourceStore, opts ...Option) *Service {
	svc := &Service{
		store:     s,
		resources: resources,
		runtime:   MockRuntime{},
		urlSuffix: DefaultURLSuffix,
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
func (s *Service) CreateService(ctx context.Context, project, location, serviceID string, body map[string]any) (runstore.Operation, error) {
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

	if err := s.runtime.EnsureRevision(ctx, svc, rev); err != nil {
		return runstore.Operation{}, err
	}
	if err := s.store.CreateService(ctx, project, location, svc); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	if err := s.store.CreateRevision(ctx, project, location, serviceID, rev); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	return s.recordOperation(ctx, project, location, "create", svc)
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

// UpdateService applies an update mask and returns the update operation. A
// template change mints the next revision.
func (s *Service) UpdateService(ctx context.Context, project, location, id string, body map[string]any, updateMask string) (runstore.Operation, error) {
	existing, err := s.store.GetService(ctx, project, location, id)
	if err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	now := clock.Now()
	updated := existing
	if updated.Data == nil {
		updated.Data = map[string]any{}
	}
	applyUpdate(updated.Data, body, updateMask)

	templateChanged := templateMasked(updateMask) || (strings.TrimSpace(updateMask) == "" && bodyHasTemplate(body))
	updated.Generation++
	updated.Etag = newUUID()
	updated.UpdateTime = now

	var rev runstore.Revision
	if templateChanged {
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
		if err := s.runtime.EnsureRevision(ctx, updated, rev); err != nil {
			return runstore.Operation{}, err
		}
	}
	if err := s.store.UpdateService(ctx, project, location, updated); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	if templateChanged {
		if err := s.store.CreateRevision(ctx, project, location, id, rev); err != nil {
			return runstore.Operation{}, mapStoreErr(err)
		}
	}
	return s.recordOperation(ctx, project, location, "update", updated)
}

// DeleteService removes a service and its revisions and returns the delete
// operation. The operation's response/metadata is the removed service snapshot.
func (s *Service) DeleteService(ctx context.Context, project, location, id string) (runstore.Operation, error) {
	existing, err := s.store.GetService(ctx, project, location, id)
	if err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	if err := s.runtime.RemoveService(ctx, existing); err != nil {
		return runstore.Operation{}, err
	}
	deleted := existing
	deleted.DeleteTime = clock.Now()
	deleted.UpdateTime = deleted.DeleteTime
	if err := s.store.DeleteService(ctx, project, location, id); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	return s.recordOperation(ctx, project, location, "delete", deleted)
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
func (s *Service) URLSuffix() string { return s.urlSuffix }

// uri synthesizes the service uri. Mock mode uses the real
// https://{id}-{token}.{location}.{suffix} form; W1.2 rewrites it for k8s mode.
func (s *Service) uri(project, location, id string) string {
	return "https://" + id + "-" + projectToken(project) + "." + strings.ToLower(location) + "." + s.urlSuffix
}

// projectToken is the 12-hex-char SHA-256 prefix of the project, matching the
// real Cloud Run URL token shape.
func projectToken(project string) string {
	sum := sha256Sum(project)
	return hex.EncodeToString(sum)[:12]
}

// --- helpers ---

// recordOperation builds and persists the operation for a service mutation.
func (s *Service) recordOperation(ctx context.Context, project, location, verb string, svc runstore.Service) (runstore.Operation, error) {
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
	if !s.lro.Async() {
		op.Done = true
		op.EndTime = now
	}
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return runstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
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
