// Package resourcemanager is the transport-neutral core of Cloud Resource
// Manager (cloudresourcemanager.googleapis.com).
//
// The emulator's REST surface is the legacy v1 project API
// (cloudresourcemanager.googleapis.com/v1), while the proto-defined gRPC
// surface is v3 (google.cloud.resourcemanager.v3.Projects). This core owns the
// canonical v3 project semantics and both transports map onto it deliberately:
//
//   - v3 gRPC (internal/gcp/transport/grpc/resourcemanager) maps Project to
//     resourcemanagerpb.Project and addresses it by "projects/{id}".
//   - v1 REST (internal/gcp/transport/rest/resourcemanager) maps Project to the
//     v1 Project schema: projectId, projectNumber, name = DisplayName, and
//     lifecycleState = State. The v1 API has no separate resource name or etag
//     field, so the REST adapter emits only the v1 fields.
//
// Multi-tenancy is keyed by project id. The emulator synthesizes an ACTIVE
// project with a stable synthetic projectNumber and a stable etag for any id
// that has not been explicitly created, so existing clients and tests that
// address arbitrary projects keep working. On top of that fallback a lightweight
// project registry (registry.go) persists created ids — displayName, labels,
// lifecycle state, and create/delete times — in the shared ResourceStore, so
// create/get/list/delete/undelete round-trip and are enumerable. Project
// administration is modelled on top of that registry (projectadmin.go): the
// parent reference (organizations/{id} or folders/{id}) is stored and mutable
// via MoveProject, mutable metadata (display name, labels) via UpdateProject,
// and SearchProjects evaluates the v3 query grammar over the same set. A
// project's etag rotates on every successful update/move and a supplied stale
// etag is rejected with ABORTED. Billing, quota, a real org/folder hierarchy
// (only the parent reference is kept), IAM enforcement, and real project-number
// allocation are still not modelled. IAM policies are stored in the shared ResourceStore
// through internal/gcp/policy (etag optimistic concurrency control, fresh etag
// per set), so memory and PostgreSQL backends behave identically and no
// provider-level Reset/Snapshotter is needed — the registry shares the same
// store, so reset and snapshot cover it too. Bindings are not enforced — they
// never restrict access to emulated resources. As with the other GCP IAM
// surfaces in this emulator, only role+members bindings are modelled: binding
// conditions are not preserved and the reported policy version stays at the
// default.
package resourcemanager

import (
	"context"
	"strings"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// rtProjectPolicy is the resource type for a project's IAM policy in the shared
// ResourceStore. The entry id and owning account are both the project id.
const rtProjectPolicy = "gcp_resourcemanager_project_iam"

// StateActive is the v3 Project state the emulator reports for a live project.
const StateActive = "ACTIVE"

// StateDeleteRequested is the v3 Project state for a project marked for
// deletion (real GCP keeps it restorable for a 30-day window).
const StateDeleteRequested = "DELETE_REQUESTED"

// Service is the transport-neutral Cloud Resource Manager core.
type Service struct {
	resources store.ResourceStore
	// defaultProject and extraProjects are the configured project ids that
	// always exist (cfg.ProjectID and JAISCLOUD_EXTRA_ACCOUNTS). They are listed
	// alongside created projects and cannot be created again.
	defaultProject string
	extraProjects  []string
	// lroMode controls project mutation operation timing. The zero value is
	// synchronous: every operation is returned done=true inline. An enabled mode
	// stores operations done=false and settles them lazily on read.
	lroMode lro.Mode
	// tracker publishes the one-time "operation settled" event when a lazily
	// settled operation completes. Nil (no bus) makes it inert.
	tracker *lro.Tracker
}

// Option configures Service.
type Option func(*Service)

// WithKnownProjects seeds the project ids that always exist: the configured
// default project (cfg.ProjectID) and the additional accounts
// (JAISCLOUD_EXTRA_ACCOUNTS). They appear in ListProjects and a CreateProject
// for one is AlreadyExists. An empty defaultProject is ignored.
func WithKnownProjects(defaultProject string, extra []string) Option {
	return func(s *Service) {
		s.defaultProject = defaultProject
		s.extraProjects = append([]string(nil), extra...)
	}
}

// WithLROMode sets the long-running-operation timing mode. The zero value is
// synchronous; Mode{Enabled: true, Delay: d} stores project mutation operations
// done=false and settles them on read once d has elapsed.
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

// NewService returns a Service backed by the shared ResourceStore.
func NewService(resources store.ResourceStore, opts ...Option) *Service {
	s := &Service{resources: resources}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Project is the canonical v3 project resource. A synthesized project (an id
// that was never explicitly created) leaves the lifecycle timestamps and Labels
// zero and Parent empty; a registry-backed project carries the values supplied
// at creation.
type Project struct {
	ProjectID     string
	ProjectNumber string
	DisplayName   string
	State         string
	Etag          string
	Parent        string
	CreateTime    time.Time
	UpdateTime    time.Time
	DeleteTime    time.Time
	Labels        map[string]string
}

// ProjectName formats a project's canonical v3 resource name
// ("projects/{id}") through the shared resource formatter.
func ProjectName(project string) string {
	return resource.ResourceID(project)("project", "")
}

// ParseProjectName returns the project id from a "projects/{id}" resource name.
// A bare id (no prefix) is accepted so callers holding only an id work too; any
// other shape (empty, extra segments) reports ok=false.
func ParseProjectName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	rest := name
	if strings.HasPrefix(name, "projects/") {
		rest = name[len("projects/"):]
	}
	if rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	return rest, true
}

// GetProject returns the registry-backed project for a created id, or a
// synthesized ACTIVE project for an id that was never explicitly created
// (backward compatibility: arbitrary ids keep resolving).
func (s *Service) GetProject(ctx context.Context, project string) (Project, error) {
	if project == "" {
		return Project{}, invalidArgument("project is required")
	}
	p, ok, err := s.registryProject(ctx, project)
	if err != nil {
		return Project{}, err
	}
	if ok {
		return p, nil
	}
	return synthesizeProject(project), nil
}

// synthesizeProject builds the ACTIVE placeholder the emulator returns for any
// id it has no registry entry for.
func synthesizeProject(project string) Project {
	return Project{
		ProjectID:     project,
		ProjectNumber: resource.ProjectNumber(project),
		DisplayName:   project,
		State:         StateActive,
		Etag:          policy.Etag(ProjectName(project)),
	}
}

// GetIamPolicy returns the stored project policy (or an empty default policy).
func (s *Service) GetIamPolicy(ctx context.Context, project string) (policy.Policy, error) {
	if project == "" {
		return policy.Policy{}, invalidArgument("project is required")
	}
	return policy.Load(ctx, s.resources, project, rtProjectPolicy, project), nil
}

// PolicyInput carries the caller-supplied IAM policy fields of a setIamPolicy.
type PolicyInput struct {
	Bindings []any
	Etag     string
	Version  int
}

// SetIamPolicy stores the project policy, enforcing etag OCC (a stale etag is
// rejected with ABORTED/409). A fresh etag is derived from the new bindings.
func (s *Service) SetIamPolicy(ctx context.Context, project string, in PolicyInput) (policy.Policy, error) {
	if project == "" {
		return policy.Policy{}, invalidArgument("project is required")
	}
	body := map[string]any{"bindings": in.Bindings}
	if in.Etag != "" {
		body["etag"] = in.Etag
	}
	if in.Version != 0 {
		body["version"] = in.Version
	}
	return policy.Set(ctx, s.resources, project, rtProjectPolicy, project, body)
}

// TestIamPermissions echoes the requested permissions (the emulator treats the
// caller as owner — no authz enforcement).
func (s *Service) TestIamPermissions(_ context.Context, project string, permissions []string) ([]string, error) {
	if project == "" {
		return nil, invalidArgument("project is required")
	}
	return policy.TestPermissions(permissions), nil
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}
