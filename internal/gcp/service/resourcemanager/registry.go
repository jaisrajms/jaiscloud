package resourcemanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// rtProject is the resource type for a project registered in the shared
// ResourceStore. The entry id and owning account are both the project id, so
// Get/List/Delete are naturally project-scoped and a cross-scope List
// (account="", region="") enumerates every created project.
const rtProject = "gcp_resourcemanager_project"

// rtOperation is the resource type for a persisted google.longrunning.Operation.
// The entry id is the bare operation id (without the "operations/" prefix); the
// account scope is the project the mutation targeted.
const rtOperation = "gcp_resourcemanager_operation"

// projectIDRe is the documented project-id grammar (Cloud Resource Manager v1/v3
// Project.project_id): 6 to 30 characters, lowercase ASCII letters, digits, or
// hyphens, must start with a letter, and must not end with a hyphen. Because the
// first character is a letter it also rejects an all-numeric id.
var projectIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)

// CreateProjectInput carries the caller-supplied fields of a project create.
// ProjectNumber and the lifecycle state/timestamps are derived by the core.
type CreateProjectInput struct {
	ProjectID   string
	DisplayName string
	Parent      string
	Labels      map[string]string
}

// CreateProject registers a new project and returns it with the completed (or,
// in async mode, in-flight) operation. The project id grammar is validated, a
// duplicate id (including a configured default/extra project) is AlreadyExists,
// and ProjectNumber stays the deterministic synthesized value — the emulator
// allocates no real project numbers.
func (s *Service) CreateProject(ctx context.Context, in CreateProjectInput) (Project, Operation, error) {
	id := strings.TrimSpace(in.ProjectID)
	if id == "" {
		return Project{}, Operation{}, invalidArgument("projectId is required")
	}
	if !projectIDRe.MatchString(id) {
		return Project{}, Operation{}, invalidArgument(fmt.Sprintf(
			"Invalid project ID %q. It must be 6 to 30 lowercase letters, digits, or hyphens, start with a letter, and not end with a hyphen.", id))
	}
	display := strings.TrimSpace(in.DisplayName)
	if display == "" {
		display = id
	} else if len(display) < 4 || len(display) > 30 {
		return Project{}, Operation{}, invalidArgument(fmt.Sprintf(
			"Invalid display name %q. It must be between 4 and 30 characters.", display))
	}
	// The configured default/extra projects already exist.
	if s.isKnown(id) {
		return Project{}, Operation{}, alreadyExists(id)
	}

	now := clock.Now().UTC()
	proj := Project{
		ProjectID:     id,
		ProjectNumber: resource.ProjectNumber(id),
		DisplayName:   display,
		State:         StateActive,
		Etag:          policy.Etag(ProjectName(id)),
		Parent:        in.Parent,
		CreateTime:    now,
		UpdateTime:    now,
		Labels:        in.Labels,
	}
	data, err := json.Marshal(proj)
	if err != nil {
		return Project{}, Operation{}, err
	}
	// The existence check and the write are one atomic cycle so concurrent
	// creates of the same id cannot both succeed.
	_, err = s.resources.UpsertAtomic(ctx, id, store.GlobalRegion, rtProject, id,
		func(_ store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			if exists {
				return store.ResourceEntry{}, alreadyExists(id)
			}
			return store.ResourceEntry{Type: rtProject, ID: id, Data: data}, nil
		})
	if err != nil {
		return Project{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, id, "create", proj)
	if err != nil {
		return Project{}, Operation{}, err
	}
	return proj, op, nil
}

// ListProjects returns a cursor page of projects: every created project unioned
// with the configured default + extra projects, deduplicated and sorted by id.
// DELETE_REQUESTED projects are omitted unless showDeleted is set (the v3
// ListProjects.ShowDeleted semantics); a persisted registry entry always wins
// over the synthesized ACTIVE shape for a configured project. filter is the v1
// projects.list expression (empty for the gRPC v3 surface, which has none); it
// is applied before pagination so the page cursor walks the filtered set.
func (s *Service) ListProjects(ctx context.Context, pageSize int, pageToken string, showDeleted bool, filter string) ([]Project, string, error) {
	pred, err := compileProjectFilter(filter)
	if err != nil {
		return nil, "", err
	}
	byID, err := s.collectProjects(ctx)
	if err != nil {
		return nil, "", err
	}
	out := make([]Project, 0, len(byID))
	for _, p := range byID {
		if !showDeleted && p.State == StateDeleteRequested {
			continue
		}
		if !pred.match(p) {
			continue
		}
		out = append(out, p)
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next, err := paging.Page(out, func(p Project) string { return p.ProjectID }, params)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// collectProjects builds the project set the read surfaces page over: every
// registry entry unioned with the configured default + extra projects
// (deduplicated by id, a persisted entry winning over the synthesized ACTIVE
// shape). ListProjects and SearchProjects share it.
func (s *Service) collectProjects(ctx context.Context) (map[string]Project, error) {
	entries, err := s.resources.List(ctx, "", "", rtProject, "")
	if err != nil {
		return nil, err
	}
	byID := make(map[string]Project, len(entries)+len(s.extraProjects)+1)
	for _, e := range entries {
		var p Project
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return nil, err
		}
		if p.ProjectID == "" {
			p.ProjectID = e.ID
		}
		byID[p.ProjectID] = p
	}
	for _, id := range s.knownProjects() {
		if _, ok := byID[id]; !ok {
			byID[id] = synthesizeProject(id)
		}
	}
	return byID, nil
}

// DeleteProject marks a project DELETE_REQUESTED (real GCP keeps it restorable
// for a 30-day window). Deleting an unknown id is NotFound. Deleting a project
// that is already DELETE_REQUESTED is idempotent, matching the v3 proto
// ("deleting a DELETE_REQUESTED project will not cause an error, but also won't
// do anything"). A configured project with no registry entry is materialized so
// the new state persists.
func (s *Service) DeleteProject(ctx context.Context, project string) (Project, Operation, error) {
	if project == "" {
		return Project{}, Operation{}, invalidArgument("project is required")
	}
	p, err := s.transitionProject(ctx, project, func(cur Project, exists bool) (Project, error) {
		if !exists {
			if !s.isKnown(project) {
				return Project{}, notFoundProject(project)
			}
			cur = synthesizeProject(project)
			cur.CreateTime = clock.Now().UTC()
		}
		if cur.State == StateDeleteRequested {
			// Idempotent: a second delete succeeds and changes nothing.
			return cur, nil
		}
		now := clock.Now().UTC()
		cur.State = StateDeleteRequested
		cur.DeleteTime = now
		cur.UpdateTime = now
		return cur, nil
	})
	if err != nil {
		return Project{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, project, "delete", p)
	if err != nil {
		return Project{}, Operation{}, err
	}
	return p, op, nil
}

// UndeleteProject restores a DELETE_REQUESTED project to ACTIVE. An unknown id
// is NotFound and a project that is not marked for deletion is
// FailedPrecondition, matching real GCP.
func (s *Service) UndeleteProject(ctx context.Context, project string) (Project, Operation, error) {
	if project == "" {
		return Project{}, Operation{}, invalidArgument("project is required")
	}
	p, err := s.transitionProject(ctx, project, func(cur Project, exists bool) (Project, error) {
		if !exists {
			if s.isKnown(project) {
				return Project{}, failedPrecondition(fmt.Sprintf("Project %s is not marked for deletion.", project))
			}
			return Project{}, notFoundProject(project)
		}
		if cur.State != StateDeleteRequested {
			return Project{}, failedPrecondition(fmt.Sprintf("Project %s is not marked for deletion.", project))
		}
		cur.State = StateActive
		cur.DeleteTime = time.Time{}
		cur.UpdateTime = clock.Now().UTC()
		return cur, nil
	})
	if err != nil {
		return Project{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, project, "undelete", p)
	if err != nil {
		return Project{}, Operation{}, err
	}
	return p, op, nil
}

// registryProject loads a created (or delete-marked) project. ok is false when
// there is no registry entry; a non-NotFound storage error is propagated.
func (s *Service) registryProject(ctx context.Context, project string) (Project, bool, error) {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtProject, project)
	if errors.Is(err, store.ErrNotFound) {
		return Project{}, false, nil
	}
	if err != nil {
		return Project{}, false, err
	}
	var p Project
	if err := json.Unmarshal(e.Data, &p); err != nil {
		return Project{}, false, err
	}
	if p.ProjectID == "" {
		p.ProjectID = e.ID
	}
	return p, true, nil
}

// transitionProject atomically applies a state transition to a project's
// registry entry. The callback receives the current project (zero, exists=false
// when absent) and returns the new value, or an error to abort without writing.
// It returns the persisted project.
func (s *Service) transitionProject(ctx context.Context, project string, apply func(cur Project, exists bool) (Project, error)) (Project, error) {
	var out Project
	_, err := s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, rtProject, project,
		func(cur store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			var p Project
			if exists {
				if err := json.Unmarshal(cur.Data, &p); err != nil {
					return store.ResourceEntry{}, err
				}
			}
			next, err := apply(p, exists)
			if err != nil {
				return store.ResourceEntry{}, err
			}
			data, err := json.Marshal(next)
			if err != nil {
				return store.ResourceEntry{}, err
			}
			out = next
			return store.ResourceEntry{Type: rtProject, ID: project, Data: data}, nil
		})
	if err != nil {
		return Project{}, err
	}
	return out, nil
}

// isKnown reports whether id is one of the configured projects that always
// exist, independent of any registry entry.
func (s *Service) isKnown(id string) bool {
	for _, known := range s.knownProjects() {
		if id == known {
			return true
		}
	}
	return false
}

// knownProjects returns the configured default + extra project ids, default
// first, skipping an empty default.
func (s *Service) knownProjects() []string {
	out := make([]string, 0, len(s.extraProjects)+1)
	if s.defaultProject != "" {
		out = append(out, s.defaultProject)
	}
	out = append(out, s.extraProjects...)
	return out
}

// Operation is a persisted google.longrunning.Operation for a project mutation.
// In the default synchronous mode it is done=true with EndTime set; in async
// mode it is stored done=false and settles lazily on read.
type Operation struct {
	// Name is the operation resource name: operations/{id}.
	Name string
	// Verb is the mutation that produced the operation: "create", "delete", or
	// "undelete".
	Verb string
	// Project is the project state resulting from the mutation.
	Project Project
	// Done reports whether the operation has completed.
	Done bool
	// CreateTime is when the operation was created.
	CreateTime time.Time
	// EndTime is when the operation completed (zero while in flight).
	EndTime time.Time
}

// OperationName is the resource name of a project long-running operation
// (operations/{id}).
func OperationName(id string) string {
	return resource.ResourceID("")("resourcemanager-operation", id)
}

// newOperation builds and persists the operation for a mutation. The default
// synchronous mode stores it done=true inline; async mode stores it done=false
// and settles it lazily on read.
func (s *Service) newOperation(ctx context.Context, project, verb string, p Project) (Operation, error) {
	now := clock.Now().UTC()
	id := randomHex(12)
	op := Operation{Name: OperationName(id), Verb: verb, Project: p, CreateTime: now}
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

// persistOperation records a mutation operation so a poll can read it back after
// the mutation returns (and across a restart under --dsn).
func (s *Service) persistOperation(ctx context.Context, project, id string, op Operation) error {
	data, err := json.Marshal(operationRecord{
		Verb:       op.Verb,
		Project:    op.Project,
		Done:       op.Done,
		CreateTime: op.CreateTime,
		EndTime:    op.EndTime,
	})
	if err != nil {
		return err
	}
	return s.resources.Upsert(ctx, project, store.GlobalRegion,
		store.ResourceEntry{Type: rtOperation, ID: id, Data: data})
}

// GetOperation returns a persisted operation by its full name (operations/{id}),
// settling it against the configured timing mode. An unknown id is NotFound.
func (s *Service) GetOperation(ctx context.Context, project, name string) (Operation, error) {
	id, err := operationIDFromName(name)
	if err != nil {
		return Operation{}, err
	}
	if project != "" {
		if e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtOperation, id); err == nil {
			return s.operationFromEntry(e)
		} else if !errors.Is(err, store.ErrNotFound) {
			return Operation{}, err
		}
	}
	// A top-level operations/{id} name is polled without a project segment, so
	// scan across scopes after the project-scoped lookup misses.
	entries, err := s.resources.List(ctx, "", "", rtOperation, id)
	if err != nil {
		return Operation{}, err
	}
	for _, e := range entries {
		if e.ID == id {
			return s.operationFromEntry(e)
		}
	}
	return Operation{}, notFound(fmt.Sprintf("Operation %s not found.", OperationName(id)))
}

// operationFromEntry decodes a persisted operation and settles it.
func (s *Service) operationFromEntry(e store.ResourceEntry) (Operation, error) {
	var rec operationRecord
	if err := json.Unmarshal(e.Data, &rec); err != nil {
		return Operation{}, err
	}
	op := Operation{
		Name:       OperationName(e.ID),
		Verb:       rec.Verb,
		Project:    rec.Project,
		Done:       rec.Done,
		CreateTime: rec.CreateTime,
		EndTime:    rec.EndTime,
	}
	return s.settle(op), nil
}

// settle derives the rendered state of a persisted operation from its stored
// done flag and the configured timing mode. An in-flight operation becomes done
// once the delay has elapsed, with a deterministic EndTime of
// createTime+delay; the flip is derived on read, never written back.
func (s *Service) settle(op Operation) Operation {
	if op.Done || s.lroMode.Pending(op.CreateTime) {
		return op
	}
	op.Done = true
	op.EndTime = op.CreateTime.Add(s.lroMode.Delay)
	s.tracker.EmitOperation("resourcemanager", op.Name, op.Name, op.Verb)
	return op
}

// operationRecord is the persisted per-operation state.
type operationRecord struct {
	Verb       string    `json:"verb"`
	Project    Project   `json:"project"`
	Done       bool      `json:"done"`
	CreateTime time.Time `json:"createTime"`
	EndTime    time.Time `json:"endTime,omitempty"`
}

// operationIDFromName validates an operation resource name (operations/{id}) and
// returns the bare id.
func operationIDFromName(name string) (string, error) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) == 2 && parts[0] == "operations" && parts[1] != "" {
		return parts[1], nil
	}
	return "", invalidArgument("invalid operation name")
}

// alreadyExists builds the canonical AlreadyExists provider error.
func alreadyExists(project string) error {
	return model.NewProviderError("AlreadyExists",
		fmt.Sprintf("Project %s already exists.", project), 409)
}

// notFoundProject builds the canonical NotFound provider error for a project.
func notFoundProject(project string) error {
	return notFound(fmt.Sprintf("Project %s not found.", project))
}

// notFound builds the canonical NotFound provider error.
func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

// failedPrecondition builds the canonical FailedPrecondition provider error.
func failedPrecondition(msg string) error {
	return model.NewProviderError("FailedPrecondition", msg, 400)
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
