package resourcemanager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newRegistryService returns a core with the configured default + extra
// projects set, so list/create know the seeded ids.
func newRegistryService(opts ...Option) (*Service, store.ResourceStore) {
	res := store.NewMemoryResourceStore()
	base := []Option{WithKnownProjects("default-proj", []string{"extra-proj1", "extra-proj2"})}
	return NewService(res, append(base, opts...)...), res
}

func codeOf(t *testing.T, err error) string {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %T (%v), want *model.ProviderError", err, err)
	}
	return pe.Code
}

func TestCreateProjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()

	p, op, err := s.CreateProject(ctx, CreateProjectInput{
		ProjectID:   "my-project-123",
		DisplayName: "My Project",
		Labels:      map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if p.State != StateActive {
		t.Errorf("state = %q, want ACTIVE", p.State)
	}
	if p.DisplayName != "My Project" {
		t.Errorf("displayName = %q, want My Project", p.DisplayName)
	}
	if p.ProjectNumber == "" {
		t.Error("projectNumber must be set")
	}
	if p.CreateTime.IsZero() {
		t.Error("createTime must be set")
	}
	if p.Labels["env"] != "test" {
		t.Errorf("labels = %v, want env=test", p.Labels)
	}
	if !op.Done || op.Name == "" {
		t.Errorf("sync operation = %+v, want done with a name", op)
	}

	// Get reads back the persisted shape, not the synthesized placeholder.
	got, err := s.GetProject(ctx, "my-project-123")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.DisplayName != "My Project" || !got.CreateTime.Equal(p.CreateTime) {
		t.Errorf("get = %+v, want the created project", got)
	}
}

func TestCreateProjectDuplicate(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "my-project-123"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "my-project-123"})
	if got := codeOf(t, err); got != "AlreadyExists" {
		t.Errorf("duplicate code = %q, want AlreadyExists", got)
	}
	// The configured default/extra projects already exist too.
	for _, id := range []string{"default-proj", "extra-proj1"} {
		_, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: id})
		if got := codeOf(t, err); got != "AlreadyExists" {
			t.Errorf("create(%s) code = %q, want AlreadyExists", id, got)
		}
	}
}

func TestCreateProjectGrammar(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	// Valid grammar.
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "abcdef"}); err != nil {
		t.Errorf("create(abcdef) = %v, want success", err)
	}
	for _, id := range []string{
		"",                      // empty
		"abc",                   // too short
		"1abcde",                // must start with a letter
		"abcde-",                // trailing hyphen
		"ABCdef",                // uppercase
		"abc_def",               // underscore
		strings.Repeat("a", 31), // too long
	} {
		_, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: id})
		if got := codeOf(t, err); got != "InvalidArgument" {
			t.Errorf("create(%q) code = %q, want InvalidArgument", id, got)
		}
	}
}

func TestCreateProjectDisplayName(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "short-name", DisplayName: "ab"}); err == nil {
		t.Error("short displayName must be InvalidArgument")
	}
	// An omitted display name defaults to the project id.
	p, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "short-name"})
	if err != nil {
		t.Fatalf("create with default displayName: %v", err)
	}
	if p.DisplayName != "short-name" {
		t.Errorf("displayName = %q, want short-name", p.DisplayName)
	}
}

func TestDeleteAndUndelete(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "delete-me-123"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	p, op, err := s.DeleteProject(ctx, "delete-me-123")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if p.State != StateDeleteRequested || p.DeleteTime.IsZero() {
		t.Errorf("deleted project = %+v, want DELETE_REQUESTED with deleteTime", p)
	}
	if !op.Done {
		t.Errorf("delete operation = %+v, want done inline", op)
	}

	// A second delete is idempotent (v3): it succeeds and changes nothing.
	second, secondOp, err := s.DeleteProject(ctx, "delete-me-123")
	if err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if second.State != StateDeleteRequested || !second.DeleteTime.Equal(p.DeleteTime) {
		t.Errorf("second delete = %+v, want unchanged DELETE_REQUESTED", second)
	}
	if !secondOp.Done {
		t.Errorf("second delete operation = %+v, want done inline", secondOp)
	}

	// A project that is not marked for deletion cannot be undeleted.
	if _, _, err := s.UndeleteProject(ctx, "extra-proj1"); codeOf(t, err) != "FailedPrecondition" {
		t.Errorf("undelete(active) code = %q, want FailedPrecondition", codeOf(t, err))
	}

	// Undelete restores ACTIVE and clears deleteTime.
	p, _, err = s.UndeleteProject(ctx, "delete-me-123")
	if err != nil {
		t.Fatalf("undelete: %v", err)
	}
	if p.State != StateActive || !p.DeleteTime.IsZero() {
		t.Errorf("undeleted project = %+v, want ACTIVE with zero deleteTime", p)
	}
}

func TestDeleteUnknownNotFound(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	if _, _, err := s.DeleteProject(ctx, "never-created"); codeOf(t, err) != "NotFound" {
		t.Errorf("delete unknown code = %q, want NotFound", codeOf(t, err))
	}
	if _, _, err := s.UndeleteProject(ctx, "never-created"); codeOf(t, err) != "NotFound" {
		t.Errorf("undelete unknown code = %q, want NotFound", codeOf(t, err))
	}
}

func TestListProjectsUnionAndDeleted(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	for _, id := range []string{"created-one", "created-two"} {
		if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: id}); err != nil {
			t.Fatalf("create(%s): %v", id, err)
		}
	}
	if _, _, err := s.DeleteProject(ctx, "created-two"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// showDeleted=false: created ACTIVE + configured projects, no deleted one.
	page, next, err := s.ListProjects(ctx, 0, "", false, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	ids := projectIDs(page)
	want := []string{"created-one", "default-proj", "extra-proj1", "extra-proj2"}
	if !equalStrings(ids, want) {
		t.Errorf("list (default) = %v, want %v", ids, want)
	}
	if next != "" {
		t.Errorf("nextPageToken = %q, want empty", next)
	}

	// showDeleted=true includes the marked project.
	page, _, err = s.ListProjects(ctx, 0, "", true, "")
	if err != nil {
		t.Fatalf("list deleted: %v", err)
	}
	wantAll := []string{"created-one", "created-two", "default-proj", "extra-proj1", "extra-proj2"}
	if ids := projectIDs(page); !equalStrings(ids, wantAll) {
		t.Errorf("list (showDeleted) = %v, want %v", ids, wantAll)
	}
}

func TestListProjectsPagination(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService(WithKnownProjects("", nil))
	for _, id := range []string{"page-one", "page-two", "page-three"} {
		if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: id}); err != nil {
			t.Fatalf("create(%s): %v", id, err)
		}
	}
	first, next, err := s.ListProjects(ctx, 2, "", false, "")
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if len(first) != 2 || next == "" {
		t.Fatalf("page 1 = %v next=%q, want 2 items and a token", projectIDs(first), next)
	}
	second, next2, err := s.ListProjects(ctx, 2, next, false, "")
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(second) != 1 || next2 != "" {
		t.Fatalf("page 2 = %v next=%q, want 1 item and no token", projectIDs(second), next2)
	}
	if first[0].ProjectID != "page-one" || second[0].ProjectID != "page-two" {
		t.Errorf("pagination order wrong: %v / %v", projectIDs(first), projectIDs(second))
	}
}

func TestResetClearsRegistry(t *testing.T) {
	ctx := context.Background()
	s, res := newRegistryService(WithKnownProjects("", nil))
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "reset-me-123"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	res.Reset(ctx)
	// An id that was created now falls back to the synthesized ACTIVE shape
	// (unknown ids always resolve), so assert via the store directly.
	if _, err := res.Get(ctx, "reset-me-123", store.GlobalRegion, rtProject, "reset-me-123"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("registry entry survived reset: %v", err)
	}
}

func TestOperationAsyncSettle(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	s, _ := newRegistryService(WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))
	_, op, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "async-project"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if op.Done {
		t.Error("async operation must start done=false")
	}
	// Still pending before the delay.
	got, err := s.GetOperation(ctx, "async-project", op.Name)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if got.Done {
		t.Error("operation must still be pending at create time")
	}
	// Once the delay elapses it settles on read.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err = s.GetOperation(ctx, "async-project", op.Name)
	if err != nil {
		t.Fatalf("get settled operation: %v", err)
	}
	if !got.Done || !got.EndTime.Equal(t0.Add(30*time.Second)) {
		t.Errorf("settled operation = %+v, want done at create+delay", got)
	}
}

// TestOperationAsyncSettleEmitsStatusEvent verifies the settle publishes exactly
// one cloud-neutral status event (keyed for the Resource Manager pages) and that
// a repeated poll does not re-emit.
func TestOperationAsyncSettleEmitsStatusEvent(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	bus := events.NewEventBus()
	var mu sync.Mutex
	var published []events.StatusEvent
	bus.Subscribe(events.EventStatus, func(e events.Event) {
		if p, ok := e.Payload.(events.StatusEvent); ok {
			mu.Lock()
			published = append(published, p)
			mu.Unlock()
		}
	})

	s, _ := newRegistryService(
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}),
		WithEventBus(bus))
	_, op, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "async-project"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Still pending: a read does not settle, so nothing is published.
	if _, err := s.GetOperation(ctx, "async-project", op.Name); err != nil {
		t.Fatalf("get pending: %v", err)
	}
	if n := len(published); n != 0 {
		t.Fatalf("published %d events before settle, want 0", n)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	if _, err := s.GetOperation(ctx, "async-project", op.Name); err != nil {
		t.Fatalf("get settled: %v", err)
	}
	mu.Lock()
	if len(published) != 1 {
		mu.Unlock()
		t.Fatalf("published %d events after settle, want 1", len(published))
	}
	ev := published[0]
	mu.Unlock()
	want := events.StatusEvent{
		Cloud:    model.CloudGCP,
		Keys:     []string{"gcp", "resourcemanager"},
		Resource: "gcp-resourcemanager-operation",
		ID:       op.Name,
		State:    "DONE",
		Detail:   "create",
	}
	if !reflect.DeepEqual(ev, want) {
		t.Fatalf("settle event = %+v, want %+v", ev, want)
	}

	// A repeated poll must not re-emit.
	if _, err := s.GetOperation(ctx, "async-project", op.Name); err != nil {
		t.Fatalf("second get: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(published) != 1 {
		t.Fatalf("published %d events after repeat poll, want 1", len(published))
	}
}

func TestGetOperationUnknown(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	_, err := s.GetOperation(ctx, "proj", "operations/nope")
	if codeOf(t, err) != "NotFound" {
		t.Errorf("unknown operation code = %q, want NotFound", codeOf(t, err))
	}
	if _, err := s.GetOperation(ctx, "proj", "bogus"); codeOf(t, err) != "InvalidArgument" {
		t.Errorf("malformed operation name code = %q, want InvalidArgument", codeOf(t, err))
	}
}

func projectIDs(ps []Project) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.ProjectID
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
