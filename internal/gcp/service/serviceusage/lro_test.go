package serviceusage

import (
	"context"
	"regexp"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/store"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// TestAsyncLROLifecycle verifies the opt-in async timing: a mutation persists an
// in-flight operation, a poll before the delay keeps it in flight, and a poll
// after the delay settles it done with the affected service snapshot.
func TestAsyncLROLifecycle(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	api, op, err := s.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	if op.Done {
		t.Fatalf("async enable must be in flight: %+v", op)
	}
	if !regexp.MustCompile(`^operations/[^/]+$`).MatchString(op.Name) {
		t.Fatalf("operation name = %q, want operations/{id}", op.Name)
	}
	if op.Verb != "enable" || len(op.ResourceNames) != 1 || op.ResourceNames[0] != api.Name {
		t.Fatalf("operation = %+v", op)
	}

	got, err := s.GetOperation(ctx, "proj", op.Name)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.Done {
		t.Fatalf("operation before delay must be in flight: %+v", got)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err = s.GetOperation(ctx, "proj", op.Name)
	if err != nil {
		t.Fatalf("GetOperation after delay: %v", err)
	}
	if !got.Done {
		t.Fatalf("operation after delay must be done: %+v", got)
	}
	if got.EndTime.IsZero() || len(got.Services) != 1 || got.Services[0].Name != api.Name {
		t.Fatalf("settled operation = %+v", got)
	}
	if got.Services[0].State != StateEnabled {
		t.Fatalf("settled service state = %q, want ENABLED", got.Services[0].State)
	}
}

// TestAsyncDisableOperation verifies disable follows the same async lifecycle
// and settles with the DISABLED service snapshot.
func TestAsyncDisableOperation(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	if _, _, err := s.EnableAPI(ctx, "proj", "run.googleapis.com"); err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	_, op, err := s.DisableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("DisableAPI: %v", err)
	}
	if op.Done || op.Verb != "disable" {
		t.Fatalf("async disable = %+v, want in-flight", op)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err := s.GetOperation(ctx, "proj", op.Name)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !got.Done || got.Verb != "disable" || len(got.Services) != 1 || got.Services[0].State != StateDisabled {
		t.Fatalf("settled disable = %+v", got)
	}
}

// TestSyncDefaultPersistsDoneOperation verifies the default synchronous mode is
// unchanged: the operation is done inline, with an end time, and is still
// addressable by operations.get.
func TestSyncDefaultPersistsDoneOperation(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())

	_, op, err := s.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	if !op.Done || op.EndTime.IsZero() {
		t.Fatalf("sync operation = %+v, want done with end time", op)
	}
	got, err := s.GetOperation(ctx, "proj", op.Name)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !got.Done || got.Verb != "enable" {
		t.Fatalf("stored operation = %+v", got)
	}
}

// TestBatchEnableOperationSnapshot verifies the batchEnable operation persists
// every affected service so a poll can rebuild the batch response.
func TestBatchEnableOperationSnapshot(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())

	apis, op, err := s.BatchEnableAPIs(ctx, "proj", []string{"run.googleapis.com", "storage.googleapis.com"})
	if err != nil {
		t.Fatalf("BatchEnableAPIs: %v", err)
	}
	if op.Verb != "batchEnable" || len(op.Services) != 2 || len(op.ResourceNames) != len(apis) {
		t.Fatalf("operation = %+v", op)
	}
	got, err := s.GetOperation(ctx, "proj", op.Name)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if len(got.Services) != 2 {
		t.Fatalf("settled services = %d, want 2", len(got.Services))
	}
}

// TestGetOperationResolvesAcrossProjectScopes verifies a top-level operations/{id}
// is resolvable even when the caller's project differs from the creating project:
// a poll carries no project segment, so the resolver falls back to a cross-scope
// lookup (and delete removes it from its true scope).
func TestGetOperationResolvesAcrossProjectScopes(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())

	_, op, err := s.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	got, err := s.GetOperation(ctx, "jaiscloud-project", op.Name)
	if err != nil {
		t.Fatalf("GetOperation across scopes: %v", err)
	}
	if got.Name != op.Name {
		t.Fatalf("operation = %+v, want %s", got, op.Name)
	}
	if err := s.DeleteOperation(ctx, "jaiscloud-project", op.Name); err != nil {
		t.Fatalf("DeleteOperation across scopes: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", op.Name); !IsNotFound(err) {
		t.Fatalf("GetOperation after cross-scope delete: %v, want NotFound", err)
	}
}

// TestGetOperationUnknownAndCancelDelete covers the negative paths: an unknown
// id is NotFound, cancel is a no-op for a known id, and delete removes it.
func TestGetOperationUnknownAndCancelDelete(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())

	if _, err := s.GetOperation(ctx, "proj", "operations/missing"); !IsNotFound(err) {
		t.Fatalf("GetOperation unknown: %v, want NotFound", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "bad-name"); err == nil {
		t.Fatalf("GetOperation malformed name must error")
	}

	_, op, err := s.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	if err := s.CancelOperation(ctx, "proj", op.Name); err != nil {
		t.Fatalf("CancelOperation known: %v", err)
	}
	if err := s.CancelOperation(ctx, "proj", "operations/missing"); !IsNotFound(err) {
		t.Fatalf("CancelOperation unknown: %v, want NotFound", err)
	}
	if err := s.DeleteOperation(ctx, "proj", op.Name); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", op.Name); !IsNotFound(err) {
		t.Fatalf("GetOperation after delete: %v, want NotFound", err)
	}
	if err := s.DeleteOperation(ctx, "proj", op.Name); !IsNotFound(err) {
		t.Fatalf("DeleteOperation again: %v, want NotFound", err)
	}
}

// TestListOperations verifies the persisted operations are listed for the
// project with cursor paging.
func TestListOperations(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())

	for _, id := range []string{"a.googleapis.com", "b.googleapis.com", "c.googleapis.com"} {
		if _, _, err := s.EnableAPI(ctx, "proj", id); err != nil {
			t.Fatalf("EnableAPI %s: %v", id, err)
		}
	}
	page, next, err := s.ListOperations(ctx, "proj", 2, "")
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(page) != 2 || next == "" {
		t.Fatalf("page 1 = %d items, next=%q", len(page), next)
	}
	page2, next2, err := s.ListOperations(ctx, "proj", 2, next)
	if err != nil {
		t.Fatalf("ListOperations page 2: %v", err)
	}
	if len(page2) != 1 || next2 != "" {
		t.Fatalf("page 2 = %d items, next=%q", len(page2), next2)
	}
}
