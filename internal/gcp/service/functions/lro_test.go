package functions

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// lroCreateInput is the minimal v1 create body used by the LRO tests.
func lroCreateInput() FunctionInput {
	return FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1)
}

// TestFunctionsLROSynchronousDefault verifies the default (zero) mode is
// unchanged: create returns a done operation carrying the inline response and a
// completion timestamp.
func TestFunctionsLROSynchronousDefault(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())

	_, op, err := s.CreateFunction(ctx, "proj", "us-central1", "hello", lroCreateInput(), V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !op.Done {
		t.Fatalf("default mode must return a done operation")
	}
	if op.EndTime.IsZero() {
		t.Fatalf("default mode must set endTime")
	}

	got, err := s.LoadOperation(ctx, "proj", OperationName(V1, "proj", op))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !got.Done || got.EndTime.IsZero() {
		t.Fatalf("default-mode read = done=%v endTime=%v; want done with endTime", got.Done, got.EndTime)
	}
	js := OperationJSON(V1, "proj", got)
	if js["done"] != true {
		t.Fatalf("default json done = %v, want true", js["done"])
	}
	if _, ok := js["response"]; !ok {
		t.Fatalf("default json must include response: %v", js)
	}
	if md, _ := js["metadata"].(map[string]any); md == nil || md["updateTime"] == nil {
		t.Fatalf("default json metadata missing updateTime: %v", js)
	}
}

// TestFunctionsLROAsyncSettlesOnRead verifies async mode stores an in-flight
// operation that omits the response/completion timestamp, and settles it
// lazily once the delay elapses.
func TestFunctionsLROAsyncSettlesOnRead(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	_, op, err := s.CreateFunction(ctx, "proj", "us-central1", "hello", lroCreateInput(), V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if op.Done {
		t.Fatalf("async create must return an in-flight operation")
	}
	if !op.EndTime.IsZero() {
		t.Fatalf("in-flight operation endTime = %v, want zero", op.EndTime)
	}

	// The pending wire shape omits response and the completion timestamp.
	pendingJSON := OperationJSON(V1, "proj", op)
	if pendingJSON["done"] != false {
		t.Fatalf("pending done = %v, want false", pendingJSON["done"])
	}
	if _, ok := pendingJSON["response"]; ok {
		t.Fatalf("pending operation must not include response: %v", pendingJSON)
	}
	if md, _ := pendingJSON["metadata"].(map[string]any); md != nil {
		if _, ok := md["updateTime"]; ok {
			t.Fatalf("pending v1 metadata must not include updateTime: %v", md)
		}
	} else {
		t.Fatalf("pending metadata missing: %v", pendingJSON)
	}
	if md, _ := OperationJSON(V2, "proj", op)["metadata"].(map[string]any); md != nil {
		if _, ok := md["endTime"]; ok {
			t.Fatalf("pending v2 metadata must not include endTime: %v", md)
		}
	}

	name := OperationName(V1, "proj", op)

	// Before the delay a direct load stays in flight.
	if got, err := s.LoadOperation(ctx, "proj", name); err != nil || got.Done {
		t.Fatalf("read before delay = done=%v err=%v; want in flight", got.Done, err)
	}
	// The REST get surface settles through the same path.
	if js, err := s.GetOperationJSON(ctx, "proj", name, V1); err != nil || js["done"] != false {
		t.Fatalf("get before delay = %v err=%v; want in flight", js, err)
	}

	// Past the delay the read settles deterministically at createTime+delay.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err := s.LoadOperation(ctx, "proj", name)
	if err != nil {
		t.Fatalf("load past delay: %v", err)
	}
	if !got.Done {
		t.Fatalf("read past delay must be done")
	}
	if want := t0.Add(30 * time.Second); !got.EndTime.Equal(want) {
		t.Fatalf("settled endTime = %v, want %v", got.EndTime, want)
	}
	js := OperationJSON(V1, "proj", got)
	if js["done"] != true {
		t.Fatalf("settled done = %v, want true", js["done"])
	}
	if md, _ := js["metadata"].(map[string]any); md == nil || md["updateTime"] == nil {
		t.Fatalf("settled metadata missing updateTime: %v", js)
	}
	if resp, _ := js["response"].(map[string]any); resp == nil || resp["@type"] != functionTypeURLV1 {
		t.Fatalf("settled response = %v", resp)
	}

	// The persisted record is unchanged: settling is derived on read.
	stored, err := s.functions.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("store get: %v", err)
	}
	if stored.Done {
		t.Fatalf("settling must not mutate the persisted done flag")
	}
}

// TestFunctionsLROAsyncListSettles verifies ListOperations settles every
// returned operation once the delay has elapsed.
func TestFunctionsLROAsyncListSettles(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "hello", lroCreateInput(), V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	page, _, err := s.ListOperations(ctx, "proj", "us-central1", V1, "", 0, "")
	if err != nil {
		t.Fatalf("list before delay: %v", err)
	}
	if len(page) != 1 || page[0].Done {
		t.Fatalf("list before delay = %+v, want one in-flight operation", page)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	page, _, err = s.ListOperations(ctx, "proj", "us-central1", V1, "", 0, "")
	if err != nil {
		t.Fatalf("list past delay: %v", err)
	}
	if len(page) != 1 || !page[0].Done || page[0].EndTime.IsZero() {
		t.Fatalf("list past delay = %+v, want one settled operation", page)
	}
}

// TestFunctionsLROAsyncListFilterSettles verifies the `done` filter is evaluated
// against the settled operation: an in-flight async operation is not reported
// by done=true until its delay elapses, then it is.
func TestFunctionsLROAsyncListFilterSettles(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "hello", lroCreateInput(), V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	before, _, err := s.ListOperations(ctx, "proj", "us-central1", V2, "done=true", 0, "")
	if err != nil {
		t.Fatalf("filter before delay: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("done=true before delay = %+v, want empty", before)
	}
	if inflight, _, err := s.ListOperations(ctx, "proj", "us-central1", V2, "done=false", 0, ""); err != nil || len(inflight) != 1 {
		t.Fatalf("done=false before delay = %+v err=%v, want one in-flight op", inflight, err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	after, _, err := s.ListOperations(ctx, "proj", "us-central1", V2, "done=true", 0, "")
	if err != nil {
		t.Fatalf("filter past delay: %v", err)
	}
	if len(after) != 1 || !after[0].Done {
		t.Fatalf("done=true past delay = %+v, want one settled op", after)
	}
}

// TestFunctionsLROAsyncUpgradeOp verifies the v2 upgrade/traffic operations
// honour the async mode through the shared applyUpgrade path.
func TestFunctionsLROAsyncUpgradeOp(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "hello", lroCreateInput(), V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	op, err := s.SetupFunctionUpgradeConfig(ctx, "proj", "us-central1", "hello", UpgradeConfig{}, V2)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if op.Done {
		t.Fatalf("async upgrade operation must be in flight")
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err := s.LoadOperation(ctx, "proj", OperationName(V2, "proj", op))
	if err != nil {
		t.Fatalf("load upgrade operation: %v", err)
	}
	if !got.Done || got.Function == nil {
		t.Fatalf("settled upgrade operation = done=%v function=%v", got.Done, got.Function)
	}
}
