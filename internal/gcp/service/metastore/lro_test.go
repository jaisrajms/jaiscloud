package metastore

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// TestMetastoreLROSynchronousDefault verifies the default (zero) mode is
// unchanged: create returns a done operation with an endTime and an inline
// response.
func TestMetastoreLROSynchronousDefault(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	_, op, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil)
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.Done {
		t.Fatalf("default mode must return a done operation")
	}
	if op.EndTime.IsZero() {
		t.Fatalf("default mode must set endTime")
	}
	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !got.Done || got.EndTime.IsZero() {
		t.Fatalf("default-mode read = done=%v endTime=%v; want done with endTime", got.Done, got.EndTime)
	}
	// The rendered wire shape keeps endTime and the inline response.
	rendered := OperationJSON(got, "proj")
	if rendered["done"] != true {
		t.Fatalf("sync done = %v, want true", rendered["done"])
	}
	if md, _ := rendered["metadata"].(map[string]any); md == nil || md["endTime"] == nil {
		t.Fatalf("sync metadata must include endTime: %v", rendered)
	}
	if _, ok := rendered["response"]; !ok {
		t.Fatalf("sync operation must include response: %v", rendered)
	}
}

// TestMetastoreLROAsyncSettlesOnRead verifies async mode stores an in-flight
// operation and settles it lazily once the delay elapses.
func TestMetastoreLROAsyncSettlesOnRead(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(metastorestore.NewMemoryStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	_, op, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil)
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if op.Done {
		t.Fatalf("async create must return an in-flight operation")
	}
	if !op.EndTime.IsZero() {
		t.Fatalf("in-flight operation endTime = %v, want zero", op.EndTime)
	}

	// The pending wire shape omits response and metadata.endTime.
	pendingJSON := OperationJSON(op, "proj")
	if pendingJSON["done"] != false {
		t.Fatalf("pending done = %v, want false", pendingJSON["done"])
	}
	if _, ok := pendingJSON["response"]; ok {
		t.Fatalf("pending operation must not include response: %v", pendingJSON)
	}
	if md, _ := pendingJSON["metadata"].(map[string]any); md == nil {
		t.Fatalf("pending metadata missing: %v", pendingJSON)
	} else if _, ok := md["endTime"]; ok {
		t.Fatalf("pending metadata must not include endTime: %v", md)
	}

	// Before the delay, a read stays in flight; ListOperations settles too.
	if got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID); err != nil || got.Done {
		t.Fatalf("read before delay = done=%v err=%v; want in flight", got.Done, err)
	}
	page, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "")
	if err != nil || len(page) != 1 || page[0].Done {
		t.Fatalf("list before delay = %+v err=%v; want one in-flight op", page, err)
	}

	// Past the delay, the read settles deterministically at createTime+delay.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !got.Done {
		t.Fatalf("read past delay must be done")
	}
	if want := t0.Add(30 * time.Second); !got.EndTime.Equal(want) {
		t.Fatalf("settled endTime = %v, want %v", got.EndTime, want)
	}

	doneJSON := OperationJSON(got, "proj")
	if doneJSON["done"] != true {
		t.Fatalf("settled done = %v, want true", doneJSON["done"])
	}
	if md, _ := doneJSON["metadata"].(map[string]any); md == nil || md["endTime"] == nil {
		t.Fatalf("settled metadata missing endTime: %v", doneJSON)
	}
	response, _ := doneJSON["response"].(map[string]any)
	if response == nil || response["name"] != "projects/proj/locations/us-central1/services/svc1" {
		t.Fatalf("settled response = %v", response)
	}
	if response["@type"] != serviceTypeURL {
		t.Fatalf("settled response @type = %v, want %v", response["@type"], serviceTypeURL)
	}

	// The persisted record is unchanged: settling is derived on read.
	stored, err := s.store.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("store get: %v", err)
	}
	if stored.Done {
		t.Fatalf("settling must not mutate the persisted done flag")
	}
}
