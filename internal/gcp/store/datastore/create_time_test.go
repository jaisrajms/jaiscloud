package datastore

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// TestApplyMutationStampsAndPreservesCreateTime checks that the store stamps a
// create time on first write and preserves it across later writes while the
// update time advances (AUD6-5).
func TestApplyMutationStampsAndPreservesCreateTime(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	inserted, err := s.ApplyMutation(ctx, "p", MutationInsert, testEntity("Task", "a", map[string]Value{"Done": bl(false)}), nil)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if inserted.CreateTime.IsZero() {
		t.Fatal("insert must stamp create_time")
	}
	if !inserted.CreateTime.Equal(inserted.UpdateTime) {
		t.Fatalf("insert create_time %v != update_time %v", inserted.CreateTime, inserted.UpdateTime)
	}

	updated, err := s.ApplyMutation(ctx, "p", MutationUpdate, testEntity("Task", "a", map[string]Value{"Done": bl(true)}), nil)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !updated.CreateTime.Equal(inserted.CreateTime) {
		t.Fatalf("update create_time %v, want preserved %v", updated.CreateTime, inserted.CreateTime)
	}
	if !updated.UpdateTime.After(inserted.UpdateTime) {
		t.Fatalf("update update_time %v must advance past %v", updated.UpdateTime, inserted.UpdateTime)
	}
}

// TestUpdateBackfillsZeroCreateTime checks that a stored entity whose create
// time is zero (e.g. written before create_time was tracked, or restored from a
// legacy snapshot) gains a create time on its next write rather than keeping a
// zero that every transport omits.
func TestUpdateBackfillsZeroCreateTime(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	legacy := testEntity("Task", "a", map[string]Value{"Done": bl(false)})
	legacy.Version = 1
	if err := s.Upsert(ctx, "p", legacy); err != nil {
		t.Fatalf("seed: %v", err)
	}

	updated, err := s.ApplyMutation(ctx, "p", MutationUpdate, testEntity("Task", "a", map[string]Value{"Done": bl(true)}), nil)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.CreateTime.IsZero() {
		t.Fatal("update must backfill a zero create_time")
	}
}

// TestRestoreBackfillsCreateTime checks that restoring a snapshot written before
// create_time was tracked surfaces the row's update time as its create time.
func TestRestoreBackfillsCreateTime(t *testing.T) {
	ctx := context.Background()
	// A legacy snapshot: the entity JSON has updateTime but no createTime.
	legacy := `{"entities":[{"project":"p","entity":{"kind":"Task",` +
		`"key":"` + KeyOfName("Task", "a") + `","properties":{},` +
		`"version":3,"updateTime":"2020-01-02T03:04:05Z"}}]}`
	s := NewMemoryStore()
	if err := s.Restore(ctx, strings.NewReader(legacy)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	e, err := s.Get(ctx, "p", KeyOfName("Task", "a"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if e.CreateTime.IsZero() {
		t.Fatal("restore must backfill create_time")
	}
	if !e.CreateTime.Equal(e.UpdateTime) {
		t.Fatalf("backfilled create_time = %v, want update_time %v", e.CreateTime, e.UpdateTime)
	}
	// Round-tripping the corrected snapshot keeps create_time.
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore2: %v", err)
	}
	if got, err := s2.Get(ctx, "p", KeyOfName("Task", "a")); err != nil || got.CreateTime.IsZero() {
		t.Fatalf("round-tripped create_time = %v, err=%v", got.CreateTime, err)
	}
}
