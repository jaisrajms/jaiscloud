package lro

import (
	"reflect"
	"sync"
	"testing"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

// TestTrackerEmitsOncePerKey verifies a key publishes exactly one event no
// matter how many times it is emitted, while distinct keys each publish.
func TestTrackerEmitsOncePerKey(t *testing.T) {
	bus := events.NewEventBus()
	var mu sync.Mutex
	var got []events.StatusEvent
	bus.Subscribe(events.EventStatus, func(e events.Event) {
		if p, ok := e.Payload.(events.StatusEvent); ok {
			mu.Lock()
			got = append(got, p)
			mu.Unlock()
		}
	})

	tr := NewTracker(bus)
	tr.EmitOperation("functions", "op-1", "op-1", "create")
	tr.EmitOperation("functions", "op-1", "op-1", "create")
	tr.EmitOperation("functions", "op-2", "op-2", "delete")

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("emitted %d events, want 2 (one per key)", len(got))
	}
	want := events.StatusEvent{
		Cloud:    model.CloudGCP,
		Keys:     []string{"gcp", "functions"},
		Resource: "gcp-functions-operation",
		ID:       "op-1",
		State:    "DONE",
		Detail:   "create",
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("event = %+v, want %+v", got[0], want)
	}
}

// TestTrackerNilIsInert verifies a nil Tracker and a Tracker built from a nil
// bus are both no-ops, so a core can hold one unconditionally.
func TestTrackerNilIsInert(t *testing.T) {
	var tr *Tracker
	tr.EmitOperation("run", "op", "op", "create") // must not panic

	NewTracker(nil).EmitOperation("run", "op", "op", "create") // must not panic
}
