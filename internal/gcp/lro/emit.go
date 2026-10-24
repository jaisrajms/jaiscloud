package lro

import (
	"sync"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

// Tracker publishes a cloud-neutral status event once per operation when a
// lazily-settled long-running operation first transitions to done. A core calls
// EmitOperation from its settle helper after deriving that an in-flight
// operation has completed; repeated polls of the same operation do not re-emit,
// so the console sees exactly one "settled" frame per operation.
//
// The settle flip is derived on read and never written back to the store, so
// the dedup set is in memory. An emulator restart can therefore re-emit an
// operation that had already settled before the restart. That is harmless for
// the console (an extra, idempotent query invalidation) and avoids adding an
// UpdateOperation migration to seven stores for no wire-behavior gain.
//
// A nil Tracker, or one built from a nil bus, is inert: EmitOperation is a
// no-op, so a core can hold a Tracker unconditionally and a control-plane-only
// construction never depends on the console stream.
type Tracker struct {
	bus     *events.EventBus
	mu      sync.Mutex
	settled map[string]struct{}
}

// NewTracker returns a Tracker that publishes on bus. A nil bus yields an inert
// Tracker.
func NewTracker(bus *events.EventBus) *Tracker {
	return &Tracker{bus: bus, settled: make(map[string]struct{})}
}

// EmitOperation publishes the standard "operation settled" status event for an
// operation, at most once per key. key must uniquely identify the operation
// within the core (a fully-qualified operation name or its id). service is the
// console query-key segment (e.g. "run"), id and verb identify the operation and
// the mutation that produced it; the event invalidates ["gcp", service].
func (t *Tracker) EmitOperation(service, key, id, verb string) {
	if t == nil || t.bus == nil {
		return
	}
	t.mu.Lock()
	if _, ok := t.settled[key]; ok {
		t.mu.Unlock()
		return
	}
	t.settled[key] = struct{}{}
	t.mu.Unlock()

	t.bus.Publish(events.Event{
		Type:    events.EventStatus,
		Payload: OperationStatus(service, id, verb),
	})
}

// OperationStatus builds the status event a core publishes when a long-running
// operation settles. The console invalidates the ["gcp", service] query-key
// prefix on receipt; Resource, ID, State and Detail are display/diagnostic.
func OperationStatus(service, id, verb string) events.StatusEvent {
	return events.StatusEvent{
		Cloud:    model.CloudGCP,
		Keys:     []string{"gcp", service},
		Resource: "gcp-" + service + "-operation",
		ID:       id,
		State:    "DONE",
		Detail:   verb,
	}
}
