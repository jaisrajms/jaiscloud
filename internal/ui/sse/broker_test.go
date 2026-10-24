package sse

import (
	"testing"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

// subscribe registers a raw subscriber channel so a test can observe what the
// broker maps and forwards without driving the streaming HTTP handler. The bus
// dispatches handlers synchronously, so a Publish has reached this channel by
// the time it returns.
func subscribe(b *Broker) chan Event {
	ch := make(chan Event, 4)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// The cloud-neutral EventStatus channel is how GCP cores reach the browser; the
// mapped message must carry the React Query key prefix and the transition.
func TestBrokerMapsGCPStatusEvent(t *testing.T) {
	bus := events.NewEventBus()
	b := New(bus)
	t.Cleanup(b.Shutdown)
	ch := subscribe(b)

	bus.Publish(events.Event{
		Type: events.EventStatus,
		Payload: events.StatusEvent{
			Cloud:    model.CloudGCP,
			Keys:     []string{"gcp", "run"},
			Resource: "gcp-run-service",
			ID:       "svc",
			State:    "READY",
			Detail:   "create",
		},
	})

	got := <-ch
	if got.Type != "status" || got.Resource != "gcp-run-service" || got.ID != "svc" || got.State != "READY" {
		t.Fatalf("mapped status event = %+v", got)
	}
	if len(got.Keys) != 2 || got.Keys[0] != "gcp" || got.Keys[1] != "run" {
		t.Fatalf("mapped keys = %v, want [gcp run]", got.Keys)
	}
}

// The generic channel must not disturb the existing AWS EMR mappings.
func TestBrokerKeepsAWSMappings(t *testing.T) {
	bus := events.NewEventBus()
	b := New(bus)
	t.Cleanup(b.Shutdown)
	ch := subscribe(b)

	bus.Publish(events.Event{
		Type:    events.EventEMRClusterState,
		Payload: events.EMRClusterStateEvent{ClusterID: "j-1", State: "RUNNING", Message: "ok"},
	})

	got := <-ch
	if got.Resource != "emr-cluster" || got.ID != "j-1" || got.State != "RUNNING" {
		t.Fatalf("mapped EMR event = %+v", got)
	}
}

func TestMapEventUnknownPayload(t *testing.T) {
	got := mapEvent(events.Event{Type: events.EventStatus, Payload: struct{}{}})
	if got.Type != "status" || got.Resource != "" {
		t.Fatalf("unknown payload = %+v", got)
	}
}
