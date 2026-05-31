package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"jaiscloud/internal/events"
)

// Event is a typed SSE message published to browser clients.
type Event struct {
	Type     string `json:"type"`               // "status" | "reset" | "close"
	Resource string `json:"resource,omitempty"` // "emr-cluster" | "dynamodb-table" | ...
	ID       string `json:"id,omitempty"`
	State    string `json:"state,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// Broker manages SSE subscriptions and publishes events to browser clients.
// Thread-safe. Drop-on-full semantics — a slow browser tab never blocks publishers.
type Broker struct {
	mu          sync.Mutex
	subscribers map[chan Event]struct{}
	done        chan struct{}
}

// New creates a Broker and subscribes it to the provided EventBus for EMR events.
// Call Shutdown() on process exit.
func New(bus *events.EventBus) *Broker {
	b := &Broker{
		subscribers: make(map[chan Event]struct{}),
		done:        make(chan struct{}),
	}

	// Subscribe to EMR events — these are the current EventTypes in the EventBus.
	// Phase 1b adds new EventTypes (queue/table/function state) to events package.
	for _, et := range []events.EventType{
		events.EventEMRStepState,
		events.EventEMRJobRunState,
		events.EventEMRClusterState,
		events.EventMessageDLQ,
	} {
		eventType := et // capture loop variable
		bus.Subscribe(eventType, func(e events.Event) {
			select {
			case <-b.done:
				return
			default:
			}
			sseEvt := mapEvent(e)
			b.Publish(sseEvt)
		})
	}

	return b
}

func mapEvent(e events.Event) Event {
	switch p := e.Payload.(type) {
	case events.EMRClusterStateEvent:
		return Event{Type: "status", Resource: "emr-cluster", ID: p.ClusterID, State: p.State, Detail: p.Message}
	case events.EMRStepStateEvent:
		return Event{Type: "status", Resource: "emr-step", ID: p.StepID, State: p.State, Detail: p.Message}
	case events.EMRJobRunStateEvent:
		return Event{Type: "status", Resource: "emr-jobrun", ID: p.JobRunID, State: p.State}
	case events.DLQEvent:
		return Event{Type: "status", Resource: "sqs-dlq", ID: p.MessageID, Detail: p.DLQQueueURL}
	default:
		return Event{Type: "status"}
	}
}

// Publish sends an event to all connected clients (non-blocking, drop-on-full).
func (b *Broker) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- e:
		default:
		}
	}
}

// PublishReset sends { "type": "reset" } to all connected clients.
func (b *Broker) PublishReset() {
	b.Publish(Event{Type: "reset"})
}

// ServeHTTP serves text/event-stream to a connected browser client.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := make(chan Event, 32)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		b.mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-b.done:
			writeSSE(w, Event{Type: "close"})
			flusher.Flush()
			return
		case evt := <-ch:
			writeSSE(w, evt)
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, e Event) {
	data, _ := json.Marshal(e)
	fmt.Fprintf(w, "data: %s\n\n", data) //nolint:errcheck
}

// Shutdown stops the broker: signals all subscriber goroutines to exit,
// publishes a "close" event, then closes all subscriber channels.
func (b *Broker) Shutdown() {
	close(b.done)
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		close(ch)
		delete(b.subscribers, ch)
	}
}
