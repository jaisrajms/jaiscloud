package events

import (
	"sync"
	"time"

	"jaiscloud/internal/model"
)

// EventType identifies the kind of event.
type EventType string

const (
	EventMessageDLQ      EventType = "message.dlq"       // message moved to DLQ
	EventEMRStepState    EventType = "emr.step.state"    // EMR step state changed
	EventEMRJobRunState  EventType = "emr.jobrun.state"  // EMR Containers job run state changed
	EventEMRClusterState EventType = "emr.cluster.state" // EMR cluster state changed

	// EventStatus is the cloud-neutral status-change event. Unlike the AWS
	// service-specific types above, any cloud publishes it with a StatusEvent
	// payload; the UI SSE broker maps it to a browser message.
	EventStatus EventType = "status"
)

// Event is a domain event published by a provider.
type Event struct {
	Type    EventType
	Payload any
}

// DLQEvent is emitted when a message exceeds its maxReceiveCount.
type DLQEvent struct {
	SourceQueueURL string
	DLQQueueURL    string
	MessageID      string
}

// EMRStepStateEvent is emitted when an EMR step transitions state.
// Extended to match real AWS "EMR Step Status Change" EventBridge schema.
type EMRStepStateEvent struct {
	JobFlowID         string // → detail.clusterId
	StepID            string // → detail.stepId
	Name              string // → detail.name
	State             string // → detail.state
	ActionOnFailure   string // CONTINUE | CANCEL_AND_WAIT | TERMINATE_CLUSTER
	Message           string // human-readable transition message
	StateChangeCode   string // e.g. "USER_REQUEST", "STEP_FAILURE"
	StateChangeReason string // free-form (becomes detail.stateChangeReason JSON {code,message})
	FailureReason     string // kept for backward compat; feeds detail.message for FAILED
	Severity          string // INFO | WARN | ERROR (derived if empty)
	Region            string
	AccountID         string
	Cloud             model.Cloud
	OccurredAt        time.Time // event time (provider sets; envelope reads)
}

// EMRJobRunStateEvent is emitted when an EMR Containers job run transitions state.
// Extended to match real AWS "EMR Job Run State Change" EventBridge schema.
type EMRJobRunStateEvent struct {
	VirtualClusterID string    // → detail.virtualClusterId
	JobRunID         string    // → detail.id
	Name             string    // → detail.name
	State            string    // → detail.state
	ARN              string    // arn:aws:emr-containers:<region>:<account>:/virtualclusters/<vc>/jobruns/<id>
	ReleaseLabel     string    // e.g. "emr-7.9.0-latest"
	ExecutionRoleArn string    // from StartJobRun input
	FailureReason    string    // pre-existing
	StateDetails     string    // long-form state narrative
	CreatedBy        string    // assumed-role ARN of caller
	CreatedAt        time.Time // job-run creation timestamp
	UpdatedAt        time.Time // transition timestamp
	Region           string
	AccountID        string
	Cloud            model.Cloud
}

// EMRClusterStateEvent is emitted when an EMR cluster transitions state.
// Matches real AWS "EMR Cluster State Change" EventBridge schema.
type EMRClusterStateEvent struct {
	ClusterID         string // → detail.clusterId
	Name              string // → detail.name
	State             string // STARTING | BOOTSTRAPPING | RUNNING | WAITING | TERMINATING | TERMINATED | TERMINATED_WITH_ERRORS
	Message           string // → detail.message
	StateChangeCode   string // e.g. "USER_REQUEST", "ALL_STEPS_COMPLETED"
	StateChangeReason string // free-form (becomes detail.stateChangeReason JSON)
	Severity          string // INFO | WARN | ERROR
	Region            string
	AccountID         string
	Cloud             model.Cloud
	OccurredAt        time.Time
}

// StatusEvent is a cloud-neutral resource status transition published by a
// cloud core for live UI updates. The console invalidates the React Query key
// prefix in Keys on receipt (e.g. ["gcp","run"]); Resource is a display and
// diagnostic label; ID, State and Detail describe the transition. A producer
// that has no console surface yet may still publish it with a broad key.
type StatusEvent struct {
	Cloud    model.Cloud
	Keys     []string
	Resource string
	ID       string
	State    string
	Detail   string
}

// Handler is a function that handles an event.
type Handler func(event Event)

// EventBus is a simple synchronous in-process event bus.
type EventBus struct {
	mu       sync.RWMutex
	handlers map[EventType][]Handler
}

func NewEventBus() *EventBus {
	return &EventBus{handlers: make(map[EventType][]Handler)}
}

// Subscribe registers a handler for the given event type.
func (b *EventBus) Subscribe(eventType EventType, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
}

// Publish dispatches the event to all registered handlers synchronously.
func (b *EventBus) Publish(event Event) {
	b.mu.RLock()
	handlers := b.handlers[event.Type]
	b.mu.RUnlock()
	for _, h := range handlers {
		h(event)
	}
}
