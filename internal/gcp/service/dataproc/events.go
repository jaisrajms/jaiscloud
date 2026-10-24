package dataproc

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/eventing"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// eventsTopicLabel is the cluster label that overrides the default lifecycle
// events topic for a single cluster. Its value is a topic ID or full topic name
// resolved in the cluster's project; an unset/empty label falls back to the
// core-wide eventsTopic. Setting it lets one cluster route its (and its jobs')
// state changes to a dedicated topic while the rest use the default.
const eventsTopicLabel = "jaiscloud-events-topic"

// EventPublisher publishes one Cloud Pub/Sub message on behalf of the Dataproc
// core. It is satisfied by the Pub/Sub provider, letting the core reuse
// Pub/Sub's envelope encryption, per-subscription fan-out and push delivery
// without importing provider/pubsub (no provider→provider dependency). The
// shape mirrors the storage provider's EventPublisher.
type EventPublisher interface {
	// PublishEvent delivers data (plaintext bytes) with attributes to the given
	// topic (a bare ID, "projects/{p}/topics/{t}", or the fully-qualified
	// "//pubsub.googleapis.com/..." form) and returns the message ID.
	PublishEvent(ctx context.Context, accountID, topic string, data []byte, attributes map[string]string) (string, error)
}

// WithEventPublisher wires the Pub/Sub publisher used to fan lifecycle events
// out to the configured topic. Nil disables Pub/Sub publishing (unit tests that
// exercise only the direct dispatcher).
func WithEventPublisher(p EventPublisher) Option {
	return func(s *Service) { s.eventPublisher = p }
}

// WithEventsTopic sets the default lifecycle events topic (a topic ID or full
// name, project-scoped at emit time). Empty (the default) disables Pub/Sub
// event publishing; a cluster may override it with the eventsTopicLabel label.
func WithEventsTopic(topic string) Option {
	return func(s *Service) { s.eventsTopic = strings.TrimSpace(topic) }
}

// WithEventBus wires the shared event bus the console's live status stream
// subscribes to, so Dataproc job/cluster transitions surface in the UI. Nil
// (the default) disables status events.
func WithEventBus(bus *events.EventBus) Option {
	return func(s *Service) {
		if bus != nil {
			s.eventBus = bus
		}
	}
}

// SetEventDispatcher wires the Cloud Functions event-delivery engine (mirrors
// the storage provider's SetFunctionDispatcher), so a lifecycle event also
// reaches every function whose eventTrigger subscribes to it directly (not via
// the Pub/Sub topic). It is called once at startup, after construction, because
// the dispatcher is built before the Dataproc core in main.go. Nil disables
// direct delivery.
func (s *Service) SetEventDispatcher(d eventing.Dispatcher) { s.eventDispatcher = d }

// jobStateChangeData is the CloudEvents `data` payload for a Dataproc job
// state transition. It is an emulator-defined contract: real GCP has no native
// Dataproc Pub/Sub event source, so the field set mirrors the job's own
// reference/status fields rather than a documented event schema.
type jobStateChangeData struct {
	JobID          string `json:"jobId"`
	JobUUID        string `json:"jobUuid,omitempty"`
	ClusterName    string `json:"clusterName"`
	ProjectID      string `json:"projectId"`
	Region         string `json:"region"`
	PreviousState  string `json:"previousState,omitempty"`
	State          string `json:"state"`
	StateStartTime string `json:"stateStartTime"`
	Attempt        int    `json:"attempt"`
}

// clusterStateChangeData is the CloudEvents `data` payload for a Dataproc
// cluster state transition (see jobStateChangeData on the emulator-defined
// contract).
type clusterStateChangeData struct {
	ClusterName    string `json:"clusterName"`
	ClusterUUID    string `json:"clusterUuid,omitempty"`
	ProjectID      string `json:"projectId"`
	Region         string `json:"region"`
	PreviousState  string `json:"previousState,omitempty"`
	State          string `json:"state"`
	StateStartTime string `json:"stateStartTime"`
}

// clusterDeletedState is the terminal state emitted when a DELETING cluster's
// record is removed. It is emulator-only: dataproc.v1.ClusterStatus has no
// DELETED state (a deleted cluster simply no longer exists), but the event
// stream needs a terminal signal.
const clusterDeletedState = "DELETED"

// cloudEvent is the structured CloudEvents 1.0 envelope published as the Pub/Sub
// message body. The same fields are also mirrored into the message attributes
// for subscription filtering.
type cloudEvent struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	DataContentType string `json:"datacontenttype"`
	Time            string `json:"time"`
	Data            any    `json:"data"`
}

// dataprocEventSource builds the CloudEvents `source` for a Dataproc resource:
// "//dataproc.googleapis.com/projects/{p}/regions/{r}/clusters/{c}".
func dataprocEventSource(resourceName string) string {
	return "//dataproc.googleapis.com/" + resourceName
}

// emitJobStateChange publishes one lifecycle event for a job transition from
// prev to the job's current state. It is best-effort: a publish or dispatch
// failure is logged, never returned, so a state transition can never fail
// because of eventing.
func (s *Service) emitJobStateChange(ctx context.Context, project, region string, j dpstore.Job, prev dpstore.JobStatus) {
	if s.eventPublisher == nil && s.eventDispatcher == nil && s.eventBus == nil {
		return
	}
	occurred := j.Status.StateStartTime
	if occurred.IsZero() {
		occurred = clock.Now().UTC()
	}
	attempt := jobAttemptNumber(j)
	data := jobStateChangeData{
		JobID:          j.JobID,
		JobUUID:        j.JobUUID,
		ClusterName:    j.PlacementClusterName,
		ProjectID:      project,
		Region:         region,
		PreviousState:  prev.State,
		State:          j.Status.State,
		StateStartTime: formatTimestamp(occurred),
		Attempt:        attempt,
	}
	attrs := map[string]string{
		"eventType":     eventing.TypeDataprocJobStateChange,
		"projectId":     project,
		"region":        region,
		"clusterName":   j.PlacementClusterName,
		"jobId":         j.JobID,
		"previousState": prev.State,
		"state":         j.Status.State,
		"attempt":       strconv.Itoa(attempt),
	}
	// The per-cluster topic override only matters for Pub/Sub publishing, so
	// skip the cluster lookup when no publisher is wired.
	topic := ""
	if s.eventPublisher != nil {
		topic = s.jobEventsTopic(ctx, project, region, j.PlacementClusterName)
	}
	s.emit(ctx, project, topic,
		JobName(project, region, j.JobID), eventing.TypeDataprocJobStateChange, data, attrs, occurred)
}

// emitClusterStateChange publishes one lifecycle event for a cluster transition
// from prev to the cluster's current state (best-effort, like the job variant).
func (s *Service) emitClusterStateChange(ctx context.Context, project, region string, c dpstore.Cluster, prev dpstore.ClusterStatus) {
	if s.eventPublisher == nil && s.eventDispatcher == nil && s.eventBus == nil {
		return
	}
	occurred := c.Status.StateStartTime
	if occurred.IsZero() {
		occurred = clock.Now().UTC()
	}
	data := clusterStateChangeData{
		ClusterName:    c.Name,
		ClusterUUID:    c.ClusterUUID,
		ProjectID:      project,
		Region:         region,
		PreviousState:  prev.State,
		State:          c.Status.State,
		StateStartTime: formatTimestamp(occurred),
	}
	attrs := map[string]string{
		"eventType":     eventing.TypeDataprocClusterStateChange,
		"projectId":     project,
		"region":        region,
		"clusterName":   c.Name,
		"previousState": prev.State,
		"state":         c.Status.State,
	}
	s.emit(ctx, project, eventsTopicFromLabels(c.Labels, s.eventsTopic),
		ClusterName(project, region, c.Name), eventing.TypeDataprocClusterStateChange, data, attrs, occurred)
}

// emit is the shared best-effort delivery: it dispatches the transport-neutral
// eventing.Event to the Cloud Functions engine (when wired), publishes the
// structured CloudEvents JSON to the topic (when a topic is configured and the
// publisher is wired), and pushes a cloud-neutral status event to the console's
// live stream (when an event bus is wired).
func (s *Service) emit(ctx context.Context, project, topic, resource, eventType string, data any, attrs map[string]string, occurred time.Time) {
	occurred = occurred.UTC()
	eventID := randomHex(32)
	payload, err := json.Marshal(data)
	if err != nil {
		slog.Warn("dataproc: marshal lifecycle event payload failed", "resource", resource, "err", err)
		return
	}
	if s.eventDispatcher != nil {
		s.eventDispatcher.DispatchEvent(ctx, eventing.Event{
			Project:    project,
			EventType:  eventType,
			Resource:   resource,
			EventID:    eventID,
			Source:     eventing.SourceDataproc,
			Data:       payload,
			Attributes: attrs,
			OccurredAt: occurred,
		})
	}
	if s.eventBus != nil {
		s.publishStatus(resource, eventType, attrs)
	}
	if s.eventPublisher == nil || topic == "" {
		return
	}
	body, err := json.Marshal(cloudEvent{
		SpecVersion:     "1.0",
		ID:              eventID,
		Source:          dataprocEventSource(resource),
		Type:            eventType,
		DataContentType: "application/json",
		Time:            occurred.Format(time.RFC3339Nano),
		Data:            json.RawMessage(payload),
	})
	if err != nil {
		slog.Warn("dataproc: marshal lifecycle event envelope failed", "resource", resource, "err", err)
		return
	}
	if _, err := s.eventPublisher.PublishEvent(ctx, project, topic, body, attrs); err != nil {
		slog.Warn("dataproc: publish lifecycle event failed", "resource", resource, "topic", topic, "err", err)
	}
}

// publishStatus pushes a Dataproc state transition onto the shared event bus
// for the console's live stream. Keys scope the invalidation to the GCP tree
// and the Dataproc service; Dataproc has no dedicated console page yet (UI13),
// so the broad key is intentional. Best-effort, like the other delivery paths.
func (s *Service) publishStatus(resource, eventType string, attrs map[string]string) {
	label := "gcp-dataproc"
	switch eventType {
	case eventing.TypeDataprocJobStateChange:
		label = "gcp-dataproc-job"
	case eventing.TypeDataprocClusterStateChange:
		label = "gcp-dataproc-cluster"
	}
	id := attrs["jobId"]
	if id == "" {
		id = attrs["clusterName"]
	}
	s.eventBus.Publish(events.Event{
		Type: events.EventStatus,
		Payload: events.StatusEvent{
			Cloud:    model.CloudGCP,
			Keys:     []string{"gcp", "dataproc"},
			Resource: label,
			ID:       id,
			State:    attrs["state"],
			Detail:   strings.TrimPrefix(resource, "projects/"),
		},
	})
}

// jobEventsTopic resolves the topic a job's events publish to: the label
// override on its cluster when set, otherwise the core-wide default. The
// cluster lookup is best-effort — a job whose cluster has already been deleted
// (or is momentarily absent) falls back to the default topic rather than
// dropping the event.
func (s *Service) jobEventsTopic(ctx context.Context, project, region, clusterName string) string {
	if clusterName == "" {
		return s.eventsTopic
	}
	c, err := s.store.GetCluster(ctx, project, region, clusterName)
	if err != nil {
		return s.eventsTopic
	}
	return eventsTopicFromLabels(c.Labels, s.eventsTopic)
}

// eventsTopicFromLabels returns the cluster's events-topic override when the
// label is set, otherwise the default topic.
func eventsTopicFromLabels(labels map[string]string, fallback string) string {
	if t := strings.TrimSpace(labels[eventsTopicLabel]); t != "" {
		return t
	}
	return fallback
}

// jobAttemptNumber reports the 1-based ordinal of the attempt a job transition
// belongs to. An ATTEMPT_FAILURE history entry starts a new attempt, except
// when it is the last entry and the job is already terminal: there the failure
// ends the current attempt rather than starting one. A non-restartable job (or
// one whose restarts are exhausted) therefore keeps the same attempt number on
// its terminal ERROR as the ATTEMPT_FAILURE it followed; a restartable job that
// retries continues to the next attempt (the ATTEMPT_FAILURE is mid-history).
func jobAttemptNumber(j dpstore.Job) int {
	n := 0
	for i, h := range j.StatusHistory {
		if h.State != jobStateAttemptFail {
			continue
		}
		if i == len(j.StatusHistory)-1 && jobTerminal(j.Status.State) {
			continue
		}
		n++
	}
	return 1 + n
}
