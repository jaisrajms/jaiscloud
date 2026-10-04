// Package eventing carries the transport-neutral event payload that GCP
// producers hand to the Cloud Functions event-delivery engine, plus the small
// interfaces that keep the producer and consumer packages decoupled.
//
// A producer (Pub/Sub publish, GCS object finalize/delete, an Eventarc source)
// raises an Event and hands it to a Dispatcher. The Cloud Functions core
// implements Dispatcher and delivers the event to every matching event-triggered
// function, retrying per the trigger's failure policy. Producers hold the
// Dispatcher as an interface so they never import another service's core, and
// the functions core consults a TargetIndex (implemented by the Eventarc core)
// so a trigger whose destination is a cloudFunction also routes to its function
// without importing the Eventarc core.
package eventing

import (
	"context"
	"strings"
	"time"
)

// Canonical event types emitted by the emulator's producers. Cloud Functions
// event triggers name either these v2 forms or the legacy v1
// "providers/{service}/eventTypes/{type}" forms (and the Eventarc
// "google.cloud.*" CloudEvent forms); NormalizeEventType reconciles them.
const (
	TypePubSubPublish   = "google.pubsub.topic.publish"
	TypeStorageFinalize = "google.storage.object.finalize"
	TypeStorageDelete   = "google.storage.object.delete"

	// Dataproc lifecycle event types. Unlike the storage/pubsub types above,
	// these are an emulator-defined contract: real GCP has no native Dataproc
	// Pub/Sub event source, so the emulator raises them for job/cluster state
	// transitions to let event-driven consumers observe the Dataproc lifecycle.
	TypeDataprocJobStateChange     = "google.cloud.dataproc.v1.job.v1.stateChange"
	TypeDataprocClusterStateChange = "google.cloud.dataproc.v1.cluster.v1.stateChange"

	// TypePubSubPublishCloudEvent is the Eventarc CloudEvent spelling of the
	// Pub/Sub message-published event; a materialized Eventarc trigger filters
	// on it. NormalizeEventType folds it onto TypePubSubPublish.
	TypePubSubPublishCloudEvent = "google.cloud.pubsub.topic.v1.messagePublished"

	// legacyStorageObjectChange is the v1 catch-all storage event type: a
	// trigger declaring it receives every storage object event.
	legacyStorageObjectChange = "storage.object.change"
)

// EventarcSubscriptionID is the deterministic short id of the transport Pub/Sub
// subscription the emulator provisions for an Eventarc trigger. Real Eventarc
// auto-created subscription ids begin with "eventarc-{region}-"; the emulator
// keeps that shape and appends the trigger id, so the subscription is
// discoverable in subscriptions.list and addressable by subscriptions.update.
func EventarcSubscriptionID(location, triggerID string) string {
	return "eventarc-" + location + "-" + triggerID
}

// EventarcTopicID is the deterministic short id of the transport Pub/Sub topic
// Eventarc auto-provisions for a trigger whose source is not a Pub/Sub topic
// (a Cloud Storage trigger). Real Eventarc owns both the transport topic and
// its subscription; the emulator mirrors that by creating a topic whose id has
// the same eventarc-{location}-{trigger} shape as the subscription, so the
// pairing is discoverable in topics.list / subscriptions.list (FP2).
func EventarcTopicID(location, triggerID string) string {
	return "eventarc-" + location + "-" + triggerID
}

// Source identifies the producer that raised an Event.
const (
	SourcePubSub   = "pubsub"
	SourceStorage  = "storage"
	SourceEventarc = "eventarc"
	SourceDataproc = "dataproc"
)

// Event is one event raised by a producer for delivery to any subscribed Cloud
// Functions. Project is the producing project (the emulator maps a project onto
// the account scope). Resource is the normalized source resource:
// "projects/{project}/topics/{topic}" for Pub/Sub or
// "projects/_/buckets/{bucket}" for Cloud Storage.
type Event struct {
	Project    string
	EventType  string
	Resource   string
	EventID    string
	Source     string
	Data       []byte
	Attributes map[string]string
	OccurredAt time.Time
}

// Dispatcher accepts a produced event for delivery to subscribed functions. The
// Cloud Functions core implements it; producers hold it as an interface so they
// never import another service's core. A nil Dispatcher disables delivery.
type Dispatcher interface {
	DispatchEvent(ctx context.Context, ev Event)
}

// Fanout delivers each event to every member in order. Producers hold a single
// Dispatcher; the binary composes the Cloud Functions and Eventarc delivery
// engines with a Fanout so both observe the event without either core importing
// the other. Nil members (a disabled service) are skipped, and a nil Fanout
// delivers nothing.
type Fanout []Dispatcher

// DispatchEvent calls each non-nil member's DispatchEvent. Each engine filters
// the event to its own destination kind, so an event is never delivered twice
// by two engines.
func (f Fanout) DispatchEvent(ctx context.Context, ev Event) {
	for _, d := range f {
		if d == nil {
			continue
		}
		d.DispatchEvent(ctx, ev)
	}
}

// Target is a Cloud Functions function that an external trigger (an Eventarc
// trigger whose destination is a cloudFunction) routes matching events to.
type Target struct {
	Project    string
	Location   string
	FunctionID string
	// Retry mirrors the trigger's retryPolicy: true when the trigger retries a
	// failed delivery.
	Retry bool
	// Subscription is the short id of the transport Pub/Sub subscription that
	// backs the trigger (real Eventarc's transport.pubsub.subscription). It is
	// the surface a user configures a deadLetterPolicy on. Empty when the
	// trigger has no backing subscription.
	Subscription string
}

// TargetIndex resolves external trigger targets for an event. The Eventarc core
// implements it; the Cloud Functions delivery engine consults it so a trigger
// created directly (or by another client) routes to its function.
type TargetIndex interface {
	TargetsForEvent(ctx context.Context, ev Event) []Target
}

// FunctionTriggerSpec describes an event trigger a Cloud Functions function
// declares. The Eventarc core materializes it as a backing trigger: one Eventarc
// trigger per event-triggered function, backed by a platform-provisioned Pub/Sub
// subscription whose deadLetterPolicy is the user-configurable dead-letter
// surface (FD9). A Pub/Sub trigger observes the declared topic; a Cloud Storage
// trigger observes the declared bucket and is backed by an Eventarc-managed
// transport topic (FP2).
type FunctionTriggerSpec struct {
	Project    string
	Location   string
	FunctionID string
	// EventType is the declared trigger event type (a v1/v2 or CloudEvent
	// spelling); NormalizeEventType selects the transport. A Pub/Sub event type
	// uses Resource as the observed topic; a Cloud Storage event type
	// provisions an Eventarc-managed transport topic instead.
	EventType string
	// Resource is the declared source resource: "projects/{p}/topics/{t}" for a
	// Pub/Sub trigger or "projects/_/buckets/{bucket}" for a Cloud Storage one.
	Resource string
}

// IsPubSubEventType reports whether a declared event type names a Pub/Sub
// message-published event, in any of the v1, v2, or CloudEvent spellings.
func IsPubSubEventType(t string) bool { return NormalizeEventType(t) == TypePubSubPublish }

// IsStorageEventType reports whether a declared event type names a Cloud Storage
// object event the emulator materializes/executes: a v2 CloudEvent, a v1
// finalize/delete, or the v1 object.change catch-all. It is deliberately the
// narrower set the delivery engine supports.
func IsStorageEventType(t string) bool {
	switch NormalizeEventType(t) {
	case TypeStorageFinalize, TypeStorageDelete, legacyStorageObjectChange:
		return true
	}
	return false
}

// IsCloudStorageEventType reports whether a declared event type names a Cloud
// Storage object event in any of the v1, v2, or CloudEvent spellings, including
// the archived/metadataUpdated types the emulator does not itself produce. Real
// Eventarc requires a Cloud Storage trigger to also declare a bucket filter, so
// trigger validation keys off this broader set than the delivery matcher's
// IsStorageEventType.
func IsCloudStorageEventType(t string) bool {
	switch NormalizeEventType(t) {
	case TypeStorageFinalize, TypeStorageDelete, legacyStorageObjectChange:
		return true
	}
	t = strings.TrimSpace(t)
	return strings.HasPrefix(t, "google.cloud.storage.object.") ||
		strings.HasPrefix(t, "google.storage.object.") ||
		strings.HasPrefix(t, "providers/cloud.storage/eventTypes/")
}

// TriggerProvisioner materializes and removes the backing Eventarc trigger of a
// function's Pub/Sub or Cloud Storage event trigger. The Eventarc core implements
// it; the Cloud Functions core holds it as this interface so it never imports
// Eventarc and the two cores cannot drift.
type TriggerProvisioner interface {
	// EnsureFunctionTrigger creates or updates the backing Eventarc trigger and
	// returns its resource name plus the short id of its transport Pub/Sub
	// subscription (the dead-letter surface).
	EnsureFunctionTrigger(ctx context.Context, spec FunctionTriggerSpec) (triggerName, subscription string, err error)
	// DeleteFunctionTrigger removes the backing trigger and its subscription,
	// tolerating an absent trigger.
	DeleteFunctionTrigger(ctx context.Context, project, location, functionID string) error
}

// SubscriptionProvisioner manages the platform-provisioned Pub/Sub subscription
// that backs an Eventarc trigger and forwards an exhausted delivery to its
// configured dead-letter topic. The Pub/Sub provider implements it; the
// Eventarc and Cloud Functions cores hold it as this interface so they never
// import the Pub/Sub provider.
type SubscriptionProvisioner interface {
	// EnsureEventarcTopic idempotently creates the Eventarc-managed transport
	// topic that backs a non-Pub/Sub trigger (a Cloud Storage trigger),
	// returning its short id. An existing topic is left untouched.
	EnsureEventarcTopic(ctx context.Context, project, location, triggerID string) (string, error)
	// DeleteEventarcTopic removes an Eventarc-managed transport topic,
	// tolerating absence.
	DeleteEventarcTopic(ctx context.Context, project, topic string) error
	// EnsureEventarcSubscription idempotently creates the transport
	// subscription of an Eventarc trigger on topic, returning its short id. An
	// existing subscription is left untouched so a user-configured
	// deadLetterPolicy survives re-provisioning.
	EnsureEventarcSubscription(ctx context.Context, project, location, triggerID, topic string) (string, error)
	// DeleteEventarcSubscription removes a trigger's transport subscription,
	// tolerating absence.
	DeleteEventarcSubscription(ctx context.Context, project, subscription string) error
	// SubscriptionDeadLetter reports the subscription's configured
	// deadLetterPolicy: the dead-letter topic (short id), the maximum delivery
	// attempts, and whether a policy is set.
	SubscriptionDeadLetter(ctx context.Context, project, subscription string) (topic string, maxAttempts int, ok bool, err error)
	// PublishDeadLetter publishes data with attributes to a dead-letter topic
	// (short id or full name) on behalf of a subscription's deadLetterPolicy.
	PublishDeadLetter(ctx context.Context, project, topic string, data []byte, attrs map[string]string) error
}

// NormalizeEventType maps a declared Cloud Functions or Eventarc event type onto
// the canonical form emitted by the emulator's producers. Recognized aliases:
//
//	google.pubsub.topic.publish                                → TypePubSubPublish
//	providers/cloud.pubsub/eventTypes/topic.publish            → TypePubSubPublish
//	google.cloud.pubsub.topic.v1.messagePublished             → TypePubSubPublish
//	google.storage.object.finalize                             → TypeStorageFinalize
//	google.cloud.storage.object.v1.finalized                   → TypeStorageFinalize
//	google.storage.object.delete                               → TypeStorageDelete
//	google.cloud.storage.object.v1.deleted                     → TypeStorageDelete
//	providers/cloud.storage/eventTypes/object.change           → legacyStorageObjectChange
//
// Anything else is returned unchanged (lower-cased and trimmed), so an exact
// match between a producer and a trigger still works.
func NormalizeEventType(t string) string {
	t = strings.TrimSpace(t)
	switch t {
	case TypePubSubPublish, "providers/cloud.pubsub/eventTypes/topic.publish",
		TypePubSubPublishCloudEvent:
		return TypePubSubPublish
	case TypeStorageFinalize, "google.cloud.storage.object.v1.finalized":
		return TypeStorageFinalize
	case TypeStorageDelete, "google.cloud.storage.object.v1.deleted":
		return TypeStorageDelete
	case "providers/cloud.storage/eventTypes/object.change":
		return legacyStorageObjectChange
	}
	return t
}

// TypeMatches reports whether a trigger declaring declaredType subscribes to an
// event of actualType. The v1 object.change catch-all subscribes to every
// storage object event.
func TypeMatches(declaredType, actualType string) bool {
	d := NormalizeEventType(declaredType)
	a := NormalizeEventType(actualType)
	if d == "" || a == "" {
		return false
	}
	if d == a {
		return true
	}
	return d == legacyStorageObjectChange && strings.HasPrefix(a, "google.storage.object.")
}

// ResourceID returns the last non-empty path segment of a resource name
// ("projects/p/topics/t" → "t", "projects/_/buckets/b" → "b", "b" → "b"). It is
// used to compare a trigger's declared resource against an event's normalized
// resource, tolerating the short and fully-qualified forms real GCP accepts.
func ResourceID(name string) string {
	name = strings.TrimRight(strings.TrimSpace(name), "/")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}
