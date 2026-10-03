package eventarc

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/resource"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// FunctionTriggerName is the deterministic trigger id the emulator provisions
// for a Cloud Functions Pub/Sub event trigger (real GCP materializes one
// Eventarc trigger per Pub/Sub-triggered function and sets the function's
// output-only eventTrigger.trigger to it).
func FunctionTriggerName(functionID string) string { return "functions-" + functionID }

// EnsureFunctionTrigger materializes (or updates) the backing Eventarc trigger
// for a function's event trigger and provisions its transport Pub/Sub
// subscription. It returns the trigger's resource name and the short id of its
// subscription, which is the surface a user configures a deadLetterPolicy on.
//
// A Pub/Sub trigger observes the user's declared topic. A Cloud Storage trigger
// observes the declared bucket and is backed by an Eventarc-managed transport
// topic the platform provisions (FP2), so it too has a user-configurable
// dead-letter subscription.
func (s *Service) EnsureFunctionTrigger(ctx context.Context, spec eventing.FunctionTriggerSpec) (string, string, error) {
	if spec.Project == "" || spec.Location == "" || spec.FunctionID == "" {
		return "", "", invalidArgument("function trigger spec is incomplete")
	}
	triggerID := FunctionTriggerName(spec.FunctionID)
	if !eventing.IsPubSubEventType(spec.EventType) && !eventing.IsStorageEventType(spec.EventType) {
		return "", "", invalidArgument("unsupported trigger event source")
	}

	// Resolve the transport topic and the trigger's event filters. A Pub/Sub
	// trigger uses the user's declared topic; a Cloud Storage trigger gets an
	// Eventarc-managed transport topic (real Eventarc auto-creates one).
	var (
		topicID    string
		filters    []any
		staleTopic string // an Eventarc-managed topic to reap after a source change
	)
	if eventing.IsPubSubEventType(spec.EventType) {
		topicID = lastSegment(spec.Resource)
		if topicID == "" {
			return "", "", invalidArgument("missing trigger topic")
		}
		filters = []any{
			map[string]any{"attribute": "type", "value": eventing.TypePubSubPublishCloudEvent},
		}
		// If a previous Cloud Storage provisioning left an Eventarc-managed
		// topic behind, reap it once the subscription points at the user's topic
		// (below), so a failed re-point leaves the old transport intact.
		staleTopic = eventing.EventarcTopicID(spec.Location, triggerID)
	} else {
		topicID = eventing.EventarcTopicID(spec.Location, triggerID)
		filters = storageEventFilters(spec.Resource, spec.EventType)
		if s.subscriptions != nil {
			if _, err := s.subscriptions.EnsureEventarcTopic(ctx, spec.Project, spec.Location, triggerID); err != nil {
				return "", "", err
			}
		}
	}

	cfg := map[string]any{
		"destination": map[string]any{
			"cloudFunction": resource.ResourceID(spec.Project)("cloud-function", spec.Location+"/"+spec.FunctionID),
		},
		"transport": map[string]any{
			"pubsub": map[string]any{"topic": resource.ResourceID(spec.Project)("pubsub-topic", topicID)},
		},
		"eventFilters":         filters,
		"eventDataContentType": "application/json",
	}
	raw, _ := json.Marshal(cfg)

	// Provision the transport subscription first: it validates that the topic
	// exists, so a function whose trigger names a missing topic does not leave a
	// dangling backing trigger behind.
	sub := ""
	if s.subscriptions != nil {
		name, err := s.subscriptions.EnsureEventarcSubscription(ctx, spec.Project, spec.Location, triggerID, topicID)
		if err != nil {
			return "", "", err
		}
		sub = name
		if staleTopic != "" && staleTopic != topicID {
			_ = s.subscriptions.DeleteEventarcTopic(ctx, spec.Project, staleTopic)
		}
	}

	if _, err := s.store.GetTrigger(ctx, spec.Project, spec.Location, triggerID); err == nil {
		if _, err := s.store.UpdateTriggerAtomic(ctx, spec.Project, spec.Location, triggerID, func(t eventarcstore.Trigger) (eventarcstore.Trigger, error) {
			t.Config = raw
			t.UpdateTime = clock.Now().UTC()
			t.Etag = triggerEtag(t)
			return t, nil
		}); err != nil {
			return "", "", mapErr(err)
		}
	} else if errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		now := clock.Now().UTC()
		t := eventarcstore.Trigger{
			Location:   spec.Location,
			Name:       triggerID,
			Config:     raw,
			UID:        uuid.NewString(),
			CreateTime: now,
			UpdateTime: now,
		}
		t.Etag = triggerEtag(t)
		if err := s.store.CreateTrigger(ctx, spec.Project, spec.Location, t); err != nil && !errors.Is(err, eventarcstore.ErrAlreadyExists) {
			return "", "", mapErr(err)
		}
	} else {
		return "", "", mapErr(err)
	}

	return TriggerName(spec.Project, spec.Location, triggerID), sub, nil
}

// storageEventFilters builds the eventFilters of a Cloud Storage trigger: the
// object event type and the bucket's short id. The type filter uses the
// Eventarc CloudEvent spelling where one exists; the v1 object.change catch-all
// has no single Eventarc equivalent, so its normalized value is carried through
// (filters are metadata here: the emulator's storage producer dispatches
// directly and never evaluates them).
func storageEventFilters(resource, eventType string) []any {
	return []any{
		map[string]any{"attribute": "type", "value": storageFilterType(eventing.NormalizeEventType(eventType))},
		map[string]any{"attribute": "bucket", "value": eventing.ResourceID(resource)},
	}
}

// storageFilterType maps a normalized storage event type onto its Eventarc
// CloudEvent spelling.
func storageFilterType(normalized string) string {
	switch normalized {
	case eventing.TypeStorageFinalize:
		return "google.cloud.storage.object.v1.finalized"
	case eventing.TypeStorageDelete:
		return "google.cloud.storage.object.v1.deleted"
	}
	return normalized
}

// DeleteFunctionTrigger removes a function's backing Eventarc trigger, its
// transport subscription, and — for a Cloud Storage trigger — the
// Eventarc-managed transport topic, tolerating absence.
func (s *Service) DeleteFunctionTrigger(ctx context.Context, project, location, functionID string) error {
	if project == "" || location == "" || functionID == "" {
		return nil
	}
	triggerID := FunctionTriggerName(functionID)
	if s.subscriptions != nil {
		// Delete the transport subscription, then an Eventarc-managed transport
		// topic if the trigger's source was not a user Pub/Sub topic (a Cloud
		// Storage trigger). Ownership is verified through the label
		// EnsureEventarcTopic stamps, so a user topic that merely shares the
		// auto-generated id is never deleted.
		autoTopic := ""
		if t, err := s.store.GetTrigger(ctx, project, location, triggerID); err == nil {
			if topic := lastSegment(triggerTransportTopic(decodeBody(t.Config))); topic == eventing.EventarcTopicID(location, triggerID) && s.isEventarcManagedTopic(ctx, project, topic, triggerID) {
				autoTopic = topic
			}
		}
		_ = s.subscriptions.DeleteEventarcSubscription(ctx, project, eventing.EventarcSubscriptionID(location, triggerID))
		if autoTopic != "" {
			_ = s.subscriptions.DeleteEventarcTopic(ctx, project, autoTopic)
		}
	}
	if err := s.store.DeleteTrigger(ctx, project, location, triggerID); err != nil && !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		return mapErr(err)
	}
	return nil
}

// SyncTriggerSubscription provisions the transport subscription of a trigger
// whose destination is a Cloud Functions function, and removes it for any other
// destination (the emulator only executes functions). It is called after a
// trigger create or update, so a changed transport topic re-points the
// subscription and a destination change tears it down.
func (s *Service) SyncTriggerSubscription(ctx context.Context, project string, t eventarcstore.Trigger) error {
	if s.subscriptions == nil {
		return nil
	}
	body := decodeBody(t.Config)
	topic := triggerTransportTopic(body)
	dest := bodyMap(body, "destination")
	cf, _ := dest["cloudFunction"].(string)
	if topic == "" || cf == "" {
		s.DeleteTriggerSubscription(ctx, project, t)
		return nil
	}
	_, err := s.subscriptions.EnsureEventarcSubscription(ctx, project, t.Location, t.Name, lastSegment(topic))
	return err
}

// DeleteTriggerSubscription removes a user-created Eventarc trigger's transport
// subscription, tolerating absence.
func (s *Service) DeleteTriggerSubscription(ctx context.Context, project string, t eventarcstore.Trigger) {
	if s.subscriptions == nil {
		return
	}
	_ = s.subscriptions.DeleteEventarcSubscription(ctx, project, eventing.EventarcSubscriptionID(t.Location, t.Name))
}

// triggerTransportTopic returns a trigger config's transport.pubsub.topic, or "".
func triggerTransportTopic(body map[string]any) string {
	transport := bodyMap(body, "transport")
	if transport == nil {
		return ""
	}
	pubsub := bodyMap(transport, "pubsub")
	if pubsub == nil {
		return ""
	}
	topic, _ := pubsub["topic"].(string)
	return topic
}

// isEventarcManagedTopic reports whether a topic is the Eventarc-managed
// transport topic the platform provisioned for triggerID, identified by the
// ownership label EnsureEventarcTopic stamps on it. A user Pub/Sub topic that
// happens to share the auto-generated id carries no such label and is never
// matched, so it is not deleted with the function.
func (s *Service) isEventarcManagedTopic(ctx context.Context, project, topicID, triggerID string) bool {
	if s.resources == nil || topicID == "" || triggerID == "" {
		return false
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, topicID)
	if err != nil {
		return false
	}
	var meta map[string]any
	if json.Unmarshal(e.Data, &meta) != nil {
		return false
	}
	labels, _ := meta["labels"].(map[string]any)
	owner, _ := labels["goog-eventarc-trigger"].(string)
	return owner == triggerID
}
