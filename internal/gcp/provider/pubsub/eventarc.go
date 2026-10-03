package pubsub

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// EnsureEventarcTopic idempotently creates the Eventarc-managed transport topic
// that backs a trigger whose source is not a Pub/Sub topic (a Cloud Storage
// trigger), returning its short id. Real Eventarc auto-creates this topic
// alongside the trigger's subscription; the emulator mirrors that naming so the
// paired topic/subscription are discoverable in topics.list /
// subscriptions.list. An existing topic is left untouched.
func (p *Provider) EnsureEventarcTopic(ctx context.Context, project, location, triggerID string) (string, error) {
	id := eventing.EventarcTopicID(location, triggerID)
	meta := map[string]any{
		"name":   resource.ResourceID(project)("pubsub-topic", id),
		"labels": map[string]string{"goog-eventarc-trigger": triggerID},
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: id, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return id, nil
		}
		return "", err
	}
	return id, nil
}

// DeleteEventarcTopic removes an Eventarc-managed transport topic, detaching
// any subscriptions (they are re-pointed at the "_deleted-topic_" sentinel, the
// real Pub/Sub behaviour) and tolerating absence.
func (p *Provider) DeleteEventarcTopic(ctx context.Context, project, topic string) error {
	if err := p.deleteTopic(ctx, project, lastSegment(topic)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// EnsureEventarcSubscription idempotently creates the transport subscription of
// an Eventarc trigger on topic, returning its short id. An existing subscription
// is left untouched so a user-configured deadLetterPolicy survives
// re-provisioning (a function PATCH re-ensures the trigger).
func (p *Provider) EnsureEventarcSubscription(ctx context.Context, project, location, triggerID, topic string) (string, error) {
	id := eventing.EventarcSubscriptionID(location, triggerID)
	topicID := lastSegment(topic)
	if !p.topicExists(ctx, project, topicID) {
		return "", model.NewProviderError("NotFound", "dead-letter transport topic not found: "+topic, 404)
	}
	if e, err := p.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, id); err == nil {
		// Re-ensure keeps a user-configured deadLetterPolicy intact but follows a
		// changed trigger topic.
		var meta map[string]any
		if json.Unmarshal(e.Data, &meta) == nil && meta != nil {
			if cur, _ := meta["topic"].(string); lastSegment(cur) != topicID {
				meta["topic"] = resource.ResourceID(project)("pubsub-topic", topicID)
				data, _ := json.Marshal(meta)
				if err := p.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: id, Data: data}); err != nil {
					return "", err
				}
			}
		}
		return id, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	meta := map[string]any{
		"name":                     resource.ResourceID(project)("pubsub-subscription", id),
		"topic":                    resource.ResourceID(project)("pubsub-topic", topicID),
		"ackDeadlineSeconds":       10,
		"state":                    "ACTIVE",
		"messageRetentionDuration": "604800s",
		"expirationPolicy":         map[string]any{"ttl": "2678400s"},
		"labels":                   map[string]string{"goog-eventarc-trigger": triggerID},
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: id, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return id, nil
		}
		return "", err
	}
	return id, nil
}

// DeleteEventarcSubscription removes a trigger's transport subscription and its
// queued messages, tolerating absence.
func (p *Provider) DeleteEventarcSubscription(ctx context.Context, project, subscription string) error {
	id := lastSegment(subscription)
	if err := p.resources.Delete(ctx, project, store.GlobalRegion, rtSubscription, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if msgs, err := p.messages.List(ctx, id); err == nil {
		for _, m := range msgs {
			_ = p.messages.Delete(ctx, id, m.MessageID)
		}
	}
	return nil
}

// SubscriptionDeadLetter reports a subscription's configured deadLetterPolicy:
// the dead-letter topic (short id), the maximum delivery attempts, and whether a
// policy is set. A missing subscription reports ok=false rather than an error so
// the delivery engine can degrade to a terminal dead_letter record.
func (p *Provider) SubscriptionDeadLetter(ctx context.Context, project, subscription string) (string, int, bool, error) {
	if subscription == "" {
		return "", 0, false, nil
	}
	e, err := p.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, lastSegment(subscription))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	var sub map[string]any
	if json.Unmarshal(e.Data, &sub) != nil {
		return "", 0, false, nil
	}
	dp, ok := sub["deadLetterPolicy"].(map[string]any)
	if !ok {
		return "", 0, false, nil
	}
	topic, _ := dp["deadLetterTopic"].(string)
	attempts := 0
	switch mda := dp["maxDeliveryAttempts"].(type) {
	case float64:
		attempts = int(mda)
	case int:
		attempts = mda
	}
	return lastSegment(topic), attempts, true, nil
}

// PublishDeadLetter forwards data with attributes to a dead-letter topic
// (short id or full name) on behalf of a subscription's deadLetterPolicy. It
// reuses the normal publish path's envelope-encryption and fan-out, so the
// forwarded message reaches the dead-letter topic's pull subscriptions (messages
// published to a topic with no subscription are dropped — real Pub/Sub
// behaviour). It deliberately does NOT re-dispatch the event to the Cloud
// Functions delivery engine: a function subscribed to its own dead-letter topic
// would otherwise recurse.
func (p *Provider) PublishDeadLetter(ctx context.Context, project, topic string, data []byte, attrs map[string]string) error {
	_, err := p.publishMessage(ctx, project, topic, data, attrs, false)
	return err
}
