package seams

import (
	"context"
	"errors"
	"strings"

	"jaiscloud/internal/gcp/eventing"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	"jaiscloud/internal/model"
)

// PubSub is Pub/Sub as seen by the services that depend on it. It satisfies
// storage.EventPublisher, the secretmanager and dataproc EventPublisher, the
// monitoring and scheduler publishers' underlying publish, and
// eventing.SubscriptionProvisioner.
type PubSub struct{ s *Set }

// PubSub returns the Pub/Sub seam.
func (s *Set) PubSub() *PubSub { return &PubSub{s: s} }

var _ eventing.SubscriptionProvisioner = (*PubSub)(nil)

func (p *PubSub) call(ctx context.Context, action, project, name string, body map[string]any, meta map[string]string) (*model.ProviderResponse, error) {
	params := map[string]any{"apiVersion": "v1", "project": project, "name": name}
	if body != nil {
		params["body"] = body
	}
	return p.s.dispatch(ctx, "pubsub", action, project, params, meta)
}

// notFound reports a NotFound ProviderError.
func notFound(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.HTTPStatus == 404
}

func alreadyExists(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.HTTPStatus == 409
}

// topicProjectAndID splits any accepted topic-name form ("id", "topics/id",
// "projects/p/topics/id", "//pubsub.googleapis.com/projects/p/topics/id") into
// its project ("" when absent) and short id.
func topicProjectAndID(name string) (project, id string) {
	name = strings.TrimPrefix(name, "//pubsub.googleapis.com/")
	if i := strings.LastIndex(name, "/topics/"); i >= 0 {
		return strings.TrimPrefix(name[:i], "projects/"), name[i+len("/topics/"):]
	}
	return "", strings.TrimPrefix(name, "topics/")
}

func lastSegment(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (p *PubSub) publish(ctx context.Context, account, topic string, data []byte, attrs map[string]string, meta map[string]string) (string, error) {
	project, t := topicProjectAndID(topic)
	if t == "" {
		return "", model.NewProviderError("InvalidArgument", "missing topic", 400)
	}
	if project == "" {
		project = account
	}
	msg := map[string]any{"data": b64(data)}
	if len(attrs) > 0 {
		a := make(map[string]any, len(attrs))
		for k, v := range attrs {
			a[k] = v
		}
		msg["attributes"] = a
	}
	resp, err := p.call(ctx, "TopicPublish", project, "topics/"+t, map[string]any{"messages": []any{msg}}, meta)
	if err != nil {
		return "", err
	}
	if ids, _ := resp.Data["messageIds"].([]any); len(ids) > 0 {
		id, _ := ids[0].(string)
		return id, nil
	}
	return "", nil
}

// PublishEvent publishes one message to a topic (any accepted name form).
func (p *PubSub) PublishEvent(ctx context.Context, account, topic string, data []byte, attrs map[string]string) (string, error) {
	return p.publish(ctx, account, topic, data, attrs, nil)
}

// PublishDeadLetter forwards a message to a dead-letter topic without handing it
// to the event-delivery engine again.
func (p *PubSub) PublishDeadLetter(ctx context.Context, project, topic string, data []byte, attrs map[string]string) error {
	_, err := p.publish(ctx, project, topic, data, attrs, map[string]string{pubsubprovider.NoDispatchMeta: "1"})
	return err
}

// TopicExists reports whether the topic exists, by asking for it.
func (p *PubSub) TopicExists(ctx context.Context, account, topic string) (bool, error) {
	project, t := topicProjectAndID(topic)
	if t == "" {
		return false, nil
	}
	if project == "" {
		project = account
	}
	if _, err := p.call(ctx, "TopicGet", project, "topics/"+t, nil, nil); err != nil {
		if notFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// EnsureEventarcTopic creates the transport topic of an Eventarc trigger, or
// leaves it as it is.
func (p *PubSub) EnsureEventarcTopic(ctx context.Context, project, location, triggerID string) (string, error) {
	id := eventing.EventarcTopicID(location, triggerID)
	_, err := p.call(ctx, "TopicCreate", project, "topics/"+id,
		map[string]any{"labels": map[string]any{"goog-eventarc-trigger": triggerID}}, nil)
	if err != nil && !alreadyExists(err) {
		return "", err
	}
	return id, nil
}

// DeleteEventarcTopic removes a transport topic, tolerating its absence.
func (p *PubSub) DeleteEventarcTopic(ctx context.Context, project, topic string) error {
	_, err := p.call(ctx, "TopicDelete", project, "topics/"+lastSegment(topic), nil, nil)
	if err != nil && !notFound(err) {
		return err
	}
	return nil
}

// EnsureEventarcSubscription creates the transport subscription of a trigger on
// topic, or keeps the one that exists. When the trigger now uses another topic
// the subscription is recreated on it with its dead-letter policy and labels,
// because a client cannot move a subscription to another topic.
func (p *PubSub) EnsureEventarcSubscription(ctx context.Context, project, location, triggerID, topic string) (string, error) {
	id := eventing.EventarcSubscriptionID(location, triggerID)
	topicID := lastSegment(topic)
	name := "subscriptions/" + id
	body := map[string]any{
		"topic":  p.s.ad.ResourceIDFor(p.s.region, project)("pubsub-topic", topicID),
		"labels": map[string]any{"goog-eventarc-trigger": triggerID},
	}
	if _, err := p.call(ctx, "TopicGet", project, "topics/"+topicID, nil, nil); err != nil {
		if notFound(err) {
			return "", model.NewProviderError("NotFound", "dead-letter transport topic not found: "+topic, 404)
		}
		return "", err
	}
	cur, err := p.call(ctx, "SubscriptionGet", project, name, nil, nil)
	switch {
	case err == nil:
		curTopic, _ := cur.Data["topic"].(string)
		if lastSegment(curTopic) == topicID {
			return id, nil
		}
		if dlp, ok := cur.Data["deadLetterPolicy"]; ok {
			body["deadLetterPolicy"] = dlp
		}
		if _, err := p.call(ctx, "SubscriptionDelete", project, name, nil, nil); err != nil && !notFound(err) {
			return "", err
		}
	case !notFound(err):
		return "", err
	}
	if _, err := p.call(ctx, "SubscriptionCreate", project, name, body, nil); err != nil && !alreadyExists(err) {
		return "", err
	}
	return id, nil
}

// DeleteEventarcSubscription removes a trigger's transport subscription,
// tolerating its absence.
func (p *PubSub) DeleteEventarcSubscription(ctx context.Context, project, subscription string) error {
	_, err := p.call(ctx, "SubscriptionDelete", project, "subscriptions/"+lastSegment(subscription), nil, nil)
	if err != nil && !notFound(err) {
		return err
	}
	return nil
}

// SubscriptionDeadLetter reads the subscription's deadLetterPolicy. A missing
// subscription is "no policy", not an error.
func (p *PubSub) SubscriptionDeadLetter(ctx context.Context, project, subscription string) (string, int, bool, error) {
	if subscription == "" {
		return "", 0, false, nil
	}
	resp, err := p.call(ctx, "SubscriptionGet", project, "subscriptions/"+lastSegment(subscription), nil, nil)
	if err != nil {
		if notFound(err) {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	dp, ok := resp.Data["deadLetterPolicy"].(map[string]any)
	if !ok {
		return "", 0, false, nil
	}
	topic, _ := dp["deadLetterTopic"].(string)
	attempts := 0
	switch v := dp["maxDeliveryAttempts"].(type) {
	case float64:
		attempts = int(v)
	case int:
		attempts = v
	}
	return lastSegment(topic), attempts, true, nil
}
