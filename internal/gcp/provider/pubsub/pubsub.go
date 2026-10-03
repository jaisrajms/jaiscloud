// Package pubsub implements the Google Cloud Pub/Sub provider.
package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/pubsubfilter"
	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtTopic              = "gcp_topic"
	rtSubscription       = "gcp_subscription"
	rtTopicPolicy        = "gcp_topic_policy"
	rtSubscriptionPolicy = "gcp_subscription_policy"

	// longPollTimeout bounds the returnImmediately=false poll window. Real GCP
	// waits ~20s for a message; the emulator uses a 1s window so empty-pull
	// tests (and SDK DLQ tests that end with an empty pull) don't hang.
	longPollTimeout  = time.Second
	longPollInterval = 50 * time.Millisecond
)

// pushClient bounds push-subscription delivery so a hung or slow endpoint
// cannot block a publish request indefinitely.
var pushClient = &http.Client{Timeout: 10 * time.Second}

// Provider handles Pub/Sub topics and subscriptions.
type Provider struct {
	resources  store.ResourceStore  // topics + subscriptions (control-plane)
	messages   pubsubstore.Messages // published messages (data plane)
	encryptor  crypto.EnvelopeEncryptor
	dispatcher eventing.Dispatcher // event-trigger delivery; nil = disabled
}

func New(resources store.ResourceStore, messages pubsubstore.Messages, encryptor crypto.EnvelopeEncryptor) *Provider {
	return &Provider{resources: resources, messages: messages, encryptor: encryptor}
}

// SetFunctionDispatcher wires the Cloud Functions event-delivery engine, so a
// published message reaches every function whose eventTrigger subscribes to the
// topic. The provider holds only the interface, so it never imports the
// functions core.
func (p *Provider) SetFunctionDispatcher(d eventing.Dispatcher) { p.dispatcher = d }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"PubSub.TopicCreate":                    p.TopicCreate,
		"PubSub.TopicGet":                       p.TopicGet,
		"PubSub.TopicDelete":                    p.TopicDelete,
		"PubSub.TopicList":                      p.TopicList,
		"PubSub.TopicPublish":                   p.TopicPublish,
		"PubSub.SubscriptionCreate":             p.SubscriptionCreate,
		"PubSub.SubscriptionGet":                p.SubscriptionGet,
		"PubSub.SubscriptionUpdate":             p.SubscriptionUpdate,
		"PubSub.SubscriptionDelete":             p.SubscriptionDelete,
		"PubSub.SubscriptionDetach":             p.SubscriptionDetach,
		"PubSub.SubscriptionList":               p.SubscriptionList,
		"PubSub.SubscriptionPull":               p.SubscriptionPull,
		"PubSub.SubscriptionAcknowledge":        p.SubscriptionAcknowledge,
		"PubSub.SubscriptionModifyAckDeadline":  p.SubscriptionModifyAckDeadline,
		"PubSub.TopicGetIamPolicy":              p.TopicGetIamPolicy,
		"PubSub.TopicSetIamPolicy":              p.TopicSetIamPolicy,
		"PubSub.TopicTestIamPermissions":        p.TopicTestIamPermissions,
		"PubSub.SubscriptionGetIamPolicy":       p.SubscriptionGetIamPolicy,
		"PubSub.SubscriptionSetIamPolicy":       p.SubscriptionSetIamPolicy,
		"PubSub.SubscriptionTestIamPermissions": p.SubscriptionTestIamPermissions,
	}
}

// resourceName returns the "name" path param as a string, or a 400 error when
// it is absent (defensive — the codec guarantees it for these actions).
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

func (p *Provider) TopicCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	if t == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing topic name", 400)
	}
	meta := map[string]any{"name": nr.ResourceID("pubsub-topic", t)}
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if ret, ok := body["messageRetentionDuration"].(string); ok && ret != "" {
			meta["messageRetentionDuration"] = ret
		}
		if k, ok := body["kmsKeyName"].(string); ok && k != "" {
			meta["kmsKeyName"] = k
		}
		if labels, ok := body["labels"].(map[string]any); ok {
			lbls := make(map[string]string, len(labels))
			for k, v := range labels {
				if s, ok := v.(string); ok {
					lbls[k] = s
				}
			}
			if len(lbls) > 0 {
				meta["labels"] = lbls
			}
		}
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: t, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "topic already exists", 409)
		}
		return nil, err
	}
	return provider.OK(meta), nil
}

func (p *Provider) TopicGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtTopic, t)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	var m map[string]any
	json.Unmarshal(e.Data, &m)
	if m == nil {
		m = map[string]any{"name": nr.ResourceID("pubsub-topic", t)}
	}
	return provider.OK(m), nil
}

func (p *Provider) TopicDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	if err := p.deleteTopic(ctx, nr.AccountID, t); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// deleteTopic removes a topic and detaches its subscriptions: per real Pub/Sub,
// each subscription whose topic was deleted has its topic set to the
// "_deleted-topic_" sentinel. A missing topic reports store.ErrNotFound.
func (p *Provider) deleteTopic(ctx context.Context, account, t string) error {
	if err := p.resources.Delete(ctx, account, store.GlobalRegion, rtTopic, t); err != nil {
		return err
	}
	topicFull := resource.ResourceID(account)("pubsub-topic", t)
	if entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtSubscription, ""); err == nil {
		for _, e := range entries {
			var meta map[string]any
			if json.Unmarshal(e.Data, &meta) != nil {
				continue
			}
			if st, _ := meta["topic"].(string); st != topicFull {
				continue
			}
			meta["topic"] = "_deleted-topic_"
			data, _ := json.Marshal(meta)
			_ = p.resources.Update(ctx, account, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: e.ID, Data: data})
		}
	}
	return nil
}

// SubscriptionDetach implements subscriptions.detach: the subscription stops
// receiving messages (its backlog is dropped) and Pull returns
// FailedPrecondition. The subscription itself is retained.
func (p *Provider) SubscriptionDetach(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)
	meta["detached"] = true
	data, _ := json.Marshal(meta)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: s, Data: data}); err != nil {
		return nil, err
	}
	// Drop the backlog: a detached subscription receives no further messages.
	if msgs, err := p.messages.List(ctx, s); err == nil {
		for _, m := range msgs {
			_ = p.messages.Delete(ctx, s, m.MessageID)
		}
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) TopicList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtTopic, "")
	if err != nil {
		return nil, err
	}
	page, nextToken := paging.Apply(entries, nr.Params)
	items := make([]any, 0, len(page))
	for _, e := range page {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, m)
		}
	}
	resp := map[string]any{"topics": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) TopicPublish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	topicEntry, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtTopic, t)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	// Resolve the topic's CMEK key (empty → server DEK via Wrap).
	kmsKeyName := ""
	var topicMeta map[string]any
	if json.Unmarshal(topicEntry.Data, &topicMeta) == nil {
		kmsKeyName, _ = topicMeta["kmsKeyName"].(string)
	}

	body, _ := nr.Params["body"].(map[string]any)
	msgs, _ := body["messages"].([]any)
	ids := make([]string, 0, len(msgs))
	stored := make([]pubsubstore.Message, 0, len(msgs))
	plainData := make([]string, 0, len(msgs))
	plainBytes := make([][]byte, 0, len(msgs))
	publishTime := clock.Now()
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		id, err := p.messages.NextID(ctx)
		if err != nil {
			return nil, err
		}
		data, _ := mm["data"].(string)

		// Envelope-encrypt the message payload: base64(data) → AES-GCM with a
		// fresh DEK, wrapping the DEK under the topic's CMEK key (or the server
		// DEK when no key is configured).
		rawDEK, wrappedDEK, err := p.encryptor.Wrap(ctx, nr.AccountID, kmsKeyName)
		if err != nil {
			return nil, err
		}
		plain, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			plain = []byte(data)
		}
		ciphertext, err := kmsstore.EncryptData(rawDEK, plain, nil)
		if err != nil {
			return nil, err
		}

		attrs := map[string]string{}
		if a, ok := mm["attributes"].(map[string]any); ok {
			for k, v := range a {
				if s, ok := v.(string); ok {
					attrs[k] = s
				}
			}
		}
		msg := pubsubstore.Message{
			Topic: t, MessageID: id, Data: base64.StdEncoding.EncodeToString(ciphertext),
			Attributes: attrs, PublishTime: publishTime,
			KmsKeyName: kmsKeyName, WrappedDEK: wrappedDEK,
		}
		if ok, _ := mm["orderingKey"].(string); ok != "" {
			msg.OrderingKey = ok
		}
		stored = append(stored, msg)
		plainData = append(plainData, data)
		plainBytes = append(plainBytes, plain)
		ids = append(ids, id)
	}

	// Fan out: Pub/Sub delivers a copy to every pull subscription of the topic,
	// so each subscription has an independent delivery/ack state.
	subIDs := p.pullSubscriptionIDs(ctx, nr.AccountID, t)
	for _, msg := range stored {
		for _, sid := range subIDs {
			copy := msg
			copy.Subscription = sid
			if err := p.messages.Put(ctx, copy); err != nil {
				return nil, err
			}
		}
	}

	// Push subscriptions: deliver each message to the push endpoint with the
	// plaintext payload (the stored Data is ciphertext).
	topicFull := nr.ResourceID("pubsub-topic", t)
	for i, msg := range stored {
		p.deliverPush(ctx, nr.AccountID, topicFull, msg, plainData[i])
	}
	p.dispatchTopicEvents(ctx, nr.AccountID, topicFull, stored, plainBytes)
	return provider.OK(map[string]any{"messageIds": ids}), nil
}

// dispatchTopicEvents hands each published message to the Cloud Functions
// event-delivery engine so a function whose eventTrigger subscribes to the topic
// receives it. payloads holds the decoded (plaintext) message bytes indexed to
// match msgs. No-op when no dispatcher is wired.
func (p *Provider) dispatchTopicEvents(ctx context.Context, project, topicFull string, msgs []pubsubstore.Message, payloads [][]byte) {
	if p.dispatcher == nil {
		return
	}
	for i, m := range msgs {
		var data []byte
		if i < len(payloads) {
			data = payloads[i]
		}
		p.dispatcher.DispatchEvent(ctx, eventing.Event{
			Project:    project,
			EventType:  eventing.TypePubSubPublish,
			Resource:   topicFull,
			EventID:    m.MessageID,
			Source:     eventing.SourcePubSub,
			Data:       data,
			Attributes: m.Attributes,
			OccurredAt: m.PublishTime,
		})
	}
}

// PublishEvent publishes one message (plaintext bytes + string attributes) to a
// topic on behalf of another provider (currently GCS object notifications). It
// runs the same envelope-encryption, per-pull-subscription fan-out and push
// delivery as TopicPublish, but takes already-decoded input rather than a
// request body. topicName may be a bare topic ID, "projects/{p}/topics/{t}", or
// the fully-qualified "//pubsub.googleapis.com/projects/{p}/topics/{t}" form.
// A missing topic is NotFound (real Pub/Sub rejects a publish to it).
func (p *Provider) PublishEvent(ctx context.Context, accountID, topicName string, data []byte, attributes map[string]string) (string, error) {
	return p.publishMessage(ctx, accountID, topicName, data, attributes, true)
}

// publishMessage is the shared publish path. dispatch controls whether the
// produced message is also handed to the Cloud Functions event-delivery engine:
// a dead-letter republish passes false so forwarding cannot re-enter the
// delivery engine (a function subscribed to its own dead-letter topic would
// otherwise recurse). The message still fans out to the topic's pull
// subscriptions and push endpoints.
func (p *Provider) publishMessage(ctx context.Context, accountID, topicName string, data []byte, attributes map[string]string, dispatch bool) (string, error) {
	project, t := topicProjectAndID(topicName)
	if t == "" {
		return "", model.NewProviderError("InvalidArgument", "missing topic", 400)
	}
	if project == "" {
		project = accountID
	}
	topicEntry, err := p.resources.Get(ctx, project, store.GlobalRegion, rtTopic, t)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", model.NewProviderError("NotFound", "topic not found", 404)
		}
		return "", err
	}
	kmsKeyName := ""
	var topicMeta map[string]any
	if json.Unmarshal(topicEntry.Data, &topicMeta) == nil {
		kmsKeyName, _ = topicMeta["kmsKeyName"].(string)
	}

	id, err := p.messages.NextID(ctx)
	if err != nil {
		return "", err
	}
	rawDEK, wrappedDEK, err := p.encryptor.Wrap(ctx, project, kmsKeyName)
	if err != nil {
		return "", err
	}
	ciphertext, err := kmsstore.EncryptData(rawDEK, data, nil)
	if err != nil {
		return "", err
	}
	msg := pubsubstore.Message{
		Topic:       t,
		MessageID:   id,
		Data:        base64.StdEncoding.EncodeToString(ciphertext),
		Attributes:  attributes,
		PublishTime: clock.Now(),
		KmsKeyName:  kmsKeyName,
		WrappedDEK:  wrappedDEK,
	}
	// Fan out: one copy per pull subscription, each with independent delivery.
	for _, sid := range p.pullSubscriptionIDs(ctx, project, t) {
		copy := msg
		copy.Subscription = sid
		if err := p.messages.Put(ctx, copy); err != nil {
			return "", err
		}
	}
	topicFull := resource.ResourceID(project)("pubsub-topic", t)
	p.deliverPush(ctx, project, topicFull, msg, base64.StdEncoding.EncodeToString(data))
	if dispatch && p.dispatcher != nil {
		p.dispatcher.DispatchEvent(ctx, eventing.Event{
			Project:    project,
			EventType:  eventing.TypePubSubPublish,
			Resource:   topicFull,
			EventID:    id,
			Source:     eventing.SourcePubSub,
			Data:       data,
			Attributes: attributes,
			OccurredAt: msg.PublishTime,
		})
	}
	return id, nil
}

// topicShortID extracts the short topic ID from any accepted topic-name form.
func topicShortID(name string) string {
	_, id := topicProjectAndID(name)
	return id
}

// topicProjectAndID splits any accepted topic-name form into the project that
// owns it ("" when the name carries no project) and the short topic ID. Real
// GCS allows a notification topic in any project, so the owning project must be
// resolved from the name rather than assumed to be the bucket's.
func topicProjectAndID(name string) (project, id string) {
	name = strings.TrimPrefix(name, "//pubsub.googleapis.com/")
	if i := strings.LastIndex(name, "/topics/"); i >= 0 {
		return strings.TrimPrefix(name[:i], "projects/"), name[i+len("/topics/"):]
	}
	return "", strings.TrimPrefix(name, "topics/")
}

// TopicExists reports whether a topic (any accepted name form) exists in the
// project. It backs GCS notificationConfigs.insert's topic validation.
func (p *Provider) TopicExists(ctx context.Context, accountID, topicName string) (bool, error) {
	project, t := topicProjectAndID(topicName)
	if t == "" {
		return false, nil
	}
	if project == "" {
		project = accountID
	}
	if _, err := p.resources.Get(ctx, project, store.GlobalRegion, rtTopic, t); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// deliverPush POSTs a message to every push subscription of the topic (SNS
// deliverToHTTP analogue).
func (p *Provider) deliverPush(ctx context.Context, accountID, topicFull string, msg pubsubstore.Message, data string) {
	entries, err := p.resources.List(ctx, accountID, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return
	}
	for _, e := range entries {
		var sub map[string]any
		if json.Unmarshal(e.Data, &sub) != nil {
			continue
		}
		if st, _ := sub["topic"].(string); st != topicFull {
			continue
		}
		pc, _ := sub["pushConfig"].(map[string]any)
		endpoint, _ := pc["pushEndpoint"].(string)
		if endpoint == "" {
			continue
		}
		payload := map[string]any{
			"message": map[string]any{
				"messageId":   msg.MessageID,
				"data":        data,
				"publishTime": msg.PublishTime.UTC().Format("2006-01-02T15:04:05.000000Z"),
			},
			"subscription": sub["name"],
		}
		if len(msg.Attributes) > 0 {
			payload["message"].(map[string]any)["attributes"] = msg.Attributes
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if resp, err := pushClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}
}

func (p *Provider) SubscriptionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	if s == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing subscription name", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	topic, _ := body["topic"].(string)
	if topic == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing topic", 400)
	}
	// The topic must already exist (real Pub/Sub rejects a subscription for a
	// missing topic with NotFound).
	if !p.topicExists(ctx, nr.AccountID, lastSegment(topic)) {
		return nil, model.NewProviderError("NotFound", "topic not found", 404)
	}
	// Exactly-once delivery is a pull-only feature; a push subscription cannot
	// support it, so requesting both fails loud.
	exactlyOnce, _ := body["enableExactlyOnceDelivery"].(bool)
	pushEndpoint := ""
	if pc, ok := body["pushConfig"].(map[string]any); ok {
		pushEndpoint, _ = pc["pushEndpoint"].(string)
	}
	if exactlyOnce && pushEndpoint != "" {
		return nil, model.NewProviderError("InvalidArgument",
			"exactly-once delivery is not supported for push subscriptions", 400)
	}
	ackDeadline := 10
	if ad, ok := body["ackDeadlineSeconds"].(float64); ok {
		v := int(ad)
		// Proto: the value must be between 10 and 600 seconds; 0 selects the
		// default. Anything else (including negatives) is invalid.
		if v != 0 && (v < 10 || v > 600) {
			return nil, model.NewProviderError("InvalidArgument", fmt.Sprintf("ackDeadlineSeconds must be between 10 and 600 (got %d)", v), 400)
		}
		if v != 0 {
			ackDeadline = v
		} else if exactlyOnce {
			// Exactly-once subscriptions default to a 60-second ack deadline.
			ackDeadline = 60
		}
	} else if exactlyOnce {
		ackDeadline = 60
	}
	meta := map[string]any{
		"name":               nr.ResourceID("pubsub-subscription", s),
		"topic":              topic,
		"ackDeadlineSeconds": ackDeadline,
		// A freshly created subscription is immediately usable, i.e. ACTIVE.
		"state": "ACTIVE",
	}
	// Only persist non-default flags: real Pub/Sub omits `false` booleans from
	// the JSON response, and the differential goldens rely on that.
	if exactlyOnce {
		meta["enableExactlyOnceDelivery"] = true
	}
	if om, _ := body["enableMessageOrdering"].(bool); om {
		meta["enableMessageOrdering"] = true
	}
	// Real Pub/Sub defaults: retain undelivered messages for 7 days and expire
	// an inactive subscription after 31 days. Explicit request values win.
	if ret, _ := body["messageRetentionDuration"].(string); ret != "" {
		meta["messageRetentionDuration"] = ret
	} else {
		meta["messageRetentionDuration"] = "604800s"
	}
	if ep, ok := body["expirationPolicy"].(map[string]any); ok {
		meta["expirationPolicy"] = ep
	} else {
		meta["expirationPolicy"] = map[string]any{"ttl": "2678400s"}
	}
	if filter, _ := body["filter"].(string); filter != "" {
		if _, err := pubsubfilter.Compile(filter); err != nil {
			return nil, model.NewProviderError("InvalidArgument", "invalid subscription filter: "+err.Error(), 400)
		}
		meta["filter"] = filter
	}
	if dp, ok := body["deadLetterPolicy"].(map[string]any); ok {
		normalized, err := p.normalizeDeadLetterPolicy(ctx, nr.AccountID, dp)
		if err != nil {
			return nil, err
		}
		meta["deadLetterPolicy"] = normalized
	}
	if raw, ok := body["retryPolicy"]; ok {
		rp, isObject := raw.(map[string]any)
		if !isObject {
			return nil, model.NewProviderError("InvalidArgument", "retryPolicy must be an object", 400)
		}
		normalized, err := normalizeRetryPolicy(rp)
		if err != nil {
			return nil, err
		}
		if len(normalized) > 0 {
			meta["retryPolicy"] = normalized
		}
	}
	if pc, ok := body["pushConfig"].(map[string]any); ok {
		meta["pushConfig"] = pc
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: s, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "subscription already exists", 409)
		}
		return nil, err
	}
	return provider.OK(meta), nil
}

func (p *Provider) SubscriptionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	var m map[string]any
	json.Unmarshal(e.Data, &m)
	return provider.OK(m), nil
}

func (p *Provider) SubscriptionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) SubscriptionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return nil, err
	}
	page, nextToken := paging.Apply(entries, nr.Params)
	items := make([]any, 0, len(page))
	for _, e := range page {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, m)
		}
	}
	resp := map[string]any{"subscriptions": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) SubscriptionPull(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	var sub map[string]any
	json.Unmarshal(e.Data, &sub)
	if detached, _ := sub["detached"].(bool); detached {
		return nil, &model.ProviderError{
			Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
			Message: "subscription " + s + " is detached",
		}
	}
	filterExpr, _ := sub["filter"].(string)
	filter, _ := pubsubfilter.Compile(filterExpr)
	topic, _ := sub["topic"].(string)
	// topic full name → topic ID (last segment).
	topicID := topic
	if i := strings.LastIndex(topicID, "/"); i >= 0 {
		topicID = topicID[i+1:]
	}

	// Dead-letter policy (mirrors SQS RedrivePolicy/maxReceiveCount). Its mere
	// presence controls whether deliveryAttempt is reported (proto: 0 when no
	// DeadLetterPolicy is set).
	dlqTopic := ""
	maxDeliveryAttempts := 0
	hasDeadLetterPolicy := false
	if dp, ok := sub["deadLetterPolicy"].(map[string]any); ok {
		hasDeadLetterPolicy = true
		if dt, _ := dp["deadLetterTopic"].(string); dt != "" {
			dlqTopic = dt
			if i := strings.LastIndex(dlqTopic, "/"); i >= 0 {
				dlqTopic = dlqTopic[i+1:]
			}
		}
		switch mda := dp["maxDeliveryAttempts"].(type) {
		case float64:
			maxDeliveryAttempts = int(mda)
		case int:
			maxDeliveryAttempts = mda
		}
	}

	ackDeadline := 10
	if ad, ok := sub["ackDeadlineSeconds"].(float64); ok {
		ackDeadline = int(ad)
	}
	retention := p.topicRetention(ctx, nr.AccountID, topicID)

	maxMsgs := 100
	if mm, ok := nr.Params["maxMessages"].(string); ok {
		var n int
		if _, err := fmt.Sscanf(mm, "%d", &n); err == nil && n > 0 {
			maxMsgs = n
		}
	}

	// returnImmediately controls long-polling. Most clients omit the field, so
	// its default is false and the emulator should block briefly for a message
	// rather than returning an empty batch right away.
	body, _ := nr.Params["body"].(map[string]any)
	ri, _ := body["returnImmediately"].(bool)

	var msgs []pubsubstore.Message
	if ri {
		msgs, err = p.messages.Pull(ctx, s, maxMsgs, ackDeadline, retention, clock.Now())
	} else {
		msgs, err = p.longPoll(ctx, s, maxMsgs, ackDeadline, retention)
	}
	if err != nil {
		return nil, err
	}
	received := make([]any, 0, len(msgs))
	for _, m := range msgs {
		// Filtering: a message that does not match the subscription's filter is
		// dropped for this subscription (filters are immutable).
		if filter != nil && !filter.Match(m.Attributes) {
			_ = p.messages.Delete(ctx, s, m.MessageID)
			continue
		}
		// DLQ: once delivery attempts exceed maxDeliveryAttempts, republish to the
		// dead-letter topic and drop the original (mirrors SQS checkDLQ, strictly
		// greater threshold).
		if dlqTopic != "" && maxDeliveryAttempts > 0 && m.DeliveryAttempt > maxDeliveryAttempts {
			_ = p.messages.Delete(ctx, s, m.MessageID)
			for _, sid := range p.pullSubscriptionIDs(ctx, nr.AccountID, dlqTopic) {
				_ = p.messages.Put(ctx, pubsubstore.Message{
					Topic: dlqTopic, Subscription: sid, MessageID: m.MessageID, Data: m.Data, Attributes: m.Attributes,
					PublishTime: m.PublishTime, DeliveryAttempt: 0,
					KmsKeyName: m.KmsKeyName, WrappedDEK: m.WrappedDEK,
				})
			}
			continue
		}

		// Decrypt the stored ciphertext back to the plaintext base64 payload.
		rawDEK, err := p.encryptor.Unwrap(ctx, nr.AccountID, m.KmsKeyName, m.WrappedDEK)
		if err != nil {
			return nil, err
		}
		ciphertext, err := base64.StdEncoding.DecodeString(m.Data)
		if err != nil {
			return nil, err
		}
		plain, err := kmsstore.DecryptData(rawDEK, ciphertext, nil)
		if err != nil {
			return nil, err
		}
		data := base64.StdEncoding.EncodeToString(plain)

		msg := map[string]any{
			"messageId":   m.MessageID,
			"data":        data,
			"publishTime": m.PublishTime.UTC().Format("2006-01-02T15:04:05.000000Z"),
		}
		if len(m.Attributes) > 0 {
			msg["attributes"] = m.Attributes
		}
		if m.OrderingKey != "" {
			msg["orderingKey"] = m.OrderingKey
		}
		received = append(received, map[string]any{
			"ackId":           encodeAckID(s, m.MessageID),
			"message":         msg,
			"deliveryAttempt": deliveryAttempt(m.DeliveryAttempt, hasDeadLetterPolicy),
		})
	}
	return provider.OK(map[string]any{"receivedMessages": received}), nil
}

// longPoll repeatedly claims messages until at least one is deliverable or the
// bounded window elapses. The deadline is derived from clock.Now() (so a frozen
// test clock yields a consistent remaining duration) while time.Sleep drives the
// real poll cadence — the emulator's analogue of SQS WaitForMessages.
func (p *Provider) longPoll(ctx context.Context, queue string, maxMsgs, ackDeadline, retention int) ([]pubsubstore.Message, error) {
	deadline := clock.Now().Add(longPollTimeout)
	for {
		msgs, err := p.messages.Pull(ctx, queue, maxMsgs, ackDeadline, retention, clock.Now())
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 || !clock.Now().Before(deadline) {
			return msgs, nil
		}
		time.Sleep(longPollInterval)
	}
}

func (p *Provider) SubscriptionAcknowledge(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	ackIDs, _ := body["ackIds"].([]any)
	for _, a := range ackIDs {
		if id, ok := a.(string); ok {
			decoded, ok := decodeAckID(id)
			if !ok {
				continue
			}
			// decoded ackId = topicID + "/" + messageID
			parts := strings.SplitN(decoded, "/", 2)
			if len(parts) == 2 {
				_ = p.messages.Delete(ctx, parts[0], parts[1])
			}
		}
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) SubscriptionModifyAckDeadline(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	if _, err = p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	ackIDs := toStrings(body["ackIds"])
	decoded := make([]string, 0, len(ackIDs))
	for _, id := range ackIDs {
		if d, ok := decodeAckID(id); ok {
			decoded = append(decoded, d)
		}
	}
	seconds := 0
	if ad, ok := body["ackDeadlineSeconds"].(float64); ok {
		seconds = int(ad)
		// Proto ModifyAckDeadline: valid values are 0 to 600 seconds.
		if seconds < 0 || seconds > 600 {
			return nil, model.NewProviderError("InvalidArgument", fmt.Sprintf("ackDeadlineSeconds must be between 0 and 600 (got %d)", seconds), 400)
		}
	}
	if err := p.messages.ModifyAckDeadline(ctx, s, decoded, seconds, clock.Now()); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// encodeAckID renders the opaque wire ackId: base64url of "topicID/messageID".
func encodeAckID(topicID, messageID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topicID + "/" + messageID))
}

// decodeAckID reverses encodeAckID, returning "topicID/messageID".
func decodeAckID(s string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// pullSubscriptionIDs returns the IDs of a topic's pull subscriptions. Push
// subscriptions are delivered over HTTP at publish time and are not queued.
func (p *Provider) pullSubscriptionIDs(ctx context.Context, accountID, topicID string) []string {
	entries, err := p.resources.List(ctx, accountID, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range entries {
		var sub map[string]any
		if json.Unmarshal(e.Data, &sub) != nil {
			continue
		}
		st, _ := sub["topic"].(string)
		if lastSegment(st) != topicID {
			continue
		}
		if pc, _ := sub["pushConfig"].(map[string]any); pc != nil {
			if ep, _ := pc["pushEndpoint"].(string); ep != "" {
				continue
			}
		}
		ids = append(ids, e.ID)
	}
	return ids
}

func lastSegment(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

// deliveryAttempt reports the wire delivery_attempt. Per the proto, it is 0
// unless the subscription has a DeadLetterPolicy; with one it is the number of
// delivery attempts made so far (>= 1).
func deliveryAttempt(attempt int, hasDeadLetterPolicy bool) int {
	if !hasDeadLetterPolicy {
		return 0
	}
	return attempt
}

// normalizeDeadLetterPolicy validates and canonicalizes a subscription's
// deadLetterPolicy: a configured dead_letter_topic must already exist in the
// same project (NotFound otherwise), and max_delivery_attempts must be between
// 5 and 100 (0 selects the 5 default).
func (p *Provider) normalizeDeadLetterPolicy(ctx context.Context, account string, dp map[string]any) (map[string]any, error) {
	out := map[string]any{}
	if topic, _ := dp["deadLetterTopic"].(string); topic != "" {
		if !p.topicExists(ctx, account, lastSegment(topic)) {
			return nil, model.NewProviderError("NotFound", "dead letter topic not found", 404)
		}
		out["deadLetterTopic"] = topic
	}
	attempts := 0
	switch mda := dp["maxDeliveryAttempts"].(type) {
	case float64:
		attempts = int(mda)
	case int:
		attempts = mda
	}
	if attempts != 0 && (attempts < 5 || attempts > 100) {
		return nil, model.NewProviderError("InvalidArgument", fmt.Sprintf("maxDeliveryAttempts must be between 5 and 100 (got %d)", attempts), 400)
	}
	if attempts == 0 {
		attempts = 5
	}
	out["maxDeliveryAttempts"] = attempts
	return out, nil
}

// topicRetention resolves a topic's messageRetentionDuration in seconds,
// defaulting to GCP's 7-day default.
func (p *Provider) topicRetention(ctx context.Context, account, topicID string) int {
	const defaultRetentionSecs = 604800
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtTopic, topicID)
	if err != nil {
		return defaultRetentionSecs
	}
	var meta map[string]any
	if json.Unmarshal(e.Data, &meta) != nil {
		return defaultRetentionSecs
	}
	ret, _ := meta["messageRetentionDuration"].(string)
	if d, err := time.ParseDuration(ret); err == nil && d > 0 {
		return int(d.Seconds())
	}
	return defaultRetentionSecs
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (p *Provider) requireTopic(ctx context.Context, account, t string) error {
	if _, err := p.resources.Get(ctx, account, store.GlobalRegion, rtTopic, t); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.NewProviderError("NotFound", "topic not found", 404)
		}
		return err
	}
	return nil
}

func (p *Provider) requireSubscription(ctx context.Context, account, s string) error {
	if _, err := p.resources.Get(ctx, account, store.GlobalRegion, rtSubscription, s); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return err
	}
	return nil
}

func (p *Provider) TopicGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	if err := p.requireTopic(ctx, nr.AccountID, t); err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtTopicPolicy, t))), nil
}

func (p *Provider) TopicSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	if err := p.requireTopic(ctx, nr.AccountID, t); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtTopicPolicy, t, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TopicTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	body, _ := nr.Params["body"].(map[string]any)
	// Fail open: testIamPermissions does not require the topic to exist. A
	// missing topic yields an empty permission list rather than NotFound.
	perms := []string{}
	if p.topicExists(ctx, nr.AccountID, t) {
		perms = policy.TestPermissions(policy.Permissions(body))
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// topicExists reports whether a topic exists without erroring (used by
// testIamPermissions' fail-open behavior).
func (p *Provider) topicExists(ctx context.Context, account, t string) bool {
	_, err := p.resources.Get(ctx, account, store.GlobalRegion, rtTopic, t)
	return err == nil
}

func (p *Provider) SubscriptionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	if err := p.requireSubscription(ctx, nr.AccountID, s); err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtSubscriptionPolicy, s))), nil
}

func (p *Provider) SubscriptionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	if err := p.requireSubscription(ctx, nr.AccountID, s); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtSubscriptionPolicy, s, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) SubscriptionTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	body, _ := nr.Params["body"].(map[string]any)
	// Fail open: testIamPermissions does not require the subscription to exist.
	perms := []string{}
	if p.subscriptionExists(ctx, nr.AccountID, s) {
		perms = policy.TestPermissions(policy.Permissions(body))
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// subscriptionExists reports whether a subscription exists without erroring
// (used by testIamPermissions' fail-open behavior).
func (p *Provider) subscriptionExists(ctx context.Context, account, s string) bool {
	_, err := p.resources.Get(ctx, account, store.GlobalRegion, rtSubscription, s)
	return err == nil
}
