package gcplifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"jaiscloud/internal/gcp/crypto"
	grpcpubsub "jaiscloud/internal/gcp/grpc/pubsub"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const (
	pubsubTypeTopic        = "gcp_topic"
	pubsubTypeSubscription = "gcp_subscription"
	pubsubTypeTopicPolicy  = "gcp_topic_policy"
	pubsubTypeSubPolicy    = "gcp_subscription_policy"
	pubsubDeletedTopic     = "_deleted-topic_"
)

// pubsubTopicManifest is the FC5 ownership contract for a Pub/Sub topic: real
// Pub/Sub keeps a topic's subscriptions and rewrites their topic to
// "_deleted-topic_" (detach). The topic's IAM policy goes with the topic.
var pubsubTopicManifest = manifest{
	parent: "pubsub topic",
	policy: Detach,
	children: []childSpec{
		{"subscriptions", Detach},
		{"topicIAM", Cascade},
	},
	note: "subscriptions survive their topic detached; the message backlog is keyed by subscription, not topic",
}

// pubsubSubscriptionManifest is the ownership contract for a subscription:
// its message backlog and its IAM policy are owned by the subscription, so a
// delete purges both and a same-name recreate starts empty.
var pubsubSubscriptionManifest = manifest{
	parent: "pubsub subscription",
	policy: Cascade,
	children: []childSpec{
		{"messages", Cascade},
		{"subscriptionIAM", Cascade},
	},
	note: "Pub/Sub has no delete precondition: a second delete answers NotFound instead of a blocked delete",
}

type pubsubAdapter struct {
	seq       int
	parent    string
	prov      *pubsubprovider.Provider
	resources store.ResourceStore
	messages  pubsubstore.Messages
	pub       pubsubpb.PublisherClient
	sub       pubsubpb.SubscriberClient
	project   string
}

func newPubsubTopicAdapter(t *testing.T) adapter {
	a := newPubsubAdapter(t, "topic")
	return a
}

func newPubsubSubscriptionAdapter(t *testing.T) adapter {
	a := newPubsubAdapter(t, "subscription")
	return a
}

func newPubsubAdapter(t *testing.T, parent string) *pubsubAdapter {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	enc := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	prov := pubsubprovider.New(resources, messages, enc)
	conn := serveGRPC(t, func(srv *grpc.Server) {
		svc := grpcpubsub.NewService(resources, messages, enc, "proj")
		pubsubpb.RegisterPublisherServer(srv, svc)
		pubsubpb.RegisterSubscriberServer(srv, svc)
	})
	return &pubsubAdapter{
		parent:    parent,
		prov:      prov,
		resources: resources,
		messages:  messages,
		pub:       pubsubpb.NewPublisherClient(conn),
		sub:       pubsubpb.NewSubscriberClient(conn),
		project:   "proj",
	}
}

func (a *pubsubAdapter) name() string { return "pubsub " + a.parent }

func (a *pubsubAdapter) topicName(id string) string {
	return "projects/" + a.project + "/topics/" + id
}

func (a *pubsubAdapter) subName(id string) string {
	return "projects/" + a.project + "/subscriptions/" + id
}

func (a *pubsubAdapter) create(ctx context.Context, tr transport, id string) (string, error) {
	if a.parent == "subscription" {
		return a.createSubscription(ctx, tr, id)
	}
	if tr == grpcT {
		topic, err := a.pub.CreateTopic(ctx, &pubsubpb.Topic{Name: a.topicName(id)})
		if err != nil {
			return "", err
		}
		return topic.GetName(), nil
	}
	if _, err := a.prov.TopicCreate(ctx, a.nr("topics/"+id, nil)); err != nil {
		return "", err
	}
	return a.topicName(id), nil
}

func (a *pubsubAdapter) createSubscription(ctx context.Context, tr transport, id string) (string, error) {
	// A subscription needs a live topic; create the shared parent idempotently.
	if _, err := a.prov.TopicCreate(ctx, a.nr("topics/lc-parent", nil)); err != nil {
		if code, status := codeOf(err); !codeMatches(code, status, "AlreadyExists") {
			return "", err
		}
	}
	topic := a.topicName("lc-parent")
	if tr == grpcT {
		sub, err := a.sub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: a.subName(id), Topic: topic})
		if err != nil {
			return "", err
		}
		return sub.GetName(), nil
	}
	if _, err := a.prov.SubscriptionCreate(ctx, a.nr("subscriptions/"+id, map[string]any{"topic": topic})); err != nil {
		return "", err
	}
	return a.subName(id), nil
}

func (a *pubsubAdapter) get(ctx context.Context, tr transport, id string) (bool, error) {
	if a.parent == "subscription" {
		if tr == grpcT {
			_, err := a.sub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: a.subName(id)})
			return err == nil, ignoreNotFound(err)
		}
		_, err := a.prov.SubscriptionGet(ctx, a.nr("subscriptions/"+id, nil))
		return err == nil, ignoreNotFound(err)
	}
	if tr == grpcT {
		_, err := a.pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: a.topicName(id)})
		return err == nil, ignoreNotFound(err)
	}
	_, err := a.prov.TopicGet(ctx, a.nr("topics/"+id, nil))
	return err == nil, ignoreNotFound(err)
}

func (a *pubsubAdapter) list(ctx context.Context, tr transport) ([]string, error) {
	if a.parent == "subscription" {
		if tr == grpcT {
			resp, err := a.sub.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: "projects/" + a.project})
			if err != nil {
				return nil, err
			}
			out := make([]string, 0, len(resp.GetSubscriptions()))
			for _, s := range resp.GetSubscriptions() {
				out = append(out, shortName(s.GetName()))
			}
			return out, nil
		}
		resp, err := a.prov.SubscriptionList(ctx, a.nr("", nil))
		if err != nil {
			return nil, err
		}
		items, _ := resp.Data["subscriptions"].([]any)
		out := make([]string, 0, len(items))
		for _, it := range items {
			m, _ := it.(map[string]any)
			if name, _ := m["name"].(string); name != "" {
				out = append(out, shortName(name))
			}
		}
		return out, nil
	}
	if tr == grpcT {
		resp, err := a.pub.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + a.project})
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetTopics()))
		for _, t := range resp.GetTopics() {
			out = append(out, shortName(t.GetName()))
		}
		return out, nil
	}
	resp, err := a.prov.TopicList(ctx, a.nr("", nil))
	if err != nil {
		return nil, err
	}
	items, _ := resp.Data["topics"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		if name, _ := m["name"].(string); name != "" {
			out = append(out, shortName(name))
		}
	}
	return out, nil
}

func (a *pubsubAdapter) update(ctx context.Context, tr transport, id string) error {
	if a.parent == "subscription" {
		if tr == grpcT {
			_, err := a.sub.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
				Subscription: &pubsubpb.Subscription{Name: a.subName(id), Labels: map[string]string{"lifecycle": "update"}},
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
			})
			return err
		}
		_, err := a.prov.SubscriptionUpdate(ctx, a.nr("subscriptions/"+id, map[string]any{
			"subscription": map[string]any{"labels": map[string]any{"lifecycle": "update"}},
			"updateMask":   "labels",
		}))
		return err
	}
	if tr == grpcT {
		_, err := a.pub.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
			Topic:      &pubsubpb.Topic{Name: a.topicName(id), Labels: map[string]string{"lifecycle": "update"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		})
		return err
	}
	_, err := a.prov.TopicUpdate(ctx, a.nr("topics/"+id, map[string]any{
		"labels":     map[string]any{"lifecycle": "update"},
		"updateMask": "labels",
	}))
	return err
}

func (a *pubsubAdapter) remove(ctx context.Context, tr transport, id string) error {
	if a.parent == "subscription" {
		if tr == grpcT {
			_, err := a.sub.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: a.subName(id)})
			return err
		}
		_, err := a.prov.SubscriptionDelete(ctx, a.nr("subscriptions/"+id, nil))
		return err
	}
	if tr == grpcT {
		_, err := a.pub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: a.topicName(id)})
		return err
	}
	_, err := a.prov.TopicDelete(ctx, a.nr("topics/"+id, nil))
	return err
}

// blockedDelete: the Pub/Sub API defines no delete precondition, so the only
// negative is a second delete answering NotFound. The adapter reports it under
// the "no-precondition" name and the harness asserts it is NotFound.
func (a *pubsubAdapter) blockedDelete(ctx context.Context, tr transport, id string) []blockedResult {
	return nil
}

func (a *pubsubAdapter) populate(ctx context.Context, id string) error {
	a.seq++
	n := fmt.Sprintf("%d", a.seq)
	if a.parent == "subscription" {
		if err := a.messages.Put(ctx, pubsubstore.Message{
			MessageID:    n,
			Subscription: id,
			Topic:        a.topicName("lc-parent"),
			Data:         "aGk=",
			PublishTime:  time.Now(),
		}); err != nil {
			return err
		}
		return a.resources.Upsert(ctx, a.project, store.GlobalRegion, store.ResourceEntry{
			Type: pubsubTypeSubPolicy, ID: id, Data: []byte(`{"bindings":[]}`),
		})
	}
	// A subscription plus a topic IAM policy.
	if _, err := a.prov.SubscriptionCreate(ctx, a.nr("subscriptions/lc-sub-"+n, map[string]any{"topic": a.topicName(id)})); err != nil {
		return err
	}
	return a.resources.Upsert(ctx, a.project, store.GlobalRegion, store.ResourceEntry{
		Type: pubsubTypeTopicPolicy, ID: id, Data: []byte(`{"bindings":[]}`),
	})
}

func (a *pubsubAdapter) counts(ctx context.Context, id string) (map[string]childCount, error) {
	out := map[string]childCount{}
	if a.parent == "subscription" {
		msgs, err := a.messages.List(ctx, id)
		if err != nil {
			return nil, err
		}
		out["messages"] = childCount{total: len(msgs), attached: len(msgs)}
		_, err = a.resources.Get(ctx, a.project, store.GlobalRegion, pubsubTypeSubPolicy, id)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		n := 0
		if err == nil {
			n = 1
		}
		out["subscriptionIAM"] = childCount{total: n, attached: n}
		return out, nil
	}

	subs, err := a.resources.List(ctx, a.project, store.GlobalRegion, pubsubTypeSubscription, "")
	if err != nil {
		return nil, err
	}
	full := a.topicName(id)
	total, attached := 0, 0
	for _, row := range subs {
		var meta map[string]any
		if json.Unmarshal(row.Data, &meta) != nil {
			meta = map[string]any{}
		}
		topic, _ := meta["topic"].(string)
		if topic != full && topic != pubsubDeletedTopic {
			continue
		}
		total++
		if topic == full {
			attached++
		}
	}
	out["subscriptions"] = childCount{total: total, attached: attached}

	_, err = a.resources.Get(ctx, a.project, store.GlobalRegion, pubsubTypeTopicPolicy, id)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	n := 0
	if err == nil {
		n = 1
	}
	out["topicIAM"] = childCount{total: n, attached: n}
	return out, nil
}

func (a *pubsubAdapter) clearRejecting(context.Context, string) error { return nil }

// nr builds a Pub/Sub NormalizedRequest for the REST provider.
func (a *pubsubAdapter) nr(name string, body map[string]any) *model.NormalizedRequest {
	params := map[string]any{}
	if name != "" {
		params["name"] = name
	}
	if body != nil {
		params["body"] = body
	}
	return &model.NormalizedRequest{AccountID: a.project, Params: params, ResourceID: resource.ResourceID(a.project)}
}

func ignoreNotFound(err error) error {
	if isNotFound(err) {
		return nil
	}
	return err
}
