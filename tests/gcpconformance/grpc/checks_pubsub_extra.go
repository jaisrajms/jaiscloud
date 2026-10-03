package grpcconformance

import (
	"context"
	"fmt"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pubSubExtraChecks covers the Pub/Sub Subscriber + snapshot/seek surface on top
// of the topic-admin probes in checks_pubsub.go. Every probe shares one fixture
// topic and a default pull subscription created by the CreateSubscription
// probe, then each test creates its own dedicated subscription where a shared
// one would be disturbed (detach, streaming pull, ack-deadline experiments,
// seek). Names are run-unique via cfg.ResourceName, so a long-lived emulator
// never sees cross-run collisions.
func pubSubExtraChecks() []Check {
	return []Check{
		// ── subscription CRUD ───────────────────────────────────────────────
		{Service: "pubsub", RPC: "CreateSubscription", Method: "CreateSubscription", KeyField: "subscription.name/topic; exactly-once flag + 60s default", Run: checkPubSubCreateSubscription},
		{Service: "pubsub", RPC: "GetSubscription", Method: "GetSubscription", KeyField: "subscription.name/topic", Run: checkPubSubGetSubscription},
		{Service: "pubsub", RPC: "ListSubscriptions", Method: "ListSubscriptions", KeyField: "subscriptions[].name", Run: checkPubSubListSubscriptions},
		{Service: "pubsub", RPC: "UpdateSubscription", Method: "UpdateSubscription", KeyField: "masked labels applied", Run: checkPubSubUpdateSubscription},

		// ── topic + attached-resource admin ─────────────────────────────────
		{Service: "pubsub", RPC: "UpdateTopic", Method: "UpdateTopic", KeyField: "masked labels + retention applied", Run: checkPubSubUpdateTopic},
		{Service: "pubsub", RPC: "ListTopicSubscriptions", Method: "ListTopicSubscriptions", KeyField: "subscriptions[] contains fixture", Run: checkPubSubListTopicSubscriptions},

		// ── messaging ───────────────────────────────────────────────────────
		{Service: "pubsub", RPC: "Publish", Method: "Publish", KeyField: "messageIds has one non-empty id", Run: checkPubSubPublish},
		{Service: "pubsub", RPC: "Pull", Method: "Pull", KeyField: "receivedMessages[].message.data", Run: checkPubSubPull},
		{Service: "pubsub", RPC: "Acknowledge", Method: "Acknowledge", KeyField: "acked message not redelivered", Run: checkPubSubAcknowledge},
		{Service: "pubsub", RPC: "ModifyAckDeadline", Method: "ModifyAckDeadline", KeyField: "deadline extension keeps message invisible", Run: checkPubSubModifyAckDeadline},
		{Service: "pubsub", RPC: "ModifyPushConfig", Method: "ModifyPushConfig", KeyField: "push_config.push_endpoint round-trips", Run: checkPubSubModifyPushConfig},

		// ── snapshots ───────────────────────────────────────────────────────
		{Service: "pubsub", RPC: "CreateSnapshot", Method: "CreateSnapshot", KeyField: "snapshot.name/topic/expire_time", Run: checkPubSubCreateSnapshot},
		{Service: "pubsub", RPC: "GetSnapshot", Method: "GetSnapshot", KeyField: "snapshot.name/topic", Run: checkPubSubGetSnapshot},
		{Service: "pubsub", RPC: "ListSnapshots", Method: "ListSnapshots", KeyField: "snapshots[] contains fixture", Run: checkPubSubListSnapshots},
		{Service: "pubsub", RPC: "UpdateSnapshot", Method: "UpdateSnapshot", KeyField: "masked labels applied", Run: checkPubSubUpdateSnapshot},
		{Service: "pubsub", RPC: "ListTopicSnapshots", Method: "ListTopicSnapshots", KeyField: "snapshots[] contains fixture", Run: checkPubSubListTopicSnapshots},
		{Service: "pubsub", RPC: "DeleteSnapshot", Method: "DeleteSnapshot", KeyField: "snapshot absent after delete", Run: checkPubSubDeleteSnapshot},
		{Service: "pubsub", RPC: "Seek", Method: "Seek", KeyField: "time + snapshot ack-state reset", Run: checkPubSubSeek},

		// ── detach / streaming / delete ─────────────────────────────────────
		{Service: "pubsub", RPC: "DetachSubscription", Method: "DetachSubscription", KeyField: "detached=true; Pull FailedPrecondition", Run: checkPubSubDetachSubscription},
		{Service: "pubsub", RPC: "StreamingPull", Method: "StreamingPull", KeyField: "published message delivered over stream", Run: checkPubSubStreamingPull},
		{Service: "pubsub", RPC: "DeleteSubscription", Method: "DeleteSubscription", KeyField: "subscription absent after delete", Run: checkPubSubDeleteSubscription},
	}
}

// ─── fixtures ─────────────────────────────────────────────────────────────────

// pubSubExtraTopic is the shared fixture topic for the Subscriber probes. It is
// distinct from pubSubTopicName (the topic-admin fixture) so the two sequences
// never interfere.
func pubSubExtraTopic(cfg Config) string {
	return fmt.Sprintf("projects/%s/topics/%s", cfg.Project, cfg.ResourceName("gcpc-grpc-pubsub"))
}

// pubSubExtraSub is the shared default pull subscription.
func pubSubExtraSub(cfg Config) string {
	return pubSubSubName(cfg, "gcpc-grpc-pubsub-sub")
}

func pubSubSubName(cfg Config, prefix string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", cfg.Project, cfg.ResourceName(prefix))
}

func pubSubSnapshotName(cfg Config, prefix string) string {
	return fmt.Sprintf("projects/%s/snapshots/%s", cfg.Project, cfg.ResourceName(prefix))
}

// pubSubEnsureTopic makes the fixture topic exist (idempotent).
func pubSubEnsureTopic(ctx context.Context, client *pubsub.Client, name string) error {
	_, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetTopic(%s): %w", name, err)
	}
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateTopic(%s): %w", name, err)
	}
	return nil
}

// pubSubEnsureSubscription makes a pull subscription exist (idempotent).
func pubSubEnsureSubscription(ctx context.Context, client *pubsub.Client, name, topic string, ackDeadline int32) error {
	_, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetSubscription(%s): %w", name, err)
	}
	sub := &pubsubpb.Subscription{Name: name, Topic: topic}
	if ackDeadline > 0 {
		sub.AckDeadlineSeconds = ackDeadline
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, sub); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateSubscription(%s): %w", name, err)
	}
	return nil
}

// pubSubPublish publishes one message and returns its id.
func pubSubPublish(ctx context.Context, client *pubsub.Client, topic string, data []byte) (string, error) {
	resp, err := client.TopicAdminClient.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: data}},
	})
	if err != nil {
		return "", fmt.Errorf("Publish: %w", err)
	}
	if len(resp.GetMessageIds()) != 1 || resp.GetMessageIds()[0] == "" {
		return "", fmt.Errorf("Publish returned messageIds %v, want one non-empty id", resp.GetMessageIds())
	}
	return resp.GetMessageIds()[0], nil
}

// pubSubPullFor publishes nothing; it polls a subscription until a message with
// the given body is delivered or the deadline elapses. It does not ack.
func pubSubPullFor(ctx context.Context, client *pubsub.Client, subscription string, body []byte) (*pubsubpb.ReceivedMessage, error) {
	deadline := time.Now().Add(8 * time.Second)
	for {
		resp, err := client.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: subscription, MaxMessages: 100, ReturnImmediately: true,
		})
		if err != nil {
			return nil, fmt.Errorf("Pull(%s): %w", subscription, err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == string(body) {
				return rm, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("Pull(%s) did not deliver body %q within deadline", subscription, body)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ─── subscription CRUD ────────────────────────────────────────────────────────

func checkPubSubCreateSubscription(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	topic := pubSubExtraTopic(cfg)
	if err := pubSubEnsureTopic(ctx, client, topic); err != nil {
		return err
	}
	sub, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: pubSubExtraSub(cfg), Topic: topic, AckDeadlineSeconds: 10,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateSubscription: %w", err)
	}
	if err == nil {
		if sub.GetName() != pubSubExtraSub(cfg) {
			return fmt.Errorf("CreateSubscription name = %q, want %q", sub.GetName(), pubSubExtraSub(cfg))
		}
		if sub.GetTopic() != topic {
			return fmt.Errorf("CreateSubscription topic = %q, want %q", sub.GetTopic(), topic)
		}
	}
	return checkPubSubExactlyOnceCreate(ctx, client, cfg, topic)
}

// checkPubSubExactlyOnceCreate exercises the exactly-once delivery surface of
// CreateSubscription: the flag round-trips, an unspecified ack deadline defaults
// to 60s, and the unsupported push + exactly-once combination fails loud with
// InvalidArgument. It is folded into the CreateSubscription probe so it does not
// add a new conformance cell.
func checkPubSubExactlyOnceCreate(ctx context.Context, client *pubsub.Client, cfg Config, topic string) error {
	sub := pubSubSubName(cfg, "gcpc-grpc-eod-sub")
	created, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                      sub,
		Topic:                     topic,
		EnableExactlyOnceDelivery: true,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateSubscription(exactly-once): %w", err)
	}
	if err == nil {
		if !created.GetEnableExactlyOnceDelivery() {
			return fmt.Errorf("CreateSubscription dropped enable_exactly_once_delivery")
		}
		if created.GetAckDeadlineSeconds() != 60 {
			return fmt.Errorf("exactly-once ack_deadline_seconds = %d, want 60", created.GetAckDeadlineSeconds())
		}
	}
	got, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		return fmt.Errorf("GetSubscription(exactly-once): %w", err)
	}
	if !got.GetEnableExactlyOnceDelivery() {
		return fmt.Errorf("GetSubscription enable_exactly_once_delivery = false, want true")
	}
	if got.GetAckDeadlineSeconds() != 60 {
		return fmt.Errorf("GetSubscription ack_deadline_seconds = %d, want 60", got.GetAckDeadlineSeconds())
	}

	// Exactly-once delivery is pull-only: a push subscription cannot request it.
	pushSub := pubSubSubName(cfg, "gcpc-grpc-eod-push-sub")
	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                      pushSub,
		Topic:                     topic,
		EnableExactlyOnceDelivery: true,
		PushConfig:                &pubsubpb.PushConfig{PushEndpoint: "https://example.invalid/gcpc-push"},
	})
	if status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("exactly-once + push CreateSubscription = %v, want InvalidArgument", err)
	}
	return nil
}

func checkPubSubGetSubscription(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	sub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: pubSubExtraSub(cfg)})
	if err != nil {
		return fmt.Errorf("GetSubscription: %w", err)
	}
	if sub.GetName() != pubSubExtraSub(cfg) {
		return fmt.Errorf("GetSubscription name = %q, want %q", sub.GetName(), pubSubExtraSub(cfg))
	}
	if sub.GetTopic() != pubSubExtraTopic(cfg) {
		return fmt.Errorf("GetSubscription topic = %q, want %q", sub.GetTopic(), pubSubExtraTopic(cfg))
	}
	return nil
}

func checkPubSubListSubscriptions(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	it := client.SubscriptionAdminClient.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{
		Project: "projects/" + cfg.Project,
	})
	for {
		sub, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListSubscriptions: %w", err)
		}
		if sub.GetName() == pubSubExtraSub(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListSubscriptions did not include %q", pubSubExtraSub(cfg))
}

func checkPubSubUpdateSubscription(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	updated, err := client.SubscriptionAdminClient.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{
			Name:   pubSubExtraSub(cfg),
			Labels: map[string]string{"conformance": "updated"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateSubscription: %w", err)
	}
	if got := updated.GetLabels()["conformance"]; got != "updated" {
		return fmt.Errorf("UpdateSubscription labels[conformance] = %q, want updated", got)
	}
	return nil
}

// ─── topic admin ──────────────────────────────────────────────────────────────

func checkPubSubUpdateTopic(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	topic := pubSubExtraTopic(cfg)
	if err := pubSubEnsureTopic(ctx, client, topic); err != nil {
		return err
	}
	const retention = 10 * time.Minute
	updated, err := client.TopicAdminClient.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic: &pubsubpb.Topic{
			Name:                     topic,
			Labels:                   map[string]string{"conformance": "updated"},
			MessageRetentionDuration: durationpb.New(retention),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "message_retention_duration"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateTopic: %w", err)
	}
	if got := updated.GetLabels()["conformance"]; got != "updated" {
		return fmt.Errorf("UpdateTopic labels[conformance] = %q, want updated", got)
	}
	if got := updated.GetMessageRetentionDuration().AsDuration(); got != retention {
		return fmt.Errorf("UpdateTopic message_retention_duration = %v, want %v", got, retention)
	}
	return nil
}

func checkPubSubListTopicSubscriptions(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	it := client.TopicAdminClient.ListTopicSubscriptions(ctx, &pubsubpb.ListTopicSubscriptionsRequest{
		Topic: pubSubExtraTopic(cfg),
	})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListTopicSubscriptions: %w", err)
		}
		if name == pubSubExtraSub(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListTopicSubscriptions did not include %q", pubSubExtraSub(cfg))
}

// ─── messaging ────────────────────────────────────────────────────────────────

func checkPubSubPublish(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	if _, err := pubSubPublish(ctx, client, pubSubExtraTopic(cfg), []byte("gcpc-pubsub-publish")); err != nil {
		return err
	}
	return nil
}

func checkPubSubPull(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	body := []byte("gcpc-pubsub-pull")
	if _, err := pubSubPublish(ctx, client, pubSubExtraTopic(cfg), body); err != nil {
		return err
	}
	rm, err := pubSubPullFor(ctx, client, pubSubExtraSub(cfg), body)
	if err != nil {
		return err
	}
	if rm.GetAckId() == "" {
		return fmt.Errorf("Pull receivedMessage has an empty ack_id")
	}
	// Ack so the fixture subscription is clean for the following probes.
	if err := client.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: pubSubExtraSub(cfg), AckIds: []string{rm.GetAckId()},
	}); err != nil {
		return fmt.Errorf("Acknowledge: %w", err)
	}
	return nil
}

func checkPubSubAcknowledge(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	body := []byte("gcpc-pubsub-ack")
	if _, err := pubSubPublish(ctx, client, pubSubExtraTopic(cfg), body); err != nil {
		return err
	}
	rm, err := pubSubPullFor(ctx, client, pubSubExtraSub(cfg), body)
	if err != nil {
		return err
	}
	messageID := rm.GetMessage().GetMessageId()
	if err := client.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: pubSubExtraSub(cfg), AckIds: []string{rm.GetAckId()},
	}); err != nil {
		return fmt.Errorf("Acknowledge: %w", err)
	}
	resp, err := client.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: pubSubExtraSub(cfg), MaxMessages: 100, ReturnImmediately: true,
	})
	if err != nil {
		return fmt.Errorf("Pull after Acknowledge: %w", err)
	}
	for _, got := range resp.GetReceivedMessages() {
		if got.GetMessage().GetMessageId() == messageID {
			return fmt.Errorf("message %s redelivered after Acknowledge", messageID)
		}
	}
	return nil
}

func checkPubSubModifyAckDeadline(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	// The minimum valid ack deadline (10s) makes an ignored ModifyAckDeadline
	// observable: without the extension the message becomes visible again after
	// ~10s. A shorter value is rejected by real Pub/Sub (proto: 10–600s).
	sub := pubSubSubName(cfg, "gcpc-grpc-mad-sub")
	if err := pubSubEnsureSubscription(ctx, client, sub, pubSubExtraTopic(cfg), 10); err != nil {
		return err
	}
	body := []byte("gcpc-pubsub-modify-ack-deadline")
	if _, err := pubSubPublish(ctx, client, pubSubExtraTopic(cfg), body); err != nil {
		return err
	}
	rm, err := pubSubPullFor(ctx, client, sub, body)
	if err != nil {
		return err
	}
	if err := client.SubscriptionAdminClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{rm.GetAckId()}, AckDeadlineSeconds: 600,
	}); err != nil {
		return fmt.Errorf("ModifyAckDeadline: %w", err)
	}
	// Wait out the original 10s deadline; the extended deadline must keep the
	// message invisible.
	time.Sleep(10500 * time.Millisecond)
	resp, err := client.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: sub, MaxMessages: 10, ReturnImmediately: true,
	})
	if err != nil {
		return fmt.Errorf("Pull after ModifyAckDeadline: %w", err)
	}
	for _, got := range resp.GetReceivedMessages() {
		if got.GetMessage().GetMessageId() == rm.GetMessage().GetMessageId() {
			return fmt.Errorf("message redelivered after ModifyAckDeadline(600s): deadline not extended")
		}
	}
	return nil
}

func checkPubSubModifyPushConfig(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	const endpoint = "https://example.invalid/gcpc-push"
	if err := client.SubscriptionAdminClient.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{
		Subscription: pubSubExtraSub(cfg),
		PushConfig:   &pubsubpb.PushConfig{PushEndpoint: endpoint},
	}); err != nil {
		return fmt.Errorf("ModifyPushConfig(set): %w", err)
	}
	got, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: pubSubExtraSub(cfg)})
	if err != nil {
		return fmt.Errorf("GetSubscription after ModifyPushConfig: %w", err)
	}
	if got.GetPushConfig().GetPushEndpoint() != endpoint {
		return fmt.Errorf("push_config.push_endpoint = %q, want %q", got.GetPushConfig().GetPushEndpoint(), endpoint)
	}
	// Clear the push config so later Publish probes still fan out to this pull
	// subscription instead of attempting HTTP delivery.
	if err := client.SubscriptionAdminClient.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{
		Subscription: pubSubExtraSub(cfg),
		PushConfig:   &pubsubpb.PushConfig{},
	}); err != nil {
		return fmt.Errorf("ModifyPushConfig(clear): %w", err)
	}
	cleared, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: pubSubExtraSub(cfg)})
	if err != nil {
		return fmt.Errorf("GetSubscription after clear: %w", err)
	}
	if ep := cleared.GetPushConfig().GetPushEndpoint(); ep != "" {
		return fmt.Errorf("push_config.push_endpoint = %q after clear, want empty", ep)
	}
	return nil
}

// ─── snapshots ────────────────────────────────────────────────────────────────

func checkPubSubCreateSnapshot(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	if err := pubSubEnsureSubscription(ctx, client, pubSubExtraSub(cfg), pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	snap, err := client.SubscriptionAdminClient.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name: pubSubSnapshotName(cfg, "gcpc-grpc-snap"), Subscription: pubSubExtraSub(cfg),
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateSnapshot: %w", err)
	}
	if err == nil {
		if snap.GetName() != pubSubSnapshotName(cfg, "gcpc-grpc-snap") {
			return fmt.Errorf("CreateSnapshot name = %q, want %q", snap.GetName(), pubSubSnapshotName(cfg, "gcpc-grpc-snap"))
		}
		if snap.GetTopic() != pubSubExtraTopic(cfg) {
			return fmt.Errorf("CreateSnapshot topic = %q, want %q", snap.GetTopic(), pubSubExtraTopic(cfg))
		}
		if snap.GetExpireTime() == nil {
			return fmt.Errorf("CreateSnapshot returned no expire_time")
		}
	}
	return nil
}

func checkPubSubGetSnapshot(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	snap, err := client.SubscriptionAdminClient.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{
		Snapshot: pubSubSnapshotName(cfg, "gcpc-grpc-snap"),
	})
	if err != nil {
		return fmt.Errorf("GetSnapshot: %w", err)
	}
	if snap.GetName() != pubSubSnapshotName(cfg, "gcpc-grpc-snap") {
		return fmt.Errorf("GetSnapshot name = %q, want %q", snap.GetName(), pubSubSnapshotName(cfg, "gcpc-grpc-snap"))
	}
	if snap.GetTopic() != pubSubExtraTopic(cfg) {
		return fmt.Errorf("GetSnapshot topic = %q, want %q", snap.GetTopic(), pubSubExtraTopic(cfg))
	}
	return nil
}

func checkPubSubListSnapshots(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.SubscriptionAdminClient.ListSnapshots(ctx, &pubsubpb.ListSnapshotsRequest{
		Project: "projects/" + cfg.Project,
	})
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListSnapshots: %w", err)
		}
		if snap.GetName() == pubSubSnapshotName(cfg, "gcpc-grpc-snap") {
			return nil
		}
	}
	return fmt.Errorf("ListSnapshots did not include %q", pubSubSnapshotName(cfg, "gcpc-grpc-snap"))
}

func checkPubSubUpdateSnapshot(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	updated, err := client.SubscriptionAdminClient.UpdateSnapshot(ctx, &pubsubpb.UpdateSnapshotRequest{
		Snapshot: &pubsubpb.Snapshot{
			Name:   pubSubSnapshotName(cfg, "gcpc-grpc-snap"),
			Labels: map[string]string{"conformance": "updated"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateSnapshot: %w", err)
	}
	if got := updated.GetLabels()["conformance"]; got != "updated" {
		return fmt.Errorf("UpdateSnapshot labels[conformance] = %q, want updated", got)
	}
	return nil
}

func checkPubSubListTopicSnapshots(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.TopicAdminClient.ListTopicSnapshots(ctx, &pubsubpb.ListTopicSnapshotsRequest{
		Topic: pubSubExtraTopic(cfg),
	})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListTopicSnapshots: %w", err)
		}
		if name == pubSubSnapshotName(cfg, "gcpc-grpc-snap") {
			return nil
		}
	}
	return fmt.Errorf("ListTopicSnapshots did not include %q", pubSubSnapshotName(cfg, "gcpc-grpc-snap"))
}

func checkPubSubDeleteSnapshot(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := pubSubSnapshotName(cfg, "gcpc-grpc-snap")
	if err := client.SubscriptionAdminClient.DeleteSnapshot(ctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: name}); err != nil {
		return fmt.Errorf("DeleteSnapshot: %w", err)
	}
	if _, err := client.SubscriptionAdminClient.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetSnapshot after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── seek ─────────────────────────────────────────────────────────────────────

func checkPubSubSeek(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	topic := pubSubExtraTopic(cfg)
	if err := pubSubEnsureTopic(ctx, client, topic); err != nil {
		return err
	}
	if err := checkPubSubSeekTime(ctx, client, cfg, topic); err != nil {
		return err
	}
	return checkPubSubSeekSnapshot(ctx, client, cfg, topic)
}

// checkPubSubSeekTime exercises time-based Seek: a message published before the
// seek timestamp becomes acknowledged, one published after becomes deliverable
// again.
func checkPubSubSeekTime(ctx context.Context, client *pubsub.Client, cfg Config, topic string) error {
	sub := pubSubSubName(cfg, "gcpc-grpc-seek-time")
	if err := pubSubEnsureSubscription(ctx, client, sub, topic, 10); err != nil {
		return err
	}

	// Seek into the future acknowledges the retained message.
	acked := []byte("gcpc-pubsub-seek-acked")
	if _, err := pubSubPublish(ctx, client, topic, acked); err != nil {
		return err
	}
	if _, err := pubSubPullFor(ctx, client, sub, acked); err != nil {
		return err
	}
	if _, err := client.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now().Add(time.Hour))},
	}); err != nil {
		return fmt.Errorf("Seek(future): %w", err)
	}
	resp, err := client.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 100, ReturnImmediately: true})
	if err != nil {
		return fmt.Errorf("Pull after Seek(future): %w", err)
	}
	for _, rm := range resp.GetReceivedMessages() {
		if string(rm.GetMessage().GetData()) == string(acked) {
			return fmt.Errorf("message published before Seek(future) was redelivered, want acknowledged")
		}
	}

	// Seek into the past makes a retained (unacked) message deliverable again.
	redeliver := []byte("gcpc-pubsub-seek-redeliver")
	if _, err := pubSubPublish(ctx, client, topic, redeliver); err != nil {
		return err
	}
	if _, err := pubSubPullFor(ctx, client, sub, redeliver); err != nil {
		return err
	}
	if _, err := client.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now().Add(-time.Hour))},
	}); err != nil {
		return fmt.Errorf("Seek(past): %w", err)
	}
	if _, err := pubSubPullFor(ctx, client, sub, redeliver); err != nil {
		return fmt.Errorf("message not redelivered after Seek(past): %w", err)
	}
	return nil
}

// checkPubSubSeekSnapshot exercises snapshot-based Seek: the snapshot captures
// the unacked backlog, and seeking to it restores a message acked afterwards.
func checkPubSubSeekSnapshot(ctx context.Context, client *pubsub.Client, cfg Config, topic string) error {
	sub := pubSubSubName(cfg, "gcpc-grpc-seek-snap")
	if err := pubSubEnsureSubscription(ctx, client, sub, topic, 10); err != nil {
		return err
	}
	snap := pubSubSnapshotName(cfg, "gcpc-grpc-seek-snap")

	body := []byte("gcpc-pubsub-seek-snapshot")
	if _, err := pubSubPublish(ctx, client, topic, body); err != nil {
		return err
	}
	rm, err := pubSubPullFor(ctx, client, sub, body)
	if err != nil {
		return err
	}
	if _, err := client.SubscriptionAdminClient.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{Name: snap, Subscription: sub}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateSnapshot: %w", err)
	}
	// Ack the message after the snapshot: the snapshot backlog must still hold
	// it, so Seek restores it.
	if err := client.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{rm.GetAckId()}}); err != nil {
		return fmt.Errorf("Acknowledge: %w", err)
	}
	gone, err := client.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 100, ReturnImmediately: true})
	if err != nil {
		return fmt.Errorf("Pull after Acknowledge: %w", err)
	}
	for _, m := range gone.GetReceivedMessages() {
		if m.GetMessage().GetMessageId() == rm.GetMessage().GetMessageId() {
			return fmt.Errorf("message still delivered after Acknowledge")
		}
	}
	if _, err := client.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Snapshot{Snapshot: snap},
	}); err != nil {
		return fmt.Errorf("Seek(snapshot): %w", err)
	}
	if _, err := pubSubPullFor(ctx, client, sub, body); err != nil {
		return fmt.Errorf("message not restored by Seek(snapshot): %w", err)
	}
	if err := client.SubscriptionAdminClient.DeleteSnapshot(ctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: snap}); err != nil {
		return fmt.Errorf("DeleteSnapshot(cleanup): %w", err)
	}
	return nil
}

// ─── detach / streaming / delete ──────────────────────────────────────────────

func checkPubSubDetachSubscription(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	sub := pubSubSubName(cfg, "gcpc-grpc-detach-sub")
	if err := pubSubEnsureSubscription(ctx, client, sub, pubSubExtraTopic(cfg), 0); err != nil {
		return err
	}
	if _, err := client.TopicAdminClient.DetachSubscription(ctx, &pubsubpb.DetachSubscriptionRequest{Subscription: sub}); err != nil {
		return fmt.Errorf("DetachSubscription: %w", err)
	}
	got, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		return fmt.Errorf("GetSubscription after detach: %w", err)
	}
	if !got.GetDetached() {
		return fmt.Errorf("GetSubscription detached = false, want true")
	}
	if _, err := client.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true}); status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("Pull detached subscription = %v, want FailedPrecondition", err)
	}
	return nil
}

func checkPubSubStreamingPull(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := pubSubEnsureTopic(ctx, client, pubSubExtraTopic(cfg)); err != nil {
		return err
	}
	sub := pubSubSubName(cfg, "gcpc-grpc-stream-sub")
	if err := pubSubEnsureSubscription(ctx, client, sub, pubSubExtraTopic(cfg), 10); err != nil {
		return err
	}
	body := []byte("gcpc-pubsub-streaming-pull")
	if _, err := pubSubPublish(ctx, client, pubSubExtraTopic(cfg), body); err != nil {
		return err
	}

	// Bounded stream: the official generated client's bidirectional
	// StreamingPull, closed by the timeout if the message never arrives.
	streamCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	stream, err := client.SubscriptionAdminClient.StreamingPull(streamCtx)
	if err != nil {
		return fmt.Errorf("StreamingPull: %w", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             sub,
		StreamAckDeadlineSeconds: 10,
	}); err != nil {
		return fmt.Errorf("StreamingPull send: %w", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("StreamingPull recv: %w", err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) != string(body) {
				continue
			}
			// Ack over the same stream, then let the deferred cancel close it.
			if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{rm.GetAckId()}}); err != nil {
				return fmt.Errorf("StreamingPull ack send: %w", err)
			}
			return nil
		}
	}
}

func checkPubSubDeleteSubscription(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: pubSubExtraSub(cfg)}); err != nil {
		return fmt.Errorf("DeleteSubscription: %w", err)
	}
	if _, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: pubSubExtraSub(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetSubscription after delete = %v, want NotFound", err)
	}
	return nil
}
