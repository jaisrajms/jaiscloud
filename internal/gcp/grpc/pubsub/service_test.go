package pubsub

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	restpubsub "jaiscloud/internal/gcp/provider/pubsub"
	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pubsubTestService dials a real in-process gRPC server backed by the memory
// stores and returns the three clients (Publisher, Subscriber, IAMPolicy) plus
// the raw message store (for asserting ack/delete effects).
func pubsubTestService(t *testing.T) (pubsubpb.PublisherClient, pubsubpb.SubscriberClient, iampb.IAMPolicyClient, pubsubstore.Messages, func()) {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	encryptor := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	svc := NewService(resources, messages, encryptor, "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pubsubpb.RegisterPublisherServer(srv, svc)
	pubsubpb.RegisterSubscriberServer(srv, svc)
	iampb.RegisterIAMPolicyServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return pubsubpb.NewPublisherClient(conn), pubsubpb.NewSubscriberClient(conn), iampb.NewIAMPolicyClient(conn), messages, cleanup
}

// TestCreateSubscriptionAppliesRealGCPDefaults pins the REST↔gRPC parity fix for
// AUD3-12: the gRPC CreateSubscription must persist and return the same
// real-GCP defaults the REST provider applies (state ACTIVE, 7-day retention,
// 31-day expiration policy), and an explicit request value must win.
func TestCreateSubscriptionAppliesRealGCPDefaults(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/defaults-topic"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	created, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: "projects/test/subscriptions/defaults-sub", Topic: topic,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if created.GetState() != pubsubpb.Subscription_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", created.GetState())
	}
	if got := created.GetMessageRetentionDuration().AsDuration(); got != 7*24*time.Hour {
		t.Errorf("messageRetentionDuration = %v, want 168h", got)
	}
	if got := created.GetExpirationPolicy().GetTtl().AsDuration(); got != 31*24*time.Hour {
		t.Errorf("expirationPolicy.ttl = %v, want 744h", got)
	}

	// A read renders the same defaults (they are persisted, not synthesized on
	// create only).
	got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: created.GetName()})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.GetState() != pubsubpb.Subscription_ACTIVE || got.GetExpirationPolicy().GetTtl().AsDuration() != 31*24*time.Hour {
		t.Errorf("GetSubscription defaults = state %v, ttl %v", got.GetState(), got.GetExpirationPolicy().GetTtl())
	}

	// Explicit request values win.
	explicit, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                     "projects/test/subscriptions/explicit-sub",
		Topic:                    topic,
		MessageRetentionDuration: durationpb.New(2 * time.Hour),
		ExpirationPolicy:         &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(3 * time.Hour)},
	})
	if err != nil {
		t.Fatalf("CreateSubscription(explicit): %v", err)
	}
	if got := explicit.GetMessageRetentionDuration().AsDuration(); got != 2*time.Hour {
		t.Errorf("explicit messageRetentionDuration = %v, want 2h", got)
	}
	if got := explicit.GetExpirationPolicy().GetTtl().AsDuration(); got != 3*time.Hour {
		t.Errorf("explicit expirationPolicy.ttl = %v, want 3h", got)
	}
}

func TestPubSubEndToEnd(t *testing.T) {
	pub, subc, iam, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/my-topic"
	const subscription = "projects/test/subscriptions/my-sub"

	// Create topic.
	created, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if created.GetName() != topic {
		t.Fatalf("CreateTopic name = %q, want %q", created.GetName(), topic)
	}

	// Duplicate topic → AlreadyExists.
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateTopic err = %v, want AlreadyExists", err)
	}

	// Get topic.
	got, err := pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topic})
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.GetName() != topic {
		t.Fatalf("GetTopic name = %q, want %q", got.GetName(), topic)
	}

	// List topics.
	listed, err := pub.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/test"})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(listed.GetTopics()) != 1 || listed.GetTopics()[0].GetName() != topic {
		t.Fatalf("ListTopics = %v, want exactly [%s]", listed.GetTopics(), topic)
	}

	// Create the subscription before publishing (a subscription only receives
	// messages published after it exists).
	sub, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: subscription, Topic: topic, AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if sub.GetTopic() != topic || sub.GetAckDeadlineSeconds() != 30 {
		t.Fatalf("CreateSubscription = %+v, want topic=%s ackDeadline=30", sub, topic)
	}

	// Publish.
	pubRes, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{Data: []byte("hello world"), Attributes: map[string]string{"k": "v"}},
		},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(pubRes.GetMessageIds()) != 1 {
		t.Fatalf("Publish messageIds = %v, want 1 id", pubRes.GetMessageIds())
	}

	// Pull (returnImmediately) → the published message.
	pull, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 1 {
		t.Fatalf("Pull received %d messages, want 1", len(pull.GetReceivedMessages()))
	}
	rm := pull.GetReceivedMessages()[0]
	if rm.GetAckId() == "" {
		t.Fatal("Pull ackId is empty")
	}
	if string(rm.GetMessage().GetData()) != "hello world" {
		t.Fatalf("Pull data = %q, want %q", string(rm.GetMessage().GetData()), "hello world")
	}
	if rm.GetMessage().GetAttributes()["k"] != "v" {
		t.Fatalf("Pull attributes = %v, want k=v", rm.GetMessage().GetAttributes())
	}

	// Ack.
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: subscription, AckIds: []string{rm.GetAckId()}}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}

	// After ack, the message is gone.
	pull2, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull after ack: %v", err)
	}
	if len(pull2.GetReceivedMessages()) != 0 {
		t.Fatalf("Pull after ack received %d messages, want 0", len(pull2.GetReceivedMessages()))
	}

	// Topic IAM: empty policy by default.
	pol, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 0 {
		t.Fatalf("GetIamPolicy bindings = %v, want empty", pol.GetBindings())
	}

	// Set topic IAM policy.
	_, err = iam.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: topic,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: "roles/pubsub.publisher", Members: []string{"user:a@example.com"}}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}

	// Read it back.
	pol, err = iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	if err != nil {
		t.Fatalf("GetIamPolicy after set: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/pubsub.publisher" {
		t.Fatalf("GetIamPolicy bindings = %v, want roles/pubsub.publisher", pol.GetBindings())
	}

	// Test permissions echoes the request (no-authz posture).
	tp, err := iam.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: topic, Permissions: []string{"pubsub.topics.publish", "pubsub.topics.get"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(tp.GetPermissions()) != 2 {
		t.Fatalf("TestIamPermissions = %v, want 2 granted", tp.GetPermissions())
	}
}

func TestPullOrderingKeyGating(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/ordered"
	const subscription = "projects/test/subscriptions/ordered-sub"

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: subscription, Topic: topic}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	// Two messages in the same ordering-key group, published in separate
	// requests so the first has a strictly earlier publish time.
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("first"), OrderingKey: "grp"}},
	}); err != nil {
		t.Fatalf("Publish first: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("second"), OrderingKey: "grp"}},
	}); err != nil {
		t.Fatalf("Publish second: %v", err)
	}

	// A single pull returns the whole key batch, in publish order.
	pull, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 2 {
		t.Fatalf("Pull received %d messages, want 2 (one keyed batch)", len(pull.GetReceivedMessages()))
	}
	if got := string(pull.GetReceivedMessages()[0].GetMessage().GetData()); got != "first" {
		t.Fatalf("Pull data[0] = %q, want first", got)
	}
	if got := string(pull.GetReceivedMessages()[1].GetMessage().GetData()); got != "second" {
		t.Fatalf("Pull data[1] = %q, want second", got)
	}

	// The key is gated while that batch is outstanding.
	pull, err = subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull 2: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 0 {
		t.Fatalf("Pull 2 received %d messages, want 0 (key gated while in-flight)", len(pull.GetReceivedMessages()))
	}
}

func TestStreamingPullDeliversAndAcks(t *testing.T) {
	pub, subc, _, messages, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/stream-topic"
	const subscription = "projects/test/subscriptions/stream-sub"

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: subscription, Topic: topic}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{Data: []byte("msg-a")},
			{Data: []byte("msg-b")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subc.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription,
		StreamAckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatalf("StreamingPull Send: %v", err)
	}

	// Collect both published messages (bounded deadline so a failure doesn't hang).
	got := map[string]bool{}
	var ackIDs []string
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for messages; received %v", got)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("StreamingPull Recv: %v", err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			got[string(rm.GetMessage().GetData())] = true
			ackIDs = append(ackIDs, rm.GetAckId())
		}
	}
	if !got["msg-a"] || !got["msg-b"] {
		t.Fatalf("received = %v, want msg-a and msg-b", got)
	}

	// Ack both messages over the same stream.
	if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: ackIDs}); err != nil {
		t.Fatalf("StreamingPull ack Send: %v", err)
	}

	// The server processes acks asynchronously; poll the store until the topic
	// is drained or the deadline elapses.
	deadline = time.Now().Add(5 * time.Second)
	for {
		remaining, err := messages.List(ctx, "stream-topic")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(remaining) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("messages not acked within deadline; %d remain", len(remaining))
		}
		time.Sleep(20 * time.Millisecond)
	}

	stream.CloseSend()
}

func TestStreamingPullHonorsMaxOutstandingMessages(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "projects/test/topics/flow-topic"
	const subscription = "projects/test/subscriptions/flow-sub"

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: subscription, Topic: topic}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{Data: []byte("flow-1")},
			{Data: []byte("flow-2")},
			{Data: []byte("flow-3")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subc.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription,
		StreamAckDeadlineSeconds: 10,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("StreamingPull Send: %v", err)
	}

	type recvResult struct {
		msgs []*pubsubpb.ReceivedMessage
		err  error
	}
	recvCh := make(chan recvResult, 4)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvCh <- recvResult{err: err}
				return
			}
			recvCh <- recvResult{msgs: resp.GetReceivedMessages()}
		}
	}()

	// The first response must carry exactly one message: the batch is clamped
	// to max_outstanding_messages=1.
	var first *pubsubpb.ReceivedMessage
	select {
	case r := <-recvCh:
		if r.err != nil {
			t.Fatalf("StreamingPull Recv: %v", r.err)
		}
		if len(r.msgs) != 1 {
			t.Fatalf("first response delivered %d messages, want 1", len(r.msgs))
		}
		first = r.msgs[0]
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first message")
	}

	// While that message is unacked, no further messages may be delivered.
	select {
	case r := <-recvCh:
		if r.err != nil {
			t.Fatalf("StreamingPull Recv before ack: %v", r.err)
		}
		if len(r.msgs) != 0 {
			t.Fatalf("received %d more messages before ack, want 0", len(r.msgs))
		}
	case <-time.After(500 * time.Millisecond):
	}

	// Acking the outstanding message frees the single slot, so the next message
	// must be delivered.
	if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{first.GetAckId()}}); err != nil {
		t.Fatalf("StreamingPull ack Send: %v", err)
	}
	select {
	case r := <-recvCh:
		if r.err != nil {
			t.Fatalf("StreamingPull Recv after ack: %v", r.err)
		}
		if len(r.msgs) != 1 {
			t.Fatalf("after ack delivered %d messages, want 1", len(r.msgs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the message after ack")
	}

	stream.CloseSend()
}

func TestTestIamPermissionsFailsOpenOnMissingTopic(t *testing.T) {
	_, _, iam, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	got, err := iam.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    "projects/test/topics/missing",
		Permissions: []string{"pubsub.topics.publish"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions on missing topic = %v, want fail-open (nil)", err)
	}
	if len(got.GetPermissions()) != 0 {
		t.Fatalf("TestIamPermissions on missing topic = %v, want empty", got.GetPermissions())
	}
}

// TestPubSubFanOutGRPC verifies per-subscription fan-out over gRPC: two pull
// subscriptions of one topic both receive a copy of a published message.
func TestPubSubFanOutGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/fan-grpc"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subs := []string{
		"projects/test/subscriptions/fan-grpc-a",
		"projects/test/subscriptions/fan-grpc-b",
	}
	for _, s := range subs {
		if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: s, Topic: topic, AckDeadlineSeconds: 30}); err != nil {
			t.Fatalf("CreateSubscription %s: %v", s, err)
		}
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("broadcast")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	for _, s := range subs {
		pull, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: s, MaxMessages: 10, ReturnImmediately: true})
		if err != nil {
			t.Fatalf("Pull %s: %v", s, err)
		}
		if len(pull.GetReceivedMessages()) != 1 {
			t.Fatalf("subscription %s: got %d messages, want 1", s, len(pull.GetReceivedMessages()))
		}
		if got := string(pull.GetReceivedMessages()[0].GetMessage().GetData()); got != "broadcast" {
			t.Fatalf("subscription %s: data = %q, want broadcast", s, got)
		}
	}
}

func TestPubSubFilterGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/filter-topic"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	filtered := "projects/test/subscriptions/filtered"
	unfiltered := "projects/test/subscriptions/unfiltered"
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: filtered, Topic: topic, AckDeadlineSeconds: 30, Filter: `attributes.event_type = "a"`,
	}); err != nil {
		t.Fatalf("CreateSubscription filtered: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: unfiltered, Topic: topic, AckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("CreateSubscription unfiltered: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{
		{Data: []byte("match"), Attributes: map[string]string{"event_type": "a"}},
		{Data: []byte("nope"), Attributes: map[string]string{"event_type": "b"}},
	}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	fp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: filtered, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull filtered: %v", err)
	}
	if len(fp.GetReceivedMessages()) != 1 || string(fp.GetReceivedMessages()[0].GetMessage().GetData()) != "match" {
		t.Fatalf("filtered pull = %v, want exactly [match]", fp.GetReceivedMessages())
	}
	up, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: unfiltered, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull unfiltered: %v", err)
	}
	if len(up.GetReceivedMessages()) != 2 {
		t.Fatalf("unfiltered pull got %d, want 2", len(up.GetReceivedMessages()))
	}
}

func TestPubSubUnparseableFilterRejectedGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/bad-filter-topic"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: "projects/test/subscriptions/bad-filter", Topic: topic, Filter: "this is not a filter (((",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateSubscription bad filter err = %v, want InvalidArgument", err)
	}
}

func TestPubSubDetachGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/detach-topic"
	const sub = "projects/test/subscriptions/detach-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.DetachSubscription(ctx, &pubsubpb.DetachSubscriptionRequest{Subscription: sub}); err != nil {
		t.Fatalf("DetachSubscription: %v", err)
	}
	got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if !got.GetDetached() {
		t.Fatal("expected detached=true")
	}
	if _, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Pull detached err = %v, want FailedPrecondition", err)
	}
}

func TestPubSubUpdateFilterImmutableGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/immutable-topic"
	const sub = "projects/test/subscriptions/immutable-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, Filter: `attributes.event_type = "a"`,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	_, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: sub, Filter: `attributes.event_type = "b"`},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"filter"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateSubscription filter err = %v, want InvalidArgument", err)
	}

	relabelled, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: sub, Labels: map[string]string{"env": "local"}},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateSubscription labels: %v", err)
	}
	if relabelled.GetLabels()["env"] != "local" {
		t.Fatalf("labels = %v, want env=local", relabelled.GetLabels())
	}
	if relabelled.GetFilter() != `attributes.event_type = "a"` {
		t.Fatalf("filter changed to %q", relabelled.GetFilter())
	}
}

func TestPubSubTopicDeleteOrphansGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/orphan-topic"
	const sub = "projects/test/subscriptions/orphan-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic}); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.GetTopic() != "_deleted-topic_" {
		t.Fatalf("topic = %q, want _deleted-topic_", got.GetTopic())
	}
}

// pubsubSeedTopic creates a topic and a pull subscription for a test.
func pubsubSeedTopic(t *testing.T, ctx context.Context, pub pubsubpb.PublisherClient, subc pubsubpb.SubscriberClient, topic, sub string) {
	t.Helper()
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic, AckDeadlineSeconds: 10}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
}

func pubsubPublishOne(t *testing.T, ctx context.Context, pub pubsubpb.PublisherClient, topic, data string) {
	t.Helper()
	resp, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte(data)}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(resp.GetMessageIds()) != 1 {
		t.Fatalf("Publish messageIds = %v, want 1", resp.GetMessageIds())
	}
}

// pubsubPullBody claims until it receives a message with the given body.
func pubsubPullBody(t *testing.T, ctx context.Context, subc pubsubpb.SubscriberClient, sub, body string) *pubsubpb.ReceivedMessage {
	t.Helper()
	for i := 0; i < 100; i++ {
		resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 100, ReturnImmediately: true})
		if err != nil {
			t.Fatalf("Pull: %v", err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == body {
				return rm
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Pull never delivered body %q", body)
	return nil
}

func TestPubSubUpdateTopicGRPC(t *testing.T) {
	pub, _, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/update-topic"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	const retention = 10 * time.Minute
	updated, err := pub.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic:      &pubsubpb.Topic{Name: topic, Labels: map[string]string{"env": "test"}, MessageRetentionDuration: durationpb.New(retention)},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "message_retention_duration"}},
	})
	if err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if updated.GetLabels()["env"] != "test" {
		t.Fatalf("labels = %v, want env=test", updated.GetLabels())
	}
	if got := updated.GetMessageRetentionDuration().AsDuration(); got != retention {
		t.Fatalf("retention = %v, want %v", got, retention)
	}
	got, err := pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topic})
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.GetMessageRetentionDuration().AsDuration() != retention {
		t.Fatalf("GetTopic retention = %v, want %v", got.GetMessageRetentionDuration().AsDuration(), retention)
	}
}

// TestTopicRetentionStoredAsProtoJSON guards F1: the stored
// messageRetentionDuration must be the protobuf-JSON form ("600s"), not Go's
// "10m0s", or the official REST client cannot unmarshal every later read of the
// topic as a google.protobuf.Duration.
func TestTopicRetentionStoredAsProtoJSON(t *testing.T) {
	resources := store.NewMemoryResourceStore()
	svc := NewService(resources, pubsubstore.NewMemoryMessages(), crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore()), "test")
	ctx := context.Background()

	if _, err := svc.CreateTopic(ctx, &pubsubpb.Topic{
		Name:                     "projects/test/topics/ret",
		MessageRetentionDuration: durationpb.New(10 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	assertStoredRetention(t, resources, "ret", "600s")

	if _, err := svc.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic:      &pubsubpb.Topic{Name: "projects/test/topics/ret", MessageRetentionDuration: durationpb.New(15 * time.Minute)},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"message_retention_duration"}},
	}); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	assertStoredRetention(t, resources, "ret", "900s")
}

func assertStoredRetention(t *testing.T, resources store.ResourceStore, id, want string) {
	t.Helper()
	e, err := resources.Get(context.Background(), "test", store.GlobalRegion, rtTopic, id)
	if err != nil {
		t.Fatalf("store get: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(e.Data, &meta); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, _ := meta["messageRetentionDuration"].(string); got != want {
		t.Errorf("stored retention = %q, want %q", got, want)
	}
}

// grpcTopicWithAllFields builds a Topic carrying every field the
// terraform-google-pubsub module sets on its topic resource.
func grpcTopicWithAllFields(name string) *pubsubpb.Topic {
	return &pubsubpb.Topic{
		Name:                     name,
		Labels:                   map[string]string{"env": "prod", "team": "data"},
		KmsKeyName:               "projects/test/locations/global/keyRings/tf-keyring/cryptoKeys/tf-key",
		MessageRetentionDuration: durationpb.New(10 * time.Minute),
		MessageStoragePolicy: &pubsubpb.MessageStoragePolicy{
			AllowedPersistenceRegions: []string{"us-central1", "europe-west1"},
			EnforceInTransit:          true,
		},
		SchemaSettings: &pubsubpb.SchemaSettings{
			Schema:   "projects/test/schemas/orders",
			Encoding: pubsubpb.Encoding_JSON,
		},
	}
}

// assertTopicFieldsEqual compares the mutable Topic surface both transports must
// round-trip (nil-able getters make an absent field comparable too).
func assertTopicFieldsEqual(t *testing.T, where string, got, want *pubsubpb.Topic) {
	t.Helper()
	if got.GetName() != want.GetName() {
		t.Errorf("%s: name = %q, want %q", where, got.GetName(), want.GetName())
	}
	if !reflect.DeepEqual(got.GetLabels(), want.GetLabels()) {
		t.Errorf("%s: labels = %v, want %v", where, got.GetLabels(), want.GetLabels())
	}
	if got.GetKmsKeyName() != want.GetKmsKeyName() {
		t.Errorf("%s: kmsKeyName = %q, want %q", where, got.GetKmsKeyName(), want.GetKmsKeyName())
	}
	if got.GetMessageRetentionDuration().AsDuration() != want.GetMessageRetentionDuration().AsDuration() {
		t.Errorf("%s: messageRetentionDuration = %v, want %v", where,
			got.GetMessageRetentionDuration().AsDuration(), want.GetMessageRetentionDuration().AsDuration())
	}
	if !reflect.DeepEqual(got.GetMessageStoragePolicy().GetAllowedPersistenceRegions(),
		want.GetMessageStoragePolicy().GetAllowedPersistenceRegions()) {
		t.Errorf("%s: allowedPersistenceRegions = %v, want %v", where,
			got.GetMessageStoragePolicy().GetAllowedPersistenceRegions(),
			want.GetMessageStoragePolicy().GetAllowedPersistenceRegions())
	}
	if got.GetMessageStoragePolicy().GetEnforceInTransit() != want.GetMessageStoragePolicy().GetEnforceInTransit() {
		t.Errorf("%s: enforceInTransit = %v, want %v", where,
			got.GetMessageStoragePolicy().GetEnforceInTransit(), want.GetMessageStoragePolicy().GetEnforceInTransit())
	}
	if got.GetSchemaSettings().GetSchema() != want.GetSchemaSettings().GetSchema() {
		t.Errorf("%s: schemaSettings.schema = %q, want %q", where,
			got.GetSchemaSettings().GetSchema(), want.GetSchemaSettings().GetSchema())
	}
	if got.GetSchemaSettings().GetEncoding() != want.GetSchemaSettings().GetEncoding() {
		t.Errorf("%s: schemaSettings.encoding = %v, want %v", where,
			got.GetSchemaSettings().GetEncoding(), want.GetSchemaSettings().GetEncoding())
	}
}

// TestCreateTopicFullFieldRoundTripGRPC covers PTF1 on the gRPC side: every
// module field survives create, get and list. Before PTF1 the transcoder
// rendered only name/kmsKeyName/labels/retention, so storage policy and schema
// settings never reached a gRPC client.
func TestCreateTopicFullFieldRoundTripGRPC(t *testing.T) {
	pub, _, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	want := grpcTopicWithAllFields("projects/test/topics/full-fields")
	req := grpcTopicWithAllFields(want.GetName())
	// An output-only field a client echoes back must be ignored, not stored.
	req.State = pubsubpb.Topic_ACTIVE

	created, err := pub.CreateTopic(ctx, req)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	assertTopicFieldsEqual(t, "CreateTopic", created, want)
	if created.GetState() != pubsubpb.Topic_STATE_UNSPECIFIED {
		t.Errorf("CreateTopic state = %v, want STATE_UNSPECIFIED (output-only is stripped)", created.GetState())
	}

	got, err := pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: want.GetName()})
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	assertTopicFieldsEqual(t, "GetTopic", got, want)

	list, err := pub.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/test"})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	var found *pubsubpb.Topic
	for _, topic := range list.GetTopics() {
		if topic.GetName() == want.GetName() {
			found = topic
		}
	}
	if found == nil {
		t.Fatalf("ListTopics did not return %q", want.GetName())
	}
	assertTopicFieldsEqual(t, "ListTopics", found, want)
}

// TestTopicRESTGRPCParity asserts both transports serve one topic as the same
// protobuf-JSON map: a topic created over gRPC reads back identically from the
// REST provider and the gRPC renderer, so a field can never be visible on only
// one side.
func TestTopicRESTGRPCParity(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	enc := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	svc := NewService(resources, messages, enc, "test")
	rest := restpubsub.New(resources, messages, enc)

	if _, err := svc.CreateTopic(ctx, grpcTopicWithAllFields("projects/test/topics/parity")); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	got, err := svc.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: "projects/test/topics/parity"})
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	grpcMap, err := protoToJSONMap(got)
	if err != nil {
		t.Fatalf("protoToJSONMap: %v", err)
	}

	restResp, err := rest.TopicGet(ctx, &model.NormalizedRequest{
		AccountID:  "test",
		Params:     map[string]any{"name": "topics/parity"},
		ResourceID: resource.ResourceID("test"),
	})
	if err != nil {
		t.Fatalf("REST TopicGet: %v", err)
	}
	if !reflect.DeepEqual(restResp.Data, grpcMap) {
		t.Fatalf("REST and gRPC topic maps differ\n  REST: %#v\n  gRPC: %#v", restResp.Data, grpcMap)
	}
}

// TestTopicCreateValidationErrorParity asserts the shared field model's error is
// the same message on both transports: REST answers 400 with the message, gRPC
// answers INVALID_ARGUMENT with the identical text.
func TestTopicCreateValidationErrorParity(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	enc := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	svc := NewService(resources, messages, enc, "test")
	rest := restpubsub.New(resources, messages, enc)

	_, restErr := rest.TopicCreate(ctx, &model.NormalizedRequest{
		AccountID:  "test",
		Params:     map[string]any{"name": "topics/bad", "body": map[string]any{"messageRetentionDuration": "599s"}},
		ResourceID: resource.ResourceID("test"),
	})
	pe, ok := restErr.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 {
		t.Fatalf("REST TopicCreate err = %v, want a 400 ProviderError", restErr)
	}

	_, grpcErr := svc.CreateTopic(ctx, &pubsubpb.Topic{
		Name:                     "projects/test/topics/bad",
		MessageRetentionDuration: durationpb.New(599 * time.Second),
	})
	st, ok := status.FromError(grpcErr)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("gRPC CreateTopic err = %v, want INVALID_ARGUMENT", grpcErr)
	}
	if st.Message() != pe.Message {
		t.Fatalf("validation error text differs\n  REST: %q\n  gRPC: %q", pe.Message, st.Message())
	}
}

func TestPubSubListTopicSubscriptionsGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/lts-topic"
	const other = "projects/test/topics/lts-other"
	pubsubSeedTopic(t, ctx, pub, subc, topic, "projects/test/subscriptions/lts-a")
	pubsubSeedTopic(t, ctx, pub, subc, topic, "projects/test/subscriptions/lts-b")
	pubsubSeedTopic(t, ctx, pub, subc, other, "projects/test/subscriptions/lts-other")

	resp, err := pub.ListTopicSubscriptions(ctx, &pubsubpb.ListTopicSubscriptionsRequest{Topic: topic})
	if err != nil {
		t.Fatalf("ListTopicSubscriptions: %v", err)
	}
	got := map[string]bool{}
	for _, s := range resp.GetSubscriptions() {
		got[s] = true
	}
	if !got["projects/test/subscriptions/lts-a"] || !got["projects/test/subscriptions/lts-b"] {
		t.Fatalf("ListTopicSubscriptions = %v, want both lts-a and lts-b", resp.GetSubscriptions())
	}
	if got["projects/test/subscriptions/lts-other"] {
		t.Fatalf("ListTopicSubscriptions leaked another topic's subscription: %v", resp.GetSubscriptions())
	}
}

func TestPubSubModifyPushConfigGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/push-topic"
	const sub = "projects/test/subscriptions/push-sub"
	pubsubSeedTopic(t, ctx, pub, subc, topic, sub)

	const endpoint = "https://example.invalid/push"
	if _, err := subc.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{
		Subscription: sub, PushConfig: &pubsubpb.PushConfig{PushEndpoint: endpoint},
	}); err != nil {
		t.Fatalf("ModifyPushConfig: %v", err)
	}
	got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.GetPushConfig().GetPushEndpoint() != endpoint {
		t.Fatalf("push endpoint = %q, want %q", got.GetPushConfig().GetPushEndpoint(), endpoint)
	}
	if _, err := subc.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{Subscription: sub, PushConfig: &pubsubpb.PushConfig{}}); err != nil {
		t.Fatalf("ModifyPushConfig(clear): %v", err)
	}
	cleared, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription after clear: %v", err)
	}
	if ep := cleared.GetPushConfig().GetPushEndpoint(); ep != "" {
		t.Fatalf("push endpoint = %q after clear, want empty", ep)
	}
}

func TestPubSubSnapshotsGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/snap-topic"
	const sub = "projects/test/subscriptions/snap-sub"
	const snap = "projects/test/snapshots/snap-one"
	pubsubSeedTopic(t, ctx, pub, subc, topic, sub)
	pubsubPublishOne(t, ctx, pub, topic, "snap-body")
	pubsubPullBody(t, ctx, subc, sub, "snap-body")

	created, err := subc.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{Name: snap, Subscription: sub, Labels: map[string]string{"env": "test"}})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if created.GetName() != snap || created.GetTopic() != topic || created.GetExpireTime() == nil {
		t.Fatalf("CreateSnapshot = %+v, want name=%s topic=%s expire", created, snap, topic)
	}
	if got := created.GetLabels()["env"]; got != "test" {
		t.Fatalf("CreateSnapshot labels = %v, want env=test", created.GetLabels())
	}

	got, err := subc.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snap})
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.GetName() != snap {
		t.Fatalf("GetSnapshot name = %q, want %q", got.GetName(), snap)
	}

	listed, err := subc.ListSnapshots(ctx, &pubsubpb.ListSnapshotsRequest{Project: "projects/test"})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	found := false
	for _, s := range listed.GetSnapshots() {
		if s.GetName() == snap {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListSnapshots did not include %q", snap)
	}

	byTopic, err := pub.ListTopicSnapshots(ctx, &pubsubpb.ListTopicSnapshotsRequest{Topic: topic})
	if err != nil {
		t.Fatalf("ListTopicSnapshots: %v", err)
	}
	if len(byTopic.GetSnapshots()) != 1 || byTopic.GetSnapshots()[0] != snap {
		t.Fatalf("ListTopicSnapshots = %v, want [%s]", byTopic.GetSnapshots(), snap)
	}

	updated, err := subc.UpdateSnapshot(ctx, &pubsubpb.UpdateSnapshotRequest{
		Snapshot:   &pubsubpb.Snapshot{Name: snap, Labels: map[string]string{"env": "updated"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateSnapshot: %v", err)
	}
	if got := updated.GetLabels()["env"]; got != "updated" {
		t.Fatalf("UpdateSnapshot labels = %v, want env=updated", updated.GetLabels())
	}

	if _, err := subc.DeleteSnapshot(ctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: snap}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if _, err := subc.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snap}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSnapshot after delete = %v, want NotFound", err)
	}
}

func TestPubSubSeekTimeGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/seek-time-topic"
	const sub = "projects/test/subscriptions/seek-time-sub"
	pubsubSeedTopic(t, ctx, pub, subc, topic, sub)

	pubsubPublishOne(t, ctx, pub, topic, "seek-time-a")
	rm := pubsubPullBody(t, ctx, subc, sub, "seek-time-a")
	// Seek into the future acknowledges the retained message.
	if _, err := subc.Seek(ctx, &pubsubpb.SeekRequest{Subscription: sub, Target: &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now().Add(time.Hour))}}); err != nil {
		t.Fatalf("Seek(future): %v", err)
	}
	if got := countBody(t, ctx, subc, sub, "seek-time-a"); got != 0 {
		t.Fatalf("message redelivered after Seek(future): %d", got)
	}
	_ = rm

	// Seek into the past makes a retained (unacked) message deliverable again.
	pubsubPublishOne(t, ctx, pub, topic, "seek-time-b")
	pubsubPullBody(t, ctx, subc, sub, "seek-time-b")
	if _, err := subc.Seek(ctx, &pubsubpb.SeekRequest{Subscription: sub, Target: &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now().Add(-time.Hour))}}); err != nil {
		t.Fatalf("Seek(past): %v", err)
	}
	if got := countBody(t, ctx, subc, sub, "seek-time-b"); got != 1 {
		t.Fatalf("message not redelivered after Seek(past): got %d, want 1", got)
	}
}

// countBody drains the visible messages and counts those with the given body.
func countBody(t *testing.T, ctx context.Context, subc pubsubpb.SubscriberClient, sub, body string) int {
	t.Helper()
	resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 100, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	n := 0
	for _, rm := range resp.GetReceivedMessages() {
		if string(rm.GetMessage().GetData()) == body {
			n++
		}
	}
	return n
}

func TestPubSubSeekSnapshotGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/seek-snap-topic"
	const sub = "projects/test/subscriptions/seek-snap-sub"
	const snap = "projects/test/snapshots/seek-snap"
	pubsubSeedTopic(t, ctx, pub, subc, topic, sub)

	pubsubPublishOne(t, ctx, pub, topic, "seek-snap-body")
	rm := pubsubPullBody(t, ctx, subc, sub, "seek-snap-body")
	if _, err := subc.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{Name: snap, Subscription: sub}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	// Ack after the snapshot: Seek must restore the captured backlog.
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{rm.GetAckId()}}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if got := countBody(t, ctx, subc, sub, "seek-snap-body"); got != 0 {
		t.Fatalf("message still delivered after ack: %d", got)
	}
	if _, err := subc.Seek(ctx, &pubsubpb.SeekRequest{Subscription: sub, Target: &pubsubpb.SeekRequest_Snapshot{Snapshot: snap}}); err != nil {
		t.Fatalf("Seek(snapshot): %v", err)
	}
	if got := countBody(t, ctx, subc, sub, "seek-snap-body"); got != 1 {
		t.Fatalf("message not restored by Seek(snapshot): got %d, want 1", got)
	}
}

func TestPubSubSeekMissingTargetGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/seek-missing-topic"
	const sub = "projects/test/subscriptions/seek-missing-sub"
	pubsubSeedTopic(t, ctx, pub, subc, topic, sub)
	if _, err := subc.Seek(ctx, &pubsubpb.SeekRequest{Subscription: sub}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Seek(no target) = %v, want InvalidArgument", err)
	}
}

// TestPubSubDeliveryAttemptRequiresDLQGRPC verifies delivery_attempt is
// reported only when the subscription has a DeadLetterPolicy (0 otherwise).
func TestPubSubDeliveryAttemptRequiresDLQGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/da-src"
	const dlq = "projects/test/topics/da-dlq"
	for _, tp := range []string{topic, dlq} {
		if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: tp}); err != nil {
			t.Fatalf("CreateTopic %s: %v", tp, err)
		}
	}
	const plainSub = "projects/test/subscriptions/da-plain"
	const dlqSub = "projects/test/subscriptions/da-dlq"
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: plainSub, Topic: topic}); err != nil {
		t.Fatalf("plain sub: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: dlqSub, Topic: topic,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dlq, MaxDeliveryAttempts: 10},
	}); err != nil {
		t.Fatalf("dlq sub: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hi")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pull := func(sub string) *pubsubpb.ReceivedMessage {
		resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
		if err != nil {
			t.Fatalf("Pull %s: %v", sub, err)
		}
		if len(resp.GetReceivedMessages()) != 1 {
			t.Fatalf("Pull %s: got %d messages, want 1", sub, len(resp.GetReceivedMessages()))
		}
		return resp.GetReceivedMessages()[0]
	}
	if got := pull(plainSub).GetDeliveryAttempt(); got != 0 {
		t.Fatalf("no-DLQ deliveryAttempt = %d, want 0", got)
	}
	if got := pull(dlqSub).GetDeliveryAttempt(); got < 1 {
		t.Fatalf("DLQ deliveryAttempt = %d, want >= 1", got)
	}
}

// TestPubSubDLQAttributesGRPC verifies the gRPC republish path attaches real
// GCP's CloudPubSubDeadLetterSource* attributes (source subscription short id,
// project, delivery count, original publish time) and preserves the publisher's
// own attributes.
func TestPubSubDLQAttributesGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/dlqsrc"
	const dlq = "projects/test/topics/dlqtopic"
	for _, tp := range []string{topic, dlq} {
		if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: tp}); err != nil {
			t.Fatalf("CreateTopic %s: %v", tp, err)
		}
	}
	const srcSub = "projects/test/subscriptions/dlqsrc-sub"
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: srcSub, Topic: topic, AckDeadlineSeconds: 10,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dlq, MaxDeliveryAttempts: 5},
	}); err != nil {
		t.Fatalf("source sub: %v", err)
	}
	const dlqSub = "projects/test/subscriptions/dlqtopic-sub"
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: dlqSub, Topic: dlq, AckDeadlineSeconds: 10}); err != nil {
		t.Fatalf("dlq sub: %v", err)
	}

	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hi"), Attributes: map[string]string{"k": "v"}, OrderingKey: "ord-key"}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Five delivering pulls (each nacked), then the next claim forwards it.
	// Capture the source message's identity from its first delivery: real GCP
	// mints a new messageId and a forward-time publishTime on the copy (PSM5).
	var srcID string
	var srcPublishTime time.Time
	for i := 0; i < 6; i++ {
		resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: srcSub, MaxMessages: 1, ReturnImmediately: true})
		if err != nil {
			t.Fatalf("Pull: %v", err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			if srcID == "" {
				srcID = rm.GetMessage().GetMessageId()
				srcPublishTime = rm.GetMessage().GetPublishTime().AsTime()
			}
			if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription: srcSub, AckIds: []string{rm.GetAckId()}, AckDeadlineSeconds: 0,
			}); err != nil {
				t.Fatalf("ModifyAckDeadline: %v", err)
			}
		}
	}

	resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: dlqSub, MaxMessages: 1, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull DLQ: %v", err)
	}
	if len(resp.GetReceivedMessages()) != 1 {
		t.Fatalf("DLQ pull: got %d messages, want 1", len(resp.GetReceivedMessages()))
	}
	msg := resp.GetReceivedMessages()[0].GetMessage()
	attrs := msg.GetAttributes()
	if attrs[pubsubstore.AttrDeadLetterSourceSubscription] != "dlqsrc-sub" {
		t.Errorf("source subscription = %q, want dlqsrc-sub", attrs[pubsubstore.AttrDeadLetterSourceSubscription])
	}
	if attrs[pubsubstore.AttrDeadLetterSourceSubscriptionProject] != "test" {
		t.Errorf("source project = %q, want test", attrs[pubsubstore.AttrDeadLetterSourceSubscriptionProject])
	}
	if attrs[pubsubstore.AttrDeadLetterSourceDeliveryCount] != "5" {
		t.Errorf("delivery count = %q, want 5", attrs[pubsubstore.AttrDeadLetterSourceDeliveryCount])
	}
	wantTime := srcPublishTime.UTC().Format("2006-01-02T15:04:05.000-07:00")
	if got := attrs[pubsubstore.AttrDeadLetterSourceTopicPublishTime]; got != wantTime {
		t.Errorf("source publish time = %q, want %q", got, wantTime)
	}
	if attrs["k"] != "v" {
		t.Errorf("publisher attribute k = %q, want v", attrs["k"])
	}
	// PSM5: real GCP wraps the original in a new message — a fresh messageId, a
	// forward-time publishTime, and the source ordering key preserved.
	if msg.GetMessageId() == srcID {
		t.Errorf("forwarded message id = %q, want a fresh id (source %q)", msg.GetMessageId(), srcID)
	}
	if msg.GetPublishTime().AsTime().Equal(srcPublishTime) {
		t.Errorf("forwarded publishTime = %v, want the forward time (source %v)", msg.GetPublishTime().AsTime(), srcPublishTime)
	}
	if msg.GetOrderingKey() != "ord-key" {
		t.Errorf("forwarded ordering key = %q, want ord-key", msg.GetOrderingKey())
	}
}

// TestPubSubAckDeadlineValidationGRPC verifies the [10,600] create bounds, the
// 0→10 default, the [0,600] ModifyAckDeadline bounds, and the same bounds on
// UpdateSubscription.
func TestPubSubAckDeadlineValidationGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/ad"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, v := range []int32{1, 601} {
		_, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name: "projects/test/subscriptions/ad-bad", Topic: topic, AckDeadlineSeconds: v,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CreateSubscription ackDeadline=%d err = %v, want InvalidArgument", v, err)
		}
	}
	const sub = "projects/test/subscriptions/ad-zero"
	created, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic, AckDeadlineSeconds: 0})
	if err != nil {
		t.Fatalf("CreateSubscription 0: %v", err)
	}
	if created.GetAckDeadlineSeconds() != 10 {
		t.Fatalf("ackDeadlineSeconds = %d, want 10", created.GetAckDeadlineSeconds())
	}
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckDeadlineSeconds: 601,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ModifyAckDeadline 601 err = %v, want InvalidArgument", err)
	}
	if _, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: sub, AckDeadlineSeconds: 601},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"ack_deadline_seconds"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateSubscription ackDeadline=601 err = %v, want InvalidArgument", err)
	}
	updated, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: sub, AckDeadlineSeconds: 0},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"ack_deadline_seconds"}},
	})
	if err != nil {
		t.Fatalf("UpdateSubscription ackDeadline=0: %v", err)
	}
	if updated.GetAckDeadlineSeconds() != 10 {
		t.Fatalf("updated ackDeadlineSeconds = %d, want 10", updated.GetAckDeadlineSeconds())
	}
}

// TestPubSubDeadLetterPolicyValidationGRPC verifies the dead-letter topic
// existence check and the [5,100] maxDeliveryAttempts bounds (0→5).
func TestPubSubDeadLetterPolicyValidationGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/dlv"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: "projects/test/subscriptions/dlv-missing", Topic: topic,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: "projects/test/topics/nope"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing DLQ topic err = %v, want NotFound", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: "projects/test/subscriptions/dlv-low", Topic: topic,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: topic, MaxDeliveryAttempts: 3},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("maxDeliveryAttempts=3 err = %v, want InvalidArgument", err)
	}
	created, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: "projects/test/subscriptions/dlv-default", Topic: topic,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: topic},
	})
	if err != nil {
		t.Fatalf("CreateSubscription default DLQ: %v", err)
	}
	if got := created.GetDeadLetterPolicy().GetMaxDeliveryAttempts(); got != 5 {
		t.Fatalf("maxDeliveryAttempts default = %d, want 5", got)
	}
}

// TestLongPollBoundedFrozenClock mirrors the REST provider's TD15 regression:
// the gRPC long-poll window is real wall time, so a frozen business clock still
// returns an empty pull within the bounded window instead of polling forever.
func TestLongPollBoundedFrozenClock(t *testing.T) {
	at := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/frozen-longpoll"
	const sub = "projects/test/subscriptions/frozen-longpoll"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	done := make(chan int, 1)
	go func() {
		resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
		if err != nil {
			t.Errorf("Pull: %v", err)
			done <- -1
			return
		}
		done <- len(resp.GetReceivedMessages())
	}()

	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("expected 0 messages, got %d", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("empty gRPC long-poll did not return under a frozen clock; deadline used the business clock")
	}
}

// TestUpdateSubscriptionProviderMasksGRPC pins the gRPC mirror of the REST
// updateMask contract: each mutable Subscription field accepts both the proto
// snake_case and the JSON camelCase spelling, persists, and is rendered back by
// subToProto (the GET after the update exercises the read path too).
func TestUpdateSubscriptionProviderMasksGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/upd-masks-topic"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	mkSub := func(id string) string {
		name := "projects/test/subscriptions/" + id
		if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic}); err != nil {
			t.Fatalf("CreateSubscription(%s): %v", id, err)
		}
		return name
	}
	read := func(name string) *pubsubpb.Subscription {
		got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
		if err != nil {
			t.Fatalf("GetSubscription(%s): %v", name, err)
		}
		return got
	}

	transforms := []*pubsubpb.MessageTransform{{
		Transform: &pubsubpb.MessageTransform_JavascriptUdf{
			JavascriptUdf: &pubsubpb.JavaScriptUDF{
				FunctionName: "redact",
				Code:         "function redact(m) { return m; }",
			},
		},
	}}
	cases := []struct {
		name  string
		id    string
		mask  []string
		sub   *pubsubpb.Subscription
		check func(t *testing.T, got *pubsubpb.Subscription)
	}{
		{
			name: "message_retention_duration snake_case",
			id:   "upd-ret-snake",
			mask: []string{"message_retention_duration"},
			sub:  &pubsubpb.Subscription{MessageRetentionDuration: durationpb.New(1200 * time.Second)},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if d := got.GetMessageRetentionDuration().AsDuration(); d != 1200*time.Second {
					t.Errorf("message_retention_duration = %v, want 1200s", d)
				}
			},
		},
		{
			name: "messageRetentionDuration camelCase",
			id:   "upd-ret-camel",
			mask: []string{"messageRetentionDuration"},
			sub:  &pubsubpb.Subscription{MessageRetentionDuration: durationpb.New(2400 * time.Second)},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if d := got.GetMessageRetentionDuration().AsDuration(); d != 2400*time.Second {
					t.Errorf("messageRetentionDuration = %v, want 2400s", d)
				}
			},
		},
		{
			name: "retain_acked_messages snake_case",
			id:   "upd-retain-snake",
			mask: []string{"retain_acked_messages"},
			sub:  &pubsubpb.Subscription{RetainAckedMessages: true},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if !got.GetRetainAckedMessages() {
					t.Error("retain_acked_messages = false, want true")
				}
			},
		},
		{
			name: "retainAckedMessages camelCase",
			id:   "upd-retain-camel",
			mask: []string{"retainAckedMessages"},
			sub:  &pubsubpb.Subscription{RetainAckedMessages: true},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if !got.GetRetainAckedMessages() {
					t.Error("retainAckedMessages = false, want true")
				}
			},
		},
		{
			name: "expiration_policy empty means never expire",
			id:   "upd-expiry-snake",
			mask: []string{"expiration_policy"},
			sub:  &pubsubpb.Subscription{ExpirationPolicy: &pubsubpb.ExpirationPolicy{}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if got.GetExpirationPolicy() == nil {
					t.Fatal("expiration_policy = nil, want a set-but-empty policy")
				}
				if got.GetExpirationPolicy().GetTtl() != nil {
					t.Errorf("expiration_policy.ttl = %v, want unset (never expire)", got.GetExpirationPolicy().GetTtl())
				}
			},
		},
		{
			name: "expirationPolicy camelCase",
			id:   "upd-expiry-camel",
			mask: []string{"expirationPolicy"},
			sub:  &pubsubpb.Subscription{ExpirationPolicy: &pubsubpb.ExpirationPolicy{}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if got.GetExpirationPolicy() == nil {
					t.Fatal("expirationPolicy = nil, want a set-but-empty policy")
				}
			},
		},
		{
			name: "push_config snake_case",
			id:   "upd-push-snake",
			mask: []string{"push_config"},
			sub:  &pubsubpb.Subscription{PushConfig: &pubsubpb.PushConfig{PushEndpoint: "https://example.com/push"}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if ep := got.GetPushConfig().GetPushEndpoint(); ep != "https://example.com/push" {
					t.Errorf("push_config.push_endpoint = %q", ep)
				}
			},
		},
		{
			name: "pushConfig camelCase",
			id:   "upd-push-camel",
			mask: []string{"pushConfig"},
			sub:  &pubsubpb.Subscription{PushConfig: &pubsubpb.PushConfig{PushEndpoint: "https://example.com/push"}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if ep := got.GetPushConfig().GetPushEndpoint(); ep != "https://example.com/push" {
					t.Errorf("pushConfig.pushEndpoint = %q", ep)
				}
			},
		},
		{
			name: "bigquery_config snake_case",
			id:   "upd-bq-snake",
			mask: []string{"bigquery_config"},
			sub: &pubsubpb.Subscription{BigqueryConfig: &pubsubpb.BigQueryConfig{
				Table: "projects/test/datasets/ds/tables/tbl",
			}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if tbl := got.GetBigqueryConfig().GetTable(); tbl != "projects/test/datasets/ds/tables/tbl" {
					t.Errorf("bigquery_config.table = %q", tbl)
				}
			},
		},
		{
			name: "bigqueryConfig camelCase",
			id:   "upd-bq-camel",
			mask: []string{"bigqueryConfig"},
			sub: &pubsubpb.Subscription{BigqueryConfig: &pubsubpb.BigQueryConfig{
				Table: "projects/test/datasets/ds/tables/tbl2",
			}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if tbl := got.GetBigqueryConfig().GetTable(); tbl != "projects/test/datasets/ds/tables/tbl2" {
					t.Errorf("bigqueryConfig.table = %q", tbl)
				}
			},
		},
		{
			name: "cloud_storage_config snake_case",
			id:   "upd-gcs-snake",
			mask: []string{"cloud_storage_config"},
			sub: &pubsubpb.Subscription{CloudStorageConfig: &pubsubpb.CloudStorageConfig{
				Bucket: "my-bucket",
			}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if b := got.GetCloudStorageConfig().GetBucket(); b != "my-bucket" {
					t.Errorf("cloud_storage_config.bucket = %q", b)
				}
			},
		},
		{
			name: "cloudStorageConfig camelCase",
			id:   "upd-gcs-camel",
			mask: []string{"cloudStorageConfig"},
			sub: &pubsubpb.Subscription{CloudStorageConfig: &pubsubpb.CloudStorageConfig{
				Bucket: "other-bucket",
			}},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if b := got.GetCloudStorageConfig().GetBucket(); b != "other-bucket" {
					t.Errorf("cloudStorageConfig.bucket = %q", b)
				}
			},
		},
		{
			name: "message_transforms snake_case",
			id:   "upd-xform-snake",
			mask: []string{"message_transforms"},
			sub:  &pubsubpb.Subscription{MessageTransforms: transforms},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if len(got.GetMessageTransforms()) != 1 {
					t.Fatalf("message_transforms = %v, want one entry", got.GetMessageTransforms())
				}
				if fn := got.GetMessageTransforms()[0].GetJavascriptUdf().GetFunctionName(); fn != "redact" {
					t.Errorf("message_transforms[0].function_name = %q", fn)
				}
			},
		},
		{
			name: "messageTransforms camelCase",
			id:   "upd-xform-camel",
			mask: []string{"messageTransforms"},
			sub:  &pubsubpb.Subscription{MessageTransforms: transforms},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if len(got.GetMessageTransforms()) != 1 {
					t.Fatalf("messageTransforms = %v, want one entry", got.GetMessageTransforms())
				}
			},
		},
		{
			name: "state is output only",
			id:   "upd-state",
			mask: []string{"state"},
			sub:  &pubsubpb.Subscription{State: pubsubpb.Subscription_RESOURCE_ERROR},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if got.GetState() != pubsubpb.Subscription_ACTIVE {
					t.Errorf("state = %v, want ACTIVE (OUTPUT_ONLY, never stored)", got.GetState())
				}
			},
		},
		{
			// The recorded provider shape: the trailing bigquery_config is
			// absent from the request, so it is a clear, not an unknown path.
			name: "ack_deadline_seconds with absent bigquery_config",
			id:   "upd-ack-bq",
			mask: []string{"ack_deadline_seconds", "bigquery_config"},
			sub:  &pubsubpb.Subscription{AckDeadlineSeconds: 20},
			check: func(t *testing.T, got *pubsubpb.Subscription) {
				if got.GetAckDeadlineSeconds() != 20 {
					t.Errorf("ack_deadline_seconds = %d, want 20", got.GetAckDeadlineSeconds())
				}
				if got.GetBigqueryConfig() != nil {
					t.Errorf("bigquery_config = %v, want cleared", got.GetBigqueryConfig())
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := mkSub(tc.id)
			tc.sub.Name = name
			updated, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
				Subscription: tc.sub,
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: tc.mask},
			})
			if err != nil {
				t.Fatalf("UpdateSubscription(%v): %v", tc.mask, err)
			}
			tc.check(t, updated)
			tc.check(t, read(name))
		})
	}

	// The same masks with invalid values are InvalidArgument.
	rejects := []struct {
		name string
		id   string
		mask []string
		sub  *pubsubpb.Subscription
	}{
		{"retention below 10m", "upd-rej-ret-low", []string{"message_retention_duration"},
			&pubsubpb.Subscription{MessageRetentionDuration: durationpb.New(599 * time.Second)}},
		{"retention above 31d", "upd-rej-ret-high", []string{"messageRetentionDuration"},
			&pubsubpb.Subscription{MessageRetentionDuration: durationpb.New(2678401 * time.Second)}},
		{"expiration ttl below 24h", "upd-rej-exp-low", []string{"expiration_policy"},
			&pubsubpb.Subscription{ExpirationPolicy: &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(time.Hour)}}},
		{"expiration ttl above 365d", "upd-rej-exp-high", []string{"expirationPolicy"},
			&pubsubpb.Subscription{ExpirationPolicy: &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(365*24*time.Hour + time.Second)}}},
		{"unknown path", "upd-rej-unknown", []string{"nope"},
			&pubsubpb.Subscription{}},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			name := mkSub(tc.id)
			tc.sub.Name = name
			_, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
				Subscription: tc.sub,
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: tc.mask},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UpdateSubscription(%v) err = %v, want InvalidArgument", tc.mask, err)
			}
		})
	}
}

// TestUpdateSubscriptionExactlyOncePushOrderIndependentGRPC pins that the
// push/ exactly-once invariant is asserted against the merged state, so a patch
// naming both fields is rejected in either path order and in either sequence.
func TestUpdateSubscriptionExactlyOncePushOrderIndependentGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-push-topic"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	push := &pubsubpb.PushConfig{PushEndpoint: "https://example.com/push"}
	mkSub := func(id string) string {
		name := "projects/test/subscriptions/" + id
		if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic}); err != nil {
			t.Fatalf("CreateSubscription(%s): %v", id, err)
		}
		return name
	}

	for i, mask := range [][]string{
		{"enable_exactly_once_delivery", "push_config"},
		{"push_config", "enable_exactly_once_delivery"},
		{"enableExactlyOnceDelivery", "pushConfig"},
		{"pushConfig", "enableExactlyOnceDelivery"},
	} {
		name := mkSub("eod-both-" + string(rune('a'+i)))
		_, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
			Subscription: &pubsubpb.Subscription{
				Name:                      name,
				EnableExactlyOnceDelivery: true,
				PushConfig:                push,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: mask},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("mask %v err = %v, want InvalidArgument", mask, err)
		}
	}

	// Setting a push endpoint on an existing exactly-once subscription is
	// rejected, and enabling exactly-once on an existing push subscription too.
	seq := mkSub("eod-seq")
	update := func(sub *pubsubpb.Subscription, paths ...string) error {
		_, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
			Subscription: sub,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: paths},
		})
		return err
	}
	if err := update(&pubsubpb.Subscription{Name: seq, PushConfig: push}, "push_config"); err != nil {
		t.Fatalf("set push: %v", err)
	}
	if err := update(&pubsubpb.Subscription{Name: seq, EnableExactlyOnceDelivery: true}, "enable_exactly_once_delivery"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("enable exactly-once on push err = %v, want InvalidArgument", err)
	}
	if err := update(&pubsubpb.Subscription{Name: seq}, "pushConfig"); err != nil {
		t.Fatalf("clear push: %v", err)
	}
	if err := update(&pubsubpb.Subscription{Name: seq, EnableExactlyOnceDelivery: true}, "enableExactlyOnceDelivery"); err != nil {
		t.Fatalf("enable exactly-once after clear: %v", err)
	}
	if err := update(&pubsubpb.Subscription{Name: seq, PushConfig: push}, "pushConfig"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("push on exactly-once err = %v, want InvalidArgument", err)
	}
}

// TestSubscriptionUpdateRESTWriteGRPCRead is the cross-transport read: a REST
// update must be rendered by subToProto, so a missing renderer fails here
// instead of silently dropping the field for gRPC clients.
func TestSubscriptionUpdateRESTWriteGRPCRead(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	encryptor := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	rest := restpubsub.New(resources, messages, encryptor)
	svc := NewService(resources, messages, encryptor, "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pubsubpb.RegisterSubscriberServer(srv, svc)
	go srv.Serve(ln)
	defer srv.Stop()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pubsubpb.NewSubscriberClient(conn)

	nr := func(params map[string]any) *model.NormalizedRequest {
		return &model.NormalizedRequest{
			AccountID:  "test",
			Params:     params,
			ResourceID: resource.ResourceID("test"),
		}
	}
	if _, err := rest.TopicCreate(ctx, nr(map[string]any{"name": "topics/xtopic"})); err != nil {
		t.Fatalf("REST TopicCreate: %v", err)
	}
	if _, err := rest.SubscriptionCreate(ctx, nr(map[string]any{
		"name": "subscriptions/xsub",
		"body": map[string]any{"topic": "projects/test/topics/xtopic"},
	})); err != nil {
		t.Fatalf("REST SubscriptionCreate: %v", err)
	}
	if _, err := rest.SubscriptionUpdate(ctx, nr(map[string]any{
		"name": "subscriptions/xsub",
		"body": map[string]any{
			"subscription": map[string]any{
				"messageRetentionDuration": "1200s",
				"retainAckedMessages":      true,
				"bigqueryConfig":           map[string]any{"table": "projects/test/datasets/ds/tables/tbl"},
				"pushConfig":               map[string]any{"pushEndpoint": "https://example.com/push"},
				"messageTransforms": []any{map[string]any{
					"javascriptUdf": map[string]any{"functionName": "redact", "code": "function redact(m) { return m; }"},
				}},
				"expirationPolicy": map[string]any{},
			},
			"updateMask": "messageRetentionDuration,retainAckedMessages,bigqueryConfig,pushConfig,messageTransforms,expirationPolicy",
		},
	})); err != nil {
		t.Fatalf("REST SubscriptionUpdate: %v", err)
	}

	const name = "projects/test/subscriptions/xsub"
	got, err := client.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err != nil {
		t.Fatalf("gRPC GetSubscription: %v", err)
	}
	if d := got.GetMessageRetentionDuration().AsDuration(); d != 1200*time.Second {
		t.Errorf("message_retention_duration = %v, want 1200s", d)
	}
	if !got.GetRetainAckedMessages() {
		t.Error("retain_acked_messages = false, want true")
	}
	if tbl := got.GetBigqueryConfig().GetTable(); tbl != "projects/test/datasets/ds/tables/tbl" {
		t.Errorf("bigquery_config.table = %q", tbl)
	}
	if len(got.GetMessageTransforms()) != 1 {
		t.Fatalf("message_transforms = %v, want one entry", got.GetMessageTransforms())
	}
	if fn := got.GetMessageTransforms()[0].GetJavascriptUdf().GetFunctionName(); fn != "redact" {
		t.Errorf("message_transforms[0].function_name = %q", fn)
	}
	if got.GetExpirationPolicy() == nil {
		t.Error("expiration_policy = nil, want a set-but-empty never-expire policy")
	} else if got.GetExpirationPolicy().GetTtl() != nil {
		t.Errorf("expiration_policy.ttl = %v, want unset", got.GetExpirationPolicy().GetTtl())
	}
	if ep := got.GetPushConfig().GetPushEndpoint(); ep != "https://example.com/push" {
		t.Errorf("push_config.push_endpoint = %q", ep)
	}

	// A REST clear must read back as cleared over gRPC.
	if _, err := rest.SubscriptionUpdate(ctx, nr(map[string]any{
		"name": "subscriptions/xsub",
		"body": map[string]any{
			"subscription": map[string]any{
				"bigqueryConfig":    nil,
				"pushConfig":        nil,
				"messageTransforms": []any{},
			},
			"updateMask": "bigqueryConfig,pushConfig,messageTransforms",
		},
	})); err != nil {
		t.Fatalf("REST SubscriptionUpdate (clear): %v", err)
	}
	got, err = client.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err != nil {
		t.Fatalf("gRPC GetSubscription after clear: %v", err)
	}
	if got.GetBigqueryConfig() != nil {
		t.Errorf("bigquery_config = %v, want cleared", got.GetBigqueryConfig())
	}
	if got.GetPushConfig() != nil {
		t.Errorf("push_config = %v, want cleared", got.GetPushConfig())
	}
	if len(got.GetMessageTransforms()) != 0 {
		t.Errorf("message_transforms = %v, want cleared", got.GetMessageTransforms())
	}
}
