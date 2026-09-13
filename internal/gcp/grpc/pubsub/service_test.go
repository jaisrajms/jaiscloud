package pubsub

import (
	"context"
	"net"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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

	// A single pull must return only the first message of the group.
	pull, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 1 {
		t.Fatalf("Pull received %d messages, want 1 (ordering-key gate)", len(pull.GetReceivedMessages()))
	}
	if string(pull.GetReceivedMessages()[0].GetMessage().GetData()) != "first" {
		t.Fatalf("Pull data = %q, want first", string(pull.GetReceivedMessages()[0].GetMessage().GetData()))
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
