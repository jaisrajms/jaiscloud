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
