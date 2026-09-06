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

	// Create subscription.
	sub, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: subscription, Topic: topic, AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if sub.GetTopic() != topic || sub.GetAckDeadlineSeconds() != 30 {
		t.Fatalf("CreateSubscription = %+v, want topic=%s ackDeadline=30", sub, topic)
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
