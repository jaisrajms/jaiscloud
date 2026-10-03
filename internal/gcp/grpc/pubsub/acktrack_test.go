package pubsub

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

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

// gatedMessages wraps a Messages store and can hold a Pull open after it has
// already claimed its messages. It lets a test deterministically ack a message
// on one path while a StreamingPull stream is between claiming it and sending it
// — the cross-stream race the ack registry closes.
type gatedMessages struct {
	pubsubstore.Messages
	mu     sync.Mutex
	block  chan struct{}
	pulled chan struct{}
}

func (g *gatedMessages) gate(block, pulled chan struct{}) {
	g.mu.Lock()
	g.block, g.pulled = block, pulled
	g.mu.Unlock()
}

func (g *gatedMessages) Pull(ctx context.Context, queue string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]pubsubstore.Message, error) {
	msgs, err := g.Messages.Pull(ctx, queue, maxMessages, ackDeadlineSec, retentionSec, now)
	g.mu.Lock()
	pulled, block := g.pulled, g.block
	g.mu.Unlock()
	if pulled != nil {
		select {
		case pulled <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}
	return msgs, err
}

// pubSubTestServiceWithMessages is pubsubTestService with an injected message
// store, so a test can interpose between Pull and Send.
func pubSubTestServiceWithMessages(t *testing.T, messages pubsubstore.Messages) (pubsubpb.PublisherClient, pubsubpb.SubscriberClient, func()) {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	encryptor := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	svc := NewService(resources, messages, encryptor, "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pubsubpb.RegisterPublisherServer(srv, svc)
	pubsubpb.RegisterSubscriberServer(srv, svc)

	go srv.Serve(ln)
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return pubsubpb.NewPublisherClient(conn), pubsubpb.NewSubscriberClient(conn), func() {
		conn.Close()
		srv.Stop()
	}
}

// TestAckRegistryRefcountStopsOnlyOnLastStream pins the shared-loop lifecycle:
// releasing one of several streams must not stop the subscription's reconcile
// loop (and must not close the shared stop channel twice, which panicked before
// the refcount was made exclusive to the final release).
func TestAckRegistryRefcountStopsOnlyOnLastStream(t *testing.T) {
	reg := newAckRegistry("sub", pubsubstore.NewMemoryMessages())
	reg.acquire()
	reg.acquire()

	reg.markAcked("m1")
	if reg.ackCount() != 1 {
		t.Fatalf("ackCount with an active stream = %d, want 1", reg.ackCount())
	}

	reg.release() // first stream leaves; loop must stay up
	select {
	case <-reg.stopped:
		t.Fatal("reconcile loop stopped while a stream was still active")
	case <-time.After(50 * time.Millisecond):
	}

	reg.release() // last stream leaves; loop stops
	select {
	case <-reg.stopped:
	case <-time.After(time.Second):
		t.Fatal("reconcile loop did not stop after the last stream released")
	}

	// A redundant release must be a no-op, not a second close, and tombstones
	// must not accumulate once no stream can hold a claimed copy.
	reg.release()
	reg.markAcked("m2")
	if reg.ackCount() != 1 {
		t.Fatalf("ackCount with no active stream = %d, want 1 (no new tombstone)", reg.ackCount())
	}
}

// TestAckRegistryReconcileClearsRestoredTombstone verifies the reconciler
// un-tombstones a message that reappears in the store (Seek restored it), so an
// acked message that is explicitly replayed is deliverable again.
func TestAckRegistryReconcileClearsRestoredTombstone(t *testing.T) {
	ctx := context.Background()
	base := pubsubstore.NewMemoryMessages()
	reg := newAckRegistry("sub", base)
	reg.acquire()
	defer reg.release()

	reg.markAcked("m1")
	if !reg.isAcked("m1") {
		t.Fatal("markAcked did not set a tombstone")
	}
	// Absent from the store: the tombstone is retained (and later TTL-pruned).
	reg.reconcile(ctx)
	if !reg.isAcked("m1") {
		t.Fatal("reconcile dropped a tombstone for an absent message")
	}
	// Seek restores the message: the stale tombstone must be cleared.
	if err := base.Put(ctx, pubsubstore.Message{Subscription: "sub", MessageID: "m1"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	reg.reconcile(ctx)
	if reg.isAcked("m1") {
		t.Fatal("reconcile kept a tombstone for a restored message")
	}
}

// TestStreamingPullCrossStreamAckRace proves that a message acked on one
// consumer path is not delivered by a StreamingPull stream that had already
// claimed it. The gated store holds stream A's Pull open after it has claimed
// the message; the ack then lands through the unary Acknowledge RPC (the
// "other stream's" fast path), and A's next send must drop the stale copy.
func TestStreamingPullCrossStreamAckRace(t *testing.T) {
	base := pubsubstore.NewMemoryMessages()
	gated := &gatedMessages{Messages: base}
	pub, subc, cleanup := pubSubTestServiceWithMessages(t, gated)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "projects/test/topics/race-topic"
	const sub = "projects/test/subscriptions/race-sub"

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic, AckDeadlineSeconds: 10}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("race-msg")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// First consumer claims the message and immediately nacks it, so a second
	// consumer can pick it up while the first still holds its ack id.
	first, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
	if err != nil || len(first.GetReceivedMessages()) != 1 {
		t.Fatalf("first Pull = %v (msgs=%d), want 1", err, len(first.GetReceivedMessages()))
	}
	staleAckID := first.GetReceivedMessages()[0].GetAckId()
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{staleAckID}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Arm the gate before stream A starts, then open the stream and wait until
	// its Pull has claimed the message and is held.
	block := make(chan struct{})
	pulled := make(chan struct{}, 1)
	gated.gate(block, pulled)

	stream, err := subc.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	defer stream.CloseSend()
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription: sub, StreamAckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatalf("StreamingPull send: %v", err)
	}

	select {
	case <-pulled:
	case <-time.After(5 * time.Second):
		t.Fatal("stream A never claimed the message")
	}

	// While A holds the claimed copy, ack the message through the other path.
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: sub, AckIds: []string{staleAckID},
	}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	close(block)

	// A must not deliver the stale copy.
	recvCh := make(chan *pubsubpb.StreamingPullResponse, 1)
	recvErr := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			recvCh <- resp
		}
	}()
	select {
	case resp := <-recvCh:
		if len(resp.GetReceivedMessages()) != 0 {
			t.Fatalf("stream A delivered a message acked on another path: %v", resp.GetReceivedMessages())
		}
	case err := <-recvErr:
		t.Fatalf("stream A recv: %v", err)
	case <-time.After(700 * time.Millisecond):
		// No delivery — the race is closed.
	}

	// The message is gone from the store as well.
	remaining, err := base.List(ctx, sub)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("store still holds %d messages after ack", len(remaining))
	}
}

// TestExactlyOnceDeliveryRoundTrip verifies the flag is persisted and surfaced,
// that an unspecified ack deadline defaults to 60s for exactly-once, and that
// the unsupported push+exactly-once combination fails loud.
func TestExactlyOnceDeliveryRoundTrip(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-topic"
	const sub = "projects/test/subscriptions/eod-sub"

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	created, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, EnableExactlyOnceDelivery: true,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if !created.GetEnableExactlyOnceDelivery() {
		t.Fatal("CreateSubscription dropped enable_exactly_once_delivery")
	}
	if created.GetAckDeadlineSeconds() != 60 {
		t.Fatalf("ack deadline = %d, want 60 for exactly-once", created.GetAckDeadlineSeconds())
	}
	got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if !got.GetEnableExactlyOnceDelivery() || got.GetAckDeadlineSeconds() != 60 {
		t.Fatalf("GetSubscription = (eod=%v, ack=%d), want (true, 60)", got.GetEnableExactlyOnceDelivery(), got.GetAckDeadlineSeconds())
	}

	// Exactly-once + push is unsupported: fail loud with InvalidArgument.
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                      "projects/test/subscriptions/eod-push",
		Topic:                     topic,
		EnableExactlyOnceDelivery: true,
		PushConfig:                &pubsubpb.PushConfig{PushEndpoint: "https://example.invalid/push"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("exactly-once + push = %v, want InvalidArgument", err)
	}

	// Enabling exactly-once on an existing push subscription is also rejected.
	const pushSub = "projects/test/subscriptions/eod-update-push"
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:       pushSub,
		Topic:      topic,
		PushConfig: &pubsubpb.PushConfig{PushEndpoint: "https://example.invalid/push"},
	}); err != nil {
		t.Fatalf("CreateSubscription(push): %v", err)
	}
	if _, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: pushSub, EnableExactlyOnceDelivery: true},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"enable_exactly_once_delivery"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("enable exactly-once on push subscription = %v, want InvalidArgument", err)
	}
}

// TestExactlyOnceAckIDDedup verifies that once a message is acked under
// exactly-once it is not redelivered, that a repeated ack is a harmless no-op,
// and that the stream advertises its exactly-once property.
func TestExactlyOnceAckIDDedup(t *testing.T) {
	pub, subc, _, messages, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-dedup-topic"
	const sub = "projects/test/subscriptions/eod-dedup-sub"

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("dedup-msg")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pull, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
	if err != nil || len(pull.GetReceivedMessages()) != 1 {
		t.Fatalf("Pull = %v (msgs=%d), want 1", err, len(pull.GetReceivedMessages()))
	}
	ackID := pull.GetReceivedMessages()[0].GetAckId()

	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ackID}}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	// Dedup: the emulator treats a repeat of the same ack id as an idempotent
	// no-op (see the ackRegistry limitation note on superseded ack ids).
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ackID}}); err != nil {
		t.Fatalf("duplicate Acknowledge: %v", err)
	}

	after, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull after ack: %v", err)
	}
	if len(after.GetReceivedMessages()) != 0 {
		t.Fatalf("acked message redelivered: %v", after.GetReceivedMessages())
	}
	remaining, err := messages.List(ctx, sub)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("store still holds %d messages after ack", len(remaining))
	}

	// A streaming pull must advertise the subscription's exactly-once property
	// so the official client keeps its exactly-once mode.
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("streamed")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	stream, err := subc.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	defer stream.CloseSend()
	if err := stream.Send(&pubsubpb.StreamingPullRequest{Subscription: sub, StreamAckDeadlineSeconds: 10}); err != nil {
		t.Fatalf("StreamingPull send: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the streamed message")
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("StreamingPull recv: %v", err)
		}
		if len(resp.GetReceivedMessages()) == 0 {
			continue
		}
		if !resp.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() {
			t.Fatal("StreamingPull did not advertise exactly_once_delivery_enabled")
		}
		if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{resp.GetReceivedMessages()[0].GetAckId()}}); err != nil {
			t.Fatalf("StreamingPull ack: %v", err)
		}
		return
	}
}
