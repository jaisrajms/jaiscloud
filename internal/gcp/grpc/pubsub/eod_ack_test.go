package pubsub

import (
	"context"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/store"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// eodErrorInfo extracts the google.rpc.ErrorInfo detail from an exactly-once
// ack failure.
func eodErrorInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	st, _ := status.FromError(err)
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			return ei
		}
	}
	t.Fatalf("no ErrorInfo detail on %v (details=%v)", err, st.Details())
	return nil
}

// pullOne polls a subscription until it delivers exactly one message.
func pullOne(t *testing.T, ctx context.Context, subc pubsubpb.SubscriberClient, sub string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
		if err != nil {
			t.Fatalf("Pull: %v", err)
		}
		if len(resp.GetReceivedMessages()) == 1 {
			return resp.GetReceivedMessages()[0].GetAckId()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no message delivered")
	return ""
}

func TestExactlyOnceAckIDVersioning(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-topic"
	const sub = "projects/test/subscriptions/eod-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("m")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ack1 := pullOne(t, ctx, subc, sub)

	// Supersede: nack then re-pull so a newer delivery version exists.
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("nack: %v", err)
	}
	ack2 := pullOne(t, ctx, subc, sub)
	if ack1 == ack2 {
		t.Fatal("re-pull returned the same ack id; version was not encoded")
	}

	// Superseded ack id → INVALID_ARGUMENT + ErrorInfo(EXACTLY_ONCE_ACKID_FAILURE).
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}}); err == nil {
		t.Fatal("Acknowledge(superseded) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge(superseded) code = %v, want InvalidArgument", st.Code())
	} else if st.Message() != pubsubstore.EODAckFailureMessage {
		t.Fatalf("Acknowledge(superseded) message = %q", st.Message())
	}
	ei := eodErrorInfo(t, func() error {
		_, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}})
		return err
	}())
	if ei.Reason != pubsubstore.EODAckFailureReason || ei.Domain != pubsubstore.EODAckFailureDomain {
		t.Fatalf("ErrorInfo reason/domain = (%q,%q)", ei.Reason, ei.Domain)
	}
	if ei.Metadata[ack1] != pubsubstore.InvalidAckIDValue {
		t.Fatalf("ErrorInfo metadata = %v, want [%s]=%s", ei.Metadata, ack1, pubsubstore.InvalidAckIDValue)
	}

	// Superseded modack is rejected the same way.
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 30,
	}); err == nil {
		t.Fatal("ModifyAckDeadline(superseded) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("ModifyAckDeadline(superseded) code = %v, want InvalidArgument", st.Code())
	}

	// The latest ack id succeeds, and a re-ack is an idempotent OK.
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack2}}); err != nil {
		t.Fatalf("Acknowledge(latest): %v", err)
	}
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack2}}); err != nil {
		t.Fatalf("Acknowledge(re-ack) = %v, want OK", err)
	}
}

func TestExactlyOnceAckIDExpired(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(nil)

	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-exp-topic"
	const sub = "projects/test/subscriptions/eod-exp-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 10, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("exp")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ack := pullOne(t, ctx, subc, sub)
	// Advance past the 10s ack deadline so the granted ack id has expired.
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(11 * time.Second)})
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack}}); err == nil {
		t.Fatal("Acknowledge(expired) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge(expired) code = %v, want InvalidArgument", st.Code())
	}
}

// TestExactlyOnceSeekInvalidatesAckID pins EOD2: a Seek that makes a message
// visible again invalidates an ack ID issued before the Seek (real GCP's
// contract), and the restored message is redelivered with a fresh, usable ack ID.
func TestExactlyOnceSeekInvalidatesAckID(t *testing.T) {
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(nil)

	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-seek-topic"
	const sub = "projects/test/subscriptions/eod-seek-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("seek")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ack1 := pullOne(t, ctx, subc, sub)

	// Seek to just before the publish time: the message is retained and made
	// visible again, which invalidates ack1.
	if _, err := subc.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(base.Add(-time.Second))},
	}); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}}); err == nil {
		t.Fatal("Acknowledge(pre-Seek ack id) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge(pre-Seek ack id) code = %v, want InvalidArgument", st.Code())
	} else if st.Message() != pubsubstore.EODAckFailureMessage {
		t.Fatalf("Acknowledge(pre-Seek ack id) message = %q", st.Message())
	}
	ei := eodErrorInfo(t, func() error {
		_, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}})
		return err
	}())
	if ei.Metadata[ack1] != pubsubstore.InvalidAckIDValue {
		t.Fatalf("ErrorInfo metadata = %v, want [%s]=%s", ei.Metadata, ack1, pubsubstore.InvalidAckIDValue)
	}

	// The restored message is redelivered with a fresh ack id that works.
	ack2 := pullOne(t, ctx, subc, sub)
	if ack2 == ack1 {
		t.Fatal("post-Seek re-pull returned the same ack id; version was not advanced")
	}
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack2}}); err != nil {
		t.Fatalf("Acknowledge(post-Seek ack id): %v", err)
	}
}

// TestExactlyOnceSnapshotSeekInvalidatesAckID pins the snapshot-based Seek path:
// restoring a subscription to a snapshot invalidates an ack ID issued before the
// Seek, exercising restoreSnapshot's version bump (EOD2).
func TestExactlyOnceSnapshotSeekInvalidatesAckID(t *testing.T) {
	base := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(nil)

	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-snap-topic"
	const sub = "projects/test/subscriptions/eod-snap-sub"
	const snap = "projects/test/snapshots/eod-snap"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("snap")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ack1 := pullOne(t, ctx, subc, sub)
	if _, err := subc.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{Name: snap, Subscription: sub}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if _, err := subc.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Snapshot{Snapshot: snap},
	}); err != nil {
		t.Fatalf("Seek(snapshot): %v", err)
	}

	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}}); err == nil {
		t.Fatal("Acknowledge(pre-Seek ack id) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge(pre-Seek ack id) code = %v, want InvalidArgument", st.Code())
	}

	ack2 := pullOne(t, ctx, subc, sub)
	if ack2 == ack1 {
		t.Fatal("post-Seek re-pull returned the same ack id; version was not advanced")
	}
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack2}}); err != nil {
		t.Fatalf("Acknowledge(post-Seek ack id): %v", err)
	}
}

// TestPlainSubscriptionStaleAckIDOK pins the out-of-scope rule: without
// exactly-once delivery a superseded ack id is still accepted.
func TestPlainSubscriptionStaleAckIDOK(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/plain-topic"
	const sub = "projects/test/subscriptions/plain-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("p")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ack1 := pullOne(t, ctx, subc, sub)
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("nack: %v", err)
	}
	_ = pullOne(t, ctx, subc, sub)
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}}); err != nil {
		t.Fatalf("plain Acknowledge(stale) = %v, want OK", err)
	}
}

// TestApplyStreamAcksExactlyOnceVersions pins the streaming path: on an
// exactly-once subscription a superseded ack id must not mutate the live
// delivery, while the latest id acks it; a non-EOD stream keeps the lenient
// accept-anything behaviour.
func TestApplyStreamAcksExactlyOnceVersions(t *testing.T) {
	ctx := context.Background()
	messages := pubsubstore.NewMemoryMessages()
	svc := NewService(store.NewMemoryResourceStore(), messages, crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore()), "test")
	now := clock.Now()
	put := func(attempt int) {
		t.Helper()
		if err := messages.Put(ctx, pubsubstore.Message{
			Subscription: "s", MessageID: "m1", Data: "x",
			DeliveryAttempt: attempt, DeliveryVersion: attempt, VisibleAt: now.Add(time.Minute),
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	streamAck := func(eod bool, ackID string) []string {
		return svc.applyStreamAcks(ctx, "s", eod, &pubsubpb.StreamingPullRequest{AckIds: []string{ackID}})
	}

	// Superseded id on an exactly-once subscription: dropped, message intact.
	put(2)
	if got := streamAck(true, pubsubstore.EncodeAckID("s", "m1", 1)); len(got) != 0 {
		t.Fatalf("stale stream ack returned %v, want none", got)
	}
	if msgs, _ := messages.List(ctx, "s"); len(msgs) != 1 {
		t.Fatalf("stale stream ack mutated the live delivery: %d messages left", len(msgs))
	}

	// Latest id acks it.
	if got := streamAck(true, pubsubstore.EncodeAckID("s", "m1", 2)); len(got) != 1 {
		t.Fatalf("latest stream ack returned %v, want [m1]", got)
	}
	if msgs, _ := messages.List(ctx, "s"); len(msgs) != 0 {
		t.Fatalf("latest stream ack did not delete the message: %d left", len(msgs))
	}

	// Non-exactly-once: a stale id is still accepted (deletes the message).
	put(2)
	if got := streamAck(false, pubsubstore.EncodeAckID("s", "m1", 1)); len(got) != 1 {
		t.Fatalf("plain stream ack returned %v, want [m1]", got)
	}
	if msgs, _ := messages.List(ctx, "s"); len(msgs) != 0 {
		t.Fatalf("plain stream ack did not delete the message: %d left", len(msgs))
	}
}

// TestExactlyOnceUnknownAckIDOK pins the lenient branch: a decodable but unknown
// ack id on an exactly-once subscription is OK (real GCP returns a plain
// INVALID_ARGUMENT for malformed ids, which is a separate, out-of-scope gap).
func TestExactlyOnceUnknownAckIDOK(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()
	const topic = "projects/test/topics/eod-unknown-topic"
	const sub = "projects/test/subscriptions/eod-unknown-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	unknown := pubsubstore.EncodeAckID(sub, "999999", 1)
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{unknown}}); err != nil {
		t.Fatalf("Acknowledge(unknown) = %v, want OK", err)
	}
}

// TestExactlyOnceMixedBatchAcksValid pins the mixed-batch behavior: the valid
// id is acked and the superseded id is reported in the ErrorInfo detail.
func TestExactlyOnceMixedBatchAcksValid(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()
	const topic = "projects/test/topics/eod-mixed-topic"
	const sub = "projects/test/subscriptions/eod-mixed-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("mixed")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ack1 := pullOne(t, ctx, subc, sub)
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("nack: %v", err)
	}
	ack2 := pullOne(t, ctx, subc, sub)

	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1, ack2}}); err == nil {
		t.Fatal("mixed Acknowledge = nil, want InvalidArgument for the superseded id")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("mixed Acknowledge code = %v, want InvalidArgument", st.Code())
	}
	// The valid (latest) id was still acked, so the subscription is empty.
	pull, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("pull after mixed ack: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 0 {
		t.Fatalf("valid id in a mixed batch was not acked: %d delivered", len(pull.GetReceivedMessages()))
	}
}
