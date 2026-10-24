package sdkpubsub_test

import (
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// assertExactlyOnceAckFailure asserts that err is the real-GCP exactly-once
// ack-id rejection: INVALID_ARGUMENT carrying a google.rpc.ErrorInfo with
// reason EXACTLY_ONCE_ACKID_FAILURE and metadata mapping the rejected ack id to
// PERMANENT_FAILURE_INVALID_ACK_ID.
func assertExactlyOnceAckFailure(t *testing.T, err error, ackID string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Acknowledge with a superseded exactly-once ack id succeeded, want INVALID_ARGUMENT")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge err = %v, want INVALID_ARGUMENT", err)
	}
	for _, d := range st.Details() {
		ei, ok := d.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if ei.GetReason() != "EXACTLY_ONCE_ACKID_FAILURE" {
			continue
		}
		if got := ei.GetMetadata()[ackID]; got != "PERMANENT_FAILURE_INVALID_ACK_ID" {
			t.Fatalf("ErrorInfo metadata = %v, want %q mapped to PERMANENT_FAILURE_INVALID_ACK_ID", ei.GetMetadata(), ackID)
		}
		return
	}
	t.Fatalf("Acknowledge err %v has no ErrorInfo reason EXACTLY_ONCE_ACKID_FAILURE (details: %v)", err, st.Details())
}

// TestPubSubExactlyOnceAckIDs pins the exactly-once delivery contract: a
// superseded (redelivered) ack id is rejected with the documented
// EXACTLY_ONCE_ACKID_FAILURE, the current ack id is accepted, and re-acking an
// already-acked id is idempotently OK.
func TestPubSubExactlyOnceAckIDs(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("eo-topic")
	sub := subName("eo-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, func(s *pubsubpb.Subscription) {
		s.EnableExactlyOnceDelivery = true
		s.AckDeadlineSeconds = 30
	})

	publish(t, c, ctx, topic, "eo-1")

	rm1 := waitForBody(t, c, ctx, sub, "eo-1", 10*time.Second)
	ackID1 := rm1.GetAckId()
	if ackID1 == "" {
		t.Fatalf("first delivery returned an empty ack id")
	}

	// Release so the message is redelivered with a newer ack id.
	release(t, c, ctx, sub, ackID1)
	rm2 := waitForBody(t, c, ctx, sub, "eo-1", 10*time.Second)
	ackID2 := rm2.GetAckId()
	if ackID2 == ackID1 {
		t.Fatalf("redelivery reused ack id %q, want a fresh id", ackID1)
	}

	// The superseded id must be rejected.
	if err := c.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: sub, AckIds: []string{ackID1},
	}); err == nil {
		t.Fatalf("ack of superseded ack id %q succeeded, want INVALID_ARGUMENT", ackID1)
	} else {
		assertExactlyOnceAckFailure(t, err, ackID1)
	}

	// The current id is accepted, and a repeat ack is OK.
	ack(t, c, ctx, sub, ackID2)
	ack(t, c, ctx, sub, ackID2)
}

// TestPubSubPlainAckIDsArePermissive pins the documented plain-subscription
// counterpart: without exactly-once, any decodable ack id is accepted.
func TestPubSubPlainAckIDsArePermissive(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("plain-topic")
	sub := subName("plain-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, nil)

	publish(t, c, ctx, topic, "plain-1")
	rm1 := waitForBody(t, c, ctx, sub, "plain-1", 10*time.Second)
	release(t, c, ctx, sub, rm1.GetAckId())

	rm2 := waitForBody(t, c, ctx, sub, "plain-1", 10*time.Second)
	// A plain subscription accepts the superseded id (it is still decodable).
	if err := c.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: sub, AckIds: []string{rm1.GetAckId()},
	}); err != nil {
		t.Fatalf("plain subscription rejected a decodable superseded ack id: %v", err)
	}
	// And a re-ack of the current id is OK too.
	ack(t, c, ctx, sub, rm2.GetAckId())
}
