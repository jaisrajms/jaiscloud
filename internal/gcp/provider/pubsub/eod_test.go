package pubsub

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
)

// TestSubscriptionExactlyOnceDelivery verifies the REST surface honors
// enableExactlyOnceDelivery: it round-trips, defaults the ack deadline to 60s,
// is omitted when false (matching real Pub/Sub's JSON), and rejects the
// unsupported push + exactly-once combination.
func TestSubscriptionExactlyOnceDelivery(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/eod"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Exactly-once, no explicit ack deadline → 60-second default.
	resp, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eod",
		"body": map[string]any{
			"topic":                     "projects/proj/topics/eod",
			"enableExactlyOnceDelivery": true,
		},
	}))
	if err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if eod, _ := resp.Data["enableExactlyOnceDelivery"].(bool); !eod {
		t.Fatalf("enableExactlyOnceDelivery = %v, want true", resp.Data["enableExactlyOnceDelivery"])
	}
	if ad, _ := resp.Data["ackDeadlineSeconds"].(int); ad != 60 {
		t.Fatalf("ackDeadlineSeconds = %v, want 60 for exactly-once", resp.Data["ackDeadlineSeconds"])
	}

	// Get echoes it.
	got, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/eod"}))
	if err != nil {
		t.Fatalf("subscription get: %v", err)
	}
	if eod, _ := got.Data["enableExactlyOnceDelivery"].(bool); !eod {
		t.Fatalf("get enableExactlyOnceDelivery = %v, want true", got.Data["enableExactlyOnceDelivery"])
	}

	// A plain subscription omits the flag entirely (real Pub/Sub drops false
	// booleans; the differential goldens depend on this).
	plain, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/plain",
		"body": map[string]any{"topic": "projects/proj/topics/eod"},
	}))
	if err != nil {
		t.Fatalf("plain subscription create: %v", err)
	}
	if _, present := plain.Data["enableExactlyOnceDelivery"]; present {
		t.Fatal("plain subscription response must omit enableExactlyOnceDelivery")
	}

	// Exactly-once + push is unsupported.
	_, err = p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eod-push",
		"body": map[string]any{
			"topic":                     "projects/proj/topics/eod",
			"enableExactlyOnceDelivery": true,
			"pushConfig":                map[string]any{"pushEndpoint": "https://example.invalid/push"},
		},
	}))
	if err == nil {
		t.Fatal("exactly-once + push create = nil, want InvalidArgument")
	}
	if status := errStatus(err); status != 400 {
		t.Fatalf("exactly-once + push HTTP status = %d, want 400 (%v)", status, err)
	}
}

// TestSubscriptionAckIDDedup verifies an acked message is not redelivered over
// REST and that the emulator treats a repeat of the same ack id as an
// idempotent no-op (see the gRPC ackRegistry limitation note on superseded ack
// ids).
func TestSubscriptionAckIDDedup(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/ack"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/ack",
		"body": map[string]any{
			"topic":                     "projects/proj/topics/ack",
			"enableExactlyOnceDelivery": true,
		},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/ack",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	pull, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/ack"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := pull.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("pull received %d messages, want 1", len(received))
	}
	ackID := received[0].(map[string]any)["ackId"].(string)

	for i := 0; i < 2; i++ {
		if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
			"name": "subscriptions/ack",
			"body": map[string]any{"ackIds": []any{ackID}},
		})); err != nil {
			t.Fatalf("ack #%d: %v", i+1, err)
		}
	}
	after, err := p.SubscriptionPull(ctx, newNR(map[string]any{
		"name": "subscriptions/ack",
		"body": map[string]any{"returnImmediately": true},
	}))
	if err != nil {
		t.Fatalf("pull after ack: %v", err)
	}
	if got, _ := after.Data["receivedMessages"].([]any); len(got) != 0 {
		t.Fatalf("acked message redelivered: %d", len(got))
	}
}

// restPullOne polls a subscription over the REST surface until one message is
// delivered, returning its ack id.
func restPullOne(t *testing.T, ctx context.Context, p *Provider, sub string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{
			"name": sub, "body": map[string]any{"returnImmediately": true},
		}))
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		received, _ := resp.Data["receivedMessages"].([]any)
		if len(received) == 1 {
			return received[0].(map[string]any)["ackId"].(string)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no message delivered")
	return ""
}

// assertEODFailure checks err is real GCP's exactly-once invalid-ack-ID
// INVALID_ARGUMENT carrying the ErrorInfo detail for ackID.
func assertEODFailure(t *testing.T, err error, ackID string) {
	t.Helper()
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("error = %v (%T), want *model.ProviderError", err, err)
	}
	if pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("error = (%s, %d), want (InvalidArgument, 400)", pe.Code, pe.HTTPStatus)
	}
	if pe.Message != pubsubstore.EODAckFailureMessage {
		t.Fatalf("message = %q", pe.Message)
	}
	if len(pe.Details) != 1 {
		t.Fatalf("details = %v, want 1 ErrorInfo", pe.Details)
	}
	d := pe.Details[0]
	if d.Type != "google.rpc.ErrorInfo" || d.Reason != pubsubstore.EODAckFailureReason || d.Domain != pubsubstore.EODAckFailureDomain {
		t.Fatalf("detail = %+v", d)
	}
	if d.Metadata[ackID] != pubsubstore.InvalidAckIDValue {
		t.Fatalf("metadata = %v, want [%s]=%s", d.Metadata, ackID, pubsubstore.InvalidAckIDValue)
	}
}

// TestSubscriptionExactlyOnceAckIDVersioning pins the REST half of EOD1:
// superseded/expired ack ids on an exactly-once subscription are INVALID_ARGUMENT
// with the ErrorInfo envelope, while the latest id (and a re-ack) is OK.
func TestSubscriptionExactlyOnceAckIDVersioning(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/eodv"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eodv",
		"body": map[string]any{"topic": "projects/proj/topics/eodv", "enableExactlyOnceDelivery": true},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/eodv", "body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ack1 := restPullOne(t, ctx, p, "subscriptions/eodv")
	if _, err := p.SubscriptionModifyAckDeadline(ctx, newNR(map[string]any{
		"name": "subscriptions/eodv", "body": map[string]any{"ackIds": []any{ack1}, "ackDeadlineSeconds": float64(0)},
	})); err != nil {
		t.Fatalf("nack: %v", err)
	}
	ack2 := restPullOne(t, ctx, p, "subscriptions/eodv")
	if ack1 == ack2 {
		t.Fatal("re-pull returned the same ack id; version was not encoded")
	}

	// Superseded ack id → INVALID_ARGUMENT + ErrorInfo.
	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/eodv", "body": map[string]any{"ackIds": []any{ack1}},
	})); err == nil {
		t.Fatal("ack(superseded) = nil, want InvalidArgument")
	} else {
		assertEODFailure(t, err, ack1)
	}
	// Superseded modack → same envelope.
	if _, err := p.SubscriptionModifyAckDeadline(ctx, newNR(map[string]any{
		"name": "subscriptions/eodv", "body": map[string]any{"ackIds": []any{ack1}, "ackDeadlineSeconds": float64(30)},
	})); err == nil {
		t.Fatal("modack(superseded) = nil, want InvalidArgument")
	} else {
		assertEODFailure(t, err, ack1)
	}

	// Latest ack id is OK; a re-ack is an idempotent OK.
	for i := 0; i < 2; i++ {
		if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
			"name": "subscriptions/eodv", "body": map[string]any{"ackIds": []any{ack2}},
		})); err != nil {
			t.Fatalf("ack(latest) #%d: %v", i+1, err)
		}
	}
}

// TestSubscriptionSeekInvalidatesAckID pins the REST half of EOD2: the ack-ID
// version a Seek advances is honored by the REST ack path. Seek itself is
// gRPC-only today, so the test applies the same store mutations a Seek performs
// (make the message visible, then advance its delivery version) and checks the
// REST ack path rejects the pre-Seek id.
func TestSubscriptionSeekInvalidatesAckID(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/eods"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eods",
		"body": map[string]any{"topic": "projects/proj/topics/eods", "enableExactlyOnceDelivery": true},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/eods", "body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ack1 := restPullOne(t, ctx, p, "subscriptions/eods")
	_, msgID, _, ok := pubsubstore.DecodeAckID(ack1)
	if !ok {
		t.Fatalf("ack id %q did not decode", ack1)
	}
	// Seek's store effect: make the message visible, then advance its version.
	if err := p.messages.ModifyAckDeadline(ctx, "eods", []string{"eods/" + msgID}, 0, clock.Now()); err != nil {
		t.Fatalf("seek visibility reset: %v", err)
	}
	if err := p.messages.BumpDeliveryVersions(ctx, "eods", []string{msgID}); err != nil {
		t.Fatalf("seek version bump: %v", err)
	}

	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/eods", "body": map[string]any{"ackIds": []any{ack1}},
	})); err == nil {
		t.Fatal("ack(pre-Seek) = nil, want InvalidArgument")
	} else {
		assertEODFailure(t, err, ack1)
	}

	// The restored message is redelivered with a fresh ack id that works.
	ack2 := restPullOne(t, ctx, p, "subscriptions/eods")
	if ack2 == ack1 {
		t.Fatal("post-Seek re-pull returned the same ack id; version was not advanced")
	}
	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/eods", "body": map[string]any{"ackIds": []any{ack2}},
	})); err != nil {
		t.Fatalf("ack(post-Seek): %v", err)
	}
}

// TestSubscriptionExactlyOnceAckIDExpired pins the expired-ID branch with a
// frozen clock advanced past the ack deadline.
func TestSubscriptionExactlyOnceAckIDExpired(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(nil)

	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/eode"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eode",
		"body": map[string]any{"topic": "projects/proj/topics/eode", "enableExactlyOnceDelivery": true},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/eode", "body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	ack := restPullOne(t, ctx, p, "subscriptions/eode")
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(61 * time.Second)})
	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/eode", "body": map[string]any{"ackIds": []any{ack}},
	})); err == nil {
		t.Fatal("ack(expired) = nil, want InvalidArgument")
	} else {
		assertEODFailure(t, err, ack)
	}
}
