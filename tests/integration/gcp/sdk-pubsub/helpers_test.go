// Package sdkpubsub_test is a behavioural depth suite for Cloud Pub/Sub. It
// drives the emulator through the official Pub/Sub v2 client over gRPC
// (PUBSUB_EMULATOR_HOST) and pins the delivery contract the emulator documents
// in README-GCP: ordering keys, exactly-once ack ids, ack-deadline redelivery,
// dead-letter forwarding, retention/snapshot/seek and push delivery.
//
// The suite asserts only what is documented as modelled — never best-effort
// timing, interleavings or numeric DeliveryCount values.
package sdkpubsub_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// emulatorHost returns the gRPC endpoint the official client should target. CI
// sets PUBSUB_EMULATOR_HOST; a local run defaults to the emulator's gRPC port.
func emulatorHost() string {
	if h := os.Getenv("PUBSUB_EMULATOR_HOST"); h != "" {
		return h
	}
	return "localhost:8081"
}

// projectID returns the project the suite operates in.
func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "test-project"
}

// randSuffix returns a short random hex string so every test run/resource name
// is unique and a re-run never collides with a previous run's state.
func randSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// newClient builds an official Pub/Sub v2 client pointed at the emulator's
// gRPC listener. The client's emulator hook installs an insecure, no-auth
// channel from PUBSUB_EMULATOR_HOST.
func newClient(t *testing.T) (*pubsub.Client, context.Context) {
	t.Helper()
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		if err := os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost()); err != nil {
			t.Fatalf("set PUBSUB_EMULATOR_HOST: %v", err)
		}
	}
	ctx := context.Background()
	c, err := pubsub.NewClient(ctx, projectID())
	if err != nil {
		t.Fatalf("pubsub.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, ctx
}

// topicName returns the unique full topic name for this test.
func topicName(prefix string) string {
	return fmt.Sprintf("projects/%s/topics/%s-%s", projectID(), prefix, randSuffix())
}

// subName returns the unique full subscription name for this test.
func subName(prefix string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s-%s", projectID(), prefix, randSuffix())
}

// snapName returns the unique full snapshot name for this test.
func snapName(prefix string) string {
	return fmt.Sprintf("projects/%s/snapshots/%s-%s", projectID(), prefix, randSuffix())
}

// createTopic creates a topic and registers cleanup-safe teardown.
func createTopic(t *testing.T, c *pubsub.Client, ctx context.Context, name string) {
	t.Helper()
	if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
		t.Fatalf("CreateTopic(%s): %v", name, err)
	}
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.TopicAdminClient.DeleteTopic(dctx, &pubsubpb.DeleteTopicRequest{Topic: name})
	})
}

// createSub creates a subscription over an existing topic.
func createSub(t *testing.T, c *pubsub.Client, ctx context.Context, name, topic string, mutate func(*pubsubpb.Subscription)) {
	t.Helper()
	sub := &pubsubpb.Subscription{Name: name, Topic: topic, AckDeadlineSeconds: 30}
	if mutate != nil {
		mutate(sub)
	}
	if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, sub); err != nil {
		t.Fatalf("CreateSubscription(%s): %v", name, err)
	}
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.SubscriptionAdminClient.DeleteSubscription(dctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: name})
	})
}

// publish sends one message and returns the server-assigned message id. An
// optional mutate hook customizes the message (attributes, ordering key).
func publish(t *testing.T, c *pubsub.Client, ctx context.Context, topic, body string, mutate ...func(*pubsubpb.PubsubMessage)) string {
	t.Helper()
	msg := &pubsubpb.PubsubMessage{Data: []byte(body)}
	if len(mutate) > 0 && mutate[0] != nil {
		mutate[0](msg)
	}
	resp, err := c.TopicAdminClient.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{msg},
	})
	if err != nil {
		t.Fatalf("Publish(%s): %v", topic, err)
	}
	if len(resp.GetMessageIds()) != 1 || resp.GetMessageIds()[0] == "" {
		t.Fatalf("Publish returned messageIds %v, want one non-empty id", resp.GetMessageIds())
	}
	return resp.GetMessageIds()[0]
}

// pullOnce issues a single immediate Pull and returns the received messages.
func pullOnce(t *testing.T, c *pubsub.Client, ctx context.Context, sub string, max int32) []*pubsubpb.ReceivedMessage {
	t.Helper()
	resp, err := c.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: sub, MaxMessages: max, ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull(%s): %v", sub, err)
	}
	return resp.GetReceivedMessages()
}

// pulledBodies maps received messages to their decoded bodies.
func pulledBodies(rms []*pubsubpb.ReceivedMessage) []string {
	out := make([]string, 0, len(rms))
	for _, rm := range rms {
		out = append(out, string(rm.GetMessage().GetData()))
	}
	return out
}

// waitForBody polls a subscription until a message whose body equals want is
// delivered, returning it. It does not ack. It fails the test on timeout.
func waitForBody(t *testing.T, c *pubsub.Client, ctx context.Context, sub, want string, timeout time.Duration) *pubsubpb.ReceivedMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, rm := range pullOnce(t, c, ctx, sub, 10) {
			if string(rm.GetMessage().GetData()) == want {
				return rm
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Pull(%s) never delivered %q within %s", sub, want, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// release hands a message straight back to the subscription (ModifyAckDeadline
// 0) so the next pull redelivers it immediately.
func release(t *testing.T, c *pubsub.Client, ctx context.Context, sub, ackID string) {
	t.Helper()
	if err := c.SubscriptionAdminClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ackID}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("ModifyAckDeadline(release %s): %v", sub, err)
	}
}

// ack acknowledges one message.
func ack(t *testing.T, c *pubsub.Client, ctx context.Context, sub, ackID string) {
	t.Helper()
	if err := c.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: sub, AckIds: []string{ackID},
	}); err != nil {
		t.Fatalf("Acknowledge(%s): %v", sub, err)
	}
}
