//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"fmt"
	"os"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// pubsubClient builds an official Pub/Sub v2 client pointed at the harness's
// gRPC listener. The v2 client's emulator hook installs the insecure,
// no-auth endpoint from PUBSUB_EMULATOR_HOST, so set that when the caller has
// not already wired one.
func (d *driver) pubsubClient(ctx context.Context) (*pubsub.Client, error) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		if err := os.Setenv("PUBSUB_EMULATOR_HOST", d.grpcAddr); err != nil {
			return nil, fmt.Errorf("set PUBSUB_EMULATOR_HOST: %w", err)
		}
	}
	return pubsub.NewClient(ctx, projectID())
}

// pubsubPublish publishes one message to a topic and returns its message id.
func pubsubPublish(ctx context.Context, c *pubsub.Client, topic, body string) (string, error) {
	resp, err := c.TopicAdminClient.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte(body)}},
	})
	if err != nil {
		return "", fmt.Errorf("Publish: %w", err)
	}
	if len(resp.GetMessageIds()) != 1 || resp.GetMessageIds()[0] == "" {
		return "", fmt.Errorf("Publish returned messageIds %v, want one non-empty id", resp.GetMessageIds())
	}
	return resp.GetMessageIds()[0], nil
}

// pubsubPullBody polls a subscription until a message with the given body is
// delivered, or the short deadline elapses. It does not ack.
func pubsubPullBody(ctx context.Context, c *pubsub.Client, sub, body string) (*pubsubpb.ReceivedMessage, error) {
	deadline := time.Now().Add(8 * time.Second)
	for {
		resp, err := c.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: sub, MaxMessages: 10, ReturnImmediately: true,
		})
		if err != nil {
			return nil, fmt.Errorf("Pull(%s): %w", sub, err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == body {
				return rm, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("Pull(%s) did not deliver %q within deadline", sub, body)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// pubsubListSnapshotsHas reports whether a project's snapshot listing contains
// the wanted snapshot name.
func pubsubListSnapshotsHas(ctx context.Context, c *pubsub.Client, project, want string) error {
	it := c.SubscriptionAdminClient.ListSnapshots(ctx, &pubsubpb.ListSnapshotsRequest{
		Project: "projects/" + project,
	})
	for {
		s, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListSnapshots: %w", err)
		}
		if s.GetName() == want {
			return nil
		}
	}
	return fmt.Errorf("ListSnapshots did not include %q", want)
}

// seedPubSub exercises the Pub/Sub snapshot/seek surface: it creates a topic
// and pull subscription, publishes and pulls one message, snapshots the
// subscription's (unacked) backlog, then seeks back to the snapshot so the
// message is deliverable again. The returned checks read the topic,
// subscription and snapshot back after a restart, list the snapshot, and pull
// the message again; the reset check asserts all three are gone.
func seedPubSub(d *driver, suffix string) (func() error, func() error, func() error, error) {
	project := projectID()
	topic := "projects/" + project + "/topics/parity-topic-" + suffix
	sub := "projects/" + project + "/subscriptions/parity-sub-" + suffix
	snap := "projects/" + project + "/snapshots/parity-snap-" + suffix
	body := "parity-seed-" + suffix

	seed := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		c, err := d.pubsubClient(ctx)
		if err != nil {
			return err
		}
		defer c.Close()

		if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
			return fmt.Errorf("CreateTopic: %w", err)
		}
		if _, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name: sub, Topic: topic, AckDeadlineSeconds: 10,
		}); err != nil {
			return fmt.Errorf("CreateSubscription: %w", err)
		}
		if _, err := pubsubPublish(ctx, c, topic, body); err != nil {
			return err
		}
		// Pull it so the snapshot captures a real unacked backlog, but do not
		// ack: the backlog is what Seek restores.
		if _, err := pubsubPullBody(ctx, c, sub, body); err != nil {
			return err
		}
		if _, err := c.SubscriptionAdminClient.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
			Name: snap, Subscription: sub,
		}); err != nil {
			return fmt.Errorf("CreateSnapshot: %w", err)
		}
		if _, err := c.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
			Subscription: sub,
			Target:       &pubsubpb.SeekRequest_Snapshot{Snapshot: snap},
		}); err != nil {
			return fmt.Errorf("Seek(snapshot): %w", err)
		}
		return nil
	}

	// verify runs the full post-seed observation set; it is used both
	// immediately after seeding and after the restart.
	verify := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := d.pubsubClient(ctx)
		if err != nil {
			return err
		}
		defer c.Close()

		if _, err := c.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topic}); err != nil {
			return fmt.Errorf("GetTopic: %w", err)
		}
		if _, err := c.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub}); err != nil {
			return fmt.Errorf("GetSubscription: %w", err)
		}
		if _, err := c.SubscriptionAdminClient.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snap}); err != nil {
			return fmt.Errorf("GetSnapshot: %w", err)
		}
		if err := pubsubListSnapshotsHas(ctx, c, project, snap); err != nil {
			return err
		}
		rm, err := pubsubPullBody(ctx, c, sub, body)
		if err != nil {
			return fmt.Errorf("pull after seek: %w", err)
		}
		// Release the message so the next phase (or the restart) sees the same
		// un-acked, deliverable backlog.
		if err := c.SubscriptionAdminClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription: sub, AckIds: []string{rm.GetAckId()}, AckDeadlineSeconds: 0,
		}); err != nil {
			return fmt.Errorf("ModifyAckDeadline(release): %w", err)
		}
		return nil
	}

	cleared := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := d.pubsubClient(ctx)
		if err != nil {
			return err
		}
		defer c.Close()
		if _, err := c.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topic}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetTopic after reset = %v, want NotFound", err)
		}
		if _, err := c.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetSubscription after reset = %v, want NotFound", err)
		}
		if _, err := c.SubscriptionAdminClient.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snap}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetSnapshot after reset = %v, want NotFound", err)
		}
		return nil
	}

	if err := seed(); err != nil {
		return nil, nil, nil, err
	}
	// Confirm the fixture landed before the restart, so a later "survived"
	// failure can only mean the state was lost.
	if err := verify(); err != nil {
		return nil, nil, nil, err
	}
	return verify, verify, cleared, nil
}
