package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const batchCount = 32

// newPubSubClient uses the PUBSUB_EMULATOR_HOST hook (set in main) so the
// official client installs an insecure, no-auth channel.
func newPubSubClient(ctx context.Context, cfg Config) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, cfg.Project)
}

func ensureTopic(ctx context.Context, client *pubsub.Client, id string) (*pubsub.Topic, error) {
	if _, err := client.CreateTopic(ctx, id); err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("create topic %s: %w", id, err)
	}
	return client.Topic(id), nil
}

func ensureSubscription(ctx context.Context, client *pubsub.Client, id string, topic *pubsub.Topic) (*pubsub.Subscription, error) {
	if _, err := client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{Topic: topic}); err != nil &&
		status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("create subscription %s: %w", id, err)
	}
	return client.Subscription(id), nil
}

func pubsubScenarios(r *runner, f *fixtures) {
	var client *pubsub.Client
	get := func(ctx context.Context) (*pubsub.Client, error) {
		if client == nil {
			c, err := newPubSubClient(ctx, r.cfg)
			if err != nil {
				return nil, err
			}
			client = c
		}
		return client, nil
	}

	r.run("pubsub.batch_publish", "OK", func(ctx context.Context) (string, string, error) {
		c, err := get(ctx)
		if err != nil {
			return "", "", err
		}
		topic, err := ensureTopic(ctx, c, f.topic)
		if err != nil {
			return "", "", err
		}
		// Enable client-side batching: many small messages flushed together.
		topic.PublishSettings = pubsub.PublishSettings{
			DelayThreshold: 50 * time.Millisecond,
			CountThreshold: 100,
			ByteThreshold:  1 << 20,
			NumGoroutines:  4,
		}
		ids := make([]string, 0, batchCount)
		for i := 0; i < batchCount; i++ {
			res := topic.Publish(ctx, &pubsub.Message{Data: []byte(fmt.Sprintf("batch-%02d", i))})
			id, err := res.Get(ctx)
			if err != nil {
				return "", "", fmt.Errorf("publish %d: %w", i, err)
			}
			ids = append(ids, id)
		}
		topic.Stop()
		return fmt.Sprintf("%d", len(ids)), fmt.Sprintf("published=%d batching=true", len(ids)), nil
	})

	r.run("pubsub.streaming_pull_ack", "OK", func(ctx context.Context) (string, string, error) {
		c, err := get(ctx)
		if err != nil {
			return "", "", err
		}
		topic, err := ensureTopic(ctx, c, f.topic)
		if err != nil {
			return "", "", err
		}
		sub, err := ensureSubscription(ctx, c, f.sub, topic)
		if err != nil {
			return "", "", err
		}
		topic.PublishSettings = pubsub.PublishSettings{
			DelayThreshold: 50 * time.Millisecond,
			CountThreshold: 100,
			ByteThreshold:  1 << 20,
			NumGoroutines:  4,
		}
		for i := 0; i < batchCount; i++ {
			if _, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(fmt.Sprintf("stream-%02d", i))}).Get(ctx); err != nil {
				return "", "", fmt.Errorf("publish %d: %w", i, err)
			}
		}
		topic.Stop()

		// Receive is the high-level streaming-pull subscriber.
		recvCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var mu sync.Mutex
		received := 0
		done := make(chan error, 1)
		go func() {
			done <- sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
				mu.Lock()
				received++
				n := received
				mu.Unlock()
				m.Ack()
				if n >= batchCount {
					cancel()
				}
			})
		}()
		select {
		case err := <-done:
			if err != nil && recvCtx.Err() == nil {
				return "", "", fmt.Errorf("streaming pull: %w", err)
			}
		case <-ctx.Done():
			return "", "", fmt.Errorf("streaming pull timed out (received=%d)", received)
		}
		mu.Lock()
		n := received
		mu.Unlock()
		if n < batchCount {
			return "", "", fmt.Errorf("received %d messages, want %d", n, batchCount)
		}
		return fmt.Sprintf("%d", n), fmt.Sprintf("received=%d acked=%d streaming=true", n, n), nil
	})
}

func iamScenarios(r *runner, f *fixtures) {
	r.run("iam.policy_read_modify_write", "OK", func(ctx context.Context) (string, string, error) {
		c, err := newPubSubClient(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		topic, err := ensureTopic(ctx, c, f.topic)
		if err != nil {
			return "", "", err
		}
		handle := topic.IAM()
		pol, err := handle.Policy(ctx)
		if err != nil {
			return "", "", fmt.Errorf("get iam policy: %w", err)
		}
		const role, member = "roles/pubsub.publisher", "allUsers"
		pol.Add(member, role)
		if err := handle.SetPolicy(ctx, pol); err != nil {
			return "", "", fmt.Errorf("set iam policy: %w", err)
		}
		pol2, err := handle.Policy(ctx)
		if err != nil {
			return "", "", fmt.Errorf("re-get iam policy: %w", err)
		}
		members := pol2.Members(role)
		found := false
		for _, m := range members {
			if strings.TrimPrefix(m, "user:") == member || m == member {
				found = true
			}
		}
		if !found {
			return "", "", fmt.Errorf("role %s members %v do not include %s", role, members, member)
		}
		return "true", fmt.Sprintf("role=%s member=%s", role, member), nil
	})
}
