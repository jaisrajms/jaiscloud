package pubsub

import (
	"context"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestSubscriptionRetryPolicyGRPC covers the gRPC transport's handling of
// subscription retry_policy (FD13): create persists it, UpdateSubscription
// accepts the root and nested mask paths, GetSubscription renders it, and an
// out-of-range backoff is InvalidArgument.
func TestSubscriptionRetryPolicyGRPC(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/retry-topic"
	const sub = "projects/test/subscriptions/retry-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	created, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  sub,
		Topic: topic,
		RetryPolicy: &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(10 * time.Second),
			MaximumBackoff: durationpb.New(600 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if got := created.GetRetryPolicy().GetMinimumBackoff().AsDuration(); got != 10*time.Second {
		t.Fatalf("created minimumBackoff = %v", got)
	}

	// Out of range at create time is InvalidArgument too.
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:        "projects/test/subscriptions/bad-retry",
		Topic:       topic,
		RetryPolicy: &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(700 * time.Second)},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("700s create err = %v, want InvalidArgument", err)
	}

	// Update a nested path: only minimum_backoff is carried, so
	// maximum_backoff must be preserved (AIP-161 leaf semantics).
	updated, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{
			Name:        sub,
			RetryPolicy: &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(20 * time.Second)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.minimum_backoff"}},
	})
	if err != nil {
		t.Fatalf("UpdateSubscription: %v", err)
	}
	if got := updated.GetRetryPolicy().GetMinimumBackoff().AsDuration(); got != 20*time.Second {
		t.Fatalf("updated minimumBackoff = %v", got)
	}
	if got := updated.GetRetryPolicy().GetMaximumBackoff().AsDuration(); got != 600*time.Second {
		t.Fatalf("nested update dropped maximumBackoff: %v", got)
	}

	// A later Get reads the same persisted policy.
	got, err := subc.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.GetRetryPolicy().GetMaximumBackoff().AsDuration() != 600*time.Second {
		t.Fatalf("persisted maximumBackoff = %v", got.GetRetryPolicy().GetMaximumBackoff())
	}

	// Out of range → InvalidArgument.
	if _, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{
			Name:        sub,
			RetryPolicy: &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(700 * time.Second)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("700s backoff err = %v, want InvalidArgument", err)
	}

	// Clearing via the root mask drops it.
	cleared, err := subc.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: sub},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"retry_policy"}},
	})
	if err != nil {
		t.Fatalf("clear retry_policy: %v", err)
	}
	if cleared.GetRetryPolicy() != nil {
		t.Fatalf("retryPolicy after clear = %v, want nil", cleared.GetRetryPolicy())
	}
}
