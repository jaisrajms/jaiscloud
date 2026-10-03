package pubsub

import (
	"context"
	"testing"
)

// TestSubscriptionRetryPolicyRoundTrip covers the REST transport's handling of
// subscription retryPolicy (FD13): it is validated on create and patch,
// persisted, and returned on get.
func TestSubscriptionRetryPolicyRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")

	// Create with a retry policy.
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/retry-sub",
		"body": map[string]any{
			"topic": "projects/proj/topics/src",
			"retryPolicy": map[string]any{
				"minimumBackoff": "10s",
				"maximumBackoff": "600s",
			},
		},
	})); err != nil {
		t.Fatalf("create with retryPolicy: %v", err)
	}
	rp := retryPolicyOf(t, p, "retry-sub")
	if rp["minimumBackoff"] != "10s" || rp["maximumBackoff"] != "600s" {
		t.Fatalf("retryPolicy after create = %v", rp)
	}

	// Patch retry_policy.minimum_backoff (REST UpdateSubscriptionRequest shape):
	// a nested mask updates only that leaf and preserves maximumBackoff.
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/retry-sub",
		"body": map[string]any{
			"subscription": map[string]any{
				"retryPolicy": map[string]any{"minimumBackoff": "20s"},
			},
			"updateMask": "retry_policy.minimum_backoff",
		},
	})); err != nil {
		t.Fatalf("patch retryPolicy: %v", err)
	}
	rp = retryPolicyOf(t, p, "retry-sub")
	if rp["minimumBackoff"] != "20s" || rp["maximumBackoff"] != "600s" {
		t.Fatalf("nested patch must preserve the other leaf: %v", rp)
	}

	// A camelCase nested mask resolves the same way.
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/retry-sub",
		"body": map[string]any{
			"subscription": map[string]any{
				"retryPolicy": map[string]any{"maximumBackoff": "120s"},
			},
			"updateMask": "retryPolicy.maximumBackoff",
		},
	})); err != nil {
		t.Fatalf("camelCase nested patch: %v", err)
	}
	rp = retryPolicyOf(t, p, "retry-sub")
	if rp["minimumBackoff"] != "20s" || rp["maximumBackoff"] != "120s" {
		t.Fatalf("camelCase nested patch = %v", rp)
	}

	// An out-of-range backoff is InvalidArgument.
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/retry-sub",
		"body": map[string]any{
			"subscription": map[string]any{
				"retryPolicy": map[string]any{"minimumBackoff": "601s"},
			},
			"updateMask": "retry_policy",
		},
	})); err == nil {
		t.Fatal("expected InvalidArgument for a 601s backoff")
	}

	// Clearing retryPolicy removes it.
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name":       "subscriptions/retry-sub",
		"body":       map[string]any{"updateMask": "retry_policy"},
		"updateMask": "retry_policy",
	})); err != nil {
		t.Fatalf("clear retryPolicy: %v", err)
	}
	if rp := retryPolicyOf(t, p, "retry-sub"); rp != nil {
		t.Fatalf("retryPolicy after clear = %v, want nil", rp)
	}
}

// TestSubscriptionCreateRejectsBadRetryPolicy covers the create-time validation.
func TestSubscriptionCreateRejectsBadRetryPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")
	for _, bad := range []string{"10m", "-1s", "601s", "10", "abc", "36028797018963968s"} {
		_, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
			"name": "subscriptions/bad-sub",
			"body": map[string]any{
				"topic":       "projects/proj/topics/src",
				"retryPolicy": map[string]any{"minimumBackoff": bad},
			},
		}))
		if err == nil {
			t.Fatalf("backoff %q: expected InvalidArgument", bad)
		}
	}

	// A non-object retryPolicy is InvalidArgument, not silently ignored.
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/bad-shape",
		"body": map[string]any{"topic": "projects/proj/topics/src", "retryPolicy": "10s"},
	})); err == nil {
		t.Fatal("expected InvalidArgument for a non-object retryPolicy")
	}
}

// retryPolicyOf reads a subscription's persisted retryPolicy (nil when unset).
func retryPolicyOf(t *testing.T, p *Provider, sub string) map[string]any {
	t.Helper()
	resp, err := p.SubscriptionGet(context.Background(), newNR(map[string]any{"name": "subscriptions/" + sub}))
	if err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	rp, _ := resp.Data["retryPolicy"].(map[string]any)
	return rp
}
