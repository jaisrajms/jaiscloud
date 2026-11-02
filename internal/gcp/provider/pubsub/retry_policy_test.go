package pubsub

import (
	"context"
	"testing"
	"time"
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

	// The delivery scheduler consumes the patched policy through the
	// SubscriptionProvisioner seam; no restart or re-create is needed.
	min, max, _, ok, err := p.SubscriptionRetryPolicy(ctx, "proj", "retry-sub")
	if err != nil || !ok || min != 20*time.Second || max != 120*time.Second {
		t.Fatalf("retry policy read back = %v..%v ok=%v err=%v", min, max, ok, err)
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
	if _, _, _, ok, err := p.SubscriptionRetryPolicy(ctx, "proj", "retry-sub"); err != nil || ok {
		t.Fatalf("cleared retry policy read = ok=%v err=%v, want ok=false", ok, err)
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

// TestSubscriptionCreateNullRetryPolicy covers the hashicorp/google provider
// shape: a JSON null for an absent nested block means "unset" and must be
// accepted (real GCP treats null as unset), unlike a non-null non-object.
func TestSubscriptionCreateNullRetryPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")

	// The provider emits an explicit null for every absent nested block.
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/null-sub",
		"body": map[string]any{
			"topic":            "projects/proj/topics/src",
			"retryPolicy":      nil,
			"deadLetterPolicy": nil,
			"expirationPolicy": nil,
		},
	})); err != nil {
		t.Fatalf("create with retryPolicy:null: %v", err)
	}
	if rp := retryPolicyOf(t, p, "null-sub"); rp != nil {
		t.Fatalf("retryPolicy = %v, want unset", rp)
	}

	// An array is still a type error (not a message).
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/arr-sub",
		"body": map[string]any{"topic": "projects/proj/topics/src", "retryPolicy": []any{}},
	})); err == nil {
		t.Fatal("expected InvalidArgument for an array retryPolicy")
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

// TestSubscriptionRetryPolicyMaxDoublings covers the emulator-extension
// maxDoublings leaf on the subscription patch mask and the read path the
// delivery scheduler uses.
func TestSubscriptionRetryPolicyMaxDoublings(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/md-sub",
		"body": map[string]any{
			"topic": "projects/proj/topics/src",
			"retryPolicy": map[string]any{
				"minimumBackoff": "0.010s",
				"maximumBackoff": "0.080s",
				"maxDoublings":   1,
			},
		},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	min, max, md, ok, err := p.SubscriptionRetryPolicy(ctx, "proj", "md-sub")
	if err != nil || !ok || min != 10*time.Millisecond || max != 80*time.Millisecond || md != 1 {
		t.Fatalf("retry policy = %v..%v md=%d ok=%v err=%v", min, max, md, ok, err)
	}

	// A nested mask updates just maxDoublings.
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/md-sub",
		"body": map[string]any{
			"subscription": map[string]any{
				"retryPolicy": map[string]any{"maxDoublings": 2},
			},
			"updateMask": "retry_policy.max_doublings",
		},
	})); err != nil {
		t.Fatalf("patch maxDoublings: %v", err)
	}
	if _, _, md, ok, err := p.SubscriptionRetryPolicy(ctx, "proj", "md-sub"); err != nil || !ok || md != 2 {
		t.Fatalf("maxDoublings after patch = %d ok=%v err=%v", md, ok, err)
	}

	// Negative and fractional values are rejected.
	for _, bad := range []any{-1, 1.5} {
		if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
			"name": "subscriptions/md-sub",
			"body": map[string]any{
				"subscription": map[string]any{
					"retryPolicy": map[string]any{"maxDoublings": bad},
				},
				"updateMask": "retry_policy.max_doublings",
			},
		})); err == nil {
			t.Errorf("maxDoublings %v accepted, want InvalidArgument", bad)
		}
	}
}
