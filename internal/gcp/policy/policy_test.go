package policy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"jaiscloud/internal/store"
)

func TestSetIamPolicyEtagOCC(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()

	body := map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{"role": "roles/pubsub.publisher", "members": []any{"user:a@example.com"}}},
		},
	}

	pol, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if pol.Etag == "" || pol.Etag == DefaultEtag {
		t.Fatalf("expected fresh etag, got %q", pol.Etag)
	}

	// A set with a stale etag must be rejected.
	body["policy"].(map[string]any)["etag"] = "stale"
	if _, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body); err == nil {
		t.Fatal("expected 409 on stale etag")
	}

	// A set with the correct etag must succeed.
	body["policy"].(map[string]any)["etag"] = pol.Etag
	if _, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body); err != nil {
		t.Fatalf("set with current etag: %v", err)
	}
}

// TestSetIamPolicyConcurrentSameEtagOnlyOneWins proves Set's etag check and
// its write happen atomically. Every goroutine reads the SAME starting etag
// (as a real client pair racing on the same base policy would) and calls Set
// with it as the precondition. Only one may succeed: if Load-check-Upsert
// were not atomic, every goroutine's check would pass against the same
// pre-race etag before any of them wrote, and all would report success even
// though only the last physical write survives — a silent lost update.
func TestSetIamPolicyConcurrentSameEtagOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()

	initial, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
		"policy": map[string]any{"bindings": []any{}},
	})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	const n = 50
	var successCount int64
	var wg sync.WaitGroup
	results := make([]Policy, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := map[string]any{
				"policy": map[string]any{
					"etag": initial.Etag,
					"bindings": []any{map[string]any{
						"role":    "roles/pubsub.publisher",
						"members": []any{fmt.Sprintf("user:%d@example.com", i)},
					}},
				},
			}
			pol, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body)
			results[i] = pol
			errs[i] = err
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 of %d concurrent same-etag Set calls to succeed, got %d", n, successCount)
	}

	// The stored policy must match the one caller that reported success —
	// not some other goroutine's overwritten-and-lost bindings.
	final := Load(ctx, s, "proj", "gcp_topic_policy", "t1")
	for i, err := range errs {
		if err == nil && final.Etag != results[i].Etag {
			t.Fatalf("stored etag %q does not match the reported winner's etag %q", final.Etag, results[i].Etag)
		}
	}
}

func TestTestPermissionsEchoesRequest(t *testing.T) {
	perms := []string{"pubsub.topics.publish", "pubsub.topics.get"}
	got := TestPermissions(perms)
	if len(got) != len(perms) {
		t.Fatalf("expected %d granted permissions, got %d", len(perms), len(got))
	}
	for i, p := range perms {
		if got[i] != p {
			t.Errorf("granted[%d] = %q, want %q", i, got[i], p)
		}
	}
}
