package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// providerError returns the canonical ProviderError behind err (nil when err is
// not one).
func providerError(err error) *model.ProviderError {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

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

// TestSetConditionalBindingInfersVersion3 covers Terraform's shape: a binding
// carrying a condition and no version field at all. IAM (AIP-149) mandates
// version 3 for conditions, so the stored policy must be version 3 and the
// condition object must round-trip verbatim.
func TestSetConditionalBindingInfersVersion3(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()
	condition := map[string]any{
		"title":       "expires-soon",
		"description": "temporary access",
		"expression":  `request.time < timestamp("2027-01-01T00:00:00Z")`,
	}
	binding := map[string]any{
		"role":      "roles/viewer",
		"members":   []any{"user:a@example.com"},
		"condition": condition,
	}

	got, err := Set(ctx, s, "proj", "gcp_project_policy", "proj", map[string]any{
		"policy": map[string]any{"bindings": []any{binding}},
	})
	if err != nil {
		t.Fatalf("set conditional policy: %v", err)
	}
	if got.Version != 3 {
		t.Fatalf("stored version = %d, want 3", got.Version)
	}

	stored := Load(ctx, s, "proj", "gcp_project_policy", "proj")
	if stored.Version != 3 {
		t.Fatalf("loaded version = %d, want 3", stored.Version)
	}
	if len(stored.Bindings) != 1 {
		t.Fatalf("loaded %d bindings, want 1", len(stored.Bindings))
	}
	bm, _ := stored.Bindings[0].(map[string]any)
	cm, _ := bm["condition"].(map[string]any)
	if cm["title"] != condition["title"] || cm["expression"] != condition["expression"] || cm["description"] != condition["description"] {
		t.Fatalf("condition did not round-trip verbatim: %v", cm)
	}

	rendered := ToMap(stored)
	if rendered["version"] != 3 {
		t.Fatalf("rendered version = %v, want 3", rendered["version"])
	}
	rb := rendered["bindings"].([]any)[0].(map[string]any)
	if _, ok := rb["condition"].(map[string]any); !ok {
		t.Fatalf("rendered binding lost its condition: %v", rb)
	}
}

// TestSetUnconditionalVersionDefaultsTo1 covers the unconditional cases:
// absent/0 stays 1, an explicit 3 is preserved, an explicit 1 stays 1, and no
// unconditional policy is silently upgraded.
func TestSetUnconditionalVersionDefaultsTo1(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		body map[string]any
		want int
	}{
		{"absent", map[string]any{"policy": map[string]any{"bindings": []any{}}}, 1},
		{"zero", map[string]any{"policy": map[string]any{"version": float64(0), "bindings": []any{}}}, 1},
		{"one", map[string]any{"policy": map[string]any{"version": float64(1), "bindings": []any{}}}, 1},
		{"three", map[string]any{"policy": map[string]any{"version": float64(3), "bindings": []any{}}}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewMemoryResourceStore()
			got, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", tc.body)
			if err != nil {
				t.Fatalf("set: %v", err)
			}
			if got.Version != tc.want {
				t.Fatalf("version = %d, want %d", got.Version, tc.want)
			}
			if loaded := Load(ctx, s, "proj", "gcp_topic_policy", "t1"); loaded.Version != tc.want {
				t.Fatalf("loaded version = %d, want %d", loaded.Version, tc.want)
			}
		})
	}
}

// TestSetConditionWithVersion1Rejected covers AIP-149: a request that touches
// conditional role bindings must specify version 3, so an explicit version 1 is
// INVALID_ARGUMENT (HTTP 400).
func TestSetConditionWithVersion1Rejected(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()
	_, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
		"policy": map[string]any{
			"version": float64(1),
			"bindings": []any{map[string]any{
				"role":      "roles/viewer",
				"members":   []any{"user:a@example.com"},
				"condition": map[string]any{"title": "t", "expression": "true"},
			}},
		},
	})
	pe := providerError(err)
	if pe == nil || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("condition+version 1 = %v, want InvalidArgument/400", err)
	}
	// No policy row may be created by a rejected write.
	if pol := Load(ctx, s, "proj", "gcp_topic_policy", "t1"); len(pol.Bindings) != 0 {
		t.Fatalf("rejected write left bindings behind: %v", pol.Bindings)
	}
}

// TestSetInvalidVersionRejected covers versions IAM does not define (2,
// negative, non-integral, non-numeric).
func TestSetInvalidVersionRejected(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		version any
	}{
		{"two", float64(2)},
		{"negative", float64(-1)},
		{"fractional", 1.5},
		{"string", "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewMemoryResourceStore()
			_, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
				"policy": map[string]any{"version": tc.version, "bindings": []any{}},
			})
			pe := providerError(err)
			if pe == nil || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
				t.Fatalf("version %v = %v, want InvalidArgument/400", tc.version, err)
			}
		})
	}
}

// TestSetStaleEtagAborted pins the etag OCC contract: a stale etag is ABORTED
// (HTTP 409) regardless of the version/condition rules.
func TestSetStaleEtagAborted(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()
	first, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/viewer", "members": []any{"user:a@example.com"},
		}}},
	})
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}
	_, err = Set(ctx, s, "proj", "gcp_topic_policy", "t1", map[string]any{
		"policy": map[string]any{
			"etag":     "stale",
			"version":  float64(3),
			"bindings": []any{map[string]any{"role": "roles/editor", "members": []any{"user:b@example.com"}}},
		},
	})
	pe := providerError(err)
	if pe == nil || pe.Status != "ABORTED" || pe.HTTPStatus != 409 {
		t.Fatalf("stale etag = %v, want ABORTED/409", err)
	}
	if Load(ctx, s, "proj", "gcp_topic_policy", "t1").Etag != first.Etag {
		t.Fatal("rejected write changed the stored policy")
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
