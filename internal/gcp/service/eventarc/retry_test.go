package eventarc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

// TestEventarcRetryPolicyDecoding covers the retryPolicy subset the dispatcher
// honours: maxAttempts is the total attempt count (clamped to 1-5) and
// maxRetryDuration bounds the whole window.
func TestEventarcRetryPolicyDecoding(t *testing.T) {
	cases := []struct {
		name string
		body string
		want retryPolicy
	}{
		{"absent", `{}`, retryPolicy{attempts: 1}},
		{"maxAttempts 3", `{"retryPolicy":{"maxAttempts":3}}`, retryPolicy{attempts: 3}},
		{"maxAttempts 0", `{"retryPolicy":{"maxAttempts":0}}`, retryPolicy{attempts: 1}},
		{"clamped", `{"retryPolicy":{"maxAttempts":99}}`, retryPolicy{attempts: maxEventarcAttempts}},
		{"duration", `{"retryPolicy":{"maxAttempts":2,"maxRetryDuration":"30s"}}`, retryPolicy{attempts: 2, maxDuration: 30 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal([]byte(tc.body), &body); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := eventarcRetryPolicy(body); got != tc.want {
				t.Errorf("eventarcRetryPolicy = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func newRetryService(t *testing.T, uri string, retry string) *Service {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "t")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	svc.deliveryRetryBase = time.Millisecond
	body := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"` + uri + `"}},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]` + retry + `}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", "retry", body, false); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	return svc
}

// TestDispatchRetriesFailedHTTPDelivery covers EV5: a non-function delivery is
// retried per the trigger's retryPolicy instead of being dropped on the first
// failure.
func TestDispatchRetriesFailedHTTPDelivery(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	svc := newRetryService(t, srv.URL, `,"retryPolicy":{"maxAttempts":3}`)
	svc.httpClient = srv.Client()
	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()

	if got := hits.Load(); got != 3 {
		t.Fatalf("sink received %d attempts, want 3 (two 500s then a 200)", got)
	}
}

// TestDispatchNoRetryPolicyDeliversOnce keeps the pre-EV5 contract for a trigger
// without a retryPolicy.
func TestDispatchNoRetryPolicyDeliversOnce(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	svc := newRetryService(t, srv.URL, "")
	svc.httpClient = srv.Client()
	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()

	if got := hits.Load(); got != 1 {
		t.Fatalf("sink received %d attempts, want 1 without a retryPolicy", got)
	}
}

// TestDispatchRetryBudgetExhausts checks that maxAttempts caps the attempts and
// a maxRetryDuration stops the sequence early instead of overshooting.
func TestDispatchRetryBudgetExhausts(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	svc := newRetryService(t, srv.URL, `,"retryPolicy":{"maxAttempts":2}`)
	svc.httpClient = srv.Client()
	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := hits.Load(); got != 2 {
		t.Fatalf("maxAttempts=2 produced %d attempts, want 2", got)
	}

	hits.Store(0)
	bounded := newRetryService(t, srv.URL, `,"retryPolicy":{"maxAttempts":5,"maxRetryDuration":"30ms"}`)
	bounded.deliveryRetryBase = 20 * time.Millisecond
	bounded.httpClient = srv.Client()
	bounded.DispatchEvent(context.Background(), pubsubEvent())
	bounded.waitDeliveries()
	if got := hits.Load(); got < 2 || got >= 5 {
		t.Fatalf("maxRetryDuration=30ms produced %d attempts, want 2-4", got)
	}
}
