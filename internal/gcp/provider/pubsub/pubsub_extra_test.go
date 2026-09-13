package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestPubSubNegativesAndPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Create three topics for pagination.
	for _, id := range []string{"a", "b", "c"} {
		nr := newNR(map[string]any{"name": "topics/" + id})
		if _, err := p.TopicCreate(ctx, nr); err != nil {
			t.Fatalf("create topic %s: %v", id, err)
		}
	}

	// Page 1.
	nr := newNR(map[string]any{"pageSize": "2"})
	resp, err := p.TopicList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	topics, _ := resp.Data["topics"].([]any)
	if len(topics) != 2 {
		t.Fatalf("page 1 expected 2 topics, got %d", len(topics))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken on page 1")
	}

	// Page 2.
	nr = newNR(map[string]any{"pageSize": "2", "pageToken": token})
	resp, err = p.TopicList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	topics, _ = resp.Data["topics"].([]any)
	if len(topics) != 1 {
		t.Fatalf("page 2 expected 1 topic, got %d", len(topics))
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("expected no nextPageToken on final page")
	}

	// 409 on duplicate create.
	nr = newNR(map[string]any{"name": "topics/a"})
	if _, err := p.TopicCreate(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate topic, got %v", err)
	}

	// 404 on missing get/delete.
	nr = newNR(map[string]any{"name": "topics/missing"})
	if _, err := p.TopicGet(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing topic get, got %v", err)
	}
	if _, err := p.TopicDelete(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing topic delete, got %v", err)
	}

	// 400 on missing name.
	nr = newNR(nil)
	if _, err := p.TopicGet(ctx, nr); err == nil || errStatus(err) != 400 {
		t.Fatalf("expected 400 on missing name, got %v", err)
	}
}

// TestPubSubDLQ verifies deadLetterPolicy: a message is delivered up to
// maxDeliveryAttempts times, then republished to the dead-letter topic.
func TestPubSubDLQ(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	for _, id := range []string{"src", "dlq"} {
		if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/" + id})); err != nil {
			t.Fatalf("create topic %s: %v", id, err)
		}
	}

	// Subscription with DLQ (maxDeliveryAttempts=2).
	nr := newNR(map[string]any{
		"name": "subscriptions/sub",
		"body": map[string]any{
			"topic": "projects/proj/topics/src",
			"deadLetterPolicy": map[string]any{
				"deadLetterTopic":     "projects/proj/topics/dlq",
				"maxDeliveryAttempts": float64(2),
			},
		},
	})
	if _, err := p.SubscriptionCreate(ctx, nr); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	// A subscription on the dead-letter topic so republished messages land in a
	// queue (fan-out stores per subscription).
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/dlq-sub",
		"body": map[string]any{"topic": "projects/proj/topics/dlq"},
	})); err != nil {
		t.Fatalf("create dlq subscription: %v", err)
	}

	// Publish one message.
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/src",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	pull := func() int {
		resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/sub"}))
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		received, _ := resp.Data["receivedMessages"].([]any)
		return len(received)
	}
	// redeliver makes every message in the source topic immediately visible
	// again, simulating the ack deadline expiring.
	redeliver := func() {
		msgs, _ := p.messages.List(ctx, "sub")
		ids := make([]string, 0, len(msgs))
		for _, m := range msgs {
			ids = append(ids, "sub/"+m.MessageID)
		}
		_ = p.messages.ModifyAckDeadline(ctx, "sub", ids, 0, time.Now())
	}

	if got := pull(); got != 1 {
		t.Fatalf("pull 1 expected 1 message, got %d", got)
	}
	redeliver()
	if got := pull(); got != 1 {
		t.Fatalf("pull 2 expected 1 message, got %d", got)
	}
	redeliver()
	if got := pull(); got != 0 {
		t.Fatalf("pull 3 expected 0 messages (moved to DLQ), got %d", got)
	}

	// The message now lives on the DLQ topic's subscription queue.
	msgs, err := p.messages.List(ctx, "dlq-sub")
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected 1 message in DLQ queue, got %d / %v", len(msgs), err)
	}
	// DLQ republish preserves the envelope-encrypted payload + key material;
	// decrypt it to verify the payload round-trips.
	rawDEK, err := p.encryptor.Unwrap(ctx, "proj", msgs[0].KmsKeyName, msgs[0].WrappedDEK)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	ct, _ := base64.StdEncoding.DecodeString(msgs[0].Data)
	plain, err := kmsstore.DecryptData(rawDEK, ct, nil)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(plain) != "hi" {
		t.Errorf("unexpected DLQ message data: %q", string(plain))
	}
}

// TestPubSubPushSubscription verifies pushConfig.pushEndpoint delivery (SNS
// deliverToHTTP analogue).
func TestPubSubPushSubscription(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/src"})); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/push",
		"body": map[string]any{
			"topic":      "projects/proj/topics/src",
			"pushConfig": map[string]any{"pushEndpoint": srv.URL},
		},
	})); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/src",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGVsbG8="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatalf("push payload not JSON: %v (body=%q)", err, gotBody)
	}
	msg, _ := payload["message"].(map[string]any)
	if msg["data"] != "aGVsbG8=" {
		t.Errorf("push message data = %v, want aGVsbG8=", msg["data"])
	}
}

// TestPubSubPushDeliveryTimeout verifies a hung push endpoint cannot block
// publish indefinitely — the delivery client has a bounded timeout.
func TestPubSubPushDeliveryTimeout(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/src"})); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// Endpoint that blocks far longer than the delivery client timeout.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
	}))
	defer srv.Close()

	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/push",
		"body": map[string]any{
			"topic":      "projects/proj/topics/src",
			"pushConfig": map[string]any{"pushEndpoint": srv.URL},
		},
	})); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	old := pushClient
	pushClient = &http.Client{Timeout: 200 * time.Millisecond}
	defer func() { pushClient = old }()

	start := time.Now()
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/src",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("publish blocked for %v despite push timeout", elapsed)
	}
}

// TestPubSubIamPolicy verifies topic getIamPolicy/setIamPolicy/testIamPermissions.
func TestPubSubIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/t1"})); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Default policy.
	resp, err := p.TopicGetIamPolicy(ctx, newNR(map[string]any{"name": "topics/t1"}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if resp.Data["version"] != 1 {
		t.Errorf("expected version 1, got %v", resp.Data["version"])
	}

	// setIamPolicy (etag OCC).
	bindings := []any{map[string]any{"role": "roles/pubsub.publisher", "members": []any{"allUsers"}}}
	if _, err := p.TopicSetIamPolicy(ctx, newNR(map[string]any{"name": "topics/t1", "body": map[string]any{"bindings": bindings, "etag": "BOGUS="}})); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on stale etag, got %v", err)
	}
	set, err := p.TopicSetIamPolicy(ctx, newNR(map[string]any{"name": "topics/t1", "body": map[string]any{"bindings": bindings}}))
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	if etag, _ := set.Data["etag"].(string); etag == "" {
		t.Error("expected fresh etag")
	}

	// testIamPermissions.
	tr, err := p.TopicTestIamPermissions(ctx, newNR(map[string]any{"name": "topics/t1", "body": map[string]any{"permissions": []any{"pubsub.topics.publish"}}}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	if perms, _ := tr.Data["permissions"].([]string); len(perms) != 1 {
		t.Errorf("expected 1 granted permission, got %v", tr.Data["permissions"])
	}

	// 404 on missing topic.
	if _, err := p.TopicGetIamPolicy(ctx, newNR(map[string]any{"name": "topics/missing"})); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing topic, got %v", err)
	}
}

// TestPubSubFanOut verifies that every pull subscription of a topic receives a
// copy of each published message, and that acking one subscription does not
// affect another's copy.
func TestPubSubFanOut(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/fan"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	for _, s := range []string{"fan-a", "fan-b"} {
		if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
			"name": "subscriptions/" + s,
			"body": map[string]any{"topic": "projects/proj/topics/fan"},
		})); err != nil {
			t.Fatalf("subscription %s: %v", s, err)
		}
	}
	// Push subscriptions must not get a queued copy.
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/fan-push",
		"body": map[string]any{
			"topic":      "projects/proj/topics/fan",
			"pushConfig": map[string]any{"pushEndpoint": "http://127.0.0.1:1/push"},
		},
	})); err != nil {
		t.Fatalf("push subscription: %v", err)
	}

	// "broadcast"
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/fan",
		"body": map[string]any{"messages": []any{map[string]any{"data": "YnJvYWRjYXN0"}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	pull := func(s string) map[string]any {
		resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{
			"name": "subscriptions/" + s,
			"body": map[string]any{"returnImmediately": true},
		}))
		if err != nil {
			t.Fatalf("pull %s: %v", s, err)
		}
		received, _ := resp.Data["receivedMessages"].([]any)
		if len(received) != 1 {
			t.Fatalf("subscription %s: expected 1 message, got %d", s, len(received))
		}
		return received[0].(map[string]any)
	}

	rmA := pull("fan-a")
	rmB := pull("fan-b")
	for _, rm := range []map[string]any{rmA, rmB} {
		if got := rm["message"].(map[string]any)["data"]; got != "YnJvYWRjYXN0" {
			t.Fatalf("fan-out payload = %v, want broadcast", got)
		}
	}

	// Acking fan-a must not consume fan-b's copy.
	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/fan-a",
		"body": map[string]any{"ackIds": []any{rmA["ackId"].(string)}},
	})); err != nil {
		t.Fatalf("ack: %v", err)
	}
	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{
		"name": "subscriptions/fan-a",
		"body": map[string]any{"returnImmediately": true},
	}))
	if err != nil {
		t.Fatalf("pull acked: %v", err)
	}
	if received, _ := resp.Data["receivedMessages"].([]any); len(received) != 0 {
		t.Fatalf("acked subscription still has %d messages", len(received))
	}
}

func pubsubPull(t *testing.T, p *Provider, sub string) []any {
	t.Helper()
	ctx := context.Background()
	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{
		"name": "subscriptions/" + sub,
		"body": map[string]any{"returnImmediately": true},
	}))
	if err != nil {
		t.Fatalf("pull %s: %v", sub, err)
	}
	msgs, _ := resp.Data["receivedMessages"].([]any)
	return msgs
}

func TestPubSubSubscriptionFilter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/ft"})); err != nil {
		t.Fatalf("topic: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/fs",
		"body": map[string]any{"topic": "projects/proj/topics/ft", "filter": `attributes.event_type = "a"`},
	})); err != nil {
		t.Fatalf("filtered sub: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/fu",
		"body": map[string]any{"topic": "projects/proj/topics/ft"},
	})); err != nil {
		t.Fatalf("unfiltered sub: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/ft",
		"body": map[string]any{"messages": []any{
			map[string]any{"data": "bWF0Y2g=", "attributes": map[string]any{"event_type": "a"}},
			map[string]any{"data": "bm9wZQ==", "attributes": map[string]any{"event_type": "b"}},
		}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	fs := pubsubPull(t, p, "fs")
	if len(fs) != 1 {
		t.Fatalf("filtered sub got %d messages, want 1", len(fs))
	}
	got := fs[0].(map[string]any)["message"].(map[string]any)["data"]
	if got != "bWF0Y2g=" {
		t.Errorf("filtered sub data = %v, want bWF0Y2g=", got)
	}
	if fu := pubsubPull(t, p, "fu"); len(fu) != 2 {
		t.Fatalf("unfiltered sub got %d messages, want 2", len(fu))
	}
}

func TestPubSubUnparseableFilterRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/bt"})); err != nil {
		t.Fatalf("topic: %v", err)
	}
	_, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/bs",
		"body": map[string]any{"topic": "projects/proj/topics/bt", "filter": "this is not a filter ((("},
	}))
	if err == nil {
		t.Fatal("expected invalid filter to be rejected")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestPubSubDetachSubscription(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/dt"})); err != nil {
		t.Fatalf("topic: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/ds", "body": map[string]any{"topic": "projects/proj/topics/dt"},
	})); err != nil {
		t.Fatalf("sub: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/dt", "body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := p.SubscriptionDetach(ctx, newNR(map[string]any{"name": "subscriptions/ds"})); err != nil {
		t.Fatalf("detach: %v", err)
	}
	get, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/ds"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if detached, _ := get.Data["detached"].(bool); !detached {
		t.Fatalf("expected detached=true, got %v", get.Data["detached"])
	}
	if _, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/ds"})); err == nil {
		t.Fatal("expected pull on detached subscription to fail")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "FailedPrecondition" {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if msgs, _ := p.messages.List(ctx, "ds"); len(msgs) != 0 {
		t.Fatalf("expected detached subscription backlog dropped, got %d", len(msgs))
	}
}

func TestPubSubTopicDeleteOrphansSubscription(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/ot"})); err != nil {
		t.Fatalf("topic: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/os", "body": map[string]any{"topic": "projects/proj/topics/ot"},
	})); err != nil {
		t.Fatalf("sub: %v", err)
	}
	if _, err := p.TopicDelete(ctx, newNR(map[string]any{"name": "topics/ot"})); err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	get, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/os"}))
	if err != nil {
		t.Fatalf("get sub: %v", err)
	}
	if get.Data["topic"] != "_deleted-topic_" {
		t.Fatalf("expected topic _deleted-topic_, got %v", get.Data["topic"])
	}
}
