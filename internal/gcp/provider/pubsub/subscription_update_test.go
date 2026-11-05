package pubsub

import (
	"context"
	"maps"
	"testing"

	"jaiscloud/internal/model"
)

// subscription_update_test.go pins the updateMask contract for the mutable
// Subscription fields the hashicorp/google provider actually sends. Every
// provider PATCH appends bigqueryConfig to the mask (the recorded requests were
// `ackDeadlineSeconds,bigqueryConfig`, `labels,bigqueryConfig` and
// `messageRetentionDuration,bigqueryConfig`), so the trailing object's absence
// must clear the target instead of 400ing.

// updateSub runs a SubscriptionUpdate and returns the response body or the
// *model.ProviderError.
func updateSub(t *testing.T, p *Provider, id, mask string, in map[string]any) (map[string]any, *model.ProviderError) {
	t.Helper()
	body := map[string]any{"subscription": in}
	if mask != "" {
		body["updateMask"] = mask
	}
	resp, err := p.SubscriptionUpdate(context.Background(), newNR(map[string]any{
		"name": "subscriptions/" + id,
		"body": body,
	}))
	if err != nil {
		pe, ok := err.(*model.ProviderError)
		if !ok {
			t.Fatalf("SubscriptionUpdate(%s) err = %v, want *model.ProviderError", mask, err)
		}
		return nil, pe
	}
	return resp.Data, nil
}

func mustUpdate(t *testing.T, p *Provider, id, mask string, in map[string]any) map[string]any {
	t.Helper()
	data, perr := updateSub(t, p, id, mask, in)
	if perr != nil {
		t.Fatalf("SubscriptionUpdate(%s) = %d %s, want 200", mask, perr.HTTPStatus, perr.Message)
	}
	return data
}

func mustReject(t *testing.T, p *Provider, id, mask string, in map[string]any) {
	t.Helper()
	_, perr := updateSub(t, p, id, mask, in)
	if perr == nil {
		t.Fatalf("SubscriptionUpdate(%s) = 200, want 400", mask)
	}
	if perr.HTTPStatus != 400 {
		t.Fatalf("SubscriptionUpdate(%s) status = %d, want 400 (%s)", mask, perr.HTTPStatus, perr.Message)
	}
}

// newUpdateFixture creates a topic and a pull subscription for update tests.
func newUpdateFixture(t *testing.T, id string, body map[string]any) *Provider {
	t.Helper()
	p := newTestProvider()
	if _, err := p.TopicCreate(context.Background(), newNR(map[string]any{"name": "topics/update-topic"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	create := map[string]any{"topic": "projects/proj/topics/update-topic"}
	maps.Copy(create, body)
	if _, err := p.SubscriptionCreate(context.Background(), newNR(map[string]any{
		"name": "subscriptions/" + id,
		"body": create,
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	return p
}

// intField coerces a just-written int or a JSON-decoded float64 to an int.
func intField(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return -1
}

func getSub(t *testing.T, p *Provider, id string) map[string]any {
	t.Helper()
	resp, err := p.SubscriptionGet(context.Background(), newNR(map[string]any{"name": "subscriptions/" + id}))
	if err != nil {
		t.Fatalf("SubscriptionGet(%s): %v", id, err)
	}
	return resp.Data
}

// TestSubscriptionUpdateAckDeadlineWithBigqueryConfig is the key recorded
// provider shape: `ackDeadlineSeconds,bigqueryConfig` with no bigqueryConfig in
// the body must succeed (the absent target is a clear, not an unknown path).
func TestSubscriptionUpdateAckDeadlineWithBigqueryConfig(t *testing.T) {
	p := newUpdateFixture(t, "ack-bq", nil)

	data := mustUpdate(t, p, "ack-bq", "ackDeadlineSeconds,bigqueryConfig", map[string]any{
		"ackDeadlineSeconds": float64(20),
	})
	if got := intField(data["ackDeadlineSeconds"]); got != 20 {
		t.Fatalf("ackDeadlineSeconds = %v, want 20", data["ackDeadlineSeconds"])
	}
	if _, ok := data["bigqueryConfig"]; ok {
		t.Fatalf("bigqueryConfig key present after clear: %v", data["bigqueryConfig"])
	}

	read := getSub(t, p, "ack-bq")
	if got := intField(read["ackDeadlineSeconds"]); got != 20 {
		t.Fatalf("GET ackDeadlineSeconds = %v, want 20", read["ackDeadlineSeconds"])
	}
	if _, ok := read["bigqueryConfig"]; ok {
		t.Fatalf("GET bigqueryConfig present, want cleared: %v", read["bigqueryConfig"])
	}
}

// TestSubscriptionUpdateLabelsWithBigqueryConfig covers `labels,bigqueryConfig`:
// labels are written and the stored config is cleared.
func TestSubscriptionUpdateLabelsWithBigqueryConfig(t *testing.T) {
	p := newUpdateFixture(t, "labels-bq", nil)

	// Seed a BigQuery target so the clear is observable.
	stored := mustUpdate(t, p, "labels-bq", "bigqueryConfig", map[string]any{
		"bigqueryConfig": map[string]any{"table": "projects/proj/datasets/ds/tables/tbl"},
	})
	if _, ok := stored["bigqueryConfig"].(map[string]any); !ok {
		t.Fatalf("bigqueryConfig not stored: %v", stored["bigqueryConfig"])
	}

	data := mustUpdate(t, p, "labels-bq", "labels,bigqueryConfig", map[string]any{
		"labels": map[string]any{"team": "platform", "managed-by": "terraform"},
	})
	if got := labelMap(t, data["labels"]); !maps.Equal(got, map[string]string{"team": "platform", "managed-by": "terraform"}) {
		t.Fatalf("labels = %v", got)
	}
	if _, ok := data["bigqueryConfig"]; ok {
		t.Fatalf("bigqueryConfig not cleared: %v", data["bigqueryConfig"])
	}
	if read := getSub(t, p, "labels-bq"); read["bigqueryConfig"] != nil {
		t.Fatalf("GET bigqueryConfig = %v, want cleared", read["bigqueryConfig"])
	}
}

// TestSubscriptionUpdateStorageConfigs covers bigqueryConfig/cloudStorageConfig
// store-and-echo and the "present then absent clears" contract.
func TestSubscriptionUpdateStorageConfigs(t *testing.T) {
	p := newUpdateFixture(t, "storage", nil)

	bq := map[string]any{
		"table":          "projects/proj/datasets/ds/tables/tbl",
		"useTopicSchema": true,
	}
	data := mustUpdate(t, p, "storage", "bigqueryConfig", map[string]any{"bigqueryConfig": bq})
	if got, _ := data["bigqueryConfig"].(map[string]any); got == nil || got["table"] != bq["table"] {
		t.Fatalf("bigqueryConfig = %v, want %v", data["bigqueryConfig"], bq)
	}
	if read := getSub(t, p, "storage"); read["bigqueryConfig"] == nil {
		t.Fatal("bigqueryConfig not persisted")
	}

	// Absent under the mask clears it.
	data = mustUpdate(t, p, "storage", "bigqueryConfig", map[string]any{})
	if _, ok := data["bigqueryConfig"]; ok {
		t.Fatalf("bigqueryConfig not cleared: %v", data["bigqueryConfig"])
	}

	gcs := map[string]any{
		"bucket":         "my-bucket",
		"filenamePrefix": "logs/",
	}
	data = mustUpdate(t, p, "storage", "cloudStorageConfig", map[string]any{"cloudStorageConfig": gcs})
	if got, _ := data["cloudStorageConfig"].(map[string]any); got == nil || got["bucket"] != "my-bucket" {
		t.Fatalf("cloudStorageConfig = %v, want %v", data["cloudStorageConfig"], gcs)
	}
	data = mustUpdate(t, p, "storage", "cloudStorageConfig", map[string]any{"cloudStorageConfig": nil})
	if _, ok := data["cloudStorageConfig"]; ok {
		t.Fatalf("cloudStorageConfig not cleared by JSON null: %v", data["cloudStorageConfig"])
	}
}

// TestSubscriptionUpdateRetention covers the protobuf-JSON duration parse, the
// 10 min–31 day bounds, the canonical stored form, and the clear-to-default.
func TestSubscriptionUpdateRetention(t *testing.T) {
	p := newUpdateFixture(t, "retention", nil)

	data := mustUpdate(t, p, "retention", "messageRetentionDuration,bigqueryConfig", map[string]any{
		"messageRetentionDuration": "1200s",
	})
	if got, _ := data["messageRetentionDuration"].(string); got != "1200s" {
		t.Fatalf("messageRetentionDuration = %q, want 1200s", got)
	}

	// Go-style and non-duration strings are not protobuf durations.
	for _, bad := range []string{"10m", "10m0s", "1200", "abc", "-600s", "0s"} {
		mustReject(t, p, "retention", "messageRetentionDuration", map[string]any{
			"messageRetentionDuration": bad,
		})
	}
	// Out of range in both directions.
	for _, bad := range []string{"599s", "2678401s"} {
		mustReject(t, p, "retention", "messageRetentionDuration", map[string]any{
			"messageRetentionDuration": bad,
		})
	}
	// Exactly the bounds are accepted.
	for _, ok := range []string{"600s", "2678400s"} {
		mustUpdate(t, p, "retention", "messageRetentionDuration", map[string]any{
			"messageRetentionDuration": ok,
		})
	}

	// Absent under the mask resets to the documented 7-day default.
	data = mustUpdate(t, p, "retention", "messageRetentionDuration", map[string]any{})
	if got, _ := data["messageRetentionDuration"].(string); got != "604800s" {
		t.Fatalf("cleared messageRetentionDuration = %q, want 604800s", got)
	}
}

// TestSubscriptionUpdateRetainAckedMessages pins the false-omits-key shape.
func TestSubscriptionUpdateRetainAckedMessages(t *testing.T) {
	p := newUpdateFixture(t, "retain", nil)

	data := mustUpdate(t, p, "retain", "retainAckedMessages", map[string]any{"retainAckedMessages": true})
	if v, _ := data["retainAckedMessages"].(bool); !v {
		t.Fatalf("retainAckedMessages = %v, want true", data["retainAckedMessages"])
	}
	data = mustUpdate(t, p, "retain", "retain_acked_messages", map[string]any{"retainAckedMessages": false})
	if _, ok := data["retainAckedMessages"]; ok {
		t.Fatalf("retainAckedMessages key present after false: %v", data["retainAckedMessages"])
	}
	if read := getSub(t, p, "retain"); read["retainAckedMessages"] != nil {
		t.Fatalf("GET retainAckedMessages = %v, want absent", read["retainAckedMessages"])
	}
}

// TestSubscriptionUpdateExpirationPolicy covers never-expire ({}), the
// 24h–365d bounds, and the distinction between {} and an unset key.
func TestSubscriptionUpdateExpirationPolicy(t *testing.T) {
	p := newUpdateFixture(t, "expiry", nil)

	// {} means never expire and is stored as a present-but-empty object.
	data := mustUpdate(t, p, "expiry", "expirationPolicy,bigqueryConfig", map[string]any{
		"expirationPolicy": map[string]any{},
	})
	policy, ok := data["expirationPolicy"].(map[string]any)
	if !ok {
		t.Fatalf("expirationPolicy = %v, want an empty object", data["expirationPolicy"])
	}
	if len(policy) != 0 {
		t.Fatalf("expirationPolicy = %v, want {}", policy)
	}
	if read := getSub(t, p, "expiry"); read["expirationPolicy"] == nil {
		t.Fatal("never-expire policy not persisted (key absent)")
	}

	// A missing ttl is the same never-expire shape.
	data = mustUpdate(t, p, "expiry", "expirationPolicy", map[string]any{
		"expirationPolicy": map[string]any{"ttl": ""},
	})
	if policy, ok := data["expirationPolicy"].(map[string]any); !ok || len(policy) != 0 {
		t.Fatalf("missing-ttl expirationPolicy = %v, want {}", data["expirationPolicy"])
	}

	// JSON null also means never-expire.
	data = mustUpdate(t, p, "expiry", "expirationPolicy", map[string]any{"expirationPolicy": nil})
	if policy, ok := data["expirationPolicy"].(map[string]any); !ok || len(policy) != 0 {
		t.Fatalf("null expirationPolicy = %v, want {}", data["expirationPolicy"])
	}

	// Bounds: 24h–365d.
	for _, bad := range []string{"3600s", "86399s", "31536001s", "400d"} {
		mustReject(t, p, "expiry", "expirationPolicy", map[string]any{
			"expirationPolicy": map[string]any{"ttl": bad},
		})
	}
	for _, ok := range []string{"86400s", "31536000s"} {
		data = mustUpdate(t, p, "expiry", "expirationPolicy", map[string]any{
			"expirationPolicy": map[string]any{"ttl": ok},
		})
		if got, _ := data["expirationPolicy"].(map[string]any)["ttl"].(string); got != ok {
			t.Fatalf("expirationPolicy.ttl = %q, want %q", got, ok)
		}
	}
}

// TestSubscriptionUpdatePushConfig covers setting, clearing, and the
// exactly-once/push invariant in both path orders.
func TestSubscriptionUpdatePushConfig(t *testing.T) {
	p := newUpdateFixture(t, "push", nil)

	data := mustUpdate(t, p, "push", "pushConfig", map[string]any{
		"pushConfig": map[string]any{"pushEndpoint": "https://example.com/push"},
	})
	pc, _ := data["pushConfig"].(map[string]any)
	if ep, _ := pc["pushEndpoint"].(string); ep != "https://example.com/push" {
		t.Fatalf("pushConfig = %v", data["pushConfig"])
	}

	// An empty endpoint switches back to pull.
	data = mustUpdate(t, p, "push", "pushConfig", map[string]any{
		"pushConfig": map[string]any{"pushEndpoint": ""},
	})
	if _, ok := data["pushConfig"]; ok {
		t.Fatalf("pushConfig key present after clear: %v", data["pushConfig"])
	}
	data = mustUpdate(t, p, "push", "pushConfig", map[string]any{"pushConfig": map[string]any{}})
	if _, ok := data["pushConfig"]; ok {
		t.Fatalf("pushConfig key present after {} clear: %v", data["pushConfig"])
	}

	// Enabling exactly-once on a push subscription is rejected...
	push := newUpdateFixture(t, "eod-push", nil)
	mustUpdate(t, push, "eod-push", "pushConfig", map[string]any{
		"pushConfig": map[string]any{"pushEndpoint": "https://example.com/push"},
	})
	mustReject(t, push, "eod-push", "enableExactlyOnceDelivery", map[string]any{
		"enableExactlyOnceDelivery": true,
	})
	// ...and so is adding push to an exactly-once subscription.
	mustUpdate(t, push, "eod-push", "pushConfig", map[string]any{})
	mustUpdate(t, push, "eod-push", "enableExactlyOnceDelivery", map[string]any{
		"enableExactlyOnceDelivery": true,
	})
	mustReject(t, push, "eod-push", "pushConfig", map[string]any{
		"pushConfig": map[string]any{"pushEndpoint": "https://example.com/push"},
	})

	// The same patch naming both fields is rejected regardless of order.
	for _, mask := range []string{
		"enableExactlyOnceDelivery,pushConfig",
		"pushConfig,enableExactlyOnceDelivery",
	} {
		p2 := newUpdateFixture(t, "eod-both", nil)
		mustReject(t, p2, "eod-both", mask, map[string]any{
			"enableExactlyOnceDelivery": true,
			"pushConfig":                map[string]any{"pushEndpoint": "https://example.com/push"},
		})
	}

	// Clearing push in the same patch as enabling exactly-once is fine.
	p3 := newUpdateFixture(t, "eod-clear", nil)
	mustUpdate(t, p3, "eod-clear", "pushConfig", map[string]any{
		"pushConfig": map[string]any{"pushEndpoint": "https://example.com/push"},
	})
	mustUpdate(t, p3, "eod-clear", "pushConfig,enableExactlyOnceDelivery", map[string]any{
		"enableExactlyOnceDelivery": true,
	})
}

// TestSubscriptionUpdateMessageTransforms round-trips the list.
func TestSubscriptionUpdateMessageTransforms(t *testing.T) {
	p := newUpdateFixture(t, "transforms", nil)

	transforms := []any{map[string]any{
		"javascriptUdf": map[string]any{
			"functionName": "redact",
			"code":         "function redact(m) { return m; }",
		},
	}}
	data := mustUpdate(t, p, "transforms", "messageTransforms", map[string]any{
		"messageTransforms": transforms,
	})
	got, _ := data["messageTransforms"].([]any)
	if len(got) != 1 {
		t.Fatalf("messageTransforms = %v, want one entry", data["messageTransforms"])
	}
	if read := getSub(t, p, "transforms"); len(read["messageTransforms"].([]any)) != 1 {
		t.Fatalf("GET messageTransforms = %v, want one entry", read["messageTransforms"])
	}

	// snake_case spelling resolves to the same field; empty clears.
	data = mustUpdate(t, p, "transforms", "message_transforms", map[string]any{"messageTransforms": []any{}})
	if _, ok := data["messageTransforms"]; ok {
		t.Fatalf("messageTransforms key present after empty list: %v", data["messageTransforms"])
	}
}

// TestSubscriptionUpdateStateIsOutputOnly pins that a client-supplied state is
// accepted as a no-op rather than misreported as an unknown mask path.
func TestSubscriptionUpdateStateIsOutputOnly(t *testing.T) {
	p := newUpdateFixture(t, "state-sub", nil)

	data := mustUpdate(t, p, "state-sub", "state", map[string]any{"state": "RESOURCE_ERROR"})
	if got, _ := data["state"].(string); got != "ACTIVE" {
		t.Fatalf("state = %q, want ACTIVE (OUTPUT_ONLY, never stored)", got)
	}
	if read := getSub(t, p, "state-sub"); read["state"] != "ACTIVE" {
		t.Fatalf("GET state = %v, want ACTIVE", read["state"])
	}
}

// TestSubscriptionUpdateClearBack is the module-shaped clear: labels {} removes
// the key and an absent retention resets to 604800s rather than deleting it.
func TestSubscriptionUpdateClearBack(t *testing.T) {
	p := newUpdateFixture(t, "clear-back", map[string]any{
		"labels":                   map[string]any{"team": "platform", "managed-by": "terraform"},
		"messageRetentionDuration": "1200s",
		"ackDeadlineSeconds":       float64(20),
	})

	data := mustUpdate(t, p, "clear-back", "labels,messageRetentionDuration,bigqueryConfig", map[string]any{
		"labels": map[string]any{},
	})
	if _, ok := data["labels"]; ok {
		t.Fatalf("labels key present after clear: %v", data["labels"])
	}
	if got, _ := data["messageRetentionDuration"].(string); got != "604800s" {
		t.Fatalf("messageRetentionDuration = %q, want 604800s", got)
	}

	read := getSub(t, p, "clear-back")
	if _, ok := read["labels"]; ok {
		t.Fatalf("GET labels present after clear: %v", read["labels"])
	}
	if got, _ := read["messageRetentionDuration"].(string); got != "604800s" {
		t.Fatalf("GET messageRetentionDuration = %q, want 604800s", got)
	}
}

// TestSubscriptionUpdateUnknownPathStillFails keeps the default branch loud.
func TestSubscriptionUpdateUnknownPathStillFails(t *testing.T) {
	p := newUpdateFixture(t, "unknown", nil)
	mustReject(t, p, "unknown", "nope", map[string]any{})
}
