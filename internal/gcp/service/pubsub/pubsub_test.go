package pubsub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newCore() (*Service, store.ResourceStore, pubsubstore.Messages) {
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	return NewService(resources, messages), resources, messages
}

// seedSubscription writes a subscription resource the way both transports store
// it.
func seedSubscription(t *testing.T, resources store.ResourceStore, project, id, topic string, eod bool) {
	t.Helper()
	meta := map[string]any{"name": "projects/" + project + "/subscriptions/" + id, "topic": topic}
	if eod {
		meta["enableExactlyOnceDelivery"] = true
	}
	data, _ := json.Marshal(meta)
	if err := resources.Create(context.Background(), project, store.GlobalRegion,
		store.ResourceEntry{Type: rtSubscription, ID: id, Data: data}); err != nil {
		t.Fatalf("seed subscription: %v", err)
	}
}

func seedSnapshot(t *testing.T, resources store.ResourceStore, project, id, topic string, backlog []pubsubstore.Message, createdAt time.Time) {
	t.Helper()
	meta := SnapshotMeta{
		Name:      "projects/" + project + "/snapshots/" + id,
		Topic:     topic,
		CreatedAt: createdAt,
		Backlog:   backlog,
	}
	data, _ := json.Marshal(meta)
	if err := resources.Create(context.Background(), project, store.GlobalRegion,
		store.ResourceEntry{Type: rtSnapshot, ID: id, Data: data}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
}

func TestNameParsers(t *testing.T) {
	if p, s, ok := SplitSubscriptionName("projects/p/subscriptions/s"); !ok || p != "p" || s != "s" {
		t.Fatalf("SplitSubscriptionName(full) = (%q,%q,%v)", p, s, ok)
	}
	if p, s, ok := SplitSubscriptionName("subscriptions/s"); !ok || p != "" || s != "s" {
		t.Fatalf("SplitSubscriptionName(relative) = (%q,%q,%v)", p, s, ok)
	}
	if _, _, ok := SplitSubscriptionName("projects/p/topics/t"); ok {
		t.Fatal("SplitSubscriptionName(topic) = ok, want false")
	}
	if p, s, ok := SplitSnapshotName("projects/p/snapshots/sn"); !ok || p != "p" || s != "sn" {
		t.Fatalf("SplitSnapshotName = (%q,%q,%v)", p, s, ok)
	}
	if _, _, ok := SplitSnapshotName("projects/p/subscriptions/s"); ok {
		t.Fatal("SplitSnapshotName(subscription) = ok, want false")
	}
}

// TestSeekToTime: messages published before the target are removed, later ones
// stay and get a fresh delivery version.
func TestSeekToTime(t *testing.T) {
	ctx := context.Background()
	core, resources, messages := newCore()
	seedSubscription(t, resources, "proj", "s", "projects/proj/topics/t", false)

	cut := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, m := range []pubsubstore.Message{
		{Subscription: "s", MessageID: "old", PublishTime: cut.Add(-time.Hour)},
		{Subscription: "s", MessageID: "new", PublishTime: cut.Add(time.Hour)},
	} {
		if err := messages.Put(ctx, m); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := core.Seek(ctx, "proj", "s", &cut, ""); err != nil {
		t.Fatalf("Seek(time): %v", err)
	}
	got, err := messages.List(ctx, "s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].MessageID != "new" {
		t.Fatalf("after seek: %+v, want only the newer message", got)
	}
	if got[0].DeliveryVersion == 0 {
		t.Fatal("retained message's delivery version was not bumped (EOD2)")
	}
}

// TestSeekToSnapshot: the snapshot backlog is restored alongside messages
// published after the snapshot was created.
func TestSeekToSnapshot(t *testing.T) {
	ctx := context.Background()
	core, resources, messages := newCore()
	seedSubscription(t, resources, "proj", "s", "projects/proj/topics/t", false)

	created := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	backlog := []pubsubstore.Message{
		{Subscription: "s", MessageID: "acked-later", PublishTime: created.Add(-time.Hour), Acked: true},
	}
	seedSnapshot(t, resources, "proj", "snap", "projects/proj/topics/t", backlog, created)
	// A message published after the snapshot, retained by the subscription.
	if err := messages.Put(ctx, pubsubstore.Message{
		Subscription: "s", MessageID: "post", PublishTime: created.Add(time.Hour),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := core.Seek(ctx, "proj", "s", nil, "projects/proj/snapshots/snap"); err != nil {
		t.Fatalf("Seek(snapshot): %v", err)
	}
	got, _ := messages.List(ctx, "s")
	ids := map[string]bool{}
	for _, m := range got {
		ids[m.MessageID] = true
		if m.Acked || m.AckPending {
			t.Fatalf("restored %s with acked/ackPending set", m.MessageID)
		}
	}
	if !ids["acked-later"] || !ids["post"] {
		t.Fatalf("after snapshot seek: %v, want the backlog plus the post-snapshot message", ids)
	}

	// A snapshot from another topic is FAILED_PRECONDITION.
	seedSnapshot(t, resources, "proj", "other", "projects/proj/topics/other", nil, created)
	err := core.Seek(ctx, "proj", "s", nil, "projects/proj/snapshots/other")
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Status != "FAILED_PRECONDITION" {
		t.Fatalf("topic-mismatch seek: err = %v, want FAILED_PRECONDITION", err)
	}
}

func TestSeekErrors(t *testing.T) {
	ctx := context.Background()
	core, resources, _ := newCore()
	seedSubscription(t, resources, "proj", "s", "projects/proj/topics/t", false)

	if err := core.Seek(ctx, "proj", "missing", &time.Time{}, ""); err == nil {
		t.Error("seek(missing subscription): want an error")
	}
	if err := core.Seek(ctx, "proj", "s", nil, ""); err == nil {
		t.Error("seek(no target): want an error")
	}
	if err := core.Seek(ctx, "proj", "s", nil, "projects/other/snapshots/x"); err == nil {
		t.Error("seek(other-project snapshot): want an error")
	}
	if err := core.Seek(ctx, "proj", "s", nil, "projects/proj/snapshots/absent"); err == nil {
		t.Error("seek(absent snapshot): want an error")
	}
	if _, err := core.GetSnapshotMeta(ctx, "proj", "absent"); err == nil {
		t.Error("GetSnapshotMeta(absent): want an error")
	}
}

// TestModifyPushConfigCore covers set/clear plus the exactly-once precondition.
func TestModifyPushConfigCore(t *testing.T) {
	ctx := context.Background()
	core, resources, _ := newCore()
	seedSubscription(t, resources, "proj", "s", "projects/proj/topics/t", false)
	seedSubscription(t, resources, "proj", "eod", "projects/proj/topics/t", true)

	if err := core.ModifyPushConfig(ctx, "proj", "s", &PushConfig{
		Endpoint:   "https://example.com/push",
		Attributes: map[string]string{"x-goog-version": "v1"},
		OIDCToken:  &OIDCToken{Audience: "aud", ServiceAccountEmail: "sa@proj.iam.gserviceaccount.com"},
	}); err != nil {
		t.Fatalf("ModifyPushConfig(set): %v", err)
	}
	e, err := resources.Get(ctx, "proj", store.GlobalRegion, rtSubscription, "s")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)
	pc, _ := meta["pushConfig"].(map[string]any)
	if pc["pushEndpoint"] != "https://example.com/push" {
		t.Fatalf("pushConfig = %v", meta["pushConfig"])
	}
	if _, ok := pc["oidcToken"].(map[string]any); !ok {
		t.Fatalf("oidcToken missing: %v", pc)
	}

	// An empty endpoint clears it.
	if err := core.ModifyPushConfig(ctx, "proj", "s", nil); err != nil {
		t.Fatalf("ModifyPushConfig(clear): %v", err)
	}
	e, _ = resources.Get(ctx, "proj", store.GlobalRegion, rtSubscription, "s")
	meta = map[string]any{}
	json.Unmarshal(e.Data, &meta)
	if _, ok := meta["pushConfig"]; ok {
		t.Fatalf("pushConfig = %v, want cleared", meta["pushConfig"])
	}

	// Exactly-once delivery cannot be a push subscription.
	if err := core.ModifyPushConfig(ctx, "proj", "eod", &PushConfig{Endpoint: "https://example.com/p"}); err == nil {
		t.Error("ModifyPushConfig(eod): want an error")
	}
	if err := core.ModifyPushConfig(ctx, "proj", "missing", &PushConfig{Endpoint: "https://example.com/p"}); err == nil {
		t.Error("ModifyPushConfig(missing): want an error")
	}
}
