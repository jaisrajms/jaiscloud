package eventing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// recordingDispatcher counts the events it receives.
type recordingDispatcher struct{ got []Event }

func (d *recordingDispatcher) DispatchEvent(_ context.Context, ev Event) {
	d.got = append(d.got, ev)
}

func TestFanoutCallsEveryNonNilMember(t *testing.T) {
	a := &recordingDispatcher{}
	b := &recordingDispatcher{}
	f := Fanout{nil, a, b}
	ev := Event{Project: "p", EventType: TypePubSubPublish}
	f.DispatchEvent(context.Background(), ev)
	if len(a.got) != 1 || len(b.got) != 1 {
		t.Fatalf("fanout delivered to %d/%d members, want 1/1", len(a.got), len(b.got))
	}
	if a.got[0].Project != "p" || b.got[0].EventType != TypePubSubPublish {
		t.Fatalf("unexpected event: %+v / %+v", a.got[0], b.got[0])
	}
}

func TestNilFanoutDeliversNothing(t *testing.T) {
	var f Fanout
	f.DispatchEvent(context.Background(), Event{}) // must not panic
}

func TestNormalizeEventType(t *testing.T) {
	cases := map[string]string{
		"google.pubsub.topic.publish":                      TypePubSubPublish,
		"providers/cloud.pubsub/eventTypes/topic.publish":  TypePubSubPublish,
		"google.storage.object.finalize":                   TypeStorageFinalize,
		"google.cloud.storage.object.v1.finalized":         TypeStorageFinalize,
		"providers/cloud.storage/eventTypes/object.change": legacyStorageObjectChange,
		"example.custom.event":                             "example.custom.event",
	}
	for in, want := range cases {
		if got := NormalizeEventType(in); got != want {
			t.Errorf("NormalizeEventType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTypeMatches(t *testing.T) {
	if !TypeMatches("google.pubsub.topic.publish", TypePubSubPublish) {
		t.Error("canonical pubsub should match")
	}
	if !TypeMatches("providers/cloud.pubsub/eventTypes/topic.publish", TypePubSubPublish) {
		t.Error("legacy pubsub should match")
	}
	// The v1 object.change catch-all receives every storage event.
	if !TypeMatches("providers/cloud.storage/eventTypes/object.change", TypeStorageFinalize) {
		t.Error("object.change should match finalize")
	}
	if !TypeMatches("providers/cloud.storage/eventTypes/object.change", TypeStorageDelete) {
		t.Error("object.change should match delete")
	}
	if TypeMatches("google.pubsub.topic.publish", TypeStorageFinalize) {
		t.Error("pubsub must not match storage")
	}
	if TypeMatches("", TypeStorageFinalize) {
		t.Error("empty declared type must not match")
	}
}

// TestIsCloudStorageEventType covers the broader validation set: every Cloud
// Storage spelling (including archived/metadataUpdated, which the delivery
// matcher's IsStorageEventType does not recognize) is a Cloud Storage event,
// while Pub/Sub and unknown types are not.
func TestIsCloudStorageEventType(t *testing.T) {
	storage := []string{
		"google.cloud.storage.object.v1.finalized",
		"google.cloud.storage.object.v1.deleted",
		"google.cloud.storage.object.v1.archived",
		"google.cloud.storage.object.v1.metadataUpdated",
		"google.storage.object.finalize",
		"google.storage.object.delete",
		"google.storage.object.archive",
		"google.storage.object.metadataUpdate",
		"providers/cloud.storage/eventTypes/object.change",
	}
	for _, in := range storage {
		if !IsCloudStorageEventType(in) {
			t.Errorf("IsCloudStorageEventType(%q) = false, want true", in)
		}
	}
	for _, in := range []string{"google.cloud.pubsub.topic.v1.messagePublished", "google.pubsub.topic.publish", "example.custom.event", ""} {
		if IsCloudStorageEventType(in) {
			t.Errorf("IsCloudStorageEventType(%q) = true, want false", in)
		}
	}
	// The delivery matcher stays narrow: it does not execute archived events.
	if IsStorageEventType("google.cloud.storage.object.v1.archived") {
		t.Error("IsStorageEventType must not claim the archived event the emulator does not produce")
	}
}

func TestResourceID(t *testing.T) {
	cases := map[string]string{
		"projects/p/topics/t":  "t",
		"projects/_/buckets/b": "b",
		"t":                    "t",
		"projects/p/topics/t/": "t",
		"//pubsub.googleapis.com/projects/p/topics/t": "t",
	}
	for in, want := range cases {
		if got := ResourceID(in); got != want {
			t.Errorf("ResourceID(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestStorageCloudEventType covers the shared Cloud Storage CloudEvent type
// mapping (used by both the Eventarc dispatcher and the Cloud Functions
// delivery engine) across every accepted spelling.
func TestStorageCloudEventType(t *testing.T) {
	cases := map[string]string{
		TypeStorageFinalize:                        "google.cloud.storage.object.v1.finalized",
		"google.cloud.storage.object.v1.finalized": "google.cloud.storage.object.v1.finalized",
		TypeStorageDelete:                          "google.cloud.storage.object.v1.deleted",
		"google.cloud.storage.object.v1.deleted":   "google.cloud.storage.object.v1.deleted",
		"example.custom.event":                     "example.custom.event",
	}
	for in, want := range cases {
		if got := StorageCloudEventType(in); got != want {
			t.Errorf("StorageCloudEventType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCloudEventSources(t *testing.T) {
	if got := PubSubSource("projects/p/topics/t"); got != "//pubsub.googleapis.com/projects/p/topics/t" {
		t.Errorf("PubSubSource = %q", got)
	}
	if got := StorageSource("projects/_/buckets/b"); got != "//storage.googleapis.com/projects/_/buckets/b" {
		t.Errorf("StorageSource = %q", got)
	}
	if got := FunctionTriggerID("fn"); got != "functions-fn" {
		t.Errorf("FunctionTriggerID = %q", got)
	}
}

func TestStorageObjectSubject(t *testing.T) {
	if got := StorageObjectSubject(map[string]string{"objectId": "dir/obj.txt"}); got != "objects/dir/obj.txt" {
		t.Errorf("StorageObjectSubject = %q, want objects/dir/obj.txt", got)
	}
	if got := StorageObjectSubject(nil); got != "" {
		t.Errorf("StorageObjectSubject(nil) = %q, want empty", got)
	}
}

// TestPubSubMessagePublished covers the shared MessagePublishedData builder both
// the Eventarc dispatcher and the Cloud Functions delivery engine use.
func TestPubSubMessagePublished(t *testing.T) {
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	body := PubSubMessagePublished("m1", when, []byte("hi"), map[string]string{"k": "v"}, "projects/p/subscriptions/s")
	var got struct {
		Message struct {
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
		} `json:"message"`
		Subscription string `json:"subscription"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("envelope: %v (%s)", err, body)
	}
	if got.Message.MessageID != "m1" || got.Message.PublishTime != when.Format(time.RFC3339Nano) {
		t.Fatalf("message = %+v", got.Message)
	}
	if got.Message.Data != base64.StdEncoding.EncodeToString([]byte("hi")) || got.Message.Attributes["k"] != "v" {
		t.Fatalf("message data/attrs = %+v", got.Message)
	}
	if got.Subscription != "projects/p/subscriptions/s" {
		t.Fatalf("subscription = %q", got.Subscription)
	}
	// An attributes-only message omits data.
	var empty struct {
		Message map[string]any `json:"message"`
	}
	if err := json.Unmarshal(PubSubMessagePublished("m2", when, nil, nil, "s"), &empty); err != nil {
		t.Fatalf("empty envelope: %v", err)
	}
	if _, ok := empty.Message["data"]; ok {
		t.Fatalf("attributes-only message should omit data: %+v", empty.Message)
	}
}
