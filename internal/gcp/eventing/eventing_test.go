package eventing

import "testing"

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
