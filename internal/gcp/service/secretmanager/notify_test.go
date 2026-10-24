package secretmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakePublisher records the messages the shared notification helpers publish.
type fakePublisher struct {
	msgs  []published
	fail  bool
	calls int
}

type published struct {
	account string
	topic   string
	data    []byte
	attrs   map[string]string
}

func (f *fakePublisher) PublishEvent(_ context.Context, accountID, topic string, data []byte, attributes map[string]string) (string, error) {
	f.calls++
	if f.fail {
		return "", errors.New("topic missing")
	}
	f.msgs = append(f.msgs, published{account: accountID, topic: topic, data: data, attrs: attributes})
	return "id", nil
}

// TestVersionStateEvent pins the lifecycle-state mapping: a destroy, a disable
// and an enable each publish their own event type.
func TestVersionStateEvent(t *testing.T) {
	for state, want := range map[string]string{
		"DESTROYED": EventSecretVersionDestroy,
		"DISABLED":  EventSecretVersionDisable,
		"ENABLED":   EventSecretVersionEnable,
	} {
		if got := VersionStateEvent(state); got != want {
			t.Errorf("VersionStateEvent(%q) = %q, want %q", state, got, want)
		}
	}
}

// TestNotificationAttributes pins the documented attribute sets: eventType,
// dataFormat, secretId and timestamp always; versionId on version events;
// deleteType on a delete.
func TestNotificationAttributes(t *testing.T) {
	at := time.Date(2026, 1, 1, 2, 3, 4, 0, time.UTC)
	const secretName = "projects/p/secrets/s"
	const versionName = secretName + "/versions/3"

	attrs := SecretEventAttributes(EventSecretCreate, secretName, at)
	if attrs["eventType"] != EventSecretCreate || attrs["dataFormat"] != DataFormatJSONAPIV1 ||
		attrs["secretId"] != secretName || attrs["timestamp"] != "2026-01-01T02:03:04Z" {
		t.Fatalf("secret attributes = %#v", attrs)
	}
	if _, ok := attrs["versionId"]; ok {
		t.Fatalf("secret event carries a versionId: %#v", attrs)
	}

	vattrs := VersionEventAttributes(EventSecretVersionAdd, secretName, versionName, at)
	if vattrs["versionId"] != versionName || vattrs["eventType"] != EventSecretVersionAdd {
		t.Fatalf("version attributes = %#v", vattrs)
	}

	dattrs := DeleteEventAttributes(secretName, DeleteTypeRequested, at)
	if dattrs["deleteType"] != "REQUESTED" || dattrs["eventType"] != EventSecretDelete {
		t.Fatalf("delete attributes = %#v", dattrs)
	}
}

// TestNotify: one message per topic, each with its own copy of the data and the
// attributes, and a failing topic never stops the fan-out.
func TestNotify(t *testing.T) {
	pub := &fakePublisher{}
	topics := []string{"projects/p/topics/a", "projects/p/topics/b"}
	attrs := SecretEventAttributes(EventSecretUpdate, "projects/p/secrets/s", time.Now())
	Notify(context.Background(), pub, "p", topics, []byte(`{"name":"x"}`), attrs)

	if len(pub.msgs) != 2 {
		t.Fatalf("published %d messages, want one per topic", len(pub.msgs))
	}
	for i, m := range pub.msgs {
		if m.account != "p" || m.topic != topics[i] {
			t.Fatalf("message %d = %#v", i, m)
		}
		if string(m.data) != `{"name":"x"}` {
			t.Fatalf("message %d data = %q", i, m.data)
		}
		if m.attrs["eventType"] != EventSecretUpdate {
			t.Fatalf("message %d attrs = %#v", i, m.attrs)
		}
	}

	// A failing publish is swallowed and does not stop the remaining topics.
	failing := &fakePublisher{fail: true}
	Notify(context.Background(), failing, "p", topics, []byte("x"), attrs)
	if failing.calls != len(topics) {
		t.Fatalf("fan-out stopped after a failure: %d calls, want %d", failing.calls, len(topics))
	}

	// A nil publisher (the default) is a no-op.
	Notify(context.Background(), nil, "p", topics, []byte("x"), attrs)
}

// TestNotifyTopicConfigured: the test message carries no data and only
// eventType, one copy per topic.
func TestNotifyTopicConfigured(t *testing.T) {
	pub := &fakePublisher{}
	topics := []string{"projects/p/topics/a", "projects/p/topics/b"}
	NotifyTopicConfigured(context.Background(), pub, "p", topics)

	if len(pub.msgs) != 2 {
		t.Fatalf("published %d messages, want %d", len(pub.msgs), len(topics))
	}
	for i, m := range pub.msgs {
		if len(m.data) != 0 {
			t.Errorf("message %d data = %q, want none", i, m.data)
		}
		if len(m.attrs) != 1 || m.attrs["eventType"] != EventTopicConfigured {
			t.Errorf("message %d attrs = %#v, want only eventType", i, m.attrs)
		}
	}

	// A nil publisher is a no-op.
	NotifyTopicConfigured(context.Background(), nil, "p", topics)
}
