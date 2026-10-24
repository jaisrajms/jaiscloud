package sdkpubsub_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// pushEnvelope mirrors the real GCP push request body.
type pushEnvelope struct {
	Message struct {
		Data        string            `json:"data"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		Attributes  map[string]string `json:"attributes"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// TestPubSubPushDelivery pins push delivery: a push subscription POSTs a real
// GCP-shaped envelope (base64 message data, subscription name, messageId and
// publishTime) to its endpoint on publish.
func TestPubSubPushDelivery(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("push-topic")
	sub := subName("push-sub")

	received := make(chan pushEnvelope, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var env pushEnvelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		select {
		case received <- env:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, func(s *pubsubpb.Subscription) {
		s.PushConfig = &pubsubpb.PushConfig{PushEndpoint: srv.URL}
	})

	publish(t, c, ctx, topic, "push-body", func(m *pubsubpb.PubsubMessage) {
		m.Attributes = map[string]string{"k": "v"}
	})

	select {
	case env := <-received:
		decoded, err := base64.StdEncoding.DecodeString(env.Message.Data)
		if err != nil {
			t.Fatalf("push data %q is not base64: %v", env.Message.Data, err)
		}
		if string(decoded) != "push-body" {
			t.Fatalf("push data decoded = %q, want %q", decoded, "push-body")
		}
		if env.Subscription != sub {
			t.Fatalf("push subscription = %q, want %q", env.Subscription, sub)
		}
		if env.Message.MessageID == "" {
			t.Fatalf("push messageId is empty")
		}
		if env.Message.PublishTime == "" {
			t.Fatalf("push publishTime is empty")
		}
		if env.Message.Attributes["k"] != "v" {
			t.Fatalf("push attributes = %v, want k=v", env.Message.Attributes)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("push endpoint never received a request")
	}
}
