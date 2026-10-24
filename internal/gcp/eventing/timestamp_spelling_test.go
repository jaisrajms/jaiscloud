package eventing

import (
	"encoding/json"
	"testing"
	"time"
)

// TestPubSubMessagePublishedPublishTimeSpelling pins the protojson spelling of
// the Pub/Sub MessagePublishedData publishTime a gen2 Function/Eventarc receives.
func TestPubSubMessagePublishedPublishTimeSpelling(t *testing.T) {
	body := PubSubMessagePublished("m1",
		time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC), nil, nil, "sub")
	var payload struct {
		Message struct {
			PublishTime string `json:"publishTime"`
		} `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := payload.Message.PublishTime; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("publishTime = %q, want 2024-03-01T12:00:00.500Z", got)
	}
}
