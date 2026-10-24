package pubsub

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
)

// TestReceivedMessagePublishTimeMicrosecondTruncation pins TD48: the gRPC
// adapter truncates a message's publishTime to the microsecond the REST renderer
// (provider/pubsub) emits.
func TestReceivedMessagePublishTimeMicrosecondTruncation(t *testing.T) {
	ctx := context.Background()
	enc := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	rawDEK, _, err := enc.Wrap(ctx, "proj", "")
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	ct, err := kmsstore.EncryptData(rawDEK, []byte("hello"), nil)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	svc := &Service{encryptor: enc}
	got, err := svc.buildReceivedMessages(ctx, "proj", "queue", "", 0, false, []pubsubstore.Message{{
		MessageID:   "m1",
		Data:        base64.StdEncoding.EncodeToString(ct),
		PublishTime: instant,
	}})
	if err != nil {
		t.Fatalf("buildReceivedMessages: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("received = %d, want 1", len(got))
	}
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	if ts := got[0].GetMessage().GetPublishTime().AsTime(); !ts.Equal(want) {
		t.Fatalf("publishTime = %v, want %v", ts, want)
	}
}
