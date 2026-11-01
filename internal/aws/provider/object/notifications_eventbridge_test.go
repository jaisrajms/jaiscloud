package object

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/model"
)

type captureEB struct {
	mu      sync.Mutex
	entries []map[string]any
}

func (c *captureEB) InternalPutEvents(_ context.Context, entries []map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, entries...)
	return nil
}

func (c *captureEB) got() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.entries...)
}

func putNotifConfig(t *testing.T, p *ObjectProvider, bucket string, cfg map[string]any) {
	t.Helper()
	nr := &model.NormalizedRequest{Params: map[string]any{"_bucket": bucket, "_notification_config": cfg}}
	if _, err := p.PutBucketNotificationConfiguration(context.Background(), nr); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration: %v", err)
	}
}

func waitForEntries(c *captureEB, want int) []map[string]any {
	deadline := time.Now().Add(2 * time.Second) //nolint:clock test deadline is real elapsed time
	for time.Now().Before(deadline) {            //nolint:clock
		if got := c.got(); len(got) >= want {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c.got()
}

// EventBridge delivery is opt-in per bucket: only <EventBridgeConfiguration/>
// turns it on, and other notification targets must not enable it.
func TestS3EventBridgeDeliveryIsOptIn(t *testing.T) {
	eb := &captureEB{}
	p := New(newStubMeta(), newStubBlob())
	p.SetFanout(S3FanoutConfig{EventBridge: eb})

	putNotifConfig(t, p, "optin", map[string]any{"EventBridgeConfiguration": map[string]any{}})
	putNotifConfig(t, p, "queue-only", map[string]any{
		"QueueConfigurations": []any{map[string]any{
			"Id": "q", "QueueArn": "arn", "Events": []string{"s3:ObjectCreated:*"},
		}},
	})

	p.dispatchS3Notification(context.Background(), "queue-only", "k", "ObjectCreated:Put", "e", 3, "us-east-1", "000000000000")
	p.dispatchS3Notification(context.Background(), "optin", "dir/k", "ObjectCreated:Put", "etag", 7, "us-east-1", "000000000000")

	got := waitForEntries(eb, 1)
	if len(got) != 1 {
		t.Fatalf("got %d EventBridge entries, want exactly 1 (the opted-in bucket): %v", len(got), got)
	}
	e := got[0]
	if e["Source"] != "aws.s3" || e["DetailType"] != "Object Created" {
		t.Fatalf("entry = %v", e)
	}
	var detail map[string]any
	if err := json.Unmarshal([]byte(e["Detail"].(string)), &detail); err != nil {
		t.Fatalf("Detail is not valid JSON: %v", err)
	}
	if detail["bucket"].(map[string]any)["name"] != "optin" || detail["object"].(map[string]any)["key"] != "dir/k" {
		t.Fatalf("detail = %v", detail)
	}
}

func TestGetBucketNotificationReportsEventBridge(t *testing.T) {
	p := New(newStubMeta(), newStubBlob())
	putNotifConfig(t, p, "b", map[string]any{"EventBridgeConfiguration": map[string]any{}})
	resp, err := p.GetBucketNotificationConfiguration(context.Background(),
		&model.NormalizedRequest{Params: map[string]any{"_bucket": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := resp.Data["EventBridgeConfiguration"]; !ok {
		t.Fatalf("EventBridgeConfiguration missing from %v", resp.Data)
	}
}

// A write to a bucket that does not exist is refused before any blob is written
// (the seams rely on this to create the bucket and retry).
func TestPutObjectMissingBucketLeavesNoBlob(t *testing.T) {
	blob := newStubBlob()
	p := New(newStubMeta(), blob)
	nr := &model.NormalizedRequest{Params: map[string]any{"_bucket": "nope", "_key": "k", "_body": []byte("x")}}
	_, err := p.PutObject(context.Background(), nr)
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "NoSuchBucket" || pe.HTTPStatus != 404 {
		t.Fatalf("err = %v, want NoSuchBucket 404", err)
	}
	if _, gerr := blob.Get(context.Background(), "nope", "k"); gerr == nil {
		t.Fatal("a blob was written for a bucket that does not exist")
	}
}
