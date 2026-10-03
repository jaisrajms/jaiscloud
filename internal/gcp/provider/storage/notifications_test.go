package storage

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"

	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// fakePublisher records the events the storage provider fans out.
type fakePublisher struct {
	mu      sync.Mutex
	calls   []publishedEvent
	noTopic bool // TopicExists reports false when set
}

type publishedEvent struct {
	account string
	topic   string
	data    []byte
	attrs   map[string]string
}

func (f *fakePublisher) PublishEvent(_ context.Context, account, topic string, data []byte, attrs map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[string]string, len(attrs))
	for k, v := range attrs {
		cp[k] = v
	}
	f.calls = append(f.calls, publishedEvent{account: account, topic: topic, data: data, attrs: cp})
	return "msg-1", nil
}

// TopicExists always reports true; fan-out tests pre-create a config with a
// well-formed topic.
func (f *fakePublisher) TopicExists(context.Context, string, string) (bool, error) {
	return !f.noTopic, nil
}

func (f *fakePublisher) snapshot() []publishedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publishedEvent(nil), f.calls...)
}

func newNotifProvider() (*Provider, *fakePublisher) {
	p := newTestProvider()
	fp := &fakePublisher{}
	p.SetEventPublisher(fp)
	return p, fp
}

func createBucket(t *testing.T, p *Provider, name string) {
	t.Helper()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": name}
	if _, err := p.BucketsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
}

func notificationParams(bucket string) *model.NormalizedRequest {
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	return nr
}

func TestNotificationCRUDRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "notif-bucket")

	ins := notificationParams("notif-bucket")
	ins.Params["body"] = map[string]any{
		"topic":              "projects/proj/topics/events",
		"payload_format":     "JSON_API_V1",
		"event_types":        []any{"OBJECT_FINALIZE", "OBJECT_DELETE"},
		"custom_attributes":  map[string]any{"env": "test"},
		"object_name_prefix": "notification/",
	}
	resp, err := p.NotificationsInsert(ctx, ins)
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	id, _ := resp.Data["id"].(string)
	if id == "" {
		t.Fatal("expected a generated notification id")
	}
	if got, _ := resp.Data["object_name_prefix"].(string); got != "notification/" {
		t.Errorf("object_name_prefix = %q, want notification/", got)
	}
	if got, _ := resp.Data["payload_format"].(string); got != "JSON_API_V1" {
		t.Errorf("payload_format = %q, want JSON_API_V1", got)
	}
	attrs, _ := resp.Data["custom_attributes"].(map[string]any)
	if attrs["env"] != "test" {
		t.Errorf("custom_attributes = %v, want env=test", resp.Data["custom_attributes"])
	}

	// List includes it.
	list := notificationParams("notif-bucket")
	lresp, err := p.NotificationsList(ctx, list)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	items, _ := lresp.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 notification, got %d (%v)", len(items), lresp.Data)
	}
	if got, _ := lresp.Data["kind"].(string); got != "storage#notifications" {
		t.Errorf("kind = %q, want storage#notifications", got)
	}

	// Get by id round-trips.
	get := notificationParams("notif-bucket")
	get.Params["notification"] = id
	gresp, err := p.NotificationsGet(ctx, get)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if got, _ := gresp.Data["topic"].(string); got != "projects/proj/topics/events" {
		t.Errorf("topic = %q", got)
	}
	if got, _ := gresp.Data["selfLink"].(string); got == "" {
		t.Error("expected a selfLink")
	}

	// Delete removes it; a subsequent get is NotFound.
	del := notificationParams("notif-bucket")
	del.Params["notification"] = id
	if _, err := p.NotificationsDelete(ctx, del); err != nil {
		t.Fatalf("delete notification: %v", err)
	}
	if _, err := p.NotificationsGet(ctx, get); err == nil {
		t.Fatal("expected get after delete to fail")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404 NotFound, got %v", err)
	}

	lresp, err = p.NotificationsList(ctx, list)
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	items, _ = lresp.Data["items"].([]any)
	if len(items) != 0 {
		t.Fatalf("expected 0 notifications after delete, got %d", len(items))
	}
}

func TestNotificationMissingBucket(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := notificationParams("nope")
	nr.Params["body"] = map[string]any{"topic": "projects/proj/topics/t"}
	if _, err := p.NotificationsInsert(ctx, nr); err == nil {
		t.Fatal("expected insert on missing bucket to fail")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404, got %v", err)
	}
}

func TestNotificationMissingTopicRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	nr := notificationParams("bkt")
	nr.Params["body"] = map[string]any{"payload_format": "JSON_API_V1"}
	if _, err := p.NotificationsInsert(ctx, nr); err == nil {
		t.Fatal("expected insert without a topic to fail")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Fatalf("expected 400, got %v", err)
	}
}

func TestNotificationUnknownTopicRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	fp := &fakePublisher{noTopic: true}
	p.SetEventPublisher(fp)
	createBucket(t, p, "bkt")
	nr := notificationParams("bkt")
	nr.Params["body"] = map[string]any{"topic": "projects/proj/topics/missing"}
	if _, err := p.NotificationsInsert(ctx, nr); err == nil {
		t.Fatal("expected insert with an unknown topic to fail")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404, got %v", err)
	}
}

func TestNotificationDefaultEventTypesMatchAll(t *testing.T) {
	ctx := context.Background()
	p, fp := newNotifProvider()
	createBucket(t, p, "bkt")

	ins := notificationParams("bkt")
	// No event_types: per the Discovery schema, an empty filter matches every
	// event type.
	ins.Params["body"] = map[string]any{"topic": "projects/proj/topics/events"}
	if _, err := p.NotificationsInsert(ctx, ins); err != nil {
		t.Fatalf("insert: %v", err)
	}

	insertObject(t, p, "bkt", "obj.txt", []byte("hi"))
	calls := fp.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(calls))
	}
	if calls[0].attrs["eventType"] != "OBJECT_FINALIZE" {
		t.Errorf("eventType = %q, want OBJECT_FINALIZE", calls[0].attrs["eventType"])
	}
}

func TestNotificationFanOutMatchesPrefixAndEventType(t *testing.T) {
	ctx := context.Background()
	p, fp := newNotifProvider()
	createBucket(t, p, "bkt")

	ins := notificationParams("bkt")
	ins.Params["body"] = map[string]any{
		"topic":              "projects/proj/topics/events",
		"event_types":        []any{"OBJECT_FINALIZE", "OBJECT_DELETE"},
		"custom_attributes":  map[string]any{"env": "test"},
		"object_name_prefix": "notification/",
	}
	if _, err := p.NotificationsInsert(ctx, ins); err != nil {
		t.Fatalf("insert notification: %v", err)
	}

	// Matching prefix + finalize event.
	insertObject(t, p, "bkt", "notification/trigger.txt", []byte("trigger"))
	calls := fp.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 event after matching upload, got %d", len(calls))
	}
	got := calls[0]
	if got.topic != "projects/proj/topics/events" {
		t.Errorf("topic = %q", got.topic)
	}
	if got.account != "proj" {
		t.Errorf("account = %q, want proj", got.account)
	}
	for k, want := range map[string]string{
		"eventType":          "OBJECT_FINALIZE",
		"bucketId":           "bkt",
		"objectId":           "notification/trigger.txt",
		"env":                "test",
		"payloadFormat":      "JSON_API_V1",
		"notificationConfig": "projects/_/buckets/bkt/notificationConfigs/1",
	} {
		if got.attrs[k] != want {
			t.Errorf("attribute %s = %q, want %q", k, got.attrs[k], want)
		}
	}
	if got.attrs["objectGeneration"] == "" {
		t.Error("expected a non-empty objectGeneration attribute")
	}
	if got.attrs["eventTime"] == "" {
		t.Error("expected a non-empty eventTime attribute")
	}
	// JSON_API_V1 payload is the storage#object resource for the object.
	var payload map[string]any
	if err := json.Unmarshal(got.data, &payload); err != nil {
		t.Fatalf("event data is not JSON: %v", err)
	}
	if payload["kind"] != "storage#object" || payload["name"] != "notification/trigger.txt" {
		t.Errorf("unexpected event payload: %v", payload)
	}

	// A non-matching prefix must not publish.
	insertObject(t, p, "bkt", "other/trigger.txt", []byte("other"))
	if len(fp.snapshot()) != 1 {
		t.Fatalf("expected no event for non-matching prefix, got %d", len(fp.snapshot()))
	}

	// Deleting a matching object publishes OBJECT_DELETE.
	del := notificationParams("bkt")
	del.Params["object"] = "notification/trigger.txt"
	if _, err := p.ObjectsDelete(ctx, del); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	calls = fp.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 events after delete, got %d", len(calls))
	}
	if calls[1].attrs["eventType"] != "OBJECT_DELETE" {
		t.Errorf("delete eventType = %q, want OBJECT_DELETE", calls[1].attrs["eventType"])
	}
}

func TestNotificationEventTypeFilterExcludesUnlisted(t *testing.T) {
	ctx := context.Background()
	p, fp := newNotifProvider()
	createBucket(t, p, "bkt")

	ins := notificationParams("bkt")
	ins.Params["body"] = map[string]any{
		"topic":       "projects/proj/topics/events",
		"event_types": []any{"OBJECT_DELETE"},
	}
	if _, err := p.NotificationsInsert(ctx, ins); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Finalize is not in the filter: no event.
	insertObject(t, p, "bkt", "obj.txt", []byte("hi"))
	if len(fp.snapshot()) != 0 {
		t.Fatalf("expected no finalize event, got %d", len(fp.snapshot()))
	}
}

func TestBucketDeleteDropsNotifications(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	ins := notificationParams("bkt")
	ins.Params["body"] = map[string]any{"topic": "projects/proj/topics/events"}
	if _, err := p.NotificationsInsert(ctx, ins); err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	del := bucketParams()
	del.Params["bucket"] = "bkt"
	if _, err := p.BucketsDelete(ctx, del); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	entries, _ := p.listBucketNotifications(ctx, "proj", "bkt")
	if len(entries) != 0 {
		t.Fatalf("expected notifications dropped with bucket, got %d", len(entries))
	}
}

// insertObject uploads raw bytes through the provider (the media path).
func insertObject(t *testing.T, p *Provider, bucket, object string, data []byte) {
	t.Helper()
	nr := notificationParams(bucket)
	nr.Params["object"] = object
	nr.Params[wire.MediaKey] = data
	if _, err := p.ObjectsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert object %s: %v", object, err)
	}
}

func createVersionedBucket(t *testing.T, p *Provider, name string) {
	t.Helper()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": name, "versioning": map[string]any{"enabled": true}}
	if _, err := p.BucketsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert versioned bucket: %v", err)
	}
}

func addNotification(t *testing.T, p *Provider, bucket string, body map[string]any) {
	t.Helper()
	nr := notificationParams(bucket)
	nr.Params["body"] = body
	if _, err := p.NotificationsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert notification: %v", err)
	}
}

func TestNotificationReservedAttributesWinOverCustom(t *testing.T) {
	p, fp := newNotifProvider()
	createBucket(t, p, "bkt")
	addNotification(t, p, "bkt", map[string]any{
		"topic":             "projects/proj/topics/events",
		"custom_attributes": map[string]any{"eventType": "SPOOFED", "bucketId": "spoofed"},
	})

	insertObject(t, p, "bkt", "obj.txt", []byte("hi"))
	calls := fp.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 event, got %d", len(calls))
	}
	if got := calls[0].attrs["eventType"]; got != "OBJECT_FINALIZE" {
		t.Errorf("eventType = %q, want the reserved OBJECT_FINALIZE", got)
	}
	if got := calls[0].attrs["bucketId"]; got != "bkt" {
		t.Errorf("bucketId = %q, want the reserved bkt", got)
	}
}

func TestNotificationInvalidEnumsRejected(t *testing.T) {
	ctx := context.Background()
	p, _ := newNotifProvider()
	createBucket(t, p, "bkt")

	for _, body := range []map[string]any{
		{"topic": "projects/proj/topics/events", "event_types": []any{"OBJECT_NONSENSE"}},
		{"topic": "projects/proj/topics/events", "payload_format": "XML"},
	} {
		nr := notificationParams("bkt")
		nr.Params["body"] = body
		if _, err := p.NotificationsInsert(ctx, nr); err == nil {
			t.Fatalf("expected %v to be rejected", body)
		} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 for %v, got %v", body, err)
		}
	}
}

// TestNotificationReplaceEvents verifies the GCS replacement pair: the new
// generation finalizes (with overwroteGeneration) and the replaced object is
// reported as OBJECT_DELETE (non-versioned) or OBJECT_ARCHIVE (versioned) with
// overwrittenByGeneration.
func TestNotificationReplaceEvents(t *testing.T) {
	for _, tc := range []struct {
		name      string
		versioned bool
		wantType  string
	}{
		{"non-versioned", false, "OBJECT_DELETE"},
		{"versioned", true, "OBJECT_ARCHIVE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, fp := newNotifProvider()
			if tc.versioned {
				createVersionedBucket(t, p, "bkt")
			} else {
				createBucket(t, p, "bkt")
			}
			addNotification(t, p, "bkt", map[string]any{
				"topic":       "projects/proj/topics/events",
				"event_types": []any{"OBJECT_FINALIZE", "OBJECT_DELETE", "OBJECT_ARCHIVE"},
			})

			insertObject(t, p, "bkt", "obj.txt", []byte("v1"))
			first := fp.snapshot()
			if len(first) != 1 {
				t.Fatalf("expected 1 finalize for the first write, got %d", len(first))
			}
			gen1 := first[0].attrs["objectGeneration"]

			insertObject(t, p, "bkt", "obj.txt", []byte("v2"))
			calls := fp.snapshot()
			if len(calls) != 3 {
				t.Fatalf("expected finalize + replace + finalize, got %d events: %v", len(calls), calls)
			}
			replace, finalize := calls[1], calls[2]
			if replace.attrs["eventType"] != tc.wantType {
				t.Errorf("replace eventType = %q, want %q", replace.attrs["eventType"], tc.wantType)
			}
			if replace.attrs["overwrittenByGeneration"] != finalize.attrs["objectGeneration"] {
				t.Errorf("overwrittenByGeneration = %q, want %q", replace.attrs["overwrittenByGeneration"], finalize.attrs["objectGeneration"])
			}
			if finalize.attrs["overwroteGeneration"] != gen1 {
				t.Errorf("overwroteGeneration = %q, want %q", finalize.attrs["overwroteGeneration"], gen1)
			}
			if finalize.attrs["eventType"] != "OBJECT_FINALIZE" {
				t.Errorf("finalize eventType = %q", finalize.attrs["eventType"])
			}
		})
	}
}

func TestNotificationVersionedDeleteEmitsArchive(t *testing.T) {
	ctx := context.Background()
	p, fp := newNotifProvider()
	createVersionedBucket(t, p, "bkt")
	addNotification(t, p, "bkt", map[string]any{
		"topic":       "projects/proj/topics/events",
		"event_types": []any{"OBJECT_FINALIZE", "OBJECT_DELETE", "OBJECT_ARCHIVE"},
	})
	insertObject(t, p, "bkt", "obj.txt", []byte("v1"))

	del := notificationParams("bkt")
	del.Params["object"] = "obj.txt"
	if _, err := p.ObjectsDelete(ctx, del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	calls := fp.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 events, got %d", len(calls))
	}
	if got := calls[1].attrs["eventType"]; got != "OBJECT_ARCHIVE" {
		t.Errorf("versioned delete eventType = %q, want OBJECT_ARCHIVE", got)
	}
}

func TestNotificationCRUDBumpsBucketMetageneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	before, _ := p.objects.GetBucket(ctx, "bkt")

	ins := notificationParams("bkt")
	ins.Params["body"] = map[string]any{"topic": "projects/proj/topics/events"}
	resp, err := p.NotificationsInsert(ctx, ins)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	afterInsert, _ := p.objects.GetBucket(ctx, "bkt")
	beforeN, _ := strconv.Atoi(gcs.BucketMetageneration(before))
	insertN, _ := strconv.Atoi(gcs.BucketMetageneration(afterInsert))
	if insertN <= beforeN {
		t.Fatalf("expected metageneration to increase on insert: %d -> %d", beforeN, insertN)
	}

	del := notificationParams("bkt")
	del.Params["notification"] = resp.Data["id"].(string)
	if _, err := p.NotificationsDelete(ctx, del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	afterDelete, _ := p.objects.GetBucket(ctx, "bkt")
	deleteN, _ := strconv.Atoi(gcs.BucketMetageneration(afterDelete))
	if deleteN <= insertN {
		t.Fatalf("expected metageneration to increase on delete: %d -> %d", insertN, deleteN)
	}
}
