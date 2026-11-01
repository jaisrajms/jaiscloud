package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	secretmanagersvc "jaiscloud/internal/gcp/service/secretmanager"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/store"
)

// publishedMessage is one Pub/Sub publish a test observed.
type publishedMessage struct {
	account string
	topic   string
	data    []byte
	attrs   map[string]string
}

// fakePublisher records every notification the provider emits.
type fakePublisher struct {
	mu   sync.Mutex
	msgs []publishedMessage
}

func (f *fakePublisher) PublishEvent(_ context.Context, accountID, topic string, data []byte, attributes map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, publishedMessage{account: accountID, topic: topic, data: data, attrs: attributes})
	return fmt.Sprintf("%d", len(f.msgs)), nil
}

// drain returns the messages observed so far and clears them.
func (f *fakePublisher) drain() []publishedMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.msgs
	f.msgs = nil
	return out
}

func newTestProvider(t *testing.T) (*Provider, *fakePublisher) {
	t.Helper()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
	pub := &fakePublisher{}
	p.SetEventPublisher(pub)
	return p, pub
}

// eventTypes lists the eventType of every observed message.
func eventTypes(msgs []publishedMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.attrs["eventType"])
	}
	return out
}

func wantEventTypes(t *testing.T, msgs []publishedMessage, want ...string) {
	t.Helper()
	got := eventTypes(msgs)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

func TestSecretTopicsRoundTrip(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)

	topics := []any{
		map[string]any{"name": "projects/proj/topics/a"},
		map[string]any{"name": "projects/proj/topics/b"},
	}
	resp, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"topics": topics},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, _ := resp.Data["topics"].([]any)
	if len(got) != 2 {
		t.Fatalf("create response topics = %#v, want 2", resp.Data["topics"])
	}
	name, _ := got[0].(map[string]any)["name"].(string)
	if name != "projects/proj/topics/a" {
		t.Fatalf("topic name = %q", name)
	}

	resp, err = p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got, _ := resp.Data["topics"].([]any); len(got) != 2 {
		t.Fatalf("get topics = %#v, want 2", resp.Data["topics"])
	}

	// A secret with no event notifications omits the field entirely.
	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "plain"})); err != nil {
		t.Fatalf("create plain: %v", err)
	}
	resp, err = p.Get(ctx, newNR(map[string]any{"name": "secrets/plain"}))
	if err != nil {
		t.Fatalf("get plain: %v", err)
	}
	if _, ok := resp.Data["topics"]; ok {
		t.Fatalf("plain secret reported topics: %#v", resp.Data["topics"])
	}
}

// TestSecretEtagCoversTopics: topics are mutable metadata, so a stale etag must
// not be able to overwrite a concurrent topic change.
func TestSecretEtagCoversTopics(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)

	resp, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	before, _ := resp.Data["etag"].(string)

	resp, err = p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "topics",
		"body": map[string]any{"topics": []any{
			map[string]any{"name": "projects/proj/topics/a"},
		}},
	}))
	if err != nil {
		t.Fatalf("update topics: %v", err)
	}
	after, _ := resp.Data["etag"].(string)
	if before == "" || before == after {
		t.Fatalf("etag did not change across a topics update: %q → %q", before, after)
	}

	// The stale etag is rejected (ABORTED, HTTP 409).
	_, err = p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "labels",
		"body":       map[string]any{"etag": before, "labels": map[string]any{"k": "v"}},
	}))
	if err == nil {
		t.Fatal("a stale etag was accepted after a topics change")
	}
}

func TestSecretEventNotifications(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)

	const secretName = "projects/proj/secrets/s"
	topics := []any{
		map[string]any{"name": "projects/proj/topics/a"},
		map[string]any{"name": "projects/proj/topics/b"},
	}

	// Create: TOPIC_CONFIGURED on every topic, then SECRET_CREATE on every topic.
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"topics": topics, "labels": map[string]any{"env": "dev"}},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	msgs := pub.drain()
	wantEventTypes(t, msgs, "TOPIC_CONFIGURED", "TOPIC_CONFIGURED", "SECRET_CREATE", "SECRET_CREATE")
	if msgs[0].topic != "projects/proj/topics/a" || msgs[1].topic != "projects/proj/topics/b" {
		t.Fatalf("TOPIC_CONFIGURED topics = %q, %q", msgs[0].topic, msgs[1].topic)
	}
	if len(msgs[0].data) != 0 || len(msgs[0].attrs) != 1 {
		t.Fatalf("TOPIC_CONFIGURED = data %q attrs %#v, want no body and only eventType", msgs[0].data, msgs[0].attrs)
	}
	created := msgs[2]
	if created.attrs["secretId"] != secretName || created.attrs["dataFormat"] != secretmanagersvc.DataFormatJSONAPIV1 || created.attrs["timestamp"] == "" {
		t.Fatalf("SECRET_CREATE attributes = %#v", created.attrs)
	}
	var secretBody map[string]any
	if err := json.Unmarshal(created.data, &secretBody); err != nil {
		t.Fatalf("SECRET_CREATE data is not JSON: %v", err)
	}
	if secretBody["name"] != secretName {
		t.Fatalf("SECRET_CREATE data name = %v", secretBody["name"])
	}
	if got, _ := secretBody["topics"].([]any); len(got) != 2 {
		t.Fatalf("SECRET_CREATE data topics = %#v, want 2", secretBody["topics"])
	}

	// Reads publish nothing (the API documents Get/List/Access as side-effect
	// free; a schedule that is not due does not rotate either).
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := p.List(ctx, newNR(nil)); err != nil {
		t.Fatalf("list: %v", err)
	}
	if msgs := pub.drain(); len(msgs) != 0 {
		t.Fatalf("reads published %v, want nothing", eventTypes(msgs))
	}

	// A version add.
	resp, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	versionName, _ := resp.Data["name"].(string)
	msgs = pub.drain()
	wantEventTypes(t, msgs, "SECRET_VERSION_ADD", "SECRET_VERSION_ADD")
	if msgs[0].attrs["versionId"] != versionName {
		t.Fatalf("SECRET_VERSION_ADD versionId = %q, want %q", msgs[0].attrs["versionId"], versionName)
	}
	var versionBody map[string]any
	if err := json.Unmarshal(msgs[0].data, &versionBody); err != nil || versionBody["name"] != versionName {
		t.Fatalf("SECRET_VERSION_ADD data = %s (%v)", msgs[0].data, err)
	}

	// Disable.
	if _, err := p.DisableVersion(ctx, newNR(map[string]any{"name": versionName})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	wantEventTypes(t, pub.drain(), "SECRET_VERSION_DISABLE", "SECRET_VERSION_DISABLE")

	// Enable.
	if _, err := p.EnableVersion(ctx, newNR(map[string]any{"name": versionName})); err != nil {
		t.Fatalf("enable: %v", err)
	}
	wantEventTypes(t, pub.drain(), "SECRET_VERSION_ENABLE", "SECRET_VERSION_ENABLE")

	// Destroy.
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": versionName})); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	wantEventTypes(t, pub.drain(), "SECRET_VERSION_DESTROY", "SECRET_VERSION_DESTROY")

	// A metadata update.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "labels",
		"body":       map[string]any{"labels": map[string]any{"env": "prod"}},
	})); err != nil {
		t.Fatalf("update: %v", err)
	}
	wantEventTypes(t, pub.drain(), "SECRET_UPDATE", "SECRET_UPDATE")

	// Delete: the pre-delete metadata, deleteType REQUESTED.
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	msgs = pub.drain()
	wantEventTypes(t, msgs, "SECRET_DELETE", "SECRET_DELETE")
	if msgs[0].attrs["deleteType"] != "REQUESTED" {
		t.Fatalf("SECRET_DELETE deleteType = %q", msgs[0].attrs["deleteType"])
	}
	if err := json.Unmarshal(msgs[0].data, &secretBody); err != nil || secretBody["name"] != secretName {
		t.Fatalf("SECRET_DELETE data = %s (%v), want the pre-delete secret", msgs[0].data, err)
	}
}

// TestManagedRotationEvents: enabling managed rotation adds a version and also
// records the schedule status on the secret, so the topics see
// SECRET_VERSION_ADD followed by SECRET_UPDATE.
func TestManagedRotationEvents(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)

	const name = "projects/proj/secrets/managed-events"
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "managed-events",
		"body":     map[string]any{"topics": []any{map[string]any{"name": "projects/proj/topics/a"}}},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	pub.drain()

	if _, err := p.EnableManagedRotation(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{"cloudSqlSingleUserCredentials": map[string]any{
			"instanceId": "inst", "username": "app",
		}},
	})); err != nil {
		t.Fatalf("EnableManagedRotation: %v", err)
	}
	wantEventTypes(t, pub.drain(), "SECRET_VERSION_ADD", "SECRET_UPDATE")
}

// TestSecretTopicConfiguredOnUpdate: a topics update sends the test message to
// every topic on the (new) list before the SECRET_UPDATE.
func TestSecretTopicConfiguredOnUpdate(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}
	pub.drain()

	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "topics",
		"body": map[string]any{"topics": []any{
			map[string]any{"name": "projects/proj/topics/a"},
		}},
	})); err != nil {
		t.Fatalf("update topics: %v", err)
	}
	msgs := pub.drain()
	wantEventTypes(t, msgs, "TOPIC_CONFIGURED", "SECRET_UPDATE")
	if len(msgs[0].data) != 0 {
		t.Fatalf("TOPIC_CONFIGURED carried data %q", msgs[0].data)
	}
}

// TestRotationSubfieldPatchKeepsNextTime: updateMask=rotation.rotation_period
// must merge the period into the stored schedule, not replace it.
func TestRotationSubfieldPatchKeepsNextTime(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	due := t0.Add(time.Hour).Format(time.RFC3339)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics":   []any{map[string]any{"name": "projects/proj/topics/a"}},
			"rotation": map[string]any{"nextRotationTime": due, "rotationPeriod": "3600s"},
		},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "rotation.rotation_period",
		"body":       map[string]any{"rotation": map[string]any{"rotationPeriod": "7200s"}},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	rot, _ := resp.Data["rotation"].(map[string]any)
	if rot["nextRotationTime"] != due {
		t.Fatalf("nextRotationTime = %v, want the stored %q", rot["nextRotationTime"], due)
	}
	if rot["rotationPeriod"] != "7200s" {
		t.Fatalf("rotationPeriod = %v, want 7200s", rot["rotationPeriod"])
	}
}

// TestSecretRotateNotification: a due schedule publishes SECRET_ROTATE to the
// secret's topics when a read advances it, and adds no version.
func TestSecretRotateNotification(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	due := t0.Add(time.Hour)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics":   []any{map[string]any{"name": "projects/proj/topics/a"}},
			"rotation": map[string]any{"nextRotationTime": due.Format(time.RFC3339), "rotationPeriod": "3600s"},
		},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	pub.drain()

	clock.SetGlobalClock(clock.FixedClock{T: due.Add(time.Minute)})
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("get: %v", err)
	}
	msgs := pub.drain()
	wantEventTypes(t, msgs, "SECRET_ROTATE")
	if msgs[0].attrs["secretId"] != "projects/proj/secrets/s" {
		t.Fatalf("SECRET_ROTATE secretId = %q", msgs[0].attrs["secretId"])
	}
	var body map[string]any
	if err := json.Unmarshal(msgs[0].data, &body); err != nil {
		t.Fatalf("SECRET_ROTATE data is not JSON: %v", err)
	}
	rot, _ := body["rotation"].(map[string]any)
	if rot["nextRotationTime"] == due.Format(time.RFC3339) {
		t.Fatalf("SECRET_ROTATE data carries the old schedule: %v", rot)
	}

	// A second read of the rescheduled secret publishes nothing.
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("second get: %v", err)
	}
	if msgs := pub.drain(); len(msgs) != 0 {
		t.Fatalf("rescheduled secret republished: %v", eventTypes(msgs))
	}
}

// TestRotationValidationREST pins the documented constraints on the REST
// surface, on both create and patch.
func TestRotationValidationREST(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	at := func(d time.Duration) string { return t0.Add(d).UTC().Format(time.RFC3339) }
	topics := []any{map[string]any{"name": "projects/proj/topics/a"}}

	createCases := []struct {
		name string
		body map[string]any
	}{
		{"next under five minutes", map[string]any{"topics": topics, "rotation": map[string]any{"nextRotationTime": at(4 * time.Minute)}}},
		{"next past 100 years", map[string]any{"topics": topics, "rotation": map[string]any{"nextRotationTime": at(101 * 365 * 24 * time.Hour)}}},
		{"period under an hour", map[string]any{"topics": topics, "rotation": map[string]any{"nextRotationTime": at(time.Hour), "rotationPeriod": "3599s"}}},
		{"period without next", map[string]any{"topics": topics, "rotation": map[string]any{"rotationPeriod": "3600s"}}},
		{"go duration syntax", map[string]any{"topics": topics, "rotation": map[string]any{"nextRotationTime": at(time.Hour), "rotationPeriod": "1h"}}},
		{"schedule without topics", map[string]any{"rotation": map[string]any{"nextRotationTime": at(time.Hour)}}},
		{"eleven topics", map[string]any{"topics": elevenTopics()}},
		{"bare topic id", map[string]any{"topics": []any{map[string]any{"name": "my-topic"}}}},
	}
	for i, tc := range createCases {
		p, _ := newTestProvider(t)
		_, err := p.Create(ctx, newNR(map[string]any{"secretId": fmt.Sprintf("s%d", i), "body": tc.body}))
		if err := err; err == nil || errStatus(err) != 400 {
			t.Errorf("%s: create = %v, want 400", tc.name, err)
		}
	}

	// Patch: a period-only schedule on an existing secret, and clearing the
	// topics of a rotating secret.
	p, _ := newTestProvider(t)
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics":   topics,
			"rotation": map[string]any{"nextRotationTime": at(time.Hour), "rotationPeriod": "3600s"},
		},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"rotation": map[string]any{"rotationPeriod": "7200s"}},
	})); err == nil || errStatus(err) != 400 {
		t.Fatalf("period-only patch = %v, want 400", err)
	}
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "topics",
		"body":       map[string]any{"topics": []any{}},
	})); err == nil || errStatus(err) != 400 {
		t.Fatalf("clearing topics on a rotating secret = %v, want 400", err)
	}
	// A fractional period is stored and echoed verbatim, matching what the gRPC
	// transport stores for the same duration.
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "frac",
		"body": map[string]any{
			"topics":   topics,
			"rotation": map[string]any{"nextRotationTime": at(time.Hour), "rotationPeriod": "3600.5s"},
		},
	})); err != nil {
		t.Fatalf("create fractional period: %v", err)
	}
	frac, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/frac"}))
	if err != nil {
		t.Fatalf("get fractional secret: %v", err)
	}
	fracRot, _ := frac.Data["rotation"].(map[string]any)
	if fracRot["rotationPeriod"] != "3600.500s" {
		t.Fatalf("fractional rotationPeriod = %v, want 3600.500s", fracRot["rotationPeriod"])
	}

	// Clearing the whole rotation is allowed, and then the topics can go too.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "rotation",
		"body":       map[string]any{"rotation": map[string]any{}},
	})); err != nil {
		t.Fatalf("clearing the rotation = %v", err)
	}
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "topics",
		"body":       map[string]any{"topics": []any{}},
	})); err != nil {
		t.Fatalf("clearing topics without a schedule = %v", err)
	}
}

func elevenTopics() []any {
	out := make([]any, 0, 11)
	for i := 0; i < 11; i++ {
		out = append(out, map[string]any{"name": fmt.Sprintf("projects/proj/topics/t%d", i)})
	}
	return out
}
