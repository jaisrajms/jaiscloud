// Tests for DC70: delayed version destruction (`versionDestroyTtl`) and secret
// expiration (`expireTime`/`ttl`) on the REST provider, including the two event
// variants that had no trigger (SECRET_VERSION_DESTROY_SCHEDULED and
// SECRET_DELETE with deleteType EXPIRATION).
package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

func fixedClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

func TestVersionDestroyTTLSchedulesDestruction(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)
	scheduledAt := t0.Add(24 * time.Hour)

	created, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics":            []any{map[string]any{"name": "projects/proj/topics/a"}},
			"versionDestroyTtl": "86400s",
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := created.Data["versionDestroyTtl"]; got != "86400s" {
		t.Fatalf("create versionDestroyTtl = %v, want 86400s", got)
	}
	added, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	versionName, _ := added.Data["name"].(string)
	enabledEtag, _ := added.Data["etag"].(string)
	pub.drain()

	// Destroy with a delay: DISABLED immediately, scheduled for destruction
	// after the TTL, and SECRET_VERSION_DESTROY_SCHEDULED rather than DESTROY.
	resp, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": versionName}))
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if resp.Data["state"] != "DISABLED" {
		t.Fatalf("scheduled destroy state = %v, want DISABLED", resp.Data["state"])
	}
	if got := resp.Data["scheduledDestroyTime"]; got != timestamp.Canonical(scheduledAt) {
		t.Fatalf("scheduledDestroyTime = %v, want %v", got, timestamp.Canonical(scheduledAt))
	}
	if _, ok := resp.Data["destroyTime"]; ok {
		t.Fatalf("a scheduled destroy carries destroyTime: %v", resp.Data["destroyTime"])
	}
	if resp.Data["etag"] == enabledEtag {
		t.Fatal("the version etag must change when destruction is scheduled")
	}
	msgs := pub.drain()
	wantEventTypes(t, msgs, "SECRET_VERSION_DESTROY_SCHEDULED")
	if msgs[0].attrs["versionId"] != versionName {
		t.Fatalf("DESTROY_SCHEDULED versionId = %q, want %q", msgs[0].attrs["versionId"], versionName)
	}
	var body map[string]any
	if err := json.Unmarshal(msgs[0].data, &body); err != nil || body["state"] != "DISABLED" {
		t.Fatalf("DESTROY_SCHEDULED data = %s (%v), want the disabled version", msgs[0].data, err)
	}

	// A second destroy before the instant is a no-op (no duplicate message).
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": versionName})); err != nil {
		t.Fatalf("second destroy: %v", err)
	}
	if msgs := pub.drain(); len(msgs) != 0 {
		t.Fatalf("a second destroy republished: %v", eventTypes(msgs))
	}
	// A scheduled (disabled) version cannot be accessed.
	if _, err := p.Access(ctx, newNR(map[string]any{"name": versionName})); err == nil || errStatus(err) != 400 {
		t.Fatalf("access of a scheduled version = %v, want 400", err)
	}

	// Once the delay expires a read performs the destruction, publishing
	// SECRET_VERSION_DESTROY with the scheduled instant as destroyTime.
	clock.SetGlobalClock(clock.FixedClock{T: scheduledAt.Add(time.Second)})
	got, err := p.GetVersion(ctx, newNR(map[string]any{"name": versionName}))
	if err != nil {
		t.Fatalf("getVersion after the delay: %v", err)
	}
	if got.Data["state"] != "DESTROYED" {
		t.Fatalf("state after the delay = %v, want DESTROYED", got.Data["state"])
	}
	if got.Data["destroyTime"] != timestamp.Canonical(scheduledAt) {
		t.Fatalf("destroyTime = %v, want the scheduled instant %v", got.Data["destroyTime"], timestamp.Canonical(scheduledAt))
	}
	wantEventTypes(t, pub.drain(), "SECRET_VERSION_DESTROY")

	// DESTROYED is terminal.
	if _, err := p.EnableVersion(ctx, newNR(map[string]any{"name": versionName})); err == nil || errStatus(err) != 400 {
		t.Fatalf("enable of a destroyed version = %v, want 400", err)
	}
}

func TestVersionDestroyTTLCancel(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics":            []any{map[string]any{"name": "projects/proj/topics/a"}},
			"versionDestroyTtl": "86400s",
		},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	added, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	name, _ := added.Data["name"].(string)
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": name})); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	pub.drain()

	// Enabling a version scheduled for destruction cancels the schedule.
	enabled, err := p.EnableVersion(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if enabled.Data["state"] != "ENABLED" {
		t.Fatalf("state = %v, want ENABLED", enabled.Data["state"])
	}
	if _, ok := enabled.Data["scheduledDestroyTime"]; ok {
		t.Fatalf("the cancelled version still reports scheduledDestroyTime: %v", enabled.Data["scheduledDestroyTime"])
	}
	wantEventTypes(t, pub.drain(), "SECRET_VERSION_ENABLE")

	// Past the original instant the version is still ENABLED (destroyed only if
	// the schedule had survived).
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(25 * time.Hour)})
	got, err := p.GetVersion(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("getVersion: %v", err)
	}
	if got.Data["state"] != "ENABLED" {
		t.Fatalf("cancelled version state = %v, want ENABLED", got.Data["state"])
	}
}

func TestSecretExpirationSweep(t *testing.T) {
	ctx := context.Background()
	p, pub := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)
	expireAt := t0.Add(24 * time.Hour)

	created, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics": []any{map[string]any{"name": "projects/proj/topics/a"}},
			"ttl":    "86400s",
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := created.Data["expireTime"]; got != timestamp.Canonical(expireAt) {
		t.Fatalf("expireTime = %v, want %v", got, timestamp.Canonical(expireAt))
	}
	if _, ok := created.Data["ttl"]; ok {
		t.Fatal("the input-only ttl is echoed in the response")
	}
	if _, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	pub.drain()

	// Before the instant, reads still work and publish nothing.
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("get before expiry: %v", err)
	}
	if msgs := pub.drain(); len(msgs) != 0 {
		t.Fatalf("a live secret published on read: %v", eventTypes(msgs))
	}

	// At the expiration instant the read deletes the secret (and its versions),
	// answering 404 and publishing SECRET_DELETE deleteType EXPIRATION with the
	// pre-delete metadata.
	clock.SetGlobalClock(clock.FixedClock{T: expireAt})
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err == nil || errStatus(err) != 404 {
		t.Fatalf("get at expiry = %v, want 404", err)
	}
	msgs := pub.drain()
	wantEventTypes(t, msgs, "SECRET_DELETE")
	if msgs[0].attrs["deleteType"] != "EXPIRATION" {
		t.Fatalf("deleteType = %q, want EXPIRATION", msgs[0].attrs["deleteType"])
	}
	var body map[string]any
	if err := json.Unmarshal(msgs[0].data, &body); err != nil || body["name"] != "projects/proj/secrets/s" {
		t.Fatalf("SECRET_DELETE data = %s (%v), want the pre-delete secret", msgs[0].data, err)
	}
	if got := body["expireTime"]; got != timestamp.Canonical(expireAt) {
		t.Fatalf("SECRET_DELETE data expireTime = %v", got)
	}

	// The versions went with it, and the list no longer reports the secret.
	if _, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"})); err == nil || errStatus(err) != 404 {
		t.Fatalf("getVersion after expiry = %v, want 404", err)
	}
	list, err := p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["secrets"].([]any); len(items) != 0 {
		t.Fatalf("expired secret still listed: %v", items)
	}
}

func TestSecretExpirationUpdateAndRemove(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}
	// A ttl patch sets the expiration...
	updated, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "ttl",
		"body":       map[string]any{"ttl": "3600s"},
	}))
	if err != nil {
		t.Fatalf("update ttl: %v", err)
	}
	first := timestamp.Canonical(t0.Add(time.Hour))
	if updated.Data["expireTime"] != first {
		t.Fatalf("expireTime = %v, want %v", updated.Data["expireTime"], first)
	}
	// ...and an explicit expireTime replaces it.
	at := t0.Add(48 * time.Hour)
	updated, err = p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "expire_time",
		"body":       map[string]any{"expireTime": at.Format(time.RFC3339)},
	}))
	if err != nil {
		t.Fatalf("update expireTime: %v", err)
	}
	if updated.Data["expireTime"] != timestamp.Canonical(at) {
		t.Fatalf("expireTime = %v, want %v", updated.Data["expireTime"], timestamp.Canonical(at))
	}
	// A masked, fieldless patch removes the expiration.
	updated, err = p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "expire_time",
		"body":       map[string]any{},
	}))
	if err != nil {
		t.Fatalf("remove expiration: %v", err)
	}
	if _, ok := updated.Data["expireTime"]; ok {
		t.Fatalf("expireTime survived removal: %v", updated.Data["expireTime"])
	}
	clock.SetGlobalClock(clock.FixedClock{T: at.Add(time.Hour)})
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("secret without expiration expired: %v", err)
	}

	// The delayed-destruction delay can be set, changed, and removed the same way.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "version_destroy_ttl",
		"body":       map[string]any{"versionDestroyTtl": "172800s"},
	})); err != nil {
		t.Fatalf("set versionDestroyTtl: %v", err)
	}
	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["versionDestroyTtl"] != "172800s" {
		t.Fatalf("versionDestroyTtl = %v, want 172800s", got.Data["versionDestroyTtl"])
	}
	got, err = p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "version_destroy_ttl",
		"body":       map[string]any{},
	}))
	if err != nil {
		t.Fatalf("remove versionDestroyTtl: %v", err)
	}
	if _, ok := got.Data["versionDestroyTtl"]; ok {
		t.Fatalf("versionDestroyTtl survived removal: %v", got.Data["versionDestroyTtl"])
	}
}

func TestExpirationValidationREST(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	cases := []struct {
		name string
		body map[string]any
	}{
		{"ttl under 60s", map[string]any{"ttl": "59s"}},
		{"ttl over 100 years", map[string]any{"ttl": "3153600001s"}},
		{"ttl in Go syntax", map[string]any{"ttl": "24h"}},
		{"both expiration arms", map[string]any{"ttl": "86400s", "expireTime": t0.Add(48 * time.Hour).Format(time.RFC3339)}},
		{"expireTime under 60s", map[string]any{"expireTime": t0.Add(30 * time.Second).Format(time.RFC3339)}},
		{"destroy ttl under a day", map[string]any{"versionDestroyTtl": "86399s"}},
		{"destroy ttl over 1000 days", map[string]any{"versionDestroyTtl": "86400001s"}},
		{"destroy ttl in Go syntax", map[string]any{"versionDestroyTtl": "24h"}},
	}
	for _, tc := range cases {
		if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "bad", "body": tc.body})); err == nil || errStatus(err) != 400 {
			t.Errorf("%s: create = %v, want 400", tc.name, err)
		}
	}

	// A patch is validated too, and leaves the stored value untouched.
	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"versionDestroyTtl": "86400s"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "version_destroy_ttl",
		"body":       map[string]any{"versionDestroyTtl": "60s"},
	})); err == nil || errStatus(err) != 400 {
		t.Fatalf("invalid patch = %v, want 400", err)
	}
	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["versionDestroyTtl"] != "86400s" {
		t.Fatalf("rejected patch changed the stored delay: %v", got.Data["versionDestroyTtl"])
	}
}

// TestExpirationEtags: the expiration and the destruction delay are mutable
// metadata, and a scheduled destruction is a version mutation, so each must be
// covered by the respective etag.
func TestExpirationEtags(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	created, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	baseEtag, _ := created.Data["etag"].(string)
	updated, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "ttl",
		"body":       map[string]any{"ttl": "86400s"},
	}))
	if err != nil {
		t.Fatalf("update ttl: %v", err)
	}
	if updated.Data["etag"] == baseEtag {
		t.Fatal("the secret etag did not change when the expiration was set")
	}

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "d",
		"body":     map[string]any{"versionDestroyTtl": "86400s"},
	})); err != nil {
		t.Fatalf("create d: %v", err)
	}
	added, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/d",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	versionEtag, _ := added.Data["etag"].(string)
	scheduled, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": "secrets/d/versions/1"}))
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if scheduled.Data["etag"] == versionEtag {
		t.Fatal("the version etag did not change when destruction was scheduled")
	}
	cancelled, err := p.EnableVersion(ctx, newNR(map[string]any{"name": "secrets/d/versions/1"}))
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if cancelled.Data["etag"] == scheduled.Data["etag"] {
		t.Fatal("the version etag did not change when the schedule was cancelled")
	}
}

// TestLifecycleFieldsRespectTheMask: a field the update mask does not name is
// never read, so a labels-only patch carrying a stale read-back expiration or
// destruction delay cannot rewrite it (AIP-161).
func TestLifecycleFieldsRespectTheMask(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"versionDestroyTtl": "86400s", "ttl": "86400s"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	expire := timestamp.Canonical(t0.Add(24 * time.Hour))
	updated, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "labels",
		"body": map[string]any{
			"labels":            map[string]any{"env": "prod"},
			"versionDestroyTtl": "172800s",
			"expireTime":        t0.Add(48 * time.Hour).Format(time.RFC3339),
		},
	}))
	if err != nil {
		t.Fatalf("update labels: %v", err)
	}
	labels, _ := updated.Data["labels"].(map[string]string)
	if labels["env"] != "prod" {
		t.Fatalf("labels = %v, want env=prod", updated.Data["labels"])
	}
	if updated.Data["versionDestroyTtl"] != "86400s" {
		t.Fatalf("an unmasked delay was applied: %v", updated.Data["versionDestroyTtl"])
	}
	if updated.Data["expireTime"] != expire {
		t.Fatalf("an unmasked expiration was applied: %v, want %v", updated.Data["expireTime"], expire)
	}
}

// TestMaskedBodylessClear: the documented removal form is a masked patch with no
// body at all (`PATCH ?updateMask=expire_time`), not only an empty JSON object.
func TestMaskedBodylessClear(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"versionDestroyTtl": "86400s", "ttl": "86400s"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	updated, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "expire_time,version_destroy_ttl",
	}))
	if err != nil {
		t.Fatalf("bodyless masked update: %v", err)
	}
	if _, ok := updated.Data["expireTime"]; ok {
		t.Fatalf("bodyless expire_time clear left %v", updated.Data["expireTime"])
	}
	if _, ok := updated.Data["versionDestroyTtl"]; ok {
		t.Fatalf("bodyless version_destroy_ttl clear left %v", updated.Data["versionDestroyTtl"])
	}
}

// TestDestroyDelayChangeKeepsScheduledVersion: the documented rule is that a
// delay change or removal only affects future destroy attempts; a version
// already scheduled is destroyed at its own instant.
func TestDestroyDelayChangeKeepsScheduledVersion(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	if _, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"versionDestroyTtl": "86400s"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"})); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	// Extend, then remove, the delay: the scheduled version keeps its instant.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "version_destroy_ttl",
		"body":       map[string]any{"versionDestroyTtl": "172800s"},
	})); err != nil {
		t.Fatalf("extend delay: %v", err)
	}
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/s",
		"updateMask": "version_destroy_ttl",
		"body":       map[string]any{},
	})); err != nil {
		t.Fatalf("remove delay: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(24*time.Hour + time.Second)})
	got, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"}))
	if err != nil {
		t.Fatalf("getVersion: %v", err)
	}
	if got.Data["state"] != "DESTROYED" {
		t.Fatalf("scheduled version state = %v, want DESTROYED at its original instant", got.Data["state"])
	}
}

// TestExpiredSecretOperations404: every mutating surface reports 404 once the
// secret expires, not just Get. Each case gets its own expired secret so the
// assertion exercises the sweep rather than a previously deleted secret.
func TestExpiredSecretOperations404(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProvider(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, t0)

	cases := []struct {
		name string
		call func(nr *model.NormalizedRequest) error
	}{
		{"update", func(nr *model.NormalizedRequest) error {
			nr.Params = map[string]any{"name": "secrets/s", "updateMask": "labels",
				"body": map[string]any{"labels": map[string]any{"k": "v"}}}
			_, err := p.Update(ctx, nr)
			return err
		}},
		{"addVersion", func(nr *model.NormalizedRequest) error {
			nr.Params = map[string]any{"name": "secrets/s",
				"body": map[string]any{"payload": map[string]any{"data": "aGk="}}}
			_, err := p.AddVersion(ctx, nr)
			return err
		}},
		{"getIamPolicy", func(nr *model.NormalizedRequest) error {
			nr.Params = map[string]any{"name": "secrets/s"}
			_, err := p.GetIamPolicy(ctx, nr)
			return err
		}},
		{"delete", func(nr *model.NormalizedRequest) error {
			nr.Params = map[string]any{"name": "secrets/s"}
			_, err := p.Delete(ctx, nr)
			return err
		}},
	}
	for i, tc := range cases {
		clock.SetGlobalClock(clock.FixedClock{T: t0})
		id := fmt.Sprintf("s%d", i)
		if _, err := p.Create(ctx, newNR(map[string]any{
			"secretId": id,
			"body":     map[string]any{"ttl": "86400s"},
		})); err != nil {
			t.Fatalf("%s: create: %v", tc.name, err)
		}
		clock.SetGlobalClock(clock.FixedClock{T: t0.Add(24 * time.Hour)})
		nr := newNR(map[string]any{"name": "secrets/" + id})
		if err := tc.call(nr); err == nil || errStatus(err) != 404 {
			t.Errorf("%s on an expired secret = %v, want 404", tc.name, err)
		}
	}
}
