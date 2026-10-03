package secretmanager

import (
	"context"
	"encoding/base64"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func TestSecretRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	// Create.
	nr := newNR(map[string]any{"secretId": "my-secret", "body": map[string]any{"replication": map[string]any{"automatic": map[string]any{}}}})
	createResp, err := p.Create(ctx, nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	createEtag, _ := createResp.Data["etag"].(string)
	if createEtag == "" {
		t.Error("expected etag on secret create response")
	}

	// Get returns a matching (stable) etag.
	nr = newNR(map[string]any{"name": "secrets/my-secret"})
	getResp, err := p.Get(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	getEtag, _ := getResp.Data["etag"].(string)
	if getEtag == "" || getEtag != createEtag {
		t.Errorf("expected stable etag on get (create=%q get=%q)", createEtag, getEtag)
	}

	// Add version.
	nr = newNR(map[string]any{"name": "secrets/my-secret", "body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}}})
	resp, err := p.AddVersion(ctx, nr)
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	if resp.Data["name"] != "projects/proj/secrets/my-secret/versions/1" {
		t.Errorf("unexpected version name: %v", resp.Data["name"])
	}

	// Access version 1.
	nr = newNR(map[string]any{"name": "secrets/my-secret/versions/1"})
	resp, err = p.Access(ctx, nr)
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	payload, _ := resp.Data["payload"].(map[string]any)
	if payload["data"] != "aGVsbG8=" {
		t.Errorf("unexpected payload data: %v", payload["data"])
	}

	// List has one secret.
	nr = newNR(nil)
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	secrets, _ := resp.Data["secrets"].([]any)
	if len(secrets) != 1 {
		t.Errorf("expected 1 secret, got %d", len(secrets))
	}
}

// TestSecretListVersions verifies the versions-collection route returns 200
// with the created versions (regression: it was decoded as Secret.Get and
// 404'd), and that a missing parent secret still 404s.
func TestSecretListVersions(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := p.AddVersion(ctx, newNR(map[string]any{
			"name": "secrets/s",
			"body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
		})); err != nil {
			t.Fatalf("addVersion %d: %v", i, err)
		}
	}

	resp, err := p.ListVersions(ctx, newNR(map[string]any{"name": "secrets/s/versions"}))
	if err != nil {
		t.Fatalf("listVersions: %v", err)
	}
	versions, _ := resp.Data["versions"].([]any)
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(versions))
	}
	if got := resp.Data["totalSize"]; got != 2 {
		t.Errorf("totalSize = %v, want 2", got)
	}
	first, _ := versions[0].(map[string]any)
	if first["name"] != "projects/proj/secrets/s/versions/1" {
		t.Errorf("first version name = %v, want projects/proj/secrets/s/versions/1", first["name"])
	}
	if first["state"] != "ENABLED" {
		t.Errorf("first version state = %v, want ENABLED", first["state"])
	}

	_, err = p.ListVersions(ctx, newNR(map[string]any{"name": "secrets/missing/versions"}))
	if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 404 {
		t.Fatalf("missing secret: err = %v, want 404 ProviderError", err)
	}
}

// tickingClock advances by one second on every read, so sequentially created
// secrets get strictly increasing create times deterministically.
type tickingClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *tickingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Second)
	return c.t
}

// TestSecretListNewestFirst verifies ListSecrets returns secrets newest-first
// (reverse order by createTime), matching the official ordering, and that the
// order is stable across pagination. The pre-fix code sorted by name ascending.
func TestSecretListNewestFirst(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	clock.SetGlobalClock(&tickingClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
	defer clock.SetGlobalClock(clock.RealClock{})

	// Created in order alpha, bravo, charlie → charlie is newest.
	for _, id := range []string{"alpha", "bravo", "charlie"} {
		if _, err := p.Create(ctx, newNR(map[string]any{"secretId": id})); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	var ids []string
	var createTimes []time.Time
	token := ""
	for page := 0; ; page++ {
		if page > 5 {
			t.Fatal("pagination did not terminate")
		}
		params := map[string]any{"pageSize": "2"}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.List(ctx, newNR(params))
		if err != nil {
			t.Fatalf("list page %d: %v", page, err)
		}
		if resp.Data["totalSize"] != 3 {
			t.Errorf("page %d totalSize = %v, want 3", page, resp.Data["totalSize"])
		}
		items, _ := resp.Data["secrets"].([]any)
		for _, it := range items {
			m := it.(map[string]any)
			name, _ := m["name"].(string)
			ids = append(ids, name[strings.LastIndex(name, "/")+1:])
			ct, err := time.Parse(time.RFC3339Nano, m["createTime"].(string))
			if err != nil {
				t.Fatalf("parse createTime %v: %v", m["createTime"], err)
			}
			createTimes = append(createTimes, ct)
		}
		next, _ := resp.Data["nextPageToken"].(string)
		if next == "" {
			break
		}
		token = next
	}

	if want := []string{"charlie", "bravo", "alpha"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("secret order = %v, want %v (newest first)", ids, want)
	}
	for i := 1; i < len(createTimes); i++ {
		if !createTimes[i].Before(createTimes[i-1]) {
			t.Fatalf("createTimes not strictly descending: %v", createTimes)
		}
	}
}

// TestSecretVersionIntegrity verifies every version resource returns the only
// integrity field the real SecretVersion carries: clientSpecifiedPayloadChecksum.
// The Discovery schema and the v1 proto define no `checksum` property, so the
// emulator must not emit one (the wire-conformance harness would flag it as an
// unknown field). gcloud's `secrets versions add` checks this boolean and aborts
// with a false data-corruption warning when it is absent.
func TestSecretVersionIntegrity(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	assert := func(label string, resp *model.ProviderResponse) {
		t.Helper()
		if got, _ := resp.Data["clientSpecifiedPayloadChecksum"].(bool); !got {
			t.Errorf("%s: clientSpecifiedPayloadChecksum = %v, want true", label, resp.Data["clientSpecifiedPayloadChecksum"])
		}
		if v, present := resp.Data["checksum"]; present {
			t.Errorf("%s: unexpected `checksum` field (not in the Discovery schema/proto): %v", label, v)
		}
	}

	payload := base64.StdEncoding.EncodeToString([]byte("hello, secrets"))
	addResp, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": payload}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	assert("addVersion", addResp)

	getResp, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"}))
	if err != nil {
		t.Fatalf("getVersion: %v", err)
	}
	assert("getVersion", getResp)

	// Lifecycle responses (setVersionState) must stay consistent too.
	disableResp, err := p.DisableVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"}))
	if err != nil {
		t.Fatalf("disableVersion: %v", err)
	}
	assert("disableVersion", disableResp)
}
