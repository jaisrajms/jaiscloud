package logging

import (
	"net/http"
	"testing"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"
	store "jaiscloud/internal/store"
)

func newAdminProvider(t *testing.T) (*Codec, *Provider) {
	t.Helper()
	return NewCodec(), NewProvider(core.NewService(loggingstore.NewMemoryStore(), "test", core.WithResources(store.NewMemoryResourceStore())), "test")
}

const (
	adminLocParent = "/v2/projects/test/locations/global"
	adminBucket    = adminLocParent + "/buckets/b1"
)

func mustCall(t *testing.T, c *Codec, p *Provider, method, path string, body any) map[string]any {
	t.Helper()
	resp, err := call(t, c, p, method, path, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return wireData(t, resp)
}

func TestRESTBucketRoundTrip(t *testing.T) {
	c, p := newAdminProvider(t)

	created := mustCall(t, c, p, http.MethodPost, adminLocParent+"/buckets?bucketId=b1", map[string]any{
		"description":   "d",
		"retentionDays": float64(30),
	})
	if created["name"] != "projects/test/locations/global/buckets/b1" {
		t.Fatalf("name = %v", created["name"])
	}
	if created["lifecycleState"] != "ACTIVE" {
		t.Fatalf("lifecycleState = %v", created["lifecycleState"])
	}

	got := mustCall(t, c, p, http.MethodGet, adminBucket, nil)
	if got["description"] != "d" {
		t.Fatalf("description = %v", got["description"])
	}

	list := mustCall(t, c, p, http.MethodGet, adminLocParent+"/buckets", nil)
	if buckets, _ := list["buckets"].([]any); len(buckets) != 1 {
		t.Fatalf("buckets = %v", list["buckets"])
	}

	patched := mustCall(t, c, p, http.MethodPatch, adminBucket+"?updateMask=description", map[string]any{
		"description": "d2",
	})
	if patched["description"] != "d2" || patched["retentionDays"] != float64(30) {
		t.Fatalf("patched = %v", patched)
	}

	restricted := mustCall(t, c, p, http.MethodPatch, adminBucket+"?updateMask=restrictedFields", map[string]any{
		"restrictedFields": []any{"jsonPayload.secret", "labels"},
	})
	if rf, _ := restricted["restrictedFields"].([]any); len(rf) != 2 || rf[0] != "jsonPayload.secret" {
		t.Fatalf("restrictedFields = %v", restricted["restrictedFields"])
	}

	// An update_mask path that names no bucket field is InvalidArgument (400).
	if _, err := call(t, c, p, http.MethodPatch, adminBucket+"?updateMask=labels", map[string]any{}); err == nil {
		t.Fatal("invalid mask = nil error, want InvalidArgument")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("invalid mask = %v, want InvalidArgument/400", err)
	}

	if _, err := call(t, c, p, http.MethodDelete, adminBucket, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	deleted := mustCall(t, c, p, http.MethodGet, adminBucket, nil)
	if deleted["lifecycleState"] != "DELETE_REQUESTED" {
		t.Fatalf("after delete = %v", deleted["lifecycleState"])
	}
	mustCall(t, c, p, http.MethodPost, adminBucket+":undelete", nil)
	restored := mustCall(t, c, p, http.MethodGet, adminBucket, nil)
	if restored["lifecycleState"] != "ACTIVE" {
		t.Fatalf("after undelete = %v", restored["lifecycleState"])
	}
}

func TestRESTBucketNestedCmekMask(t *testing.T) {
	c, p := newAdminProvider(t)
	created := mustCall(t, c, p, http.MethodPost, adminLocParent+"/buckets?bucketId=b1", map[string]any{
		"description":  "d",
		"cmekSettings": map[string]any{"kmsKeyName": "projects/test/locations/global/keyRings/r/cryptoKeys/old"},
	})
	cs, _ := created["cmekSettings"].(map[string]any)
	if cs == nil || cs["serviceAccountId"] == nil {
		t.Fatalf("created cmekSettings = %v", created["cmekSettings"])
	}
	wantSA := cs["serviceAccountId"]

	// A nested path updates only kmsKeyName; the read-only serviceAccountId is
	// preserved rather than clobbered.
	patched := mustCall(t, c, p, http.MethodPatch, adminBucket+"?updateMask=cmekSettings.kmsKeyName",
		map[string]any{"cmekSettings": map[string]any{
			"kmsKeyName": "projects/test/locations/global/keyRings/r/cryptoKeys/new",
		}})
	cs, _ = patched["cmekSettings"].(map[string]any)
	if cs == nil || cs["kmsKeyName"] != "projects/test/locations/global/keyRings/r/cryptoKeys/new" {
		t.Fatalf("patched cmekSettings = %v", patched["cmekSettings"])
	}
	if cs["serviceAccountId"] != wantSA {
		t.Fatalf("read-only serviceAccountId clobbered: %v want %v", cs["serviceAccountId"], wantSA)
	}
}

func TestRESTBucketCreateAsync(t *testing.T) {
	c, p := newAdminProvider(t)
	op := mustCall(t, c, p, http.MethodPost, adminLocParent+"/buckets:createAsync?bucketId=b2", map[string]any{
		"description": "async",
	})
	if op["done"] != true {
		t.Fatalf("operation done = %v", op["done"])
	}
	resp, _ := op["response"].(map[string]any)
	if resp == nil || resp["name"] != "projects/test/locations/global/buckets/b2" {
		t.Fatalf("operation response = %v", op["response"])
	}
}

func TestRESTViewAndIam(t *testing.T) {
	c, p := newAdminProvider(t)
	mustCall(t, c, p, http.MethodPost, adminLocParent+"/buckets?bucketId=b1", map[string]any{})

	view := mustCall(t, c, p, http.MethodPost, adminBucket+"/views?viewId=v1", map[string]any{
		"filter":      "severity>=ERROR",
		"description": "d",
	})
	if view["name"] != "projects/test/locations/global/buckets/b1/views/v1" {
		t.Fatalf("view name = %v", view["name"])
	}

	got := mustCall(t, c, p, http.MethodGet, adminBucket+"/views/v1", nil)
	if got["filter"] != "severity>=ERROR" {
		t.Fatalf("view filter = %v", got["filter"])
	}

	patched := mustCall(t, c, p, http.MethodPatch, adminBucket+"/views/v1?updateMask=description", map[string]any{
		"description": "d2",
	})
	if patched["description"] != "d2" || patched["filter"] != "severity>=ERROR" {
		t.Fatalf("view patched = %v", patched)
	}

	pol := mustCall(t, c, p, http.MethodPost, adminBucket+"/views/v1:setIamPolicy", map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:a@b.com"}}}},
	})
	if bindings, _ := pol["bindings"].([]any); len(bindings) != 1 {
		t.Fatalf("policy = %v", pol)
	}
	gotPol := mustCall(t, c, p, http.MethodPost, adminBucket+"/views/v1:getIamPolicy", map[string]any{})
	if gotPol["etag"] == nil {
		t.Fatalf("policy etag missing: %v", gotPol)
	}
	perms := mustCall(t, c, p, http.MethodPost, adminBucket+"/views/v1:testIamPermissions", map[string]any{
		"permissions": []any{"logging.views.get"},
	})
	if ps, _ := perms["permissions"].([]any); len(ps) != 1 {
		t.Fatalf("permissions = %v", perms)
	}

	if _, err := call(t, c, p, http.MethodDelete, adminBucket+"/views/v1", nil); err != nil {
		t.Fatalf("delete view: %v", err)
	}
}

func TestRESTLinkAndLogScope(t *testing.T) {
	c, p := newAdminProvider(t)
	mustCall(t, c, p, http.MethodPost, adminLocParent+"/buckets?bucketId=b1", map[string]any{})

	linkOp := mustCall(t, c, p, http.MethodPost, adminBucket+"/links?linkId=l1", map[string]any{
		"bigqueryDataset": map[string]any{"datasetId": "ds1"},
	})
	if linkOp["done"] != true {
		t.Fatalf("link operation = %v", linkOp)
	}
	got := mustCall(t, c, p, http.MethodGet, adminBucket+"/links/l1", nil)
	if ds, _ := got["bigqueryDataset"].(map[string]any); ds["datasetId"] != "ds1" {
		t.Fatalf("link = %v", got)
	}
	del := mustCall(t, c, p, http.MethodDelete, adminBucket+"/links/l1", nil)
	if del["done"] != true {
		t.Fatalf("delete link operation = %v", del)
	}

	scope := mustCall(t, c, p, http.MethodPost, adminLocParent+"/logScopes?logScopeId=s1", map[string]any{
		"description":   "d",
		"resourceNames": []any{"projects/test"},
	})
	if scope["name"] != "projects/test/locations/global/logScopes/s1" {
		t.Fatalf("log scope name = %v", scope["name"])
	}
	list := mustCall(t, c, p, http.MethodGet, adminLocParent+"/logScopes", nil)
	if scopes, _ := list["logScopes"].([]any); len(scopes) != 1 {
		t.Fatalf("log scopes = %v", list["logScopes"])
	}
	mustCall(t, c, p, http.MethodPatch, adminLocParent+"/logScopes/s1?updateMask=description", map[string]any{
		"description": "d2",
	})
	if _, err := call(t, c, p, http.MethodDelete, adminLocParent+"/logScopes/s1", nil); err != nil {
		t.Fatalf("delete log scope: %v", err)
	}
}

func TestRESTSettingsAndCmek(t *testing.T) {
	c, p := newAdminProvider(t)

	st := mustCall(t, c, p, http.MethodGet, "/v2/projects/test/settings", nil)
	if st["kmsServiceAccountId"] == nil {
		t.Fatalf("settings = %v", st)
	}
	upd := mustCall(t, c, p, http.MethodPatch, "/v2/projects/test/settings?updateMask=storageLocation", map[string]any{
		"storageLocation": "us",
	})
	if upd["storageLocation"] != "us" {
		t.Fatalf("settings = %v", upd)
	}

	cm := mustCall(t, c, p, http.MethodGet, "/v2/projects/test/cmekSettings", nil)
	if cm["serviceAccountId"] == nil {
		t.Fatalf("cmek = %v", cm)
	}
	cmUpd := mustCall(t, c, p, http.MethodPatch, "/v2/projects/test/cmekSettings?updateMask=kmsKeyName", map[string]any{
		"kmsKeyName": "projects/p/locations/global/keyRings/r/cryptoKeys/k",
	})
	if cmUpd["kmsKeyName"] == nil {
		t.Fatalf("cmek = %v", cmUpd)
	}
}

func TestRESTSettingsNestedDefaultSinkConfigMask(t *testing.T) {
	c, p := newAdminProvider(t)

	// A nested leaf path merges only the named field into the stored message.
	upd := mustCall(t, c, p, http.MethodPatch, "/v2/projects/test/settings?updateMask=defaultSinkConfig.filter",
		map[string]any{"defaultSinkConfig": map[string]any{"filter": "severity>=ERROR"}})
	cfg, _ := upd["defaultSinkConfig"].(map[string]any)
	if cfg["filter"] != "severity>=ERROR" {
		t.Fatalf("settings filter = %v", upd)
	}

	upd = mustCall(t, c, p, http.MethodPatch, "/v2/projects/test/settings?updateMask=defaultSinkConfig.mode",
		map[string]any{"defaultSinkConfig": map[string]any{"mode": "APPEND"}})
	cfg, _ = upd["defaultSinkConfig"].(map[string]any)
	if cfg["mode"] != "APPEND" || cfg["filter"] != "severity>=ERROR" {
		t.Fatalf("settings mode merge = %v", upd)
	}

	// A path naming no writable field is a 400.
	if _, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/settings?updateMask=defaultSinkConfig.bogus",
		map[string]any{"defaultSinkConfig": map[string]any{"filter": "x"}}); err == nil {
		t.Fatalf("bogus defaultSinkConfig mask path succeeded")
	}
}

func TestCodecAdminRouting(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method, path, action string
	}{
		{http.MethodGet, "/v2/projects/p/locations/global/buckets", "BucketList"},
		{http.MethodPost, "/v2/projects/p/locations/global/buckets", "BucketCreate"},
		{http.MethodPost, "/v2/projects/p/locations/global/buckets:createAsync", "BucketCreateAsync"},
		{http.MethodGet, "/v2/projects/p/locations/global/buckets/b", "BucketGet"},
		{http.MethodPatch, "/v2/projects/p/locations/global/buckets/b", "BucketUpdate"},
		{http.MethodPost, "/v2/projects/p/locations/global/buckets/b:updateAsync", "BucketUpdateAsync"},
		{http.MethodDelete, "/v2/projects/p/locations/global/buckets/b", "BucketDelete"},
		{http.MethodPost, "/v2/projects/p/locations/global/buckets/b:undelete", "BucketUndelete"},
		{http.MethodGet, "/v2/projects/p/locations/global/buckets/b/views", "ViewList"},
		{http.MethodPost, "/v2/projects/p/locations/global/buckets/b/views", "ViewCreate"},
		{http.MethodGet, "/v2/projects/p/locations/global/buckets/b/views/v", "ViewGet"},
		{http.MethodPost, "/v2/projects/p/locations/global/buckets/b/views/v:setIamPolicy", "ViewSetIamPolicy"},
		{http.MethodGet, "/v2/projects/p/locations/global/buckets/b/links", "LinkList"},
		{http.MethodGet, "/v2/projects/p/locations/global/buckets/b/links/l", "LinkGet"},
		{http.MethodGet, "/v2/projects/p/locations/global/logScopes", "LogScopeList"},
		{http.MethodGet, "/v2/projects/p/locations/global/logScopes/s", "LogScopeGet"},
		{http.MethodGet, "/v2/projects/p/settings", "SettingsGet"},
		{http.MethodPatch, "/v2/projects/p/settings", "SettingsUpdate"},
		{http.MethodGet, "/v2/projects/p/cmekSettings", "CmekGet"},
		{http.MethodPatch, "/v2/projects/p/cmekSettings", "CmekUpdate"},
	}
	for _, tc := range cases {
		nr, err := c.Decode(newRequest(t, tc.method, tc.path), nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}
