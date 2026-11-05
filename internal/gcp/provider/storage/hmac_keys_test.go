package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/model"
)

const hmacTestEmail = "jaiscloud-tf-sa@test-project.iam.gserviceaccount.com"

// hmacAccessIDRE pins the accessId shape: 24 URL-safe characters, as real GCS
// server-generated ids (which start with "GOOG1").
var hmacAccessIDRE = regexp.MustCompile(`^GOOG1[A-Z0-9]{19}$`)

// hmacCreate calls the create handler the way the codec delivers it: the
// serviceAccountEmail query binding plus the Terraform provider's
// {serviceAccountEmail, state} body.
func hmacCreate(t *testing.T, p *Provider, project, email, state string) (*model.ProviderResponse, error) {
	t.Helper()
	nr := &model.NormalizedRequest{AccountID: project, Params: map[string]any{
		"project":             project,
		"serviceAccountEmail": email,
		"body": map[string]any{
			"serviceAccountEmail": email,
			"state":               state,
		},
	}}
	return p.ProjectsHmacKeysCreate(context.Background(), nr)
}

// hmacNR builds a request for one of the item/collection handlers.
func hmacNR(project, accessID string, extra map[string]any) *model.NormalizedRequest {
	params := map[string]any{"project": project}
	if accessID != "" {
		params["accessId"] = accessID
	}
	for k, v := range extra {
		params[k] = v
	}
	return &model.NormalizedRequest{AccountID: project, Params: params}
}

func mustProviderError(t *testing.T, err error, status int, code string) {
	t.Helper()
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("err = %v, want *model.ProviderError", err)
	}
	if pe.HTTPStatus != status {
		t.Fatalf("status = %d, want %d (err: %v)", pe.HTTPStatus, status, err)
	}
	if code != "" && pe.Code != code {
		t.Fatalf("code = %q, want %q", pe.Code, code)
	}
}

// TestHmacKeysLifecycle walks the whole google_storage_hmac_key lifecycle:
// create → get (flat metadata, no secret) → deactivate (etag conditional) →
// delete (INACTIVE-only) → tombstone. It also pins the error shapes: unknown
// accessId 404, missing serviceAccountEmail 400, delete-while-ACTIVE 400 and a
// stale etag 412.
func TestHmacKeysLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	for _, action := range []string{
		"Storage.ProjectsHmacKeysCreate", "Storage.ProjectsHmacKeysList",
		"Storage.ProjectsHmacKeysGet", "Storage.ProjectsHmacKeysUpdate",
		"Storage.ProjectsHmacKeysDelete",
	} {
		if p.Routes()[action] == nil {
			t.Fatalf("%s is not registered in Routes()", action)
		}
	}

	// ── create ───────────────────────────────────────────────────────────────
	resp, err := hmacCreate(t, p, "test-project", hmacTestEmail, "ACTIVE")
	if err != nil {
		t.Fatalf("hmacKeys.create: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Errorf("create status = %d, want 200", resp.HTTPStatus)
	}
	if resp.Data["kind"] != "storage#hmacKey" {
		t.Errorf("create kind = %v, want storage#hmacKey", resp.Data["kind"])
	}
	secret, _ := resp.Data["secret"].(string)
	raw, derr := base64.StdEncoding.DecodeString(secret)
	if derr != nil || len(raw) != 40 {
		t.Fatalf("secret = %q, want base64 of 40 bytes (%v)", secret, derr)
	}
	meta, _ := resp.Data["metadata"].(map[string]any)
	if meta == nil {
		t.Fatalf("create response has no metadata object: %v", resp.Data)
	}
	accessID, _ := meta["accessId"].(string)
	if !hmacAccessIDRE.MatchString(accessID) {
		t.Fatalf("accessId = %q, want 24 chars starting GOOG1", accessID)
	}
	wantID := "projects/test-project/hmacKeys/" + accessID
	if meta["id"] != wantID {
		t.Errorf("id = %v, want %v", meta["id"], wantID)
	}
	if meta["kind"] != "storage#hmacKeyMetadata" {
		t.Errorf("metadata.kind = %v, want storage#hmacKeyMetadata", meta["kind"])
	}
	if meta["projectId"] != "test-project" {
		t.Errorf("projectId = %v, want test-project", meta["projectId"])
	}
	if meta["serviceAccountEmail"] != hmacTestEmail {
		t.Errorf("serviceAccountEmail = %v, want %v", meta["serviceAccountEmail"], hmacTestEmail)
	}
	if meta["state"] != "ACTIVE" {
		t.Errorf("state = %v, want ACTIVE", meta["state"])
	}
	if meta["timeCreated"] == "" || meta["timeCreated"] == nil {
		t.Errorf("timeCreated is empty: %v", meta["timeCreated"])
	}
	if meta["updated"] == "" || meta["updated"] == nil {
		t.Errorf("updated is empty: %v", meta["updated"])
	}
	etag, _ := meta["etag"].(string)
	if etag == "" {
		t.Fatal("etag is empty")
	}

	// The provider polls GET right after create, so the key must already be
	// readable, as the flat HmacKeyMetadata object and *without* the secret.
	got, err := p.ProjectsHmacKeysGet(ctx, hmacNR("test-project", accessID, nil))
	if err != nil {
		t.Fatalf("hmacKeys.get after create: %v", err)
	}
	if got.HTTPStatus != 200 {
		t.Errorf("get status = %d, want 200", got.HTTPStatus)
	}
	if _, ok := got.Data["secret"]; ok {
		t.Errorf("get leaked the secret: %v", got.Data)
	}
	if _, ok := got.Data["metadata"]; ok {
		t.Errorf("get must answer the flat metadata object, got %v", got.Data)
	}
	if got.Data["accessId"] != accessID || got.Data["etag"] != etag || got.Data["state"] != "ACTIVE" {
		t.Errorf("get body = %v, want accessId %q state ACTIVE etag %q", got.Data, accessID, etag)
	}

	// A second key for another account exists only to exercise the list filter
	// and paging below.
	other, err := hmacCreate(t, p, "test-project", "other-sa@test-project.iam.gserviceaccount.com", "ACTIVE")
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	otherMeta, _ := other.Data["metadata"].(map[string]any)
	otherID, _ := otherMeta["accessId"].(string)
	if otherID == accessID {
		t.Fatal("accessIds are not unique")
	}

	// ── list ─────────────────────────────────────────────────────────────────
	list, err := p.ProjectsHmacKeysList(ctx, hmacNR("test-project", "", nil))
	if err != nil {
		t.Fatalf("hmacKeys.list: %v", err)
	}
	if list.Data["kind"] != "storage#hmacKeysMetadata" {
		t.Errorf("list kind = %v, want storage#hmacKeysMetadata", list.Data["kind"])
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("list items = %d, want 2", len(items))
	}
	if _, ok := items[0].(map[string]any)["secret"]; ok {
		t.Error("list leaked a secret")
	}

	// The serviceAccountEmail filter selects one key.
	filtered, err := p.ProjectsHmacKeysList(ctx, hmacNR("test-project", "", map[string]any{
		"serviceAccountEmail": hmacTestEmail,
	}))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if got, _ := filtered.Data["items"].([]any); len(got) != 1 {
		t.Fatalf("filtered items = %d, want 1", len(got))
	}

	// maxResults=1 pages through the accessId-ordered keys with a nextPageToken.
	first, err := p.ProjectsHmacKeysList(ctx, hmacNR("test-project", "", map[string]any{"maxResults": "1"}))
	if err != nil {
		t.Fatalf("paged list: %v", err)
	}
	page1, _ := first.Data["items"].([]any)
	if len(page1) != 1 {
		t.Fatalf("page1 items = %d, want 1", len(page1))
	}
	tok, _ := first.Data["nextPageToken"].(string)
	if tok == "" {
		t.Fatal("page1 has no nextPageToken")
	}
	second, err := p.ProjectsHmacKeysList(ctx, hmacNR("test-project", "", map[string]any{
		"maxResults": "1", "pageToken": tok,
	}))
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	page2, _ := second.Data["items"].([]any)
	if len(page2) != 1 {
		t.Fatalf("page2 items = %d, want 1", len(page2))
	}
	if _, ok := second.Data["nextPageToken"]; ok {
		t.Errorf("page2 has an unexpected nextPageToken: %v", second.Data["nextPageToken"])
	}
	if page1[0].(map[string]any)["accessId"] == page2[0].(map[string]any)["accessId"] {
		t.Error("paging returned the same key twice")
	}

	// ── delete while ACTIVE → 400 ────────────────────────────────────────────
	if _, err := p.ProjectsHmacKeysDelete(ctx, hmacNR("test-project", accessID, nil)); err == nil {
		t.Fatal("delete of an ACTIVE key succeeded, want 400")
	} else {
		mustProviderError(t, err, 400, "")
	}

	// ── update ACTIVE → INACTIVE, conditional on the etag ────────────────────
	stale, err := p.ProjectsHmacKeysUpdate(ctx, hmacNR("test-project", accessID, map[string]any{
		"body": map[string]any{"etag": "bogus=", "state": "INACTIVE"},
	}))
	if err == nil {
		t.Fatalf("stale-etag update succeeded: %v", stale.Data)
	}
	mustProviderError(t, err, 412, "PreconditionFailed")

	upd, err := p.ProjectsHmacKeysUpdate(ctx, hmacNR("test-project", accessID, map[string]any{
		"body": map[string]any{"etag": etag, "state": "INACTIVE"},
	}))
	if err != nil {
		t.Fatalf("hmacKeys.update: %v", err)
	}
	if upd.HTTPStatus != 200 {
		t.Errorf("update status = %d, want 200", upd.HTTPStatus)
	}
	if upd.Data["state"] != "INACTIVE" {
		t.Errorf("updated state = %v, want INACTIVE", upd.Data["state"])
	}
	newEtag, _ := upd.Data["etag"].(string)
	if newEtag == "" || newEtag == etag {
		t.Errorf("etag = %q after update, want a new non-empty value (was %q)", newEtag, etag)
	}
	if _, ok := upd.Data["secret"]; ok {
		t.Error("update leaked the secret")
	}

	// An unsupported state value is 400 rather than silently stored.
	if _, err := p.ProjectsHmacKeysUpdate(ctx, hmacNR("test-project", accessID, map[string]any{
		"body": map[string]any{"state": "BOGUS"},
	})); err == nil {
		t.Fatal("update with an invalid state succeeded, want 400")
	} else {
		mustProviderError(t, err, 400, "")
	}

	// ── delete while INACTIVE → tombstone ────────────────────────────────────
	del, err := p.ProjectsHmacKeysDelete(ctx, hmacNR("test-project", accessID, nil))
	if err != nil {
		t.Fatalf("hmacKeys.delete: %v", err)
	}
	if del.HTTPStatus != 204 {
		t.Errorf("delete status = %d, want 204", del.HTTPStatus)
	}

	// GET still answers the tombstone: that is how the provider's read sees the
	// key is gone (state DELETED).
	tomb, err := p.ProjectsHmacKeysGet(ctx, hmacNR("test-project", accessID, nil))
	if err != nil {
		t.Fatalf("tombstone get: %v", err)
	}
	if tomb.Data["state"] != "DELETED" {
		t.Errorf("tombstone state = %v, want DELETED", tomb.Data["state"])
	}

	// The default list omits the tombstone; showDeletedKeys=true includes it.
	def, err := p.ProjectsHmacKeysList(ctx, hmacNR("test-project", "", nil))
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	defItems, _ := def.Data["items"].([]any)
	if len(defItems) != 1 {
		t.Fatalf("default list items = %d, want 1 (the live key only)", len(defItems))
	}
	if defItems[0].(map[string]any)["accessId"] != otherID {
		t.Errorf("default list returned %v, want the live key %q", defItems[0], otherID)
	}
	withDeleted, err := p.ProjectsHmacKeysList(ctx, hmacNR("test-project", "", map[string]any{"showDeletedKeys": "true"}))
	if err != nil {
		t.Fatalf("list with showDeletedKeys: %v", err)
	}
	allItems, _ := withDeleted.Data["items"].([]any)
	if len(allItems) != 2 {
		t.Fatalf("showDeletedKeys list items = %d, want 2", len(allItems))
	}
	var sawDeleted bool
	for _, it := range allItems {
		m := it.(map[string]any)
		if m["accessId"] == accessID && m["state"] == "DELETED" {
			sawDeleted = true
		}
	}
	if !sawDeleted {
		t.Errorf("showDeletedKeys list = %v, want the DELETED tombstone", allItems)
	}

	// A deleted key's second DELETE and any unknown accessId are 404.
	if _, err := p.ProjectsHmacKeysDelete(ctx, hmacNR("test-project", accessID, nil)); err == nil {
		t.Fatal("second delete succeeded, want 404")
	} else {
		mustProviderError(t, err, 404, "NotFound")
	}
	const unknown = "GOOG1NOSUCHKEY000000000"
	_, getErr := p.ProjectsHmacKeysGet(ctx, hmacNR("test-project", unknown, nil))
	mustProviderError(t, getErr, 404, "NotFound")
	_, updErr := p.ProjectsHmacKeysUpdate(ctx, hmacNR("test-project", unknown, map[string]any{
		"body": map[string]any{"state": "INACTIVE"},
	}))
	mustProviderError(t, updErr, 404, "NotFound")
	_, delErr := p.ProjectsHmacKeysDelete(ctx, hmacNR("test-project", unknown, nil))
	mustProviderError(t, delErr, 404, "NotFound")

	// ── missing serviceAccountEmail → 400 ────────────────────────────────────
	if _, err := p.ProjectsHmacKeysCreate(ctx, &model.NormalizedRequest{AccountID: "test-project", Params: map[string]any{
		"project": "test-project",
	}}); err == nil {
		t.Fatal("create without serviceAccountEmail succeeded, want 400")
	} else {
		mustProviderError(t, err, 400, "")
		if pe, _ := err.(*model.ProviderError); !strings.Contains(pe.Message, "serviceAccountEmail") {
			t.Errorf("message = %q, want it to name serviceAccountEmail", pe.Message)
		}
	}
}

// TestHmacKeysCreateInactive covers a Terraform `state = "INACTIVE"` key: the
// create response and the follow-up read agree, so the next plan is clean.
func TestHmacKeysCreateInactive(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	resp, err := hmacCreate(t, p, "test-project", hmacTestEmail, "INACTIVE")
	if err != nil {
		t.Fatalf("create INACTIVE: %v", err)
	}
	meta, _ := resp.Data["metadata"].(map[string]any)
	if meta["state"] != "INACTIVE" {
		t.Errorf("state = %v, want INACTIVE", meta["state"])
	}
	accessID, _ := meta["accessId"].(string)
	got, err := p.ProjectsHmacKeysGet(ctx, hmacNR("test-project", accessID, nil))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["state"] != "INACTIVE" {
		t.Errorf("get state = %v, want INACTIVE", got.Data["state"])
	}

	// Creating straight into DELETED is not a documented state transition.
	bad, err := hmacCreate(t, p, "test-project", hmacTestEmail, "DELETED")
	if err == nil {
		t.Fatalf("create DELETED succeeded: %v", bad.Data)
	}
	mustProviderError(t, err, 400, "")
}

// TestHmacKeysPerProject isolates keys by project: the entry ID carries the
// path project, so one project never lists another's keys.
func TestHmacKeysPerProject(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := hmacCreate(t, p, "proj-a", hmacTestEmail, "ACTIVE"); err != nil {
		t.Fatalf("create in proj-a: %v", err)
	}
	list, err := p.ProjectsHmacKeysList(ctx, hmacNR("proj-b", "", nil))
	if err != nil {
		t.Fatalf("list in proj-b: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("proj-b lists %d keys, want 0", len(items))
	}
}

// TestHmacKeysWireEncoding proves the JSON codec serialises the create
// response as a storage#hmacKey (secret included) and the get response as the
// flat HmacKeyMetadata, which is what Terraform's googleapi client decodes.
func TestHmacKeysWireEncoding(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	codec := &gcpadapter.GCSCodec{}

	created, err := hmacCreate(t, p, "test-project", hmacTestEmail, "ACTIVE")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	accessID := created.Data["metadata"].(map[string]any)["accessId"].(string)

	createNR := &model.NormalizedRequest{
		Service: "storage", Action: "ProjectsHmacKeysCreate",
		Params: map[string]any{"project": "test-project"},
	}
	status, headers, body := codec.Encode(createNR, created)
	if status != 200 {
		t.Errorf("encoded create status = %d, want 200", status)
	}
	if ct := headers.Get("Content-Type"); ct != "application/json; charset=UTF-8" {
		t.Errorf("content-type = %q", ct)
	}
	var wireCreate map[string]any
	if err := json.Unmarshal(body, &wireCreate); err != nil {
		t.Fatalf("decode create body: %v (body=%q)", err, body)
	}
	if wireCreate["kind"] != "storage#hmacKey" || wireCreate["secret"] == "" {
		t.Errorf("create wire body = %v, want kind storage#hmacKey and a secret", wireCreate)
	}

	got, err := p.ProjectsHmacKeysGet(ctx, hmacNR("test-project", accessID, nil))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	getNR := &model.NormalizedRequest{
		Service: "storage", Action: "ProjectsHmacKeysGet",
		Params: map[string]any{"project": "test-project", "accessId": accessID},
	}
	status, _, body = codec.Encode(getNR, got)
	if status != 200 {
		t.Errorf("encoded get status = %d, want 200", status)
	}
	var wireGet map[string]any
	if err := json.Unmarshal(body, &wireGet); err != nil {
		t.Fatalf("decode get body: %v (body=%q)", err, body)
	}
	if wireGet["kind"] != "storage#hmacKeyMetadata" || wireGet["accessId"] != accessID {
		t.Errorf("get wire body = %v", wireGet)
	}
	if _, ok := wireGet["secret"]; ok {
		t.Error("get wire body carries a secret")
	}
}
